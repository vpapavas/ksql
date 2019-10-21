/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;

public class UdafValidator {

  static final String missingStructErrorMsgUdaf = "Must specify '%s' for STRUCT parameter in "
      + "@UdafFactory.";

  private final Method method;
  private final String functionName;
  private final UdafFactory udafAnnotation;
  private final SqlTypeParser typeParser;
  private final AnnotatedParameterizedType annotatedReturnType;
  private final ParameterizedType parameterizedType;


  public UdafValidator(
      final Method method,
      final String functionName,
      final UdafFactory udafAnnotation,
      final SqlTypeParser typeParser) {

    this.method = method;
    this.functionName = functionName;
    this.udafAnnotation = udafAnnotation;
    this.typeParser = typeParser;
    this.annotatedReturnType = (AnnotatedParameterizedType) method.getAnnotatedReturnType();
    this.parameterizedType = (ParameterizedType) annotatedReturnType.getType();

  }


  public UdafSignature validateUdaf() {

    validateUdafMethodSignature();
    validateUdafParameterTypes();
    UdafSignature udafSignature = new UdafSignature();

    Type inputType = parameterizedType.getActualTypeArguments()[0];
    final Optional<String> paramSchema = !udafAnnotation.paramSchema().isEmpty()
        ? Optional.of(udafAnnotation.paramSchema()) : Optional.empty();
    udafSignature.setInputSchema(validateInputSchema(inputType, paramSchema));

    Type aggregateType = parameterizedType.getActualTypeArguments()[1];
    final Optional<String> aggSchema = !udafAnnotation.aggregateSchema().isEmpty()
        ? Optional.of(udafAnnotation.aggregateSchema()) : Optional.empty();
    udafSignature.setInputSchema(validateAggregateSchema(aggregateType, aggSchema));

    Type returnType = parameterizedType.getActualTypeArguments()[2];
    final Optional<String> returnSchema = !udafAnnotation.returnSchema().isEmpty()
        ? Optional.of(udafAnnotation.returnSchema()) : Optional.empty();
    udafSignature.setInputSchema(validateAggregateSchema(returnType, returnSchema));

    return udafSignature;
  }

  private void validateUdafMethodSignature() {

    final String functionInfo = String.format("method='%s', functionName='%s', UDFClass='%s'",
                                              method.getName(),
                                              functionName,
                                              method.getDeclaringClass());

    if (!(Udaf.class.equals(method.getReturnType())
        || TableUdaf.class.equals(method.getReturnType()))) {
      throw new KsqlException("UDAFs must implement " + Udaf.class.getName() + " or "
                                  + TableUdaf.class.getName() + ". "
                                  + functionInfo);
    }
    if (!Modifier.isStatic(method.getModifiers())) {
      throw new KsqlException("UDAF factory methods must be static " + method);
    }
  }

  private void validateUdafParameterTypes() {

    final Type[] types = parameterizedType.getActualTypeArguments();
    for (int i = 0; i < types.length; i++) {
      final Type type = types[i];

      if (type instanceof TypeVariable || type instanceof GenericArrayType) {
        // this is the case where the type parameter is generic
        continue;
      }

      if (ValidatorUtil.isUnsupportedType((Class<?>) ValidatorUtil.getRawType(type))) {
        throw new KsqlException(
            String.format(
                "class='%s' is not supported by UDAF methods. "
                    + "Supported types %s. method=%s, class=%s",
                type,
                ValidatorUtil.SUPPORTED_TYPES,
                method.getName(),
                method.getDeclaringClass()
            )
        );
      }
    }
  }

  private Schema validateInputSchema(final Type inputType, final Optional<String> inSchema) {
    ValidatorUtil.validateStructAnnotation(inputType, inSchema, "paramSchema");
    final Schema inputSchema = ValidatorUtil.getSchemaOfInputParameter(
        inputType,
        inSchema,
        "paramSchema",
        "",
        typeParser);
    //Currently, aggregate functions cannot have reified types as input parameters.
    if (!GenericsUtil.constituentGenerics(inputSchema).isEmpty()) {
      throw new KsqlException("Generic type parameters containing reified types are not currently"
                                  + " supported. " + functionInfo);
    }
    return inputSchema;
  }

  private Schema validateAggregateSchema(
      final Type aggregateType,
      final Optional<String> aggSchema) {
    ValidatorUtil.validateStructAnnotation(aggregateType, aggSchema, "aggregateSchema");
    return ValidatorUtil.getSchemaOfInputParameter(
        aggregateType,
        aggSchema,
        "aggregateSchema",
        "",
        typeParser);
  }

  private Schema validateReturnSchema(final Type returnType, final Optional<String> returnSchema) {
    ValidatorUtil.validateStructAnnotation(returnType, returnSchema, "returnSchema");
    return ValidatorUtil.getSchemaOfInputParameter(
        returnType,
        returnSchema,
        "returnSchema",
        "",
        typeParser);
  }

  static class UdafSignature {

    private Schema inputSchema;
    private Schema aggregateSchema;
    private Schema outputSchema;


    public void setInputSchema(final Schema inSchema) {
      this.inputSchema = inSchema;
    }

    public void  setAggregateSchema(final Schema aggSchema) {
      this.aggregateSchema = aggSchema;
    }

    public void setOutputSchema(final Schema outSchema) {
      this.outputSchema = outSchema;
    }

    public Schema getInputSchema() {
      return inputSchema;
    }

    public Schema getAggregateSchema() {
      return aggregateSchema;
    }

    public Schema getOutputSchema() {
      return outputSchema;
    }
  }
}
