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

import io.confluent.ksql.execution.function.UdfUtil;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;

public class UdfValidator {

  private static final String missingStructErrorMsgUdf = "Must specify schema for STRUCT "
      + "parameter in @UdfParameter.";
  private static final String incompatibleSchemaTypeUdf = "The schema in @UdfParameter does not "
      + "match the method parameter for UDF %s:%s";

  private final Method method;
  private final String functionName;
  private final Udf udfAnnotation;
  private final SqlTypeParser typeParser;

  public UdfValidator(
      final Method method,
      final String functionName,
      final Udf udfAnnotation,
      final SqlTypeParser typeParser) {

    this.method = method;
    this.functionName = functionName;
    this.udfAnnotation = udfAnnotation;
    this.typeParser = typeParser;
  }


  public UdfSignature validateUdf() {

    validateUdfMethodSignature();
    validateUdfParameterTypes();
    UdfSignature udfSignature = new UdfSignature();
    udfSignature.setParameters(validateParameterSchemas());
    udfSignature.setReturnSchema(validateReturnSchema());
    return udfSignature;
  }

  private void validateUdfMethodSignature() {

    for (int i = 0; i < method.getParameterTypes().length; i++) {
      if (method.getParameterTypes()[i].isArray()) {
        if (!method.isVarArgs() || i != method.getParameterCount() - 1) {
          throw new KsqlFunctionException(
              "Invalid UDF method signature (contains non var-arg array): " + method);
        }
      }
    }
  }

  private void validateUdfParameterTypes() {

    final Class<?>[] types = method.getParameterTypes();
    for (int i = 0; i < types.length; i++) {
      final Class<?> type = types[i];

      if (method.getGenericParameterTypes()[i] instanceof TypeVariable
          || method.getGenericParameterTypes()[i] instanceof GenericArrayType) {
        // this is the case where the type parameter is generic
        continue;
      }

      if (ValidatorUtil.isUnsupportedType(type)) {
        throw new KsqlException(
            String.format(
                "Type %s is not supported by UDF methods. "
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

  private List<Schema> validateParameterSchemas() {

    return parseUdfInputParameters();

  }

  private Schema validateReturnSchema() {

    Schema returnSchema;
    final Type returnType = method.getGenericReturnType();
    if (!udfAnnotation.schema().isEmpty()) {
      returnSchema = SchemaConverters.sqlToConnectConverter().toConnectSchema(
          typeParser.parse(udfAnnotation.schema()).getSqlType());
      //      final Class<?> javaReturnType = SchemaConverters.sqlToJavaConverter().toJavaType(
      //          SchemaConverters.connectToSqlConverter().toSqlType(returnSchema));
      //      if (! ((Class)returnType).isAssignableFrom(javaReturnType)) {
      //        throw new KsqlException(String.format("The schema in @Udf does not match the "
      //            + "method return type for UDF %s:%s", functionName, method.getName()));
      //      }
    } else {
      returnSchema = UdfUtil.getSchemaFromType(method.getGenericReturnType());
    }
    return SchemaUtil.ensureOptional(returnSchema);
  }

  private List<Schema> parseUdfInputParameters() {

    final List<Schema> inputSchemas = new ArrayList<>(method.getParameterCount());
    for (int idx = 0; idx < method.getParameterCount(); idx++) {

      final Type type = method.getGenericParameterTypes()[idx];
      final Optional<UdfParameter> annotation = Arrays.stream(method.getParameterAnnotations()[idx])
          .filter(UdfParameter.class::isInstance)
          .map(UdfParameter.class::cast)
          .findAny();

      final Parameter param = method.getParameters()[idx];
      final String name = annotation.map(UdfParameter::value)
          .filter(val -> !val.isEmpty())
          .orElse(param.isNamePresent() ? param.getName() : null);

      if (name == null) {
        throw new KsqlFunctionException(
            String.format("Cannot resolve parameter name for param at index %d for UDF %s:%s. "
                              + "Please specify a name in @UdfParameter or compile your JAR with"
                              + " -parameters to infer the name from the parameter name.",
                          idx, functionName, method.getName()));
      }

      final String doc = annotation.map(UdfParameter::description).orElse("");
      final Optional<String> schemaString = annotation.isPresent()
          && !annotation.get().schema().isEmpty()
          ? Optional.of(annotation.get().schema()) : Optional.empty();

      ValidatorUtil.validateStructAnnotation(
          type,
          schemaString,
          String.format(missingStructErrorMsgUdf, name));

      validateUdfParameterSchema(
          type,
          schemaString,
          name,
          doc,
          String.format(
              incompatibleSchemaTypeUdf,
              functionName,
              method.getName()));

      inputSchemas.add(ValidatorUtil.getSchemaOfInputParameter(
          type,
          schemaString,
          name,
          doc,
          typeParser));
    }
    return inputSchemas;
  }

  private void validateUdfParameterSchema(
      final Type type,
      final Optional<String> paramSchema,
      final String paramName,
      final String paramDoc,
      String errorMsg) {

    Objects.requireNonNull(paramName);
    Objects.requireNonNull(paramDoc);

    if (paramSchema.isPresent()) {
      final Schema schemaType = SchemaConverters.sqlToConnectConverter().toConnectSchema(
          typeParser.parse(paramSchema.get()).getSqlType(), paramName, paramDoc);
      final Class<?> javaType = SchemaConverters.sqlToJavaConverter().toJavaType(
          SchemaConverters.connectToSqlConverter().toSqlType(schemaType));
      if (type != javaType) {
        throw new KsqlException(errorMsg);
      }
    }
  }

  static class UdfSignature {
    private List<Schema> parameters;
    private Schema returnSchema;

    public List<Schema> getParameters() {
      return parameters;
    }

    public Schema getReturnSchema() {
      return returnSchema;
    }

    public void setParameters(final List<Schema> parameters) {
      this.parameters = parameters;
    }

    public void setReturnSchema(final Schema returnSchema) {
      this.returnSchema = returnSchema;
    }
  }

}
