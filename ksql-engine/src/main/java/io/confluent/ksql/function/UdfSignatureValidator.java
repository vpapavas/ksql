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

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class UdfSignatureValidator {

  private static final SqlTypeParser typeParser = SqlTypeParser.create(TypeRegistry.EMPTY);
  static final String missingStructErrorMsgUdaf = "Must specify '%s' for STRUCT parameter in "
      + "@UdafFactory.";
  static final String missingStructErrorMsgUdf = "Must specify schema for STRUCT parameter in "
      + "@UdfParameter.";
  static final String incompatibleSchemaTypeUdf = "The schema in @UdfParameter does not match the "
      + "method parameter for UDF %s:%s";

  static final Set<Class<?>> SUPPORTED_TYPES = ImmutableSet.<Class<?>>builder()
      .add(int.class)
      .add(long.class)
      .add(double.class)
      .add(boolean.class)
      .add(Integer.class)
      .add(Long.class)
      .add(Double.class)
      .add(BigDecimal.class)
      .add(Boolean.class)
      .add(String.class)
      .add(Struct.class)
      .add(List.class)
      .add(Map.class)
      .build();


  static void validateUdfMethodSignature(final Method method) {

    for (int i = 0; i < method.getParameterTypes().length; i++) {
      if (method.getParameterTypes()[i].isArray()) {
        if (!method.isVarArgs() || i != method.getParameterCount() - 1) {
          throw new KsqlFunctionException(
              "Invalid UDF method signature (contains non var-arg array): " + method);
        }
      }
    }
  }

  static void validateUdafMethodSignature(final Method method, final String functionName) {

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

  static void validateUdfParameterTypes(final Method method) {

    final Class<?>[] types = method.getParameterTypes();
    for (int i = 0; i < types.length; i++) {
      final Class<?> type = types[i];

      if (method.getGenericParameterTypes()[i] instanceof TypeVariable
          || method.getGenericParameterTypes()[i] instanceof GenericArrayType) {
        // this is the case where the type parameter is generic
        continue;
      }

      if (isUnsupportedType(type)) {
        throw new KsqlException(
            String.format(
                "Type %s is not supported by UDF methods. "
                    + "Supported types %s. method=%s, class=%s",
                type,
                SUPPORTED_TYPES,
                method.getName(),
                method.getDeclaringClass()
            )
        );
      }
    }
  }

  static void validateUdafParameterTypes(final Method method) {

    final AnnotatedParameterizedType annotatedReturnType
        = (AnnotatedParameterizedType) method.getAnnotatedReturnType();
    final ParameterizedType parameterizedType = (ParameterizedType) annotatedReturnType.getType();

    final Type[] types = parameterizedType.getActualTypeArguments();
    for (int i = 0; i < types.length; i++) {
      final Type type = types[i];

      if (type instanceof TypeVariable || type instanceof GenericArrayType) {
        // this is the case where the type parameter is generic
        continue;
      }

      if (isUnsupportedType((Class<?>) getRawType(type))) {
        throw new KsqlException(
            String.format(
                "class='%s' is not supported by UDAF methods. "
                    + "Supported types %s. method=%s, class=%s",
                type,
                SUPPORTED_TYPES,
                method.getName(),
                method.getDeclaringClass()
            )
        );
      }
    }
  }

  static void validateUdfParameterSchema(
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

  static void validateStructAnnotation(final Type type,
                                       final Optional<String> schema,
                                       final String errorMsg) {

    if (type.equals(Struct.class) && !schema.isPresent()) {
      throw new KsqlException(errorMsg);
    }
  }

  private static Type getRawType(final Type type) {
    if (type instanceof ParameterizedType) {
      return ((ParameterizedType) type).getRawType();
    }
    return type;
  }

  private static boolean isUnsupportedType(final Class<?> type) {
    return !SUPPORTED_TYPES.contains(type)
        && (!type.isArray() || !SUPPORTED_TYPES.contains(type.getComponentType()))
        && SUPPORTED_TYPES.stream().noneMatch(supported -> supported.isAssignableFrom(type));
  }



}
