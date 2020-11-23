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

package io.confluent.ksql.planner.plan.function;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.List;
import java.util.Map;

public class EqualFunction extends AbstractFunction {

  public static final String NAME = "=";

  private enum FunctionSignatureName {
    EUQAL_INTEGER,
    EUQAL_BIGINT,
    EQUAL_DOUBLE,
    EQUAL_BOOLEAN,
    EQUAL_STRING,
    EQUAL_DECIMAL,
    EQUAL_ARRAY,
    EQUAL_MAP,
    EQUAL_STRUCT
  }

  public EqualFunction() {
    super(NAME);

    FunctionSignature signature;

    signature = new FunctionSignature(FunctionSignatureName.EUQAL_INTEGER.name(), SqlBaseType.BOOLEAN);
    signature.addParameter("left", SqlBaseType.INTEGER);
    signature.addParameter("right", SqlBaseType.INTEGER);
    this.addFunctionSignature(signature);

    signature = new FunctionSignature(FunctionSignatureName.EUQAL_BIGINT.name(), SqlBaseType.BOOLEAN);
    signature.addParameter("left", SqlBaseType.DOUBLE);
    signature.addParameter("right", SqlBaseType.DOUBLE);
    this.addFunctionSignature(signature);

    signature = new FunctionSignature(FunctionSignatureName.EQUAL_DOUBLE.name(), SqlBaseType.BOOLEAN);
    signature.addParameter("left", SqlBaseType.DOUBLE);
    signature.addParameter("right", SqlBaseType.DOUBLE);
    this.addFunctionSignature(signature);

    signature = new FunctionSignature(FunctionSignatureName.EQUAL_STRING.name(), SqlBaseType.BOOLEAN);
    signature.addParameter("left", SqlBaseType.STRING);
    signature.addParameter("right", SqlBaseType.STRING);
    this.addFunctionSignature(signature);

    signature = new FunctionSignature(FunctionSignatureName.EQUAL_BOOLEAN.name(), SqlBaseType.BOOLEAN);
    signature.addParameter("left", SqlBaseType.BOOLEAN);
    signature.addParameter("right", SqlBaseType.BOOLEAN);
    this.addFunctionSignature(signature);

    signature = new FunctionSignature(FunctionSignatureName.EQUAL_DECIMAL.name(), SqlBaseType.BOOLEAN);
    signature.addParameter("left", SqlBaseType.DECIMAL);
    signature.addParameter("right", SqlBaseType.DECIMAL);
    this.addFunctionSignature(signature);

    signature = new FunctionSignature(FunctionSignatureName.EQUAL_MAP.name(), SqlBaseType.BOOLEAN);
    signature.addParameter("left", SqlBaseType.MAP);
    signature.addParameter("right", SqlBaseType.MAP);
    this.addFunctionSignature(signature);

    signature = new FunctionSignature(FunctionSignatureName.EQUAL_ARRAY.name(), SqlBaseType.BOOLEAN);
    signature.addParameter("left", SqlBaseType.ARRAY);
    signature.addParameter("right", SqlBaseType.ARRAY);
    this.addFunctionSignature(signature);
  }

  public Object evaluate(final AbstractFunctionCall call, final GenericRow row) {
    List<Object> arg_values = evaluateArguments(call, row);
    Object left = arg_values.get(0);
    Object right = arg_values.get(1);

    return left.equals(right);

    switch (left.getClass()) {
      case String.class:
        return left.equals(right);
        break;
      case List.class:
        return left.equals(right);
        break;
      case Map.class:
        return left.equals(right);
        break;
      case SqlBaseType.STRUCT.getClass():
        return left.equals(right);
        break;
      case Boolean.class:
        return (Boolean.compare((boolean)left, (boolean)right) == 0);
        break;
      default:
        return ((left <= right) && (left >= right));
        break;
    }

    return left.equals(right);
  }
}
