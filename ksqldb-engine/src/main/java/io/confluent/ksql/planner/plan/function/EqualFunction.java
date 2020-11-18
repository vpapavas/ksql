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
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.List;

public class EqualFunction extends AbstractFunction {

  public static final String NAME = "=";

  private enum FunctionSignatureName {
    EUQAL_INTEGER,
    EUQAL_BIGINT,
    EQUAL_DOUBLE,
    EQUAL_DATE,
    EQUAL_BOOLEAN,
    EQUAL_STRING,
    EQUAL_TIMESTAMP,
    EQUAL_DECIMAL
  }

  public EqualFunction() {
    super(NAME);

    FunctionSignature signature;

    signature = new FunctionSignature(FunctionSignatureName.EUQAL_INTEGER.name(), SqlTypes.BOOLEAN);
    signature.addArgument("left", SqlTypes.INTEGER);
    signature.addArgument("right", SqlTypes.INTEGER);
    this.addFunctionSignature(signature);
  }

  public Object evaluate(final AbstractFunctionCall call, final GenericRow row) {
    List<Object> arg_values = evaluateArguments(call, row);
    Object left = arg_values.get(0);
    Object right = arg_values.get(1);

    return left.equals(right);
  }
}
