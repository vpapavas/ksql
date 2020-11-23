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

package io.confluent.ksql.planner.plan;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.planner.plan.function.AbstractFunctionCall;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalTerm;
import scala.collection.immutable.Stream.Cons;

public class LogicalTermEvaluator {

  public static Object evaluate(
      final LogicalSchema schema, final LogicalTerm term, final GenericRow row) {

    if (term instanceof Constant) {
      return ((Constant)term).getValue();
    }
    if (term instanceof Column) {
      return evaluateColumn((Column)term, row);
    }
    if (term instanceof AbstractFunctionCall) {
      return ((AbstractFunctionCall)term).getFunction().evaluate(
          (AbstractFunctionCall)term, row);
    }
    throw new UnsupportedOperationException("Cannot evaluate the provided logical term " + term);
  }

  private static Object evaluateColumn(final Column column, final GenericRow row) {

    return row.values().get(column.index());
  }

}
