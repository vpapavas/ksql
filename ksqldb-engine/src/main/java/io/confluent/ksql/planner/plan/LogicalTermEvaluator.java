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

public class LogicalTermEvaluator {

  public static Object evaluate(LogicalTerm term, GenericRow row) {
    if (term instanceof AbstractFunctionCall) {
      return ((AbstractFunctionCall)term).evaluateArguments(row);
    } else {
      throw new UnsupportedOperationException("Cannot evaluate the provided logical term " + term);
    }

  }
}
