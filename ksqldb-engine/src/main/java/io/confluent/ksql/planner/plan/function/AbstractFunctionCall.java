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
import io.confluent.ksql.planner.plan.LogicalTerm;
import io.confluent.ksql.planner.plan.LogicalTermEvaluator;
import io.confluent.ksql.planner.plan.function.FunctionSignature.Argument;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.List;

public class AbstractFunctionCall implements LogicalTerm {

  private final String name;
  private List<LogicalTerm> arguments;
  private List<FunctionSignature> signatures;

  public AbstractFunctionCall(final String name) {
    this.name = name;
  }

  public void addFunctionSignature(FunctionSignature signature) {
    this.signatures.add(signature);
  }

  public List<Object> evaluateArguments(GenericRow row) {
    List<Object> argument_values = new ArrayList<>();
    boolean found = true;
    for(LogicalTerm arg: arguments) {
      Object value = LogicalTermEvaluator.evaluate(arg, row);
      argument_values.add(value);
    }

    // Identify function signature based on argument data types
    FunctionSignature actual_signature = null;
    for(FunctionSignature sig: signatures) {
      List<Argument> args = sig.getArguments();
      for(int i=0; i< args.size(); i++) {
        // TODO: check if java type at runtime can be cast to sql type of function signature
        if (args.get(i) != argument_values.get(i)) {
          found = false;
        }
      }
      if (found) {
        actual_signature = sig;
      }
    }

    if (actual_signature == null) {
      throw new KsqlException(String.format(
          "No matching function signature for function %s and argument data types %s",
          name, argument_values));
    }

    return argument_values;
  }
}
