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
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.planner.plan.LogicalTermEvaluator;
import io.confluent.ksql.planner.plan.function.FunctionSignature.Parameter;
import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercer;
import io.confluent.ksql.schema.ksql.JavaToSqlTypeConverter;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalTerm;
import io.confluent.ksql.schema.ksql.SqlValueCoercer.Result;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class AbstractFunction {
  private final String name;
  private List<FunctionSignature> signatures;

  public AbstractFunction(final String name) {
    this.name = Objects.requireNonNull(name);
    this.signatures = new ArrayList<>();
  }

  public String getName(){
    return name;
  }

  public List<FunctionSignature> getFunctionSignatures() {
    return signatures;
  }

  public void addFunctionSignature(final FunctionSignature signature) {
    this.signatures.add(signature);
  }

  public abstract Object evaluate(final AbstractFunctionCall call, final GenericRow row);

  public List<Object> evaluateArguments(
      final AbstractFunctionCall call, final LogicalSchema schema, final GenericRow row) {
    List<Object> argument_values = new ArrayList<>();
    boolean found = true;
    for(LogicalTerm arg: call.getArguments()) {
      Object value = LogicalTermEvaluator.evaluate(schema, arg, row);
      argument_values.add(value);
    }

    // Identify function signature based on argument data types
    FunctionSignature actual_signature = null;
    for(FunctionSignature sig: signatures) {
      List<Parameter> args = sig.getParameters();
      for(int i=0; i< args.size(); i++) {
        SqlBaseType sqlType = JavaToSqlTypeConverter.instance().toSqlType(argument_values.get(i).getClass());
        if (!sqlType.canImplicitlyCast(args.get(i).getType())) {
          found = false;
        }
      }
      if (found) {
        actual_signature = sig;
        break;
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
