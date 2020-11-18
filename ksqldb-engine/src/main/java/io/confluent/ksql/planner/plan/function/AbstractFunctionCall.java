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

import io.confluent.ksql.schema.ksql.ColumnUsage;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalTerm;
import java.util.ArrayList;
import java.util.List;

public class AbstractFunctionCall implements LogicalTerm, ColumnUsage {

  private String name;
  private List<LogicalTerm> arguments;
  private FunctionSignature signature;

  public AbstractFunctionCall() {
    this.arguments = new ArrayList<>();
  }

  public AbstractFunction getFunction() {
    return FunctionRegistry.getInstance().getFunction(name);
  }

  public FunctionSignature getFunctionSignature() {
    return signature;
  }

  public List<LogicalTerm> getArguments() {
    return arguments;
  }

  public void addArgument(LogicalTerm arg) {
    arguments.add(arg);
  }

  public void setName(final String name) {
    this.name = name;
  }

  public void setSignature(final FunctionSignature signature) {
    this.signature = signature;
  }

  @Override
  public List<Column> getColumnsUsed() {
    final List<Column> columns = new ArrayList<>();
    for (LogicalTerm arg: arguments) {
      columns.addAll(arg.getColumnsUsed());
    }
    return columns;
  }
}
