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

import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FunctionSignature {

  private final String name;
  private List<Argument> arguments;
  private SqlType return_type;

  public FunctionSignature(final String name,
                           final List<Argument> arguments,
                           final SqlType return_type) {
    this.name = name;
    this.arguments = arguments;
    this.return_type = return_type;
  }

  public FunctionSignature(final String name,
                           final SqlType return_type,
                           final Argument... arguments) {
    this.name = name;
    this.return_type = return_type;
    this.arguments = (arguments.length == 0) ? new ArrayList<>() : Arrays.asList(arguments);
  }

  public void addArgument(String name, SqlType type) {
    arguments.add(new Argument(name, type));
  }

  public List<Argument> getArguments() {
    return arguments;
  }

  public static class Argument {
    private String name;
    private SqlType type;

    public Argument(final String name, final SqlType type) {
      this.name = name;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public SqlType getType() {
      return type;
    }
  }

}
