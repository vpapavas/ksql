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

import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FunctionSignature {

  private final String name;
  private List<Parameter> parameters;
  private SqlBaseType return_type;

  public FunctionSignature(final String name,
                           final List<Parameter> parameters,
                           final SqlBaseType return_type) {
    this.name = name;
    this.parameters = parameters;
    this.return_type = return_type;
  }

  public FunctionSignature(final String name,
                           final SqlBaseType return_type,
                           final Parameter... parameters) {
    this.name = name;
    this.return_type = return_type;
    this.parameters = (parameters.length == 0) ? new ArrayList<>() : Arrays.asList(parameters);
  }

  public String getName() {
    return name;
  }

  public SqlBaseType getReturn_type() {
    return return_type;
  }

  public void addParameter(String name, SqlBaseType type) {
    parameters.add(new Parameter(name, type));
  }

  public List<Parameter> getParameters() {
    return parameters;
  }

  public static class Parameter {
    private String name;
    private SqlBaseType type;

    public Parameter(final String name, final SqlBaseType type) {
      this.name = name;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public SqlBaseType getType() {
      return type;
    }
  }

}
