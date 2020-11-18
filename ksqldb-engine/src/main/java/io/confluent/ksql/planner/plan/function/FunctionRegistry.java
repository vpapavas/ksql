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

import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.Map;

public class FunctionRegistry {

  private static Map<String, AbstractFunction> functionMap = new HashMap<>();
  private static FunctionRegistry INSTANCE;

  static {
    addFunction(EqualFunction.NAME, new EqualFunction());
  }

  public static FunctionRegistry getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new FunctionRegistry();
    }
    return INSTANCE;
  }

  static void addFunction(final String name, final AbstractFunction function) {
    functionMap.putIfAbsent(name.toUpperCase(), function);
  }

  public static AbstractFunction getFunction(final String name) {
    if (!functionMap.containsKey(name.toUpperCase())) {
      throw new KsqlException("Unknown function name " + name);
    }

    return functionMap.get(name.toUpperCase());
  }
}
