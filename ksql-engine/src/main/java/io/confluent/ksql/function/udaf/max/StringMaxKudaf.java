/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf.max;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

@UdafDescription(name = "str_max", description = "Picks max of two strings")
public final class StringMaxKudaf {

  private StringMaxKudaf() {

  }

  @UdafFactory(description = "picks maximum among strings")
  public static Udaf<String, String, String> createMaxString() {
    return new Udaf<String, String, String>() {
      @Override
      public String initialize() {
        return "";
      }

      @Override
      public String aggregate(final String value, final String aggregate) {
        return aggregate.compareTo(value) > 0 ? aggregate : value;
      }

      @Override
      public String merge(final String aggOne, final String aggTwo) {
        return aggregate(aggOne, aggTwo);
      }

      @Override
      public String map(final String agg) {
        return agg;
      }
    };
  }
}
