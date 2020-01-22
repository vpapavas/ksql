/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.streams.materialization.ks;

import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class ActiveAndStandByNodes {

  private final KsqlNode active;
  private final Set<KsqlNode> standBys;

  public ActiveAndStandByNodes(final KsqlNode active) {
    this.active = Objects.requireNonNull(active, "active");
    this.standBys = new HashSet<>();
  }

  public KsqlNode getActive() {
    return active;
  }

  public Set<KsqlNode> getStandBys() {
    return standBys;
  }

  public void addStandBy(final KsqlNode standBy) {
    this.standBys.add(standBy);
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append(String.format("active = %s ,", active))
        .append(String.format("stanbys = %s .", standBys))
        .toString();
  }
}
