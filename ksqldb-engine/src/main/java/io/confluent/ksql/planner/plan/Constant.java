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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnUsage;
import io.confluent.ksql.schema.ksql.LogicalTerm;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class Constant implements LogicalTerm, ColumnUsage {

  private final Object value;
  private final SqlType type;

  public Constant(final Object value, final SqlType type) {
    this.value = Objects.requireNonNull(value);
    this.type = Objects.requireNonNull(type);
  }

  public Object getValue() {
    return value;
  }

  public SqlType getType() {
    return type;
  }

  @Override
  public List<Column> getColumnsUsed() {
    return Collections.emptyList();
  }
}
