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

package io.confluent.ksql.physical.pull.operators;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.PullProcessingContext;
import io.confluent.ksql.execution.streams.materialization.Row;
import io.confluent.ksql.execution.streams.materialization.TableRow;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.execution.transform.sqlpredicate.SqlPredicate;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.PullFilterNode;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class SelectOperator extends AbstractPhysicalOperator implements UnaryPhysicalOperator {

  private final PullFilterNode logicalNode;
  private final ProcessingLogger logger;
  private final SqlPredicate predicate;

  private AbstractPhysicalOperator child;
  private KsqlTransformer<Object, Optional<GenericRow>> transformer;
  private TableRow row;

  public SelectOperator(final PullFilterNode logicalNode, final ProcessingLogger logger) {
    this.logicalNode = Objects.requireNonNull(logicalNode, "logicalNode");
    this.logger = Objects.requireNonNull(logger, "logger");
    this.predicate = new SqlPredicate(
        logicalNode.getPredicate(),
        logicalNode.getCompiledWhereClause()
    );

  }

  @Override
  public void open() {
    transformer = predicate.getTransformer(logger);
    child.open();
  }

  @Override
  public Object next() {
    row = (TableRow)child.next();
    if (row == null) {
      return null;
    }

    final GenericRow intermediate = getIntermediateRow(row);

    return transformer.transform(
        row.key(),
        intermediate,
        new PullProcessingContext(row.rowTime()))
        .map(r -> Row.of(logicalNode.getIntermediateSchema(), row.key(), r, row.rowTime()))
        .orElse(Row.EMPTY_ROW);
  }

  @Override
  public void close() {

  }

  @Override
  public PlanNode getLogicalNode() {
    return logicalNode;
  }

  @Override
  public void addChild(final AbstractPhysicalOperator child) {
    if (this.child != null) {
      throw new UnsupportedOperationException("The select operator already has a child.");
    }
    Objects.requireNonNull(child, "child");
    this.child = child;
  }

  @Override
  public AbstractPhysicalOperator getChild() {
    return child;
  }

  @Override
  public AbstractPhysicalOperator getChild(final int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<AbstractPhysicalOperator> getChildren() {
    throw new UnsupportedOperationException();
  }

  private GenericRow getIntermediateRow(final TableRow row) {

    if (!logicalNode.getAddAdditionalColumnsToIntermediateSchema()) {
      return row.value();
    }

    final GenericKey key = row.key();
    final GenericRow value = row.value();

    final List<?> keyFields = key.values();

    value.ensureAdditionalCapacity(
        1 // ROWTIME
            + keyFields.size()
            + row.window().map(w -> 2).orElse(0)
    );

    value.append(row.rowTime());
    value.appendAll(keyFields);

    row.window().ifPresent(window -> {
      value.append(window.start().toEpochMilli());
      value.append(window.end().toEpochMilli());
    });

    return value;
  }
}
