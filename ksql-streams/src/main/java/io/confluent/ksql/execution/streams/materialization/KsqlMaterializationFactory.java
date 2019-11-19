/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.streams.materialization;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.materialization.AggregatesInfo;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.materialization.MaterializationInfo.ProjectInfo;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.sqlpredicate.SqlPredicate;
import io.confluent.ksql.execution.streams.AggregateParamsFactory;
import io.confluent.ksql.execution.streams.SelectValueMapperFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Factor class for {@link KsqlMaterialization}.
 */
public final class KsqlMaterializationFactory {

  private static final String FILTER_OP_NAME = "filter";
  private static final String PROJECT_OP_NAME = "project";

  private final KsqlConfig ksqlConfig;
  private final FunctionRegistry functionRegistry;
  private final ProcessingLogContext processingLogContext;
  private final AggregateMapperFactory aggregateMapperFactory;
  private final SqlPredicateFactory sqlPredicateFactory;
  private final SelectMapperFactory selectMapperFactory;
  private final MaterializationFactory materializationFactory;

  public KsqlMaterializationFactory(
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final ProcessingLogContext processingLogContext
  ) {
    this(
        ksqlConfig,
        functionRegistry,
        processingLogContext,
        defaultAggregateMapperFactory(),
        SqlPredicate::new,
        defaultValueMapperFactory(),
        KsqlMaterialization::new
    );
  }

  @VisibleForTesting
  KsqlMaterializationFactory(
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final ProcessingLogContext processingLogContext,
      final AggregateMapperFactory aggregateMapperFactory,
      final SqlPredicateFactory sqlPredicateFactory,
      final SelectMapperFactory selectMapperFactory,
      final MaterializationFactory materializationFactory
  ) {
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
    this.processingLogContext = requireNonNull(processingLogContext, "processingLogContext");
    this.aggregateMapperFactory = requireNonNull(aggregateMapperFactory, "aggregateMapperFactory");
    this.sqlPredicateFactory = requireNonNull(sqlPredicateFactory, "sqlPredicateFactory");
    this.selectMapperFactory = requireNonNull(selectMapperFactory, "selectMapperFactory");
    this.materializationFactory = requireNonNull(materializationFactory, "materializationFactory");
  }

  public Materialization create(
      final Materialization delegate,
      final MaterializationInfo info
  ) {
    final TransformVisitor transformVisitor = new TransformVisitor();
    final List<TransformFunction> transforms = info
        .getTransforms()
        .stream()
        .map(xform -> xform.visit(transformVisitor))
        .collect(Collectors.toList());

    return materializationFactory.create(
        delegate,
        info.getSchema(),
        transforms
    );
  }

  private static AggregateMapperFactory defaultAggregateMapperFactory() {
    return (info, functionRegistry) ->
        new AggregateParamsFactory().create(
            info.schema(),
            info.startingColumnIndex(),
            functionRegistry,
            info.aggregateFunctions()
        ).getAggregator().getResultMapper()::apply;
  }

  private static SelectMapperFactory defaultValueMapperFactory() {
    return (selectExpressions, sourceSchema, ksqlConfig, functionRegistry, stacker, logContext) ->
        SelectValueMapperFactory.create(
            selectExpressions,
            sourceSchema,
            ksqlConfig,
            functionRegistry,
            stacker,
            logContext
        )::transform;
  }

  interface AggregateMapperFactory {

    Function<GenericRow, GenericRow> create(
        AggregatesInfo info,
        FunctionRegistry functionRegistry
    );
  }

  interface SqlPredicateFactory {

    SqlPredicate create(
        Expression filterExpression,
        LogicalSchema schema,
        KsqlConfig ksqlConfig,
        FunctionRegistry functionRegistry,
        QueryContext.Stacker stacker,
        ProcessingLogContext logContext
    );
  }

  interface SelectMapperFactory {

    BiFunction<Object, GenericRow, GenericRow> create(
        List<SelectExpression> selectExpressions,
        LogicalSchema sourceSchema,
        KsqlConfig ksqlConfig,
        FunctionRegistry functionRegistry,
        QueryContext.Stacker stacker,
        ProcessingLogContext logContext
    );
  }

  interface MaterializationFactory {

    KsqlMaterialization create(
        Materialization inner,
        LogicalSchema schema,
        List<TransformFunction> transforms
    );
  }

  private class TransformVisitor implements MaterializationInfo.TransformVisitor<
      TransformFunction> {

    private final QueryContext.Stacker stacker;

    TransformVisitor() {
      this.stacker = new Stacker();
    }

    @Override
    public TransformFunction visit(
        final MaterializationInfo.AggregateMapInfo info) {
      final Function<GenericRow, GenericRow> mapper = aggregateMapperFactory.create(
          info.getInfo(),
          functionRegistry
      );
      return (k, v, queryId) -> Optional.of(mapper.apply(v));
    }

    @Override
    public TransformFunction visit(
        final MaterializationInfo.SqlPredicateInfo info) {

      stacker.push(FILTER_OP_NAME).getQueryContext();
      /*final ProcessingLogger logger = processingLogContext.getLoggerFactory().getLogger(
          QueryLoggerUtil.queryLoggerName(queryId, stacker.push(FILTER_OP_NAME).getQueryContext())
      );*/
      final SqlPredicate predicate = sqlPredicateFactory.create(
          info.getFilterExpression(),
          info.getSchema(),
          ksqlConfig,
          functionRegistry,
          stacker,
          processingLogContext
      );
      return (s, g, queryId) -> predicate.getPredicate(queryId).test(s, g)
          ? Optional.of(g) : Optional.empty();
    }

    @Override
    public TransformFunction visit(final ProjectInfo info) {
      /*final ProcessingLogger logger = processingLogContext.getLoggerFactory().getLogger(
          QueryLoggerUtil.queryLoggerName(queryId, stacker.push(PROJECT_OP_NAME).getQueryContext())
      );*/
      stacker.push(PROJECT_OP_NAME).getQueryContext();
      final BiFunction<Object, GenericRow, GenericRow> mapper = selectMapperFactory.create(
          info.getSelectExpressions(),
          info.getSchema(),
          ksqlConfig,
          functionRegistry,
          stacker,
          processingLogContext
      );
      return (k, v, queryId) -> Optional.of(mapper.apply(k, v));
    }
  }
}
