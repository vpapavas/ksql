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

package io.confluent.ksql.planner.plan;

import static io.confluent.ksql.schema.ksql.SchemaConverters.sqlToFunctionConverter;
import static java.lang.String.format;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import io.confluent.ksql.execution.codegen.CodeGenSpec;
import io.confluent.ksql.execution.codegen.helpers.ArrayAccess;
import io.confluent.ksql.execution.codegen.helpers.ArrayBuilder;
import io.confluent.ksql.execution.codegen.helpers.CastEvaluator;
import io.confluent.ksql.execution.codegen.helpers.InListEvaluator;
import io.confluent.ksql.execution.codegen.helpers.LikeEvaluator;
import io.confluent.ksql.execution.codegen.helpers.MapBuilder;
import io.confluent.ksql.execution.codegen.helpers.NullSafe;
import io.confluent.ksql.execution.codegen.helpers.SearchedCaseFunction;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.BetweenPredicate;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.CreateArrayExpression;
import io.confluent.ksql.execution.expression.tree.CreateMapExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression.Field;
import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.ExpressionVisitor;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.IsNotNullPredicate;
import io.confluent.ksql.execution.expression.tree.IsNullPredicate;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.NotExpression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.SimpleCaseExpression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.TimeLiteral;
import io.confluent.ksql.execution.expression.tree.TimestampLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.WhenClause;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.GenericsUtil;
import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.planner.plan.function.AbstractFunctionCall;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalTerm;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlBooleans;
import io.confluent.ksql.schema.ksql.SqlDoubles;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class ExpressionToLogicalTermVisitor {

  private static final Map<Operator, String> DECIMAL_OPERATOR_NAME = ImmutableMap
      .<Operator, String>builder()
      .put(Operator.ADD, "add")
      .put(Operator.SUBTRACT, "subtract")
      .put(Operator.MULTIPLY, "multiply")
      .put(Operator.DIVIDE, "divide")
      .put(Operator.MODULUS, "remainder")
      .build();

  private static final Map<ComparisonExpression.Type, String> SQL_COMPARE_TO_JAVA = ImmutableMap
      .<ComparisonExpression.Type, String>builder()
      .put(ComparisonExpression.Type.EQUAL, "==")
      .put(ComparisonExpression.Type.NOT_EQUAL, "!=")
      .put(ComparisonExpression.Type.IS_DISTINCT_FROM, "!=")
      .put(ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, ">=")
      .put(ComparisonExpression.Type.GREATER_THAN, ">")
      .put(ComparisonExpression.Type.LESS_THAN_OR_EQUAL, "<=")
      .put(ComparisonExpression.Type.LESS_THAN, "<")
      .build();

  private final LogicalSchema schema;
  private final FunctionRegistry functionRegistry;

  private final ExpressionTypeManager expressionTypeManager;

  public static ExpressionToLogicalTermVisitor of(
      final LogicalSchema schema,
      final FunctionRegistry functionRegistry,
      final CodeGenSpec spec,
      final KsqlConfig ksqlConfig
  ) {
    final Multiset<FunctionName> nameCounts = HashMultiset.create();
    return new ExpressionToLogicalTermVisitor(
        schema,
        functionRegistry,
        spec::getCodeName,
        name -> {
          final int index = nameCounts.add(name, 1);
          return spec.getUniqueNameForFunction(name, index);
        },
        spec::getStructSchemaName,
        ksqlConfig);
  }

  @VisibleForTesting
  ExpressionToLogicalTermVisitor(
      final LogicalSchema schema, final FunctionRegistry functionRegistry,
  ) {
    this.expressionTypeManager = new ExpressionTypeManager(schema, functionRegistry);
    this.schema = Objects.requireNonNull(schema, "schema");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
  }

  public LogicalTerm process(final Expression expression) {
    return formatExpression(expression);
  }

  private LogicalTerm formatExpression(final Expression expression) {
    final LogicalTerm expressionFormatterResult =
        new Formatter(functionRegistry).process(expression, null);
    return expressionFormatterResult;
  }

  private class Formatter implements ExpressionVisitor<LogicalTerm, Void> {

    private final FunctionRegistry functionRegistry;

    Formatter(final FunctionRegistry functionRegistry) {
      this.functionRegistry = functionRegistry;
    }

    private LogicalTerm visitIllegalState(final Expression expression) {
      throw new IllegalStateException(
          format("expression type %s should never be visited", expression.getClass()));
    }

    private LogicalTerm visitUnsupported(final Expression expression) {
      throw new UnsupportedOperationException(
          format(
              "not yet implemented: %s.visit%s",
              getClass().getName(),
              expression.getClass().getSimpleName()
          )
      );
    }

    @Override
    public LogicalTerm visitType(final Type node, final Void context) {
      return visitIllegalState(node);
    }

    @Override
    public LogicalTerm visitWhenClause(final WhenClause whenClause, final Void context) {
      return visitIllegalState(whenClause);
    }

    @Override
    public LogicalTerm visitInPredicate(
        final InPredicate inPredicate,
        final Void context
    ) {
      // TODO rewrite to function
      final InPredicate preprocessed = InListEvaluator
          .preprocess(inPredicate, expressionTypeManager);

      final LogicalTerm value = process(preprocessed.getValue(), context);

      final String values = preprocessed.getValueList().getValues().stream()
          .map(v -> process(v, context))
          .map(Pair::getLeft)
          .collect(Collectors.joining(","));

      return new Pair<>(
          "InListEvaluator.matches(" + value.getLeft() + "," + values + ")",
          SqlTypes.BOOLEAN
      );
    }

    @Override
    public LogicalTerm visitInListExpression(
        final InListExpression inListExpression, final Void context
    ) {
      return visitUnsupported(inListExpression);
    }

    @Override
    public LogicalTerm visitTimestampLiteral(
        final TimestampLiteral timestampLiteral, final Void context
    ) {
      return visitUnsupported(timestampLiteral);
    }

    @Override
    public LogicalTerm visitTimeLiteral(
        final TimeLiteral timeLiteral,
        final Void context
    ) {
      return visitUnsupported(timeLiteral);
    }

    @Override
    public LogicalTerm visitSimpleCaseExpression(
        final SimpleCaseExpression simpleCaseExpression, final Void context
    ) {
      return visitUnsupported(simpleCaseExpression);
    }

    @Override
    public LogicalTerm visitBooleanLiteral(
        final BooleanLiteral node,
        final Void context
    ) {
      return new Constant(node.getValue(), SqlTypes.BOOLEAN);
    }

    @Override
    public LogicalTerm visitStringLiteral(final StringLiteral node, final Void context) {
      return new Constant(node.getValue(), SqlTypes.STRING);
    }

    @Override
    public LogicalTerm visitDoubleLiteral(final DoubleLiteral node, final Void context) {
      return new Constant(node.getValue(), SqlTypes.DOUBLE);
    }

    @Override
    public LogicalTerm visitDecimalLiteral(
        final DecimalLiteral decimalLiteral,
        final Void context
    ) {
      return new Constant(decimalLiteral.getValue(), DecimalUtil.fromValue(decimalLiteral.getValue()));
    }

    @Override
    public LogicalTerm visitNullLiteral(final NullLiteral node, final Void context) {
      return new Constant(node.getValue(), null);
    }

    @Override
    public LogicalTerm visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final Void context
    ) {
      final Column schemaColumn = schema.findValueColumn(node.getColumnName())
          .orElseThrow(() ->
              new KsqlException("Field not found: " + node.getColumnName()));

      // TODO: Not sure if this is correct
      return schemaColumn;
    }

    @Override
    public LogicalTerm visitQualifiedColumnReference(
        final QualifiedColumnReferenceExp node,
        final Void context
    ) {
      throw new UnsupportedOperationException(
          "Qualified column reference must be resolved to unqualified reference before logical plan"
      );
    }

    @Override
    public LogicalTerm visitDereferenceExpression(
        final DereferenceExpression node, final Void context
    ) {
      return visitUnsupported(node);
    }

    public LogicalTerm visitLongLiteral(final LongLiteral node, final Void context) {
      return new Constant(node.getValue(), SqlTypes.BIGINT);
    }

    @Override
    public LogicalTerm visitIntegerLiteral(
        final IntegerLiteral node,
        final Void context
    ) {
      return new Constant(node.getValue(), SqlTypes.INTEGER);
    }

    @Override
    public LogicalTerm visitFunctionCall(final FunctionCall node, final Void context) {
      return visitUnsupported(node);
    }

    @Override
    public LogicalTerm visitLogicalBinaryExpression(
        final LogicalBinaryExpression node, final Void context
    ) {
      if (node.getType() == LogicalBinaryExpression.Type.OR) {
        return new Pair<>(
            formatBinaryExpression(" || ", node.getLeft(), node.getRight(), context),
            SqlTypes.BOOLEAN
        );
      } else if (node.getType() == LogicalBinaryExpression.Type.AND) {
        return new Pair<>(
            formatBinaryExpression(" && ", node.getLeft(), node.getRight(), context),
            SqlTypes.BOOLEAN
        );
      }
      throw new UnsupportedOperationException(
          format("not yet implemented: %s.visit%s", getClass().getName(),
              node.getClass().getSimpleName()
          )
      );
    }

    @Override
    public LogicalTerm visitNotExpression(final NotExpression node, final Void context) {
      final String exprString = process(node.getValue(), context).getLeft();
      return new Pair<>("(!" + exprString + ")", SqlTypes.BOOLEAN);
    }

    private String nullCheckPrefix(final ComparisonExpression.Type type) {
      if (type == ComparisonExpression.Type.IS_DISTINCT_FROM) {
        return "(((Object)(%1$s)) == null || ((Object)(%2$s)) == null) ? "
            + "((((Object)(%1$s)) == null ) ^ (((Object)(%2$s)) == null )) : ";
      }
      return "(((Object)(%1$s)) == null || ((Object)(%2$s)) == null) ? false : ";
    }

    private String visitStringComparisonExpression(final ComparisonExpression.Type type) {
      switch (type) {
        case EQUAL:
          return "(%1$s.equals(%2$s))";
        case NOT_EQUAL:
        case IS_DISTINCT_FROM:
          return "(!%1$s.equals(%2$s))";
        case GREATER_THAN_OR_EQUAL:
        case GREATER_THAN:
        case LESS_THAN_OR_EQUAL:
        case LESS_THAN:
          return "(%1$s.compareTo(%2$s) " + type.getValue() + " 0)";
        default:
          throw new KsqlException("Unexpected string comparison: " + type.getValue());
      }
    }

    private String visitArrayComparisonExpression(final ComparisonExpression.Type type) {
      switch (type) {
        case EQUAL:
          return "(%1$s.equals(%2$s))";
        case NOT_EQUAL:
        case IS_DISTINCT_FROM:
          return "(!%1$s.equals(%2$s))";
        default:
          throw new KsqlException("Unexpected array comparison: " + type.getValue());
      }
    }

    private String visitMapComparisonExpression(final ComparisonExpression.Type type) {
      switch (type) {
        case EQUAL:
          return "(%1$s.equals(%2$s))";
        case NOT_EQUAL:
        case IS_DISTINCT_FROM:
          return "(!%1$s.equals(%2$s))";
        default:
          throw new KsqlException("Unexpected map comparison: " + type.getValue());
      }
    }

    private String visitStructComparisonExpression(final ComparisonExpression.Type type) {
      switch (type) {
        case EQUAL:
          return "(%1$s.equals(%2$s))";
        case NOT_EQUAL:
        case IS_DISTINCT_FROM:
          return "(!%1$s.equals(%2$s))";
        default:
          throw new KsqlException("Unexpected struct comparison: " + type.getValue());
      }
    }

    private String visitScalarComparisonExpression(final ComparisonExpression.Type type) {
      switch (type) {
        case EQUAL:
          return "((%1$s <= %2$s) && (%1$s >= %2$s))";
        case NOT_EQUAL:
        case IS_DISTINCT_FROM:
          return "((%1$s < %2$s) || (%1$s > %2$s))";
        case GREATER_THAN_OR_EQUAL:
        case GREATER_THAN:
        case LESS_THAN_OR_EQUAL:
        case LESS_THAN:
          return "(%1$s " + type.getValue() + " %2$s)";
        default:
          throw new KsqlException("Unexpected scalar comparison: " + type.getValue());
      }
    }

    private String visitBytesComparisonExpression(
        final ComparisonExpression.Type type, final SqlType left, final SqlType right
    ) {
      final String comparator = SQL_COMPARE_TO_JAVA.get(type);
      if (comparator == null) {
        throw new KsqlException("Unexpected scalar comparison: " + type.getValue());
      }

      return String.format(
          "(%s.compareTo(%s) %s 0)",
          toDecimal(left, 1),
          toDecimal(right, 2),
          comparator
      );
    }

    private String toDecimal(final SqlType schema, final int index) {
      switch (schema.baseType()) {
        case DECIMAL:
          return "%" + index + "$s";
        case DOUBLE:
          return "BigDecimal.valueOf(%" + index + "$s)";
        default:
          return "new BigDecimal(%" + index + "$s)";
      }
    }

    private String visitBooleanComparisonExpression(final ComparisonExpression.Type type) {
      switch (type) {
        case EQUAL:
          return "(Boolean.compare(%1$s, %2$s) == 0)";
        case NOT_EQUAL:
        case IS_DISTINCT_FROM:
          return "(Boolean.compare(%1$s, %2$s) != 0)";
        default:
          throw new KsqlException("Unexpected boolean comparison: " + type.getValue());
      }
    }

    @Override
    public LogicalTerm visitComparisonExpression(
        final ComparisonExpression node, final Void context
    ) {

      List<LogicalTerm> arguments = new ArrayList<>();
      arguments.add(process(node.getLeft(), context));
      arguments.add(process(node.getRight(), context));

      return new AbstractFunctionCall(node.getType().getValue(), arguments);

      if (left.getRight().baseType() == SqlBaseType.DECIMAL
          || right.getRight().baseType() == SqlBaseType.DECIMAL) {
        exprFormat += visitBytesComparisonExpression(
            node.getType(), left.getRight(), right.getRight());
      } else {
        switch (left.getRight().baseType()) {
          case STRING:
            exprFormat += visitStringComparisonExpression(node.getType());
            break;
          case ARRAY:
            exprFormat += visitArrayComparisonExpression(node.getType());
            break;
          case MAP:
            exprFormat += visitMapComparisonExpression(node.getType());
            break;
          case STRUCT:
            exprFormat += visitStructComparisonExpression(node.getType());
            break;
          case BOOLEAN:
            exprFormat += visitBooleanComparisonExpression(node.getType());
            break;
          default:
            exprFormat += visitScalarComparisonExpression(node.getType());
            break;
        }
      }
      final String expr = "(" + String.format(exprFormat, left.getLeft(), right.getLeft()) + ")";
      return new Pair<>(expr, SqlTypes.BOOLEAN);
    }

    @Override
    public LogicalTerm visitCast(final Cast node, final Void context) {
      return visitUnsupported(node);
    }

    @Override
    public LogicalTerm visitIsNullPredicate(
        final IsNullPredicate node,
        final Void context
    ) {
      return visitUnsupported(node);
    }

    @Override
    public LogicalTerm visitIsNotNullPredicate(
        final IsNotNullPredicate node,
        final Void context
    ) {
      return visitUnsupported(node);
    }

    @Override
    public LogicalTerm visitArithmeticUnary(
        final ArithmeticUnaryExpression node, final Void context
    ) {
      final LogicalTerm value = process(node.getValue(), context);
      switch (node.getSign()) {
        case MINUS:
          return visitArithmeticMinus(value);
        case PLUS:
          return visitArithmeticPlus(value);
        default:
          throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
      }
    }

    private LogicalTerm visitArithmeticMinus(final LogicalTerm value) {
      if (value.getRight().baseType() == SqlBaseType.DECIMAL) {
        return new Pair<>(
            String.format(
                "(%s.negate(new MathContext(%d, RoundingMode.UNNECESSARY)))",
                value.getLeft(),
                ((SqlDecimal) value.getRight()).getPrecision()
            ),
            value.getRight()
        );
      } else {
        // this is to avoid turning a sequence of "-" into a comment (i.e., "-- comment")
        final String separator = value.getLeft().startsWith("-") ? " " : "";
        return new Pair<>("-" + separator + value.getLeft(), value.getRight());
      }
    }

    private LogicalTerm visitArithmeticPlus(final LogicalTerm value) {
      if (value.getRight().baseType() == SqlBaseType.DECIMAL) {
        return new Pair<>(
            String.format(
                "(%s.plus(new MathContext(%d, RoundingMode.UNNECESSARY)))",
                value.getLeft(),
                ((SqlDecimal) value.getRight()).getPrecision()
            ),
            value.getRight()
        );
      } else {
        return new Pair<>("+" + value.getLeft(), value.getRight());
      }
    }

    @Override
    public LogicalTerm visitArithmeticBinary(
        final ArithmeticBinaryExpression node, final Void context
    ) {
      final LogicalTerm left = process(node.getLeft(), context);
      final LogicalTerm right = process(node.getRight(), context);

      final SqlType schema = expressionTypeManager.getExpressionSqlType(node);

      if (schema.baseType() == SqlBaseType.DECIMAL) {
        final SqlDecimal decimal = (SqlDecimal) schema;
        final String leftExpr = genCastCode(left, DecimalUtil.toSqlDecimal(left.right));
        final String rightExpr = genCastCode(right, DecimalUtil.toSqlDecimal(right.right));

        return new Pair<>(
            String.format(
                "(%s.%s(%s, new MathContext(%d, RoundingMode.UNNECESSARY)).setScale(%d))",
                leftExpr,
                DECIMAL_OPERATOR_NAME.get(node.getOperator()),
                rightExpr,
                decimal.getPrecision(),
                decimal.getScale()
            ),
            schema
        );
      } else {
        final String leftExpr =
            left.getRight().baseType() == SqlBaseType.DECIMAL
                ? genCastCode(left, SqlTypes.DOUBLE)
                : left.getLeft();
        final String rightExpr =
            right.getRight().baseType() == SqlBaseType.DECIMAL
                ? genCastCode(right, SqlTypes.DOUBLE)
                : right.getLeft();

        return new Pair<>(
            String.format(
                "(%s %s %s)",
                leftExpr,
                node.getOperator().getSymbol(),
                rightExpr
            ),
            schema
        );
      }
    }

    @Override
    public LogicalTerm visitSearchedCaseExpression(
        final SearchedCaseExpression node, final Void context
    ) {
      return visitUnsupported(node);
    }

    private String buildSupplierCode(final String typeString, final String code) {
      return " new " + Supplier.class.getSimpleName() + "<" + typeString + ">() {"
          + " @Override public " + typeString + " get() { return " + code + "; }}";
    }

    @Override
    public LogicalTerm visitLikePredicate(final LikePredicate node, final Void context) {

      return visitUnsupported(node);
    }

    @Override
    public LogicalTerm visitSubscriptExpression(
        final SubscriptExpression node,
        final Void context
    ) {
      return visitUnsupported(node);
    }

    @Override
    public LogicalTerm visitCreateArrayExpression(
        final CreateArrayExpression exp,
        final Void context
    ) {
      return visitUnsupported(exp);
    }

    @Override
    public LogicalTerm visitCreateMapExpression(
        final CreateMapExpression exp,
        final Void context
    ) {
      final StringBuilder map = new StringBuilder("new MapBuilder(");
      return visitUnsupported(exp);
    }

    @Override
    public LogicalTerm visitStructExpression(
        final CreateStructExpression node,
        final Void context
    ) {
      return visitUnsupported(node);
    }

    @Override
    public LogicalTerm visitBetweenPredicate(
        final BetweenPredicate node,
        final Void context
    ) {
      return visitUnsupported(node);
    }
  }
}
