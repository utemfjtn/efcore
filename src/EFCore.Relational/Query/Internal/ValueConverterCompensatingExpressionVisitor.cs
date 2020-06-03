// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Collections.Generic;
using System.Linq.Expressions;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Utilities;

namespace Microsoft.EntityFrameworkCore.Query.Internal
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public class ValueConverterCompensatingExpressionVisitor : ExpressionVisitor
    {
        private readonly ISqlExpressionFactory _sqlExpressionFactory;

        private bool _insidePredicate;
        private bool _insideBoolComparison;

        /// <summary>
        ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
        ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
        ///     any release. You should only use it directly in your code with extreme caution and knowing that
        ///     doing so can result in application failures when updating to a new Entity Framework Core release.
        /// </summary>
        public ValueConverterCompensatingExpressionVisitor(
            [NotNull] ISqlExpressionFactory sqlExpressionFactory)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
        }

        /// <summary>
        ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
        ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
        ///     any release. You should only use it directly in your code with extreme caution and knowing that
        ///     doing so can result in application failures when updating to a new Entity Framework Core release.
        /// </summary>
        protected override Expression VisitExtension(Expression extensionExpression)
        {
            Check.NotNull(extensionExpression, nameof(extensionExpression));

            return extensionExpression switch
            {
                ShapedQueryExpression shapedQueryExpression => VisitShapedQueryExpression(shapedQueryExpression),
                CaseExpression caseExpression => VisitCase(caseExpression),
                ColumnExpression columnExpression => VisitColumn(columnExpression),
                ExistsExpression existsExpression => VisitExists(existsExpression),
                InExpression inExpression => VisitIn(inExpression),
                LikeExpression likeExpression => VisitLike(likeExpression),
                SelectExpression selectExpression => VisitSelect(selectExpression),
                SqlBinaryExpression sqlBinaryExpression => VisitSqlBinary(sqlBinaryExpression),
                SqlFunctionExpression sqlFunctionExpression => VisitSqlFunction(sqlFunctionExpression),
                TableValuedFunctionExpression tableValuedFunctionExpression => VisitTableValuedFunction(tableValuedFunctionExpression),
                CrossJoinExpression crossJoinExpression => VisitCrossJoin(crossJoinExpression),
                CrossApplyExpression crossApplyExpression => VisitCrossApply(crossApplyExpression),
                OuterApplyExpression outerApplyExpression => VisitOuterApply(outerApplyExpression),
                InnerJoinExpression innerJoinExpression => VisitInnerJoin(innerJoinExpression),
                LeftJoinExpression leftJoinExpression => VisitLeftJoin(leftJoinExpression),
                ScalarSubqueryExpression scalarSubqueryExpression => VisitScalarSubquery(scalarSubqueryExpression),
                RowNumberExpression rowNumberExpression => VisitRowNumber(rowNumberExpression),
                ExceptExpression exceptExpression => VisitExcept(exceptExpression),
                IntersectExpression intersectExpression => VisitIntersect(intersectExpression),
                UnionExpression unionExpression => VisitUnion(unionExpression),
                _ => base.VisitExtension(extensionExpression),
            };
        }

        private Expression VisitShapedQueryExpression(ShapedQueryExpression shapedQueryExpression)
        {
            return shapedQueryExpression.Update(
                Visit(shapedQueryExpression.QueryExpression), shapedQueryExpression.ShaperExpression);
        }

        private Expression VisitCase(CaseExpression caseExpression)
        {
            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = false;
            _insideBoolComparison = false;

            var testIsCondition = caseExpression.Operand == null;
            _insidePredicate = false;
            var operand = (SqlExpression)Visit(caseExpression.Operand);
            var whenClauses = new List<CaseWhenClause>();
            foreach (var whenClause in caseExpression.WhenClauses)
            {
                _insidePredicate = testIsCondition;
                var test = (SqlExpression)Visit(whenClause.Test);
                _insidePredicate = false;
                var result = (SqlExpression)Visit(whenClause.Result);
                whenClauses.Add(new CaseWhenClause(test, result));
            }

            _insidePredicate = false;
            var elseResult = (SqlExpression)Visit(caseExpression.ElseResult);

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return caseExpression.Update(operand, whenClauses, elseResult);
        }

        private Expression VisitColumn(ColumnExpression columnExpression)
        {
            return _insidePredicate
                && !_insideBoolComparison
                && columnExpression.TypeMapping.ClrType == typeof(bool)
                && columnExpression.TypeMapping.Converter != null
                ? _sqlExpressionFactory.Equal(
                    columnExpression,
                    _sqlExpressionFactory.Constant(true, columnExpression.TypeMapping))
                : (Expression)columnExpression;
        }

        private Expression VisitExists(ExistsExpression existsExpression)
        {
            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = false;
            _insideBoolComparison = false;

            var subquery = (SelectExpression)Visit(existsExpression.Subquery);

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return existsExpression.Update(subquery);
        }

        private Expression VisitIn(InExpression inExpression)
        {
            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = false;
            _insideBoolComparison = false;

            var item = (SqlExpression)Visit(inExpression.Item);
            var subquery = (SelectExpression)Visit(inExpression.Subquery);
            var values = (SqlExpression)Visit(inExpression.Values);

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return inExpression.Update(item, values, subquery);
        }

        private Expression VisitLike(LikeExpression likeExpression)
        {
            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = false;
            _insideBoolComparison = false;

            var match = (SqlExpression)Visit(likeExpression.Match);
            var pattern = (SqlExpression)Visit(likeExpression.Pattern);
            var escapeChar = (SqlExpression)Visit(likeExpression.EscapeChar);

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return likeExpression.Update(match, pattern, escapeChar);
        }

        private Expression VisitSelect(SelectExpression selectExpression)
        {
            var changed = false;

            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = false;
            _insideBoolComparison = false;

            var projections = new List<ProjectionExpression>();
            foreach (var item in selectExpression.Projection)
            {
                var updatedProjection = (ProjectionExpression)Visit(item);
                projections.Add(updatedProjection);
                changed |= updatedProjection != item;
            }

            var tables = new List<TableExpressionBase>();
            foreach (var table in selectExpression.Tables)
            {
                var newTable = (TableExpressionBase)Visit(table);
                changed |= newTable != table;
                tables.Add(newTable);
            }

            _insidePredicate = true;
            var predicate = (SqlExpression)Visit(selectExpression.Predicate);
            _insidePredicate = false;
            changed |= predicate != selectExpression.Predicate;

            var groupBy = new List<SqlExpression>();
            foreach (var groupingKey in selectExpression.GroupBy)
            {
                var newGroupingKey = (SqlExpression)Visit(groupingKey);
                changed |= newGroupingKey != groupingKey;
                groupBy.Add(newGroupingKey);
            }

            _insidePredicate = true;
            var havingExpression = (SqlExpression)Visit(selectExpression.Having);
            _insidePredicate = false;
            changed |= havingExpression != selectExpression.Having;

            var orderings = new List<OrderingExpression>();
            foreach (var ordering in selectExpression.Orderings)
            {
                var orderingExpression = (SqlExpression)Visit(ordering.Expression);
                changed |= orderingExpression != ordering.Expression;
                orderings.Add(ordering.Update(orderingExpression));
            }

            var offset = (SqlExpression)Visit(selectExpression.Offset);
            changed |= offset != selectExpression.Offset;

            var limit = (SqlExpression)Visit(selectExpression.Limit);
            changed |= limit != selectExpression.Limit;

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return changed
                ? selectExpression.Update(
                    projections, tables, predicate, groupBy, havingExpression, orderings, limit, offset)
                : selectExpression;
        }

        private Expression VisitSqlBinary(SqlBinaryExpression sqlBinaryExpression)
        {
            var parentInsideBoolComparison = _insideBoolComparison;
            _insideBoolComparison = sqlBinaryExpression.OperatorType == ExpressionType.Equal
                || sqlBinaryExpression.OperatorType == ExpressionType.NotEqual;

            var left = (SqlExpression)Visit(sqlBinaryExpression.Left);
            var right = (SqlExpression)Visit(sqlBinaryExpression.Right);

            _insideBoolComparison = parentInsideBoolComparison;

            return sqlBinaryExpression.Update(left, right);
        }

        private Expression VisitSqlFunction(SqlFunctionExpression sqlFunctionExpression)
        {
            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = false;
            _insideBoolComparison = false;

            var instance = (SqlExpression)Visit(sqlFunctionExpression.Instance);
            SqlExpression[] arguments = default;
            if (!sqlFunctionExpression.IsNiladic)
            {
                arguments = new SqlExpression[sqlFunctionExpression.Arguments.Count];
                for (var i = 0; i < arguments.Length; i++)
                {
                    arguments[i] = (SqlExpression)Visit(sqlFunctionExpression.Arguments[i]);
                }
            }

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return sqlFunctionExpression.Update(instance, arguments);
        }

        // TODO: See issue #20180
        private Expression VisitTableValuedFunction(TableValuedFunctionExpression tableValuedFunctionExpression)
            => tableValuedFunctionExpression;

        private Expression VisitCrossJoin(CrossJoinExpression crossJoinExpression)
        {
            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = false;
            _insideBoolComparison = false;

            var table = (TableExpressionBase)Visit(crossJoinExpression.Table);

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return crossJoinExpression.Update(table);
        }

        private Expression VisitCrossApply(CrossApplyExpression crossApplyExpression)
        {
            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = false;
            _insideBoolComparison = false;

            var table = (TableExpressionBase)Visit(crossApplyExpression.Table);

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return crossApplyExpression.Update(table);
        }

        private Expression VisitOuterApply(OuterApplyExpression outerApplyExpression)
        {
            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = false;
            _insideBoolComparison = false;

            var table = (TableExpressionBase)Visit(outerApplyExpression.Table);

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return outerApplyExpression.Update(table);
        }

        private Expression VisitInnerJoin(InnerJoinExpression innerJoinExpression)
        {
            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = false;
            _insideBoolComparison = false;

            var table = (TableExpressionBase)Visit(innerJoinExpression.Table);

            _insidePredicate = true;
            var joinPredicate = (SqlExpression)Visit(innerJoinExpression.JoinPredicate);

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return innerJoinExpression.Update(table, joinPredicate);
        }

        private Expression VisitLeftJoin(LeftJoinExpression leftJoinExpression)
        {
            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = false;
            _insideBoolComparison = false;

            var table = (TableExpressionBase)Visit(leftJoinExpression.Table);

            _insidePredicate = true;
            var joinPredicate = (SqlExpression)Visit(leftJoinExpression.JoinPredicate);

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return leftJoinExpression.Update(table, joinPredicate);
        }

        private Expression VisitScalarSubquery(ScalarSubqueryExpression scalarSubqueryExpression)
        {
            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = false;
            _insideBoolComparison = false;

            var subquery = (SelectExpression)Visit(scalarSubqueryExpression.Subquery);

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return scalarSubqueryExpression.Update(subquery);
        }

        private Expression VisitRowNumber(RowNumberExpression rowNumberExpression)
        {
            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = false;
            _insideBoolComparison = false;

            var changed = false;
            var partitions = new List<SqlExpression>();
            foreach (var partition in rowNumberExpression.Partitions)
            {
                var newPartition = (SqlExpression)Visit(partition);
                changed |= newPartition != partition;
                partitions.Add(newPartition);
            }

            var orderings = new List<OrderingExpression>();
            foreach (var ordering in rowNumberExpression.Orderings)
            {
                var newOrdering = (OrderingExpression)Visit(ordering);
                changed |= newOrdering != ordering;
                orderings.Add(newOrdering);
            }

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return rowNumberExpression.Update(partitions, orderings);
        }

        private Expression VisitExcept(ExceptExpression exceptExpression)
        {
            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = false;
            _insideBoolComparison = false;

            var source1 = (SelectExpression)Visit(exceptExpression.Source1);
            var source2 = (SelectExpression)Visit(exceptExpression.Source2);

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return exceptExpression.Update(source1, source2);
        }

        private Expression VisitIntersect(IntersectExpression intersectExpression)
        {
            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = false;
            _insideBoolComparison = false;

            var source1 = (SelectExpression)Visit(intersectExpression.Source1);
            var source2 = (SelectExpression)Visit(intersectExpression.Source2);

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return intersectExpression.Update(source1, source2);
        }

        private Expression VisitUnion(UnionExpression unionExpression)
        {
            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = false;
            _insideBoolComparison = false;

            var source1 = (SelectExpression)Visit(unionExpression.Source1);
            var source2 = (SelectExpression)Visit(unionExpression.Source2);

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return unionExpression.Update(source1, source2);
        }
    }
}
