// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Utilities;

namespace Microsoft.EntityFrameworkCore.Cosmos.Query.Internal
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public class CosmosValueConverterCompensatingExpressionVisitor : ExpressionVisitor
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
        public CosmosValueConverterCompensatingExpressionVisitor(
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
            return extensionExpression switch
            {
                ShapedQueryExpression shapedQueryExpression => VisitShapedQueryExpression(shapedQueryExpression),
                ReadItemExpression readItemExpression => readItemExpression,
                InExpression inExpression => VisitIn(inExpression),
                KeyAccessExpression keyAccessExpression => VisitKeyAccess(keyAccessExpression),
                SelectExpression selectExpression => VisitSelect(selectExpression),
                SqlBinaryExpression sqlBinaryExpression => VisitSqlBinary(sqlBinaryExpression),
                SqlConditionalExpression sqlConditionalExpression => VisitSqlConditional(sqlConditionalExpression),
                SqlFunctionExpression sqlFunctionExpression => VisitSqlFunction(sqlFunctionExpression),
                _ => base.VisitExtension(extensionExpression),
            };
        }

        private Expression VisitShapedQueryExpression(ShapedQueryExpression shapedQueryExpression)
        {
            return shapedQueryExpression.Update(
                Visit(shapedQueryExpression.QueryExpression), shapedQueryExpression.ShaperExpression);
        }

        private Expression VisitIn(InExpression inExpression)
        {
            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = false;
            _insideBoolComparison = false;

            var item = (SqlExpression)Visit(inExpression.Item);
            var values = (SqlExpression)Visit(inExpression.Values);

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return inExpression.Update(item, values);
        }

        private Expression VisitKeyAccess(KeyAccessExpression keyAccessExpression)
        {
            Check.NotNull(keyAccessExpression, nameof(keyAccessExpression));

            var result = keyAccessExpression.Update(Visit(keyAccessExpression.AccessExpression));

            if (_insidePredicate
                && !_insideBoolComparison
                && keyAccessExpression.TypeMapping.ClrType == typeof(bool)
                && keyAccessExpression.TypeMapping.Converter != null)
            {
                return _sqlExpressionFactory.Equal(
                    result,
                    _sqlExpressionFactory.Constant(true, result.TypeMapping));
            }
            else
            {
                return result;
            }
        }

        private Expression VisitSelect(SelectExpression selectExpression)
        {
            Check.NotNull(selectExpression, nameof(selectExpression));

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

            var fromExpression = (RootReferenceExpression)Visit(selectExpression.FromExpression);
            changed |= fromExpression != selectExpression.FromExpression;

            _insidePredicate = true;
            var predicate = (SqlExpression)Visit(selectExpression.Predicate);
            _insidePredicate = false;
            changed |= predicate != selectExpression.Predicate;

            var orderings = new List<OrderingExpression>();
            foreach (var ordering in selectExpression.Orderings)
            {
                var orderingExpression = (SqlExpression)Visit(ordering.Expression);
                changed |= orderingExpression != ordering.Expression;
                orderings.Add(ordering.Update(orderingExpression));
            }

            var limit = (SqlExpression)Visit(selectExpression.Limit);
            var offset = (SqlExpression)Visit(selectExpression.Offset);

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return changed
                ? selectExpression.Update(projections, fromExpression, predicate, orderings, limit, offset)
                : selectExpression;
        }

        private Expression VisitSqlBinary(SqlBinaryExpression sqlBinaryExpression)
        {
            Check.NotNull(sqlBinaryExpression, nameof(sqlBinaryExpression));

            var parentInsideBoolComparison = _insideBoolComparison;
            _insideBoolComparison = sqlBinaryExpression.OperatorType == ExpressionType.Equal
                || sqlBinaryExpression.OperatorType == ExpressionType.NotEqual;

            var left = (SqlExpression)Visit(sqlBinaryExpression.Left);
            var right = (SqlExpression)Visit(sqlBinaryExpression.Right);

            _insideBoolComparison = parentInsideBoolComparison;

            return sqlBinaryExpression.Update(left, right);
        }

        private Expression VisitSqlConditional(SqlConditionalExpression sqlConditionalExpression)
        {
            Check.NotNull(sqlConditionalExpression, nameof(sqlConditionalExpression));

            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = true;
            _insideBoolComparison = false;

            var test = (SqlExpression)Visit(sqlConditionalExpression.Test);
            _insidePredicate = false;
            var ifTrue = (SqlExpression)Visit(sqlConditionalExpression.IfTrue);
            var ifFalse = (SqlExpression)Visit(sqlConditionalExpression.IfFalse);

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return sqlConditionalExpression.Update(test, ifTrue, ifFalse);
        }

        private Expression VisitSqlFunction(SqlFunctionExpression sqlFunctionExpression)
        {
            Check.NotNull(sqlFunctionExpression, nameof(sqlFunctionExpression));

            var parentInsidePredicate = _insidePredicate;
            var parentInsideBoolComparison = _insideBoolComparison;
            _insidePredicate = false;
            _insideBoolComparison = false;

            var arguments = new SqlExpression[sqlFunctionExpression.Arguments.Count];
            for (var i = 0; i < arguments.Length; i++)
            {
                arguments[i] = (SqlExpression)Visit(sqlFunctionExpression.Arguments[i]);
            }

            _insidePredicate = parentInsidePredicate;
            _insideBoolComparison = parentInsideBoolComparison;

            return sqlFunctionExpression.Update(arguments);
        }
    }
}
