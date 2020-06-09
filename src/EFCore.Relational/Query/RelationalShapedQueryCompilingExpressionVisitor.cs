// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Utilities;

namespace Microsoft.EntityFrameworkCore.Query
{
    /// <inheritdoc />
    public partial class RelationalShapedQueryCompilingExpressionVisitor : ShapedQueryCompilingExpressionVisitor
    {
        private readonly Type _contextType;
        private readonly ISet<string> _tags;
        private readonly bool _detailedErrorsEnabled;
        private readonly bool _useRelationalNulls;

        /// <summary>
        ///     Creates a new instance of the <see cref="ShapedQueryCompilingExpressionVisitor" /> class.
        /// </summary>
        /// <param name="dependencies"> Parameter object containing dependencies for this class. </param>
        /// <param name="relationalDependencies"> Parameter object containing relational dependencies for this class. </param>
        /// <param name="queryCompilationContext"> The query compilation context object to use. </param>
        public RelationalShapedQueryCompilingExpressionVisitor(
            [NotNull] ShapedQueryCompilingExpressionVisitorDependencies dependencies,
            [NotNull] RelationalShapedQueryCompilingExpressionVisitorDependencies relationalDependencies,
            [NotNull] QueryCompilationContext queryCompilationContext)
            : base(dependencies, queryCompilationContext)
        {
            Check.NotNull(relationalDependencies, nameof(relationalDependencies));

            RelationalDependencies = relationalDependencies;

            _contextType = queryCompilationContext.ContextType;
            _tags = queryCompilationContext.Tags;
            _detailedErrorsEnabled = relationalDependencies.CoreSingletonOptions.AreDetailedErrorsEnabled;
            _useRelationalNulls = RelationalOptionsExtension.Extract(queryCompilationContext.ContextOptions).UseRelationalNulls;
        }

        /// <summary>
        ///     Parameter object containing relational service dependencies.
        /// </summary>
        protected virtual RelationalShapedQueryCompilingExpressionVisitorDependencies RelationalDependencies { get; }

        /// <inheritdoc />
        protected override Expression VisitShapedQuery(ShapedQueryExpression shapedQueryExpression)
        {
            Check.NotNull(shapedQueryExpression, nameof(shapedQueryExpression));

            var selectExpression = (SelectExpression)shapedQueryExpression.QueryExpression;
            selectExpression.ApplyTags(_tags);

            VerifyNoClientConstant(shapedQueryExpression.ShaperExpression);
            var nonComposedFromSql = selectExpression.IsNonComposedFromSql();
            var splitQuery = ((RelationalQueryCompilationContext)QueryCompilationContext).IsSplitQuery;
            var shaper = new ShaperProcessingExpressionVisitor(this, selectExpression, splitQuery, nonComposedFromSql).ProcessShaper(
                shapedQueryExpression.ShaperExpression, out var relationalCommandCache, out var relatedDataLoaders);

            if (nonComposedFromSql)
            {
                return Expression.New(
                    typeof(FromSqlQueryingEnumerable<>).MakeGenericType(shaper.ReturnType).GetConstructors()[0],
                    Expression.Convert(QueryCompilationContext.QueryContextParameter, typeof(RelationalQueryContext)),
                    Expression.Constant(relationalCommandCache),
                    Expression.Constant(selectExpression.Projection.Select(pe => ((ColumnExpression)pe.Expression).Name).ToList(),
                        typeof(IReadOnlyList<string>)),
                    Expression.Constant(shaper.Compile()),
                    Expression.Constant(_contextType),
                    Expression.Constant(QueryCompilationContext.PerformIdentityResolution));
            }

            if (splitQuery)
            {
                if (QueryCompilationContext.IsAsync)
                {
                    return Expression.New(
                        typeof(SplitQueryingEnumerable<>).MakeGenericType(shaper.ReturnType).GetConstructors()
                            .Single(ci => ci.GetParameters()[3].ParameterType.GetGenericTypeDefinition() == typeof(Func<,,>)),
                        Expression.Convert(QueryCompilationContext.QueryContextParameter, typeof(RelationalQueryContext)),
                        Expression.Constant(relationalCommandCache),
                        Expression.Constant(shaper.Compile()),
                        Expression.Constant(relatedDataLoaders.Compile()),
                        Expression.Constant(_contextType),
                        Expression.Constant(QueryCompilationContext.PerformIdentityResolution));
                }

                return Expression.New(
                    typeof(SplitQueryingEnumerable<>).MakeGenericType(shaper.ReturnType).GetConstructors()
                        .Single(ci => ci.GetParameters()[3].ParameterType.GetGenericTypeDefinition() == typeof(Action<,>)),
                    Expression.Convert(QueryCompilationContext.QueryContextParameter, typeof(RelationalQueryContext)),
                    Expression.Constant(relationalCommandCache),
                    Expression.Constant(shaper.Compile()),
                    Expression.Constant(relatedDataLoaders.Compile()),
                    Expression.Constant(_contextType),
                    Expression.Constant(QueryCompilationContext.PerformIdentityResolution));
            }

            return Expression.New(
                    typeof(QueryingEnumerable<>).MakeGenericType(shaper.ReturnType).GetConstructors()[0],
                    Expression.Convert(QueryCompilationContext.QueryContextParameter, typeof(RelationalQueryContext)),
                    Expression.Constant(relationalCommandCache),
                    Expression.Constant(shaper.Compile()),
                    Expression.Constant(_contextType),
                    Expression.Constant(QueryCompilationContext.PerformIdentityResolution));
        }
    }
}
