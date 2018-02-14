/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.mdx.MdxVisitorImpl;
import mondrian.mdx.MemberExpr;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Level;
import mondrian.olap.Literal;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

/**
 * Computes Order() in SQL.
 *
 * Note that this utilizes the same approach as Native TopCount(), by defining an order by
 * clause based on the specified measure 
 *
 */
public class RolapNativeOrder extends RolapNativeSet {

    public RolapNativeOrder() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeOrder.get());
    }

    static class OrderConstraint extends DelegatingSetConstraint {
        Exp orderByExpr;
        boolean ascending;
        Map<String, String> preEval;
        RolapLevel level;

        public OrderConstraint(
            CrossJoinArg[] args, RolapEvaluator evaluator,
            Exp orderByExpr, boolean ascending, Map<String, String> preEval,
            RolapLevel level,
            SetConstraint parentConstraint)
        {
            super(args, evaluator, true, parentConstraint);
            this.orderByExpr = orderByExpr;
            this.ascending = ascending;
            this.preEval = preEval;
            this.level = level;
        }

        /**
         * {@inheritDoc}
         *
         * <p>We have to join to the fact table if the order
         * expression references a measure.
         */
        protected boolean isJoinRequired() {
            if (getEvaluator().isNonEmpty()) {
                return true;
            }

            List<RolapCube> baseCubes = getEvaluator().getBaseCubes();
            RolapCube baseCube = baseCubes != null && baseCubes.size() == 1
                ? baseCubes.get(0)
                : (RolapCube) getEvaluator().getMeasureCube();
            if (!SqlConstraintUtils.getRoleConstraintMembers(getEvaluator(), baseCube).isEmpty()) {
                // there are some role restrictions defined
                return true;
            }

            // Use a visitor and check all member expressions.
            // If any of them is a measure, we will have to
            // force the join to the fact table. If it is something
            // else then we don't really care. It will show up in
            // the evaluator as a non-all member and trigger the
            // join when we call RolapNativeSet.isJoinRequired().
            final AtomicBoolean mustJoin = new AtomicBoolean(true);
            if (orderByExpr != null) {
                mustJoin.set(false);
                orderByExpr.accept(
                    new MdxVisitorImpl() {
                        public Object visit(MemberExpr memberExpr) {
                            if (mustJoin.get()) {
                                turnOffVisitChildren();
                                return null;
                            }
                            if (memberExpr.getMember().isMeasure()) {
                                if (memberExpr.getMember() instanceof RolapStoredMeasure) {
                                    mustJoin.set(true);
                                    turnOffVisitChildren();
                                    return null;
                                } else if (memberExpr.getMember().isCalculated()) {
                                    memberExpr.getMember().getExpression().accept(this);
                                }
                            }
                            return super.visit(memberExpr);
                        }
                    });
            }
            return mustJoin.get()
                || (parentConstraint != null && parentConstraint.isJoinRequired());
        }

        public void addConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar)
        {
            if (orderByExpr != null) {
                RolapNativeSql sql =
                    new RolapNativeSql(
                        sqlQuery, aggStar, getEvaluator(), level, preEval);
                final String orderBySql =
                    sql.generateTopCountOrderBy(orderByExpr);
                if (!"".equals(orderBySql)) {
                    boolean nullable =
                        deduceNullability(orderByExpr);
                    final String orderByAlias =
                        sqlQuery.addSelect(orderBySql, null);
                    sqlQuery.addOrderBy(
                        orderBySql,
                        orderByAlias,
                        ascending,
                        true,
                        nullable,
                        true);
                }
            }
            super.addConstraint(sqlQuery, baseCube, aggStar);
        }

        private boolean deduceNullability(Exp expr) {
            if (!(expr instanceof MemberExpr)) {
                return true;
            }
            final MemberExpr memberExpr = (MemberExpr) expr;
            if (!(memberExpr.getMember() instanceof RolapStoredMeasure)) {
                return true;
            }
            final RolapStoredMeasure measure =
                (RolapStoredMeasure) memberExpr.getMember();
            return measure.getAggregator() != RolapAggregator.DistinctCount;
        }

        /**
         * slots 0 - 6: parent slots
         * slot 7: List<Member> slicer members or null
         * slot 8: order Exp to String or null
         * slot 9: boolean ascending
         *
         * @return
         */
        public Object getCacheKey() {
            CacheKey key = new CacheKey((CacheKey) super.getCacheKey());
            if (this.getEvaluator() instanceof RolapEvaluator) {
                key.setSlicerMembers(((RolapEvaluator) this.getEvaluator()).getSlicerMembers());
            }
            // Note: need to use string in order for caching to work
            if (orderByExpr != null) {
                key.setValue(getClass().getName() + ".orderByExpr", orderByExpr.toString());
            }
            key.setValue(getClass().getName() + ".ascending", ascending);
            return key;
        }
    }

    protected boolean restrictMemberTypes() {
        return true;
    }

    NativeEvaluator createEvaluator(
        RolapEvaluator evaluator,
        FunDef fun,
        Exp[] args)
    {
        boolean ascending = true;

        if (!isEnabled()) {
            return null;
        }
        if (!OrderConstraint.isValidContext(
                evaluator, false, new Level[]{}, restrictMemberTypes()))
        {
            return null;
        }

        // is this "Order(<set>, [<numeric expr>, <string expr>], { ASC | DESC | BASC | BDESC })"
        String funName = fun.getName();
        if (!"Order".equalsIgnoreCase(funName)) {
            return null;
        }
        if (args.length < 2 || args.length > 3) {
            return null;
        }

        // Extract Order
        boolean isHierarchical = true;
        if (args.length == 3) {
            if (!(args[2] instanceof Literal)) {
                return null;
            }

            String val = ((Literal) args[2]).getValue().toString();
            if (val.equals("ASC") || val.equals("BASC")) {
                ascending = true;
            } else if (val.equals("DESC") || val.equals("BDESC")) {
                ascending = false;
            } else {
                return null;
            }
            isHierarchical = !val.startsWith("B");
        }

        SetEvaluator eval = getNestedEvaluator(args[0], evaluator);
        CrossJoinArg[] cjArgs;
        List<CrossJoinArg[]> allArgs = null;
        SetConstraint parentConstraint = null;
        RolapLevel firstCrossjoinLevel = null;
        if (eval == null) {
            // extract the set expression
            allArgs = crossJoinArgFactory().checkCrossJoinArg(evaluator, args[0]);

            // checkCrossJoinArg returns a list of CrossJoinArg arrays.  The first
            // array is the CrossJoin dimensions.  The second array, if any,
            // contains additional constraints on the dimensions. If either the list
            // or the first array is null, then native cross join is not feasible.
            if (failedCjArg(allArgs)) {
                return null;
            }

            cjArgs = allArgs.get(0);
            if (isPreferInterpreter(cjArgs, false)) {
                return null;
            }

            if (isHierarchical && !isParentLevelAll(cjArgs)) {
                // cannot natively evaluate, parent-child hierarchies in play
                return null;
            }

            firstCrossjoinLevel = cjArgs[0].getLevel();

            if (!evaluator.isNonEmpty()) {
                if (cjArgs.length > 1) {
                    // cannot support this without joining
                    return null;
                }
                if (!firstCrossjoinLevel.isUnique()) {
                    // MemberExcludeConstraint would produce bad query
                    return null;
                }
            }

        } else {
            if (isHierarchical && !isParentLevelAll(eval.getArgs())) {
                // cannot natively evaluate, parent-child hierarchies in play
                return null;
            }

            parentConstraint = (SetConstraint) eval.getConstraint();
            if (!(parentConstraint instanceof RolapNativeFilter.FilterConstraint)
                && !(parentConstraint instanceof RolapNativeNonEmptyFunction.NonEmptyFunctionConstraint))
            {
                return null;
            }

            cjArgs = CrossJoinArg.EMPTY_ARRAY;
            firstCrossjoinLevel = parentConstraint.getArgs()[0].getLevel();
        }

        // extract "order by" expression
        SchemaReader schemaReader = evaluator.getSchemaReader();
        DataSource ds = schemaReader.getDataSource();

        // generate the ORDER BY Clause
        // Need to generate top count order by to determine whether
        // or not it can be created. The top count
        // could change to use an aggregate table later in evaulation
        SqlQuery sqlQuery = SqlQuery.newQuery(ds, "NativeOrder");
        RolapNativeSql sql =
            new RolapNativeSql(
                sqlQuery, null, evaluator, firstCrossjoinLevel, new HashMap<String, String>());
        Exp orderByExpr = null;
        if (args.length >= 2) {
            orderByExpr = args[1];
            String orderBySQL = sql.generateTopCountOrderBy(orderByExpr);
            if (orderBySQL == null) {
                return null;
            }
        }

        if (sql.addlContext.size() > 0) {
            // cannot natively evaluate
            return null;
        }

        if (eval == null) {
            LOGGER.debug("using native order");
            final int savepoint = evaluator.savepoint();
            try {
                overrideContext(evaluator, cjArgs, sql.getStoredMeasure());
                for (Member member : sql.addlContext) {
                    evaluator.setContext(member);
                }
                CrossJoinArg[] predicateArgs = null;
                if (allArgs.size() == 2) {
                    predicateArgs = allArgs.get(1);
                }

                CrossJoinArg[] combinedArgs;
                if (predicateArgs != null) {
                    // Combined the CJ and the additional predicate args
                    // to form the TupleConstraint.
                    combinedArgs =
                        Util.appendArrays(cjArgs, predicateArgs);
                } else {
                    combinedArgs = cjArgs;
                }
                TupleConstraint constraint =
                    new OrderConstraint(
                        combinedArgs, evaluator, orderByExpr, ascending, sql.preEvalExprs, firstCrossjoinLevel, parentConstraint);
                SetEvaluator sev =
                    new SetEvaluator(cjArgs, schemaReader, constraint, sql.getStoredMeasure());
                sev.setCompleteWithNullValues(!evaluator.isNonEmpty());
                sev.setCompleteWithNullValuesPosition(ascending);
                return sev;
            } finally {
                evaluator.restore(savepoint);
            }
        } else {
            TupleConstraint constraint =
                new OrderConstraint(
                    cjArgs, evaluator, orderByExpr, ascending, sql.preEvalExprs, firstCrossjoinLevel, parentConstraint);
            eval.setConstraint(constraint);
            LOGGER.debug("using nested native order");
            return eval;
        }
    }

    private boolean isParentLevelAll(CrossJoinArg[] cjArgs) {
        for (CrossJoinArg cjArg : cjArgs) {
            Level level = cjArg != null ? cjArg.getLevel() : null;
            if (level != null && !level.isAll()
                && level.getParentLevel() != null
                && !level.getParentLevel().isAll()){
                return false;
            }
        }
        return true;
    }
}

// End RolapNativeOrder.java
