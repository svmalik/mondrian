/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2015 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.mdx.MemberExpr;
import mondrian.olap.*;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

/**
 * Computes a TopCount in SQL.
 *
 * @author av
 * @since Nov 21, 2005
  */
public class RolapNativeTopCount extends RolapNativeSet {

    public RolapNativeTopCount() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeTopCount.get());
    }

    static class TopCountConstraint extends DelegatingSetConstraint {
        Exp orderByExpr;
        boolean ascending;
        Integer topCount;
        Map<String, String> preEval;

        public TopCountConstraint(
            int count,
            CrossJoinArg[] args, RolapEvaluator evaluator,
            Exp orderByExpr, boolean ascending, Map<String, String> preEval,
            SetConstraint parentConstraint)
        {
            super(args, evaluator, true, parentConstraint);
            this.orderByExpr = orderByExpr;
            this.ascending = ascending;
            this.topCount = new Integer(count);
            this.preEval = preEval;
        }

        /**
         * {@inheritDoc}
         *
         * <p>TopCount always needs to join the fact table because we want to
         * evaluate the top count expression which involves a fact.
         */
        protected boolean isJoinRequired() {
            return true;
        }

        public void addConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar)
        {
            if (orderByExpr != null) {
                RolapNativeSql sql =
                    new RolapNativeSql(
                        sqlQuery, aggStar, getEvaluator(), null, preEval);
                final String orderBySql =
                    sql.generateTopCountOrderBy(orderByExpr);
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
         * This returns a CacheKey object
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
            key.setValue(getClass().getName() + ".topCount", topCount);
            key.add(this.getEvaluator().isNonEmpty());

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
        boolean ascending;

        if (!isEnabled()) {
            return null;
        }
        if (!TopCountConstraint.isValidContext(
                evaluator, false, new Level[]{}, restrictMemberTypes()))
        {
            return null;
        }

        // is this "TopCount(<set>, <count>, [<numeric expr>])"
        String funName = fun.getName();
        if ("TopCount".equalsIgnoreCase(funName)) {
            ascending = false;
        } else if ("BottomCount".equalsIgnoreCase(funName)) {
            ascending = true;
        } else {
            return null;
        }
        if (args.length < 2 || args.length > 3) {
            return null;
        }

        // extract count
        if (!(args[1] instanceof Literal)) {
            return null;
        }
        int count = ((Literal) args[1]).getIntValue();

        // extract "order by" expression
        SchemaReader schemaReader = evaluator.getSchemaReader();
        DataSource ds = schemaReader.getDataSource();

        // generate the ORDER BY Clause
        // Need to generate top count order by to determine whether
        // or not it can be created. The top count
        // could change to use an aggregate table later in evaulation
        SqlQuery sqlQuery = SqlQuery.newQuery(ds, "NativeTopCount");
        RolapNativeSql sql =
            new RolapNativeSql(
                sqlQuery, null, evaluator, null, new HashMap<String, String>());
        Exp orderByExpr = null;
        if (args.length == 3) {
            orderByExpr = args[2];
            String orderBySQL = sql.generateTopCountOrderBy(args[2]);
            if (orderBySQL == null) {
                return null;
            }
        }

        if (sql.addlContext.size() > 0 && sql.storedMeasureCount > 1) {
            // cannot natively evaluate, multiple tuples are possibly at play here.
            return null;
        }

        // first see if subset wraps another native evaluation (other than count and sum)
        SetEvaluator eval = getNestedEvaluator(args[0], evaluator);

        final int savepoint = evaluator.savepoint();
        try {
            if (eval == null) {
                // extract the set expression
                List<CrossJoinArg[]> allArgs =
                    crossJoinArgFactory().checkCrossJoinArg(evaluator, args[0]);

                // checkCrossJoinArg returns a list of CrossJoinArg arrays.  The first
                // array is the CrossJoin dimensions.  The second array, if any,
                // contains additional constraints on the dimensions. If either the list
                // or the first array is null, then native cross join is not feasible.
                if (allArgs == null || allArgs.isEmpty() || allArgs.get(0) == null) {
                    return null;
                }

                CrossJoinArg[] cjArgs = allArgs.get(0);
                if (isPreferInterpreter(cjArgs, false)) {
                    return null;
                }

                if (SqlConstraintUtils.measuresConflictWithMembers(
                        evaluator.getQuery().getMeasuresMembers(), cjArgs))
                {
                    RolapUtil.alertNonNative("NativeTopCount",
                        "One or more calculated measures conflict with crossjoin args");
                    return null;
                }

                LOGGER.debug("using native topcount");
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
                    new TopCountConstraint(
                        count, combinedArgs, evaluator, orderByExpr, ascending, sql.preEvalExprs, null);
                SetEvaluator sev =
                    new SetEvaluator(cjArgs, schemaReader, constraint, sql.getStoredMeasure());
                sev.setMaxRows(count);
                sev.setCompleteWithNullValues(!evaluator.isNonEmpty());
                return sev;
            } else {
                SetConstraint parentConstraint = (SetConstraint)eval.getConstraint();
                evaluator = (RolapEvaluator)parentConstraint.getEvaluator();
                // empty crossjoin args, parent contains args
                CrossJoinArg[] cjArgs = new CrossJoinArg[0];
                overrideContext(evaluator, cjArgs, sql.getStoredMeasure());
                for (Member member : sql.addlContext) {
                    evaluator.setContext(member);
                }
                TupleConstraint constraint =
                    new TopCountConstraint(
                        count, cjArgs, evaluator, orderByExpr, ascending, sql.preEvalExprs,
                        parentConstraint);
                eval.setConstraint(constraint);
                eval.setMaxRows(count);
                sev.setCompleteWithNullValues(!evaluator.isNonEmpty());
                LOGGER.debug("using nested native topcount");
                return eval;
            }
        } finally {
            evaluator.restore(savepoint);
        }
    }
}

// End RolapNativeTopCount.java
