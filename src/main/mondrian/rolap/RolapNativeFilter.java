/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.mdx.MdxVisitorImpl;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

/**
 * Computes a Filter(set, condition) in SQL.
 *
 * @author av
 * @since Nov 21, 2005
 */
public class RolapNativeFilter extends RolapNativeSet {

    public RolapNativeFilter() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeFilter.get());
    }

    static class FilterConstraint extends DelegatingSetConstraint {
        Exp filterExpr;
        boolean existing = false;
        Map<String, String> preEvaluatedExpressions;
        RolapLevel level;

        public FilterConstraint(
            CrossJoinArg[] args,
            RolapEvaluator evaluator,
            Exp filterExpr,
            boolean existing,
            Map<String, String> preEvaluatedExpressions,
            RolapLevel level,
            SetConstraint parentConstraint)
        {
            super(args, evaluator, true, parentConstraint);
            this.filterExpr = filterExpr;
            this.existing = existing;
            this.preEvaluatedExpressions = preEvaluatedExpressions;
            this.level = level;
        }

        /**
         * {@inheritDoc}
         *
         * <p>Overriding isJoinRequired() for native filters because
         * we have to force a join to the fact table if the filter
         * expression references a measure.
         */
        protected boolean isJoinRequired() {
            // Use a visitor and check all member expressions.
            // If any of them is a measure, we will have to
            // force the join to the fact table. If it is something
            // else then we don't really care. It will show up in
            // the evaluator as a non-all member and trigger the
            // join when we call RolapNativeSet.isJoinRequired().
            final AtomicBoolean mustJoin = new AtomicBoolean(false);
            filterExpr.accept(
                new MdxVisitorImpl() {
                    public Object visit(MemberExpr memberExpr) {
                        if (memberExpr.getMember().isMeasure()) {
                            mustJoin.set(true);
                            return null;
                        }
                        return super.visit(memberExpr);
                    }
                });
            return mustJoin.get()
                || (getEvaluator().isNonEmpty() && super.isJoinRequired() || existing);
        }

        public void addConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar)
        {
            // Use aggregate table to generate filter condition
            RolapNativeSql sql =
                new RolapNativeSql(
                    sqlQuery, aggStar, getEvaluator(), level, preEvaluatedExpressions);
            String filterSql = sql.generateFilterCondition(filterExpr);

            // <NOOP> is used because there is a previous check to make sure filter conditions aren't null,
            // and there is a usecase where the filter is simply "COUNT(Member.CurrentMember, NONEMPTY)"
            // AKA a Non Empty Check, which just needs to join to the fact table.
            if (filterSql != null && !filterSql.equals("<NOOP>") && !filterSql.equals("(<NOOP>) ")) {
                sqlQuery.addHaving(filterSql);
            }

            if (getEvaluator().isNonEmpty() || isJoinRequired()) {
                // only apply context constraint if non empty, or
                // if a join is required to fulfill the filter condition
                super.addConstraint(sqlQuery, baseCube, aggStar);
            }
        }

        /**
         * This returns a CacheKey object
         * @return
         */
        public Object getCacheKey() {
            CacheKey key = new CacheKey((CacheKey) super.getCacheKey());
            if (this.getEvaluator() instanceof RolapEvaluator) {
                key.setSlicerMembers(((RolapEvaluator) this.getEvaluator()).getSlicerMembers());
            }
            // Note required to use string in order for caching to work
            if (filterExpr != null) {
                key.setValue(getClass().getName() + ".filterExpr", filterExpr.toString());
            }
            key.setValue(getClass().getName() + ".nonEmpty", getEvaluator().isNonEmpty());

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
        if (!isEnabled()) {
            return null;
        }
        if (!FilterConstraint.isValidContext(
                evaluator, false, new Level[]{}, restrictMemberTypes(), false))
        {
            return null;
        }
        // is this "Filter(<set>, <numeric expr>)"
        String funName = fun.getName();
        if (!"Filter".equalsIgnoreCase(funName)) {
            return null;
        }

        if (args.length != 2) {
            return null;
        }

        // Determine if this Filter wraps an Existing function, if so apply
        // the whole context to the filter.
        boolean existing = false;
//        if (args[0] instanceof ResolvedFunCall) {
//            if (((ResolvedFunCall)args[0]).getFunName().equalsIgnoreCase("existing")) {
//                existing = true;
//                arg0 = ((ResolvedFunCall)args[0]).getArg(0);
//            }
//        }

        // first see if filter wraps another native evaluation
        SetEvaluator eval = getNestedEvaluator(args[0], evaluator);

        List<CrossJoinArg[]> allArgs = null;
        CrossJoinArg[] cjArgs = null;
        RolapLevel firstCrossjoinLevel = null;
        if (eval == null) {
            // extract the set expression
            allArgs = crossJoinArgFactory().checkCrossJoinArg(evaluator, args[0]);
            // checkCrossJoinArg returns a list of CrossJoinArg arrays.  The first
            // array is the CrossJoin dimensions.  The second array, if any,
            // contains additional constraints on the dimensions. If either the
            // list or the first array is null, then native cross join is not
            // feasible.
            if (allArgs == null || allArgs.isEmpty() || allArgs.get(0) == null) {
                return null;
            }

            cjArgs = allArgs.get(0);
            if (isPreferInterpreter(cjArgs, false)) {
                return null;
            }

            if (SqlConstraintUtils.measuresConflictWithMembers(
                    cjArgs, evaluator))
            {
                RolapUtil.alertNonNative("NativeFilter",
                    "One or more calculated measures conflict with crossjoin args");
                return null;
            }

            firstCrossjoinLevel = cjArgs[0].getLevel();
        } else {
            firstCrossjoinLevel = ((SetConstraint)eval.getConstraint()).getArgs()[0].getLevel();
        }

        // extract filter expression
        SchemaReader schemaReader = evaluator.getSchemaReader();
        DataSource ds = schemaReader.getDataSource();

        // generate the WHERE condition
        // Need to generate where condition here to determine whether
        // or not the filter condition can be created. The filter
        // condition could change to use an aggregate table later in evaluation
        SqlQuery sqlQuery = SqlQuery.newQuery(ds, "NativeFilter");
        RolapNativeSql sql =
            new RolapNativeSql(
                sqlQuery, null, evaluator, firstCrossjoinLevel, new HashMap<String, String>());
        final Exp filterExpr = args[1];
        String filterExprStr = sql.generateFilterCondition(filterExpr);
        if (filterExprStr == null) {
            return null;
        }
        if (sql.addlContext.size() > 0 && sql.storedMeasureCount > 1) {
            // cannot natively evaluate, multiple tuples are possibly at play here.
            return null;
        }

        // Check to see if evaluator contains a calculated member that can't be
        // expanded.  This is necessary due to the SqlConstraintsUtils.
        // addContextConstraint()
        // method which gets called when generating the native SQL.
        if (SqlConstraintUtils.containsCalculatedMember(
                evaluator.getNonAllMembers(), true))
        {
            return null;
        }

        final int savepoint = evaluator.savepoint();
        try {
            if (eval == null) {
                if (!existing) {
                    overrideContext(evaluator, cjArgs, sql.getStoredMeasure());
                } else {
                    // exclude the crossjoin args from overriding the context
                    overrideContext(evaluator, new CrossJoinArg[]{}, sql.getStoredMeasure());
                }

                // TODO: Test potential issues, like reference members in crossjoin
                for (Member m : sql.addlContext) {
                    evaluator.setContext(m);
                }
                // no need to have any context if there is no measure, we are doing
                // a filter only on the current dimension.  This prevents
                // SqlContextConstraint from expanding unnecessary calculated members on the 
                // slicer calling expandSupportedCalculatedMembers
                if (!existing && !evaluator.isNonEmpty() && sql.getStoredMeasure() == null) {
                    // No need to have anything on the context
                    for (Member m : evaluator.getMembers()) {
                        evaluator.setContext(m.getLevel().getHierarchy().getDefaultMember());
                    }
                }

                // Now construct the TupleConstraint that contains both the CJ
                // dimensions and the additional filter on them.
                CrossJoinArg[] combinedArgs = cjArgs;
                if (allArgs.size() == 2) {
                    CrossJoinArg[] predicateArgs = allArgs.get(1);
                    if (predicateArgs != null) {
                        // Combined the CJ and the additional predicate args.
                        combinedArgs =
                            Util.appendArrays(cjArgs, predicateArgs);
                    }
                }

                SetConstraint constraint =
                    new FilterConstraint(combinedArgs, evaluator, filterExpr, existing, sql.preEvalExprs, firstCrossjoinLevel, null);
                // constraint may still fail
                if (!isValidFilterConstraint(constraint, evaluator, combinedArgs)) {
                    return null;
                }
                LOGGER.debug("using native filter");
                return new SetEvaluator(cjArgs, schemaReader, constraint, sql.getStoredMeasure());
            } else {
                // dummy crossjoin args
                CrossJoinArg[] crossjoinargs = new CrossJoinArg[0];
                SetConstraint parentConstraint = (SetConstraint)eval.getConstraint();
                evaluator = (RolapEvaluator)parentConstraint.getEvaluator();
                overrideContext(evaluator, crossjoinargs, sql.getStoredMeasure());
                for (Member m : sql.addlContext) {
                    evaluator.setContext(m);
                }
                SetConstraint constraint =
                    new FilterConstraint(crossjoinargs, evaluator, filterExpr, existing, sql.preEvalExprs, firstCrossjoinLevel, parentConstraint);
                LOGGER.debug("using native filter");
                eval.setConstraint(constraint);
                return eval;
            }
        } finally {
            evaluator.restore(savepoint);
        }
    }

    /**
     * Check if the need to avoid joining with the fact table is compatible
     * with member constraints to get the right results.<br/>
     * A proper solution would involve changing how joins can be handled in
     * {@link SqlConstraintUtils}, until then we better bail out.
     *
     * TODO: If there are only args related to the current filter (for
     * instance parent member ref for children) this still causes issues
     * because SqlConstraintUtils forces a join when adding the member
     * constraint.  We'll need to add those constraints differently vs.
     * through the fact table.
     */
    private static boolean isValidFilterConstraint(
      SetConstraint filterConstraint,
      RolapEvaluator evaluator,
      CrossJoinArg[] combinedArgs)
    {
        // in this situation any CrossJoinArg constraints will be skipped
        // to prevent fact table join and non-empty results (MONDRIAN-1133,1694)
        // this may cause wrong results for args with member constraints
        if (!evaluator.isNonEmpty() && !filterConstraint.isJoinRequired()) {
            // known constraining args have members, but this might change
            for (CrossJoinArg cjArg : combinedArgs) {
                if (cjArg.getMembers() != null) {
                    return false;
                }
            }
        }
        return true;
    }

}

// End RolapNativeFilter.java
