/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2014 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import mondrian.mdx.MemberExpr;
import mondrian.mdx.NamedSetExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.Util;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.DescendantsCrossJoinArg;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.MemberListCrossJoinArg;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.spi.Dialect;

/**
 * Computes a Sum(set) in SQL.  Should only go native if certain conditions are met,
 * including a prediction that the tuples to sum will be of a high cardinality. 
 *
 * @author Will Gorman (wgorman@pentaho.com)
 */
public class RolapNativeSum extends RolapNativeSet {

    public RolapNativeSum() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeSum.get());
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
        if (!SqlContextConstraint.isValidContext(
                evaluator, false, new Level[]{}, restrictMemberTypes()))
        {
            return null;
        }
        // is this "Sum(<set>, <numeric expr>)"
        String funName = fun.getName();
        if (!"Sum".equalsIgnoreCase(funName)) {
            return null;
        }

        if (!(args[0] instanceof ResolvedFunCall)) {
            return null;
        }

        // Check to see if evaluator contains a calculated member that can't be
        // expanded.  This is necessary due to the SqlConstraintsUtils.
        // addContextConstraint()
        // method which gets called when generating the native SQL.
        if (SqlConstraintUtils.containsCalculatedMember(
                Arrays.asList(evaluator.getNonAllMembers()), true))
        {
            return null;
        }

        ResolvedFunCall call = (ResolvedFunCall)args[0];

        if (call.getFunDef().getName().equals("Cache")) {
            if (call.getArg( 0 ) instanceof ResolvedFunCall) {
                call = (ResolvedFunCall)call.getArg(0);
            } else if (call.getArg(0) instanceof NamedSetExpr) {
                Exp exp = ((NamedSetExpr)call.getArg(0)).getNamedSet().getExp();
                if (exp instanceof ResolvedFunCall) {
                    call = (ResolvedFunCall)exp;
                } else {
                    return null;
                }
            }
            else {
                return null;
            }
        }

        // TODO: Note that this native evaluator won't work against tuples that are mapped to
        // security, because the security is usually applied after native evaluation.
        // Before this can be considered valid we need to test in that usecase and 
        // either push down the security into SQL or not natively evaluate.

        // TODO: Check to see if addRoleAccessConstraints() natively evaluates roles, if that is
        // always applicable or not.  See why there was a cleansing step at the end of native tuple
        // evaluation.

        // Determine the measure in play for the Sum() function
        Member measure = null;
        if (args.length == 1) {
            // get current measure
            measure = evaluator.getContext(evaluator.getCube().getMeasuresHierarchy()); 
        } else {
            // call findMeasure
            Set<Member> measures = new TreeSet<Member>();
            findMeasure(args[1], measures);
            if (measures.size() == 1) {
                measure = measures.iterator().next();
            } else {
                // No Measure Found
                // TODO: Support [Measures].CurrentMember
                return null;
            }
        }

        if (measure.isCalculated()) {
            alertNonNative(evaluator, fun, measure.getExpression());
            return null;
        }

        // First check to see if the function Sum is wrapping is natively evaluated.  If so, 
        // just minor changes are needed to be made to the evaluator.
        SetEvaluator eval = (SetEvaluator)evaluator.getSchemaReader().getSchema().getNativeRegistry().createEvaluator(
            evaluator, call.getFunDef(), call.getArgs());
        if (eval != null) {
            CrossJoinArg[] evalArgs =
                ((SetConstraint)eval.getConstraint()).args;
            for (CrossJoinArg evalArg : evalArgs) {
                // The level must be related to the base cube.  If not,
                // the SQL generated is bogus.
                if (!areFromSameCube(evalArg.getLevel(), (RolapStoredMeasure) measure)) {
                    LOGGER.debug("Sum() Cannot go native due to level not existing in both cubes.");
                    return null;
                }
            }

            // If TopCount or Filter measure does not share the same cube as the sum measure,
            // we may not be able to support the virtual cube scenario.
            if (eval.getMeasure() != null && !areFromSameCube(eval.getMeasure(), measure)) {
                if (evalArgs.length != 1) {
                    LOGGER.debug("Sum() Cannot go native due to measures from "
                        + "multiple base cubes and multiple members in tuple");
                    return null;
                }

                // At the moment, virtual cube sum joins only work with levels
                // that are unique due to the additional complexity
                // of bridging two fact tables.
                if (!evalArgs[0].getLevel().isUnique()) {
                    LOGGER.debug("Sum() Cannot go native due to level in play "
                        + "not having uniqueMembers = true.");
                    return null;
                }

                for (CrossJoinArg evalArg : evalArgs) {
                    // also, the level must be related to both base cubes.  If not,
                    // the SQL generated is bogus.
                    if (!areFromSameCube(evalArg.getLevel(), (RolapStoredMeasure) eval.getMeasure())) {
                        LOGGER.debug("Sum() Cannot go native due to level not existing in both cubes.");
                        return null;
                    }
                }
            }
            // create a delegating sum constraint
            int cardinality = estimateCardinality(eval.getArgs());
            if (cardinality <= 1) { // change to a reasonable cardinality limit.
              return null;
            }
            SumConstraint sumConstraint =
                new SumConstraint(
                    eval.getConstraint(),
                    new CrossJoinArg[0],
                    evaluator,
                    true,
                    measure,
                    eval.getMeasure());
            eval.setConstraint(sumConstraint);
        } else {
            if (((RolapStoredMeasure)measure).getCube() != evaluator.getCube()) {
                List<RolapCube> cubes = evaluator.getBaseCubes();
                if (cubes == null || !cubes.contains(((RolapStoredMeasure)measure).getCube())) {
                    // TEST THIS!
                    alertNonNative(evaluator, fun, new MemberExpr(measure));
                    return null;
                }
            }

            List<CrossJoinArg[]> allArgs =
                crossJoinArgFactory().checkCrossJoinArg(evaluator, call, false, false);
            if (allArgs == null || allArgs.isEmpty() || allArgs.get(0) == null) {
                return null;
            }

            // Check if arg levels are unrelated.
            for (CrossJoinArg evalArg : allArgs.get(0)) {
                if (!areFromSameCube(evalArg.getLevel(), (RolapStoredMeasure)measure)) {
                    LOGGER.debug("Sum() Cannot go native due to level not existing in both cubes.");
                    return null;
                }
            }
            int cardinality = estimateCardinality(allArgs.get(0));
            // TODO: change to a reasonable default, configurable cardinality limit.
            if (cardinality < 100) {
              return null;
            }
            final int savepoint = evaluator.savepoint();
            try {
                overrideContext(evaluator, allArgs.get(0), null);
                SumConstraint sumConstraint = new SumConstraint(
                    null,
                    Util.appendArrays(allArgs.get(0), allArgs.size() == 2 ? allArgs.get(1) : new CrossJoinArg[0]),
                    evaluator,
                    true,
                    measure,
                    null);
                eval = new SetEvaluator(allArgs.get(0), evaluator.getSchemaReader(), sumConstraint);
            } finally {
                evaluator.restore(savepoint);
            }
        }

        if (eval != null) {
            LOGGER.debug("using native sum");  
        }
        return eval;
    }

    private static boolean areFromSameCube(Member m1, Member m2) {
        if (!(m1 instanceof RolapStoredMeasure) 
            || !(m2 instanceof RolapStoredMeasure)) 
        {
          return false;
        }
        return getStar((RolapStoredMeasure)m1) 
                == getStar((RolapStoredMeasure)m2);
    }

    private static RolapStar getStar(RolapStoredMeasure m) {
      return ((RolapStar.Measure) m.getStarMeasure()).getStar();
    }

    /**
     * This method determines an estimate cardinality of the related expressions.
     */
    private int estimateCardinality(CrossJoinArg[] allArgs) {
        int cardinality = 0;
        for (CrossJoinArg arg : allArgs) {
            // TODO: implement a "CrossJoinArg.estimateCardinality()" method instead
            if (arg instanceof MemberListCrossJoinArg) {
                cardinality += arg.getMembers().size();
            } else {
                // need to handle all use cases, for now only skip the
                // obvious single member native sums()
                // TODO: extract the cardinality appropriately from the arg
                cardinality += 10000;
            }
        }
        return cardinality;
    }

    /**
     * Extracts the stored measures referenced in an expression
     *
     * @param exp expression
     * @param foundMeasure found measures
     */
    private static void findMeasure(
        Exp exp,
        Set<Member> foundMeasure)
    {
        if (exp instanceof MemberExpr) {
            MemberExpr memberExpr = (MemberExpr) exp;
            Member member = memberExpr.getMember();
            if (member instanceof RolapStoredMeasure) {
                if (!foundMeasure.contains( member )) {
                    foundMeasure.add(member);
                }
            } else if (member instanceof RolapCalculatedMember) {
                if (!foundMeasure.contains(member)) {
                    // if a measure's expression is very basic,
                    // don't add the calc to the list
                    if (!(member.getExpression() instanceof MemberExpr)) {
                        foundMeasure.add(member);
                    }
                    findMeasure(member.getExpression(),foundMeasure);
                }
            }
        } else if (exp instanceof ResolvedFunCall) {
            ResolvedFunCall funCall = (ResolvedFunCall) exp;
            Exp [] args = funCall.getArgs();
            for (Exp arg : args) {
                findMeasure(arg, foundMeasure);
            }
        }
    }

    static class SumConstraint extends SetConstraint {

        boolean virtualCubeQueryMode = false;
        SqlQuery innerSql = null;
        Member measure;
        Member evalMeasure;
        TupleConstraint delegatingConstraint;

        SumConstraint(TupleConstraint delegatingConstraint,
            CrossJoinArg[] args, RolapEvaluator evaluator, boolean restrict, Member measure, Member evalMeasure)
        {
            super(args, evaluator, restrict);
            this.measure = measure;
            this.evalMeasure = evalMeasure;
            this.delegatingConstraint = delegatingConstraint;
            virtualCubeQueryMode = !((evalMeasure == null) || areFromSameCube(this.measure, evalMeasure));
        }

        /**
         * Virtual cube query mode is a complex scenario where
         * the first native tuple evaluation is based on a different
         * base cube as the sum tuple evaluation.
         */
        public boolean isVirtualCubeQueryMode() {
          return virtualCubeQueryMode;
        }

        public boolean inFirstPhase() {
          return virtualCubeQueryMode == true && innerSql == null;
        }

        public void setInnerQuery(SqlQuery innerSql) {
            if(this.innerSql != null) {
                return;
            }
            this.innerSql = innerSql;
            // change the base cube to the summed measure
            List<RolapCube> baseCubes = new ArrayList<RolapCube>();
            baseCubes.add(((RolapStoredMeasure)measure).getCube());
            getEvaluator().setBaseCubes(baseCubes);
        }

        /**
         * Pass the handler to the delegating constraint if it supports it.
         * @param consolidationHandler
         */
        public void setConsolidationHandler(ConsolidationHandler consolidationHandler) {
            super.setConsolidationHandler(consolidationHandler);
            if(delegatingConstraint != null && delegatingConstraint instanceof SetConstraint) {
                ((SetConstraint)delegatingConstraint).setConsolidationHandler(consolidationHandler);
            }
        }


        @Override
        public void addConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar)
        {
            if (!virtualCubeQueryMode) {
                // this is the normal path
                if (delegatingConstraint != null) {
                    delegatingConstraint.addConstraint(sqlQuery, baseCube, aggStar);
                }
                super.addConstraint(sqlQuery, baseCube, aggStar);
                // Add the measure to the query as "m1", make sure the fact table is also present.
                addMeasureFact(measure, aggStar, sqlQuery);
                sqlQuery.addSelect( getMeasureExpression(measure, aggStar, sqlQuery), null, "m1");
            } else {
                // this is the virtual cube multiple base cube scenario
                if (inFirstPhase()) {
                    // the first phase generates an inner query based on the
                    // parent native evaluation context
                    delegatingConstraint.addConstraint(sqlQuery, baseCube, aggStar);
                } else {
                    // the second phase generates an outer query based on the
                    // current native evaluation context
                    super.getEvaluator().setContext(measure);
                    super.addConstraint(sqlQuery, baseCube, aggStar);
                    addMeasureFact(measure, aggStar, sqlQuery);
                    sqlQuery.addSelect(getMeasureExpression(measure, aggStar, sqlQuery), null, "m1");
                    RolapStar.Column column = null;
                    RolapLevel level = ((SetConstraint)delegatingConstraint).args[0].getLevel();
                    column = ((RolapCubeLevel)level).getBaseStarKeyColumn(baseCube);
                    String columnExpr = null;
                    // if the column is null, then there is no join to the inner
                    // query.  This could happen if the level is unrelated to
                    // the measure in play. at the moment this scenario is not supported,
                    // there is a check during native eval creation to avoid the
                    // scenario.
                    if (column != null) {
                        if (aggStar != null) {
                            int bitPos = column.getBitPosition();
                            AggStar.Table.Column aggColumn =
                                aggStar.lookupColumn(bitPos);
                            if (aggColumn == null) {
                                throw Util.newInternal(
                                    "AggStar " + aggStar + " has no column for "
                                    + column + " (bitPos " + bitPos + ")");
                            }
                            // TODO: Optimize AggTable Column
                            AggStar.Table table = aggColumn.getTable();
                            table.addToFrom(sqlQuery, false, true);
                            columnExpr = aggColumn.generateExprString(sqlQuery);
                        } else {
                            RolapStar.Column optimized = column.optimize();
                            RolapStar.Table targetTable = optimized.getTable();
                            // add a join path to the level
                            ((SetConstraint)delegatingConstraint).args[0].getLevel().getHierarchy().addToFrom(sqlQuery, targetTable);
                            columnExpr = optimized.generateExprString(sqlQuery);
                        }
                    }
                    // join the inner query to the outer query here,
                    // will depend on the current args foreign key
                    // this assumes we are working with a flat dimension
                    // a check during the evaluation creation verifies this
                    ConsolidationHandler handler = getConsolidationHandler();
                    if (handler != null) {
                        ConsolidationMembers members = handler.getLevelMembers();
                        if (members != null && members.getColumnExpression() != null && members.getMembers().size() > 0) {
                            this.innerSql.addSelect(members.getColumnExpression(), null, "c1");
                            this.innerSql.addGroupBy(members.getColumnExpression());
                        }
                    }
                    if (sqlQuery.getDialect().supportsWithClause()) {
                        String str = sqlQuery.addWith(innerSql.toString());
                        sqlQuery.addWhere(columnExpr + " IN ( select " + sqlQuery.getDialect()
                                                                                 .quoteIdentifier(
                                                                                     "c0") + " from " + str + " )");
                    } else {
                        sqlQuery.addFromQuery(innerSql.toString(),  "tbl001", true);
                        sqlQuery.addWhere(columnExpr + " = " + sqlQuery.getDialect().quoteIdentifier("tbl001")
                            + "." + sqlQuery.getDialect().quoteIdentifier("c0"));
                    }
                    if (handler != null) {
                        ConsolidationMembers members = handler.getLevelMembers();
                        if (members != null && members.getColumnExpression() != null && members.getMembers().size() > 0) {
                            sqlQuery.addSelect(members.getColumnExpression(), null, "m2");
                            sqlQuery.addGroupBy(members.getColumnExpression());
                            if (!sqlQuery.getDialect().supportsWithClause()) {
                                sqlQuery.addWhere(members.getColumnExpression()
                                        + " = "
                                        + sqlQuery.getDialect()
                                               .quoteIdentifier("tbl001")
                                        + "."
                                        + sqlQuery.getDialect().quoteIdentifier("c1"));
                            } else {
                                sqlQuery.addWhere(members.getColumnExpression()
                                    + " IN ( select "
                                    + sqlQuery.getDialect().quoteIdentifier("c1")
                                    + " from "
                                    + sqlQuery.getDialect().quoteIdentifier("wq1")
                                    + " )");
                            }
                        }
                    }
                }
            }
        }

        private void addMeasureFact(Member member, AggStar aggStar, SqlQuery sqlQuery) {
            RolapStoredMeasure measure = (RolapStoredMeasure) member;
            // this is only necessary if the sum itself is empty of tuples
            if (aggStar != null) {
                aggStar.getFactTable().addToFrom( sqlQuery, false, false);
            } else {
                measure.getCube().getStar().getFactTable().addToFrom( sqlQuery,  false,  false);
            }
        }

        private String getMeasureExpression(Member member, AggStar aggStar, SqlQuery sqlQuery) {
            RolapStoredMeasure measure = (RolapStoredMeasure) member;
            String expr;
            // Use aggregate table to create condition if available
            if (aggStar != null
                && measure.getStarMeasure() instanceof RolapStar.Column)
            {
                RolapStar.Column column =
                    (RolapStar.Column) measure.getStarMeasure();
                int bitPos = column.getBitPosition();
                AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
                AggStar.FactTable.Measure columnMeasure = (AggStar.FactTable.Measure) aggColumn;
                expr = columnMeasure.generateRollupString(sqlQuery);
            } else {
                String exprInner =
                    measure.getMondrianDefExpression().getExpression(sqlQuery);
                expr = measure.getAggregator().getExpression(exprInner);
            }

            if (sqlQuery.getDialect().getDatabaseProduct().getFamily()
                == Dialect.DatabaseProduct.DB2)
            {
                expr = "FLOAT(" + expr + ")";
            }
            return expr;
        }

        @Override
        public void constrainExtraLevels(
            RolapCube baseCube,
            BitKey levelBitKey)
        {
            super.constrainExtraLevels(baseCube, levelBitKey);
            for (CrossJoinArg arg : args) {
                if (arg instanceof DescendantsCrossJoinArg
                    || arg instanceof MemberListCrossJoinArg)
                {
                    final RolapLevel level = arg.getLevel();
                    if (level != null && !level.isAll()) {
                        RolapStar.Column column =
                            ((RolapCubeLevel)level)
                                .getBaseStarKeyColumn(baseCube);
                        levelBitKey.set(column.getBitPosition());
                    }
                }
            }
        }

        protected boolean isJoinRequired() {
            // Native Sum always join to the fact table.
            return true;
        }

        /**
         * This returns a CacheKey object
         * @return
         */
        @Override
        public Object getCacheKey() {
            CacheKey key;
            if (delegatingConstraint != null && delegatingConstraint instanceof SetConstraint) {
                key = new CacheKey((CacheKey) (delegatingConstraint).getCacheKey());
            } else {
                key = new CacheKey((CacheKey) super.getCacheKey());
            }
            if (this.getEvaluator() instanceof RolapEvaluator) {
                key.setSlicerMembers(((RolapEvaluator) this.getEvaluator()).getSlicerMembers());
            }
            // is this really needed?
            key.setValue(getClass().getName()+ ".my.class", this.getClass());
            key.setValue(getClass().getName() + ".measure", measure);
            key.setValue(getClass().getName() + ".eval.measure", evalMeasure);
            if (delegatingConstraint != null && !(delegatingConstraint instanceof SetConstraint)) {
                key.setValue(getClass().getName() + ".delegating.cache.key", delegatingConstraint.getCacheKey());
            }
            return key;
        }

        @Override
        public void addLevelConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar,
            RolapLevel level,
            boolean optimize) 
        {
            if (delegatingConstraint != null) { 
                delegatingConstraint.addLevelConstraint(sqlQuery, baseCube, aggStar, level, optimize);
            }
            // we should always join sum to the fact table, even if the delegate does not.
            super.addLevelConstraint(sqlQuery, baseCube, aggStar, level, optimize);
        }

        @Override
        public MemberChildrenConstraint getMemberChildrenConstraint(RolapMember parent) {
            if (delegatingConstraint != null) { 
                return delegatingConstraint.getMemberChildrenConstraint(parent);
            } else {
                return super.getMemberChildrenConstraint(parent);
            }
        }

        /**
         * @return the evaluator currently associated with the constraint; null
         * if there is no associated evaluator
         */
        public Evaluator getEvaluator() {
            if (delegatingConstraint != null) {
                return delegatingConstraint.getEvaluator();
            } else {
                return super.getEvaluator();
            }
        }
    }
}

// End RolapNativeSum.java
