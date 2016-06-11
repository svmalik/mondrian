/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2014-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.mdx.MemberExpr;
import mondrian.mdx.NamedSetExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.olap.fun.CrossJoinFunDef;
import mondrian.olap.fun.FunDefBase;
import mondrian.olap.fun.NonEmptyCrossJoinFunDef;
import mondrian.olap.fun.TupleFunDef;
import mondrian.rolap.RolapNativeFilter.FilterConstraint;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.SqlQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class RolapNativeNonEmptyFunction extends RolapNativeSet {

    public RolapNativeNonEmptyFunction() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeNonEmptyFunction.get());
    }

    protected boolean restrictMemberTypes() {
        // can't really handle calculated measures
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
                evaluator, false, new Level[]{}, restrictMemberTypes()))
        {
            return null;
        }

        // Native Args Validation
        // the first set
        // 0 - arguments, 1 - additional constraints
        List<CrossJoinArg[]> mainArgs =
            crossJoinArgFactory().checkCrossJoinArg(evaluator, args[0]);
        if (failedCjArg(mainArgs)) {
            alertNonNative(evaluator, fun, args[0]);
            return null;
        }
        for (CrossJoinArg cjArg : mainArgs.get(0)) {
            if (cjArg.getLevel().getDimension().isHanger()) {
                alertNonNative(evaluator, fun, args[0]);
                return null;
            }
        }
        // we want the second arg to be added just as a crossjoin constraint
        boolean hasTwoArgs = args.length == 2;
        if (hasTwoArgs) {
            // this check verifies there isn't anything that would cause
            // overall native eval to fail in both arguments
            FunDef nonEmptyCrossJoin =
                new NonEmptyCrossJoinFunDef(
                    new FunDefBase("NonEmptyCrossJoin", null, "fxxx") {}
                );
            List<CrossJoinArg[]> allArgs =
                crossJoinArgFactory().checkCrossJoin(
                    evaluator,
                    nonEmptyCrossJoin,
                    args,
                    true);
            if (failedCjArg(allArgs)) {
                alertNonNative(evaluator, fun, args[1]);
                return null;
            }
        }

        List<CrossJoinArg[]> extraArgs =
            hasTwoArgs
                ? crossJoinArgFactory().checkCrossJoinArg(evaluator, args[1])
                : null;

        RolapStoredMeasure measure = null;
        List<RolapStoredMeasure> nativeMeasures =
          new ArrayList<RolapStoredMeasure>();
        if (hasTwoArgs) {
          // investigate all measures in second param,
          // if they are all regular measures from the same base cube
          // select the final one for non-empty evaluation

            Set<RolapCube> baseCubes = new HashSet<RolapCube>();
            Set<Member> measures = new LinkedHashSet<Member>();
            findMeasures(args[1], baseCubes, measures);
            Set<RolapCube> unrelatedCubes = new HashSet<>();
            if (baseCubes.size() > 1 && mainArgs.get(0).length == 1
                && mainArgs.get(0)[0] != null && mainArgs.get(0)[0].getLevel() != null)
            {
                for (RolapCube cube : baseCubes) {
                    if (cube.findBaseCubeLevel(mainArgs.get(0)[0].getLevel()) == null) {
                        unrelatedCubes.add(cube);
                    }
                }
                baseCubes.clear();
            }
            for (Member m : measures) {
                if (m instanceof RolapStoredMeasure) {
                    measure = (RolapStoredMeasure) m;
                    if (!areFromSameCube(mainArgs.get(0), measure)) {
                        // trying to skip this measure
                        if (!unrelatedCubes.contains(measure.getCube())) {
                            return null;
                        }
                    } else {
                        nativeMeasures.add(measure);
                        baseCubes.add(measure.getCube());
                    }
                }
                if (m.isCalculated()) {
                    if (!SqlConstraintUtils.isSupportedCalculatedMember(m)) {
                        // unable to perform
                        alertNonNative(evaluator, fun, args[1]);
                        return null;
                    } else {
                        Set<Member> calcMeasures = new HashSet<>();
                        findMeasures(m.getExpression(), baseCubes, calcMeasures);
                    }
                }
            }
            if (baseCubes.size() > 1) {
                // unable to perform
                alertNonNative(evaluator, fun, args[1]);
                return null;
            }
        }
        else {
            // use context measure
            if (evaluator.getMembers()[0] instanceof RolapStoredMeasure) {
                measure = (RolapStoredMeasure) evaluator.getMembers()[0];
                if (!areFromSameCube(mainArgs.get(0), measure)) {
                    return null;
                } else {
                    nativeMeasures.add(measure);
                }
            }
        }

        if (hasTwoArgs && extraArgs == null) {
            // second arg failed, check if it's just because of a measure set
            if (!nativeMeasures.isEmpty()) {
                // see if it can be nativized without the measures
                Exp altExp = stripMeasureSets(args[1]);
                if (altExp != null) {
                    extraArgs =
                        crossJoinArgFactory().checkCrossJoinArg(
                            evaluator,
                            altExp);
                    // try to expand non native if supported
                    boolean failed = failedCjArg(extraArgs);
                    boolean expanded = false;
                    if (failed
                        && MondrianProperties.instance().ExpandNonNative.get())
                    {
                        CrossJoinArg[] cjNonNative =
                            crossJoinArgFactory().expandNonNative(evaluator, altExp);
                        if (cjNonNative != null) {
                            extraArgs = Collections.singletonList(cjNonNative);
                            expanded = true;
                        }
                    }
                    if (failed && (!expanded || failedCjArg(extraArgs))) {
                        // can't be nativized even without measures
                        alertNonNative(evaluator, fun, args[1]);
                        return null;
                    }
                } else {
                    // second arg is just a measure set
                    hasTwoArgs = false;
                }
            } else {
                // what made it fail wasn't a measure
                alertNonNative(evaluator, fun, args[1]);
                return null;
            }
        }

        // what should end up in the select
        final CrossJoinArg[] returnArgs = mainArgs.get(0);
        // what will be in the constraint
        final CrossJoinArg[] constraintArgs =
            getConstraintArgs(mainArgs, extraArgs);
        // crossjoin members that will override context
        final CrossJoinArg[] cjArgs =
            hasTwoArgs
                ? Util.appendArrays(returnArgs, extraArgs.get(0))
                : returnArgs;

        SchemaReader schemaReader = evaluator.getSchemaReader();

        final int savepoint = evaluator.savepoint();
        try {
            overrideContext(evaluator, cjArgs, measure);
            NonEmptyFunctionConstraint constraint =
                new NonEmptyFunctionConstraint(
                    constraintArgs, nativeMeasures,
                    evaluator, restrictMemberTypes());

            NativeEvaluator nativeEvaluator =
                new SetEvaluator(returnArgs, schemaReader, constraint);
            LOGGER.debug("NonEmpty() going native");
            return nativeEvaluator;
        } finally {
            evaluator.restore(savepoint);
        }
    }

    private CrossJoinArg[] getConstraintArgs(
        List<CrossJoinArg[]> firstSet, List<CrossJoinArg[]> secondSet)
    {
        //get everything into one array
        final CrossJoinArg[] empty = new CrossJoinArg[0];
        return Util.appendArrays(
            firstSet.get(0),
            (firstSet.size() > 1) ? firstSet.get(1) : empty,
            (secondSet != null) ? secondSet.get(0) : empty,
            (secondSet != null && secondSet.size() > 1)
                ? secondSet.get(1)
                : empty);
    }

    /**
     * Extracts the stored measures referenced in an expression
     *
     * @param exp expression
     * @param baseCubes set of base cubes
     */
    private static void findMeasures(
        Exp exp,
        Set<RolapCube> baseCubes,
        Set<Member> foundMeasures)
    {
        if (exp instanceof MemberExpr) {
            MemberExpr memberExpr = (MemberExpr) exp;
            Member member = memberExpr.getMember();
            if (member instanceof RolapStoredMeasure) {
                foundMeasures.add(member);
                addMeasure(
                    (RolapStoredMeasure) member, baseCubes);
            } else if (member instanceof RolapCalculatedMember) {
                if (!foundMeasures.contains(member)) {
                    foundMeasures.add(member);
                    findMeasures(
                        member.getExpression(),
                        baseCubes,
                        foundMeasures);
                }
            }
        } else if (exp instanceof ResolvedFunCall) {
            ResolvedFunCall funCall = (ResolvedFunCall) exp;
            Exp [] args = funCall.getArgs();
            for (Exp arg : args) {
                findMeasures(arg, baseCubes, foundMeasures);
            }
        } else if (exp instanceof NamedSetExpr) {
            Exp namedSetExp = ((NamedSetExpr)exp).getNamedSet().getExp();
            findMeasures(namedSetExp, baseCubes, foundMeasures);
        }
    }

    /**
     * Take measure sets from the expression. Should be otherwise equivalent.
     * @param exp
     * @return expression without measure sets, or null if only a measure set.
     */
    private Exp stripMeasureSets(Exp exp) {
        if (isMeasureSet(exp)) {
            return null;
        }
        if (exp instanceof ResolvedFunCall) {
            ResolvedFunCall funCall = (ResolvedFunCall) exp;
            FunDef funDef = funCall.getFunDef();
            if (funDef instanceof CrossJoinFunDef
                || funDef instanceof TupleFunDef)
            {
                int minArgs = (funDef instanceof CrossJoinFunDef) ? 2 : 1;
                List<Exp> nonMeasureArgs = new ArrayList<Exp>();
                for (Exp arg : funCall.getArgs()) {
                    Exp nonMeasArg = stripMeasureSets(arg);
                    if (nonMeasArg != null) {
                        nonMeasureArgs.add(nonMeasArg);
                    }
                }
                Exp[] newArgs = nonMeasureArgs.toArray(
                    new Exp[nonMeasureArgs.size()]);
                //if (nonMeasureArgs.size() < funCall.getArgCount()) {
                if (!Arrays.equals(newArgs, funCall.getArgs())) {
                    if (nonMeasureArgs.size() >= minArgs) {
                        return new ResolvedFunCall(
                            funCall.getFunDef(),
                            nonMeasureArgs.toArray(
                                new Exp[nonMeasureArgs.size()]),
                            funCall.getType());
                    } else {
                        return nonMeasureArgs.size() == 1
                            ? nonMeasureArgs.get(0)
                            : null;
                    }
                }
            }
        }
        return exp;
    }

    private boolean isMeasureSet(Exp exp) {
        if (exp instanceof ResolvedFunCall) {
            ResolvedFunCall funCall = (ResolvedFunCall) exp;
            if (funCall.getFunName().equals("{}")) {
                for (Exp arg : funCall.getArgs()) {
                    if (!isMeasure(arg) && !isMeasureSet(arg)) {
                        return false;
                    }
                }
                return true;
            }
        }
        return isMeasure(exp);
    }
    private boolean isMeasure(Exp exp) {
        return exp instanceof MemberExpr
            && ((MemberExpr)exp).getMember() instanceof RolapMeasure;
    }

    /**
     * Adds information regarding a stored measure to maps
     *
     * @param measure the stored measure
     * @param baseCubes set of base cubes
     */
    private static void addMeasure(
        RolapStoredMeasure measure,
        Set<RolapCube> baseCubes)
    {
        RolapCube baseCube = measure.getCube();
        baseCubes.add(baseCube);
    }

    private static boolean areFromSameCube(
        CrossJoinArg[] cjArgs, RolapStoredMeasure measure)
    {
        for (CrossJoinArg cjArg : cjArgs) {
            // the level must be related all base cubes.
            // If not, the SQL generated is bogus.
            if (cjArg.getLevel() != null &&
                !areFromSameCube(cjArg.getLevel(), measure))
            {
                LOGGER.debug("NonEmpty() Cannot go native due to level not existing in both cubes.");
                return false;
            }
        }
        return true;
    }

    static class NonEmptyFunctionConstraint extends SetConstraint {
        // using linked for deterministic iteration
        private Set<RolapStar.Measure> nonEmptyMeasures =
            new LinkedHashSet<RolapStar.Measure>();

        NonEmptyFunctionConstraint(
            CrossJoinArg[] args,
            Collection<RolapStoredMeasure> measures,
            RolapEvaluator evaluator,
            boolean restrict)
        {
            super(args, evaluator, restrict);
            for (RolapStoredMeasure measure : measures) {
                nonEmptyMeasures.add(
                    (RolapStar.Measure) measure.getStarMeasure());
            }
        }

        @Override
        public void constrainExtraLevels(
            RolapCube baseCube,
            BitKey levelBitKey)
        {
            super.constrainExtraLevels(baseCube, levelBitKey);
            for (CrossJoinArg arg : args) {
                final RolapLevel level = arg.getLevel();
                if (level != null && !level.isAll()) {
                    RolapStar.Column column =
                        ((RolapCubeLevel)level)
                            .getBaseStarKeyColumn(baseCube);
                    levelBitKey.set(column.getBitPosition());
                }
            }
            for (RolapStar.Measure measure : nonEmptyMeasures) {
                levelBitKey.set(measure.getBitPosition());
            }
        }

        public void addConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar)
        {
            // super handles args
            super.addConstraint(sqlQuery, baseCube, aggStar);

            if (!nonEmptyMeasures.isEmpty()) {
                // add non-null measure(s)
                StringBuilder sb = new StringBuilder();
                boolean first = true;
                final boolean useParens = nonEmptyMeasures.size() > 1;
                if (useParens) {
                    sb.append("(");
                }
                for (RolapStar.Measure measure : nonEmptyMeasures) {
                    if (first) {
                        first = false;
                    }
                    else {
                        sb.append(" or ");
                    }
                    String expr = SqlConstraintUtils.getColumnExpr(
                      sqlQuery, aggStar, measure);
                    sb.append(expr);
                    sb.append(" is not ");
                    sb.append(RolapUtil.sqlNullLiteral);
                }
                if (useParens) {
                    sb.append(")");
                }
                sqlQuery.addWhere(sb.toString());
            }
        }

        protected boolean isJoinRequired() {
            // even with just one argument still has the context measure
            return true;
        }

        @Override
        public Object getCacheKey() {
            CacheKey key = new CacheKey((CacheKey) super.getCacheKey());
            if (this.getEvaluator() instanceof RolapEvaluator) {
                key.setSlicerMembers(((RolapEvaluator) this.getEvaluator()).getSlicerMembers());
            }
            return key;
        }
    }
}
// End RolapNativeNonEmptyFunction.java
