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

import java.util.ArrayList;
import java.util.List;

import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Level;
import mondrian.olap.Literal;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.rolap.RolapNativeFilter.FilterConstraint;
import mondrian.rolap.sql.CrossJoinArg;

public class RolapNativeExists extends RolapNativeSet {

    public RolapNativeExists() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeExists.get());
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

        // Only support the three arg variant of Exists
        if (args.length != 3) {
          return null;
        }

        if (!FilterConstraint.isValidContext(
                evaluator, false, new Level[]{}, restrictMemberTypes(), false))
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
        // we want the second arg to be added just as a crossjoin constraint

        // support for empty second parameter
        List<CrossJoinArg[]> extraArgs = null;
        if (args[1] instanceof ResolvedFunCall
            && "".equals(((ResolvedFunCall)args[1]).getFunName())
            && ((ResolvedFunCall)args[1]).getArgCount() == 0)
        {
            extraArgs = new ArrayList<CrossJoinArg[]>();
            extraArgs.add(new CrossJoinArg[0]);
        } else {
            extraArgs = crossJoinArgFactory().checkCrossJoinArg(evaluator, args[1]);
        }
        if (extraArgs == null) {
            return null;
        }

        RolapStoredMeasure measure = null;
        if (!(args[2] instanceof Literal)) {
          return null;
        }

        String cubeName = ((Literal)args[2]).getValue().toString();
        if (cubeName == null) {
          return null;
        }

        // it's not enough to be null, the element must not exist in the
        //  fact table to be considered empty here.
        // we need to break into rolap namespace and use the fact count
        measure = (RolapStoredMeasure)RolapUtil.getFactCountMeasure(evaluator, cubeName, true);

        // what should end up in the select
        final CrossJoinArg[] returnArgs = mainArgs.get(0);
        // what will be in the constraint
        final CrossJoinArg[] constraintArgs =
            getConstraintArgs(mainArgs, extraArgs);
        // crossjoin members that will override context
        final CrossJoinArg[] cjArgs =
            Util.appendArrays(returnArgs, extraArgs.get(0));

        SchemaReader schemaReader = evaluator.getSchemaReader();

        final int savepoint = evaluator.savepoint();
        try {
            overrideContext(evaluator, cjArgs, measure);
            ExistsFunctionConstraint constraint =
                new ExistsFunctionConstraint(
                    constraintArgs, evaluator, restrictMemberTypes());

            NativeEvaluator nativeEvaluator =
                new SetEvaluator(returnArgs, schemaReader, constraint);
            LOGGER.debug("Exists() going native");
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

    static class ExistsFunctionConstraint extends SetConstraint {

        ExistsFunctionConstraint(
            CrossJoinArg[] args,
            RolapEvaluator evaluator,
            boolean restrict)
        {
            super(args, evaluator, restrict);
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
// End RolapNativeExists.java
