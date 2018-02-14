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
import java.util.concurrent.atomic.AtomicBoolean;

import mondrian.mdx.DimensionExpr;
import mondrian.mdx.HierarchyExpr;
import mondrian.mdx.MdxVisitorImpl;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Dimension;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Literal;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.fun.FunUtil;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.DescendantsCrossJoinArg;
import mondrian.rolap.sql.MemberListCrossJoinArg;

/**
 * Computes a Count(set) in SQL.
 *
 * @author Will Gorman (wgorman@pentaho.com)
 */
public class RolapNativeCount extends RolapNativeSet {

    public RolapNativeCount() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeCount.get());
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
                evaluator, false, new Level[]{}, restrictMemberTypes(), false))
        {
            return null;
        }
        // is this "Count(<set>, <numeric expr>)"
        String funName = fun.getName();
        if (!"Count".equalsIgnoreCase(funName)) {
            return null;
        }

        ResolvedFunCall call = FunUtil.extractResolvedFunCall(args[0]);
        if (call == null) {
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

        // TODO: Note that this native evaluator won't work against tuples that are mapped to
        // security, because the security is usually applied after native evaluation.
        // Before this can be considered valid we need to test in that usecase and 
        // either push down the security into SQL or not natively evaluate.

        // TODO: Check to see if addRoleAccessConstraints() natively evaluates roles, if that is
        // always applicable or not.  See why there was a cleansing step at the end of native tuple
        // evaluation.

        // TODO: Need to support empty and non-empty second parameter in the correct manner here

        NativeEvaluator eval = evaluator.getSchemaReader().getSchema().getNativeRegistry().createEvaluator(
            evaluator, call.getFunDef(), call.getArgs());

        if (eval == null) {
            List<CrossJoinArg[]> allArgs =
                crossJoinArgFactory().checkCrossJoinArg(evaluator, call, false, false);
            if (failedCjArg(allArgs)) {
                return null;
            }

            CrossJoinArg[] cjArgs = allArgs.get(0);
            if (failedCjArg(cjArgs, false)) {
                return null;
            }
            if (isPreferInterpreter(cjArgs, true)) {
                return null;
            }

            // We need to determine if the "All" member should be counted,
            // and inform the count constraint.
            int addlCount = 0;
            if (call.getFunDef().getName().equals("AllMembers")) {
                if (call.getArg(0) instanceof HierarchyExpr) {
                    Hierarchy h = ((HierarchyExpr)call.getArg(0)).getHierarchy();
                    if (h.hasAll()) {
                        addlCount++;
                    }
                } else if (call.getArg(0) instanceof DimensionExpr) {
                    Dimension d = ((DimensionExpr)call.getArg(0)).getDimension();
                    if (d.getHierarchies()[0].hasAll()) {
                        addlCount++;
                    }
                }
            }

            // determine if a join against the current context is necessary
            // TODO: a similar approach will need to be made for other usecases
            // with Count(CrossJoin()) etc
            final AtomicBoolean mustJoin = new AtomicBoolean(false);

            if (args.length == 2) {
                if (!(args[1] instanceof Literal)) {
                    return null;
                }
                if (((Literal)args[1]).getValue().toString().equalsIgnoreCase("EXCLUDEEMPTY")) {
                  mustJoin.set(true);
                }
            }
            // Check for nonempty functions
            call.accept(
                new MdxVisitorImpl() {
                    public Object visit(ResolvedFunCall call) {
                      if (call.getFunName().equalsIgnoreCase("NonEmpty") || call.getFunName().equalsIgnoreCase("NonEmptyCrossJoin")) {
                        mustJoin.set(true);
                      }
                      return super.visit(call);
                    }
                });

            // we need to check the context for base cubes
            if (mustJoin.get()
                && (evaluator.getCube() == null || evaluator.getCube().isVirtual())
                && (evaluator.getBaseCubes() == null || evaluator.getBaseCubes().size() == 0)) {
                LOGGER.debug("cannot natively count due to a join requirement and no base cubes");
                return null;
            }

            List<Member> resetMembers = new ArrayList<Member>();
            if (!mustJoin.get()) {
                //check restricted hierarchies
                for (Member member : evaluator.getMembers()) {
                    if (member instanceof RolapHierarchy.LimitedRollupMember) {
                        Hierarchy hierarchy = member.getHierarchy();
                        if (hierarchy == null) {
                            continue;
                        }
                        for (CrossJoinArg carg : cjArgs) {
                            RolapLevel level = carg.getLevel();
                            if (level != null) {
                                if (hierarchy.equals(level.getHierarchy())) {
                                    // cannot run native
                                    return null;
                                } else {
                                    // reset restricted hierarchy if it is not in the args
                                    // so that it does not try to join
                                    resetMembers.add(hierarchy.getAllMember());
                                }
                            }
                        }
                    }
                }
            }

            final int savepoint = evaluator.savepoint();
            try {
                overrideContext(evaluator, cjArgs, null);
                if (!resetMembers.isEmpty()) {
                    evaluator.setContext(resetMembers);
                }
                eval = new SetEvaluator(cjArgs, evaluator.getSchemaReader(), new CountConstraint(cjArgs, evaluator, false, addlCount, mustJoin.get()));
            } finally {
                evaluator.restore(savepoint);
            }
        }

        if (eval != null) {
            LOGGER.debug("using native count");  
        }
        return eval;
    }

    static class CountConstraint extends SetConstraint {

        int addlCount = 0;
        boolean joinRequired = false;
        CountConstraint(
            CrossJoinArg[] args, RolapEvaluator evaluator, boolean restrict, int addlCount, boolean joinRequired)
        {
            super(args, evaluator, restrict);
            this.addlCount = addlCount;
            this.joinRequired = joinRequired;
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
            // Depends on empty / nonempty state of the count.
            return joinRequired || (getEvaluator().isNonEmpty() && super.isJoinRequired());
        }

        /**
         * This returns a CacheKey object
         * @return
         */
        @Override
        public Object getCacheKey() {
            CacheKey key = new CacheKey((CacheKey)super.getCacheKey());
            if (this.getEvaluator() instanceof RolapEvaluator) {
                key.setSlicerMembers(((RolapEvaluator) this.getEvaluator()).getSlicerMembers());
            }
            key.setValue(getClass().getName() + ".addlCount", addlCount);
            key.setValue(getClass().getName() + ".joinRequired", joinRequired);
            return key;
        }

    }
}

// End RolapNativeFilter.java
