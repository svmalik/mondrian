/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractIntegerCalc;
import mondrian.mdx.DimensionExpr;
import mondrian.mdx.HierarchyExpr;
import mondrian.mdx.LevelExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.rolap.ManyToManyUtil;
import mondrian.rolap.RolapEvaluator;

/**
 * Definition of the <code>Count</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class CountFunDef extends AbstractAggregateFunDef {
    static final String[] ReservedWords =
        new String[] {"INCLUDEEMPTY", "EXCLUDEEMPTY"};

    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Count",
            "Count(<Set>[, EXCLUDEEMPTY | INCLUDEEMPTY])",
            "Returns the number of tuples in a set, empty cells included unless the optional EXCLUDEEMPTY flag is used.",
            new String[]{"fnx", "fnxy"},
            CountFunDef.class,
            ReservedWords);

    public CountFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(final ResolvedFunCall call, final ExpCompiler compiler) {
        final Calc calc =
            compiler.compileAs(
                call.getArg(0), null, ResultStyle.ITERABLE_ANY);
        final boolean includeEmpty =
            call.getArgCount() < 2
            || ((Literal) call.getArg(1)).getValue().equals(
                "INCLUDEEMPTY");
        return new AbstractIntegerCalc(
            call,
            new Calc[] {calc})
        {
            public int evaluateInteger(Evaluator evaluator) {
                final int savepoint = evaluator.savepoint();
                try {
                    evaluator.setNonEmpty(false);
                    final int count;
                    RolapEvaluator manyToManyEval =
                        ManyToManyUtil.getManyToManyEvaluator(
                            (RolapEvaluator)evaluator);
                    SchemaReader schemaReader = evaluator.getSchemaReader();
                    NativeEvaluator nativeEvaluator =
                        schemaReader.getNativeSetEvaluator(
                            call.getFunDef(),
                            call.getArgs(),
                            manyToManyEval,
                            this);
                    if (nativeEvaluator != null) {
                        // TODO: Test how this performs for empty and non-empty scenarios.
                        count = (Integer)nativeEvaluator.execute(ResultStyle.VALUE);
                    } else {
                        int countByLevels;
                        try {
                            countByLevels = evaluateHierarchyLevels(evaluator, manyToManyEval);
                        } catch (Exception e) {
                            // we don't want to fail because of this "hack"
                            countByLevels = FunUtil.IntegerNull;
                        }
                        if (countByLevels != FunUtil.IntegerNull) {
                            count = countByLevels;
                        } else if (calc instanceof IterCalc) {
                            IterCalc iterCalc = (IterCalc) calc;
                            TupleIterable iterable =
                            evaluateCurrentIterable(iterCalc, evaluator);
                            count = count(evaluator, iterable, includeEmpty);
                        } else {
                            // must be ListCalc
                            ListCalc listCalc = (ListCalc) calc;
                            TupleList list =
                                evaluateCurrentList(listCalc, evaluator);
                            count = count(evaluator, list, includeEmpty);
                        }
                    }
                    return count;
                    
                } finally {
                    evaluator.restore(savepoint);
                }
            }

            public boolean dependsOn(Hierarchy hierarchy) {
                // COUNT(<set>, INCLUDEEMPTY) is straightforward -- it
                // depends only on the dimensions that <Set> depends
                // on.
                if (super.dependsOn(hierarchy)) {
                    return true;
                }
                if (includeEmpty) {
                    return false;
                }
                // COUNT(<set>, EXCLUDEEMPTY) depends only on the
                // dimensions that <Set> depends on, plus all
                // dimensions not masked by the set.
                return ! calc.getType().usesHierarchy(hierarchy, true);
            }

            private int evaluateHierarchyLevels(
                Evaluator evaluator, RolapEvaluator manyToManyEval)
            {
                if (!MondrianProperties.instance().EnableNativeCount.get()) {
                    return FunUtil.IntegerNull;
                }

                ResolvedFunCall arg0 = FunUtil.extractResolvedFunCall(call.getArg(0));
                if (arg0 == null
                    || arg0.getArgs().length != 1
                    || (!"AllMembers".equalsIgnoreCase(arg0.getFunName())
                        && !"Members".equalsIgnoreCase(arg0.getFunName())))
                {
                    return FunUtil.IntegerNull;
                }

                Hierarchy hierarchy = null;
                if (arg0.getArg(0) instanceof HierarchyExpr) {
                    hierarchy = ((HierarchyExpr)arg0.getArg(0)).getHierarchy();
                } else if (arg0.getArg(0) instanceof DimensionExpr) {
                    Dimension dimension = ((DimensionExpr)arg0.getArg(0)).getDimension();
                    if (dimension.getHierarchies().length == 1) {
                        hierarchy = dimension.getHierarchies()[0];
                    }
                }

                if (hierarchy == null) {
                    return FunUtil.IntegerNull;
                }

                SchemaReader schemaReader = evaluator.getSchemaReader();
                ExpCompiler expCompiler = evaluator.getQuery().createCompiler();
                Validator validator = expCompiler.getValidator();

                Exp[] args = call.getArgs().clone();
                int result = 0;
                for (Level level : hierarchy.getLevels()) {
                    int save = evaluator.savepoint();
                    try {
                        if (level.isAll()) {
                            result++;
                            continue;
                        }
                        int levelMembers =
                            evaluateForLevel(
                                level, call.getFunDef(), args, arg0.getFunName(),
                                schemaReader, validator, manyToManyEval);
                        if (levelMembers == FunUtil.IntegerNull) {
                            return FunUtil.IntegerNull;
                        } else {
                            result += levelMembers;
                        }
                    } finally {
                        evaluator.restore(save);
                    }
                }
                return result;
            }

            private int evaluateForLevel(
                Level level, FunDef funDef, Exp[] args,
                String membersFunName, SchemaReader schemaReader,
                Validator validator, RolapEvaluator manyToManyEval)
            {
                Exp[] levelArgs = new Exp[] { new LevelExpr(level) };
                FunDef levelFun =
                    validator.getDef(levelArgs, membersFunName, Syntax.Property);
                args[0] = new ResolvedFunCall(levelFun, levelArgs, call.getType());
                NativeEvaluator nativeEvaluator =
                    schemaReader.getNativeSetEvaluator(
                        funDef,
                        args,
                        manyToManyEval,
                        this);
                if (nativeEvaluator != null) {
                    return (Integer)nativeEvaluator.execute(ResultStyle.VALUE);
                } else {
                    return FunUtil.IntegerNull;
                }
            }
        };

/*
 RME OLD STUFF
        final ListCalc memberListCalc =
                compiler.compileList(call.getArg(0));
        final boolean includeEmpty =
                call.getArgCount() < 2 ||
                ((Literal) call.getArg(1)).getValue().equals(
                        "INCLUDEEMPTY");
        return new AbstractIntegerCalc(
                call, new Calc[] {memberListCalc}) {
            public int evaluateInteger(Evaluator evaluator) {
                List memberList =
                    evaluateCurrentList(memberListCalc, evaluator);
                return count(evaluator, memberList, includeEmpty);
            }

            public boolean dependsOn(Dimension dimension) {
                // COUNT(<set>, INCLUDEEMPTY) is straightforward -- it
                // depends only on the dimensions that <Set> depends
                // on.
                if (super.dependsOn(dimension)) {
                    return true;
                }
                if (includeEmpty) {
                    return false;
                }
                // COUNT(<set>, EXCLUDEEMPTY) depends only on the
                // dimensions that <Set> depends on, plus all
                // dimensions not masked by the set.
                if (memberListCalc.getType().usesDimension(dimension, true)) {
                    return false;
                }
                return true;
            }
        };
*/
    }
}

// End CountFunDef.java
