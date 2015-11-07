/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2014-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.ResultStyle;
import mondrian.calc.TupleCollections;
import mondrian.calc.TupleCursor;
import mondrian.calc.TupleIterable;
import mondrian.calc.TupleList;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.Syntax;
import mondrian.olap.Validator;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.SetType;
import mondrian.olap.type.TupleType;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapStoredMeasure;
import mondrian.server.Locus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NonEmptyFunDef extends FunDefBase {
    static final MultiResolver NonEmptyResolver =
        new MultiResolver(
            "NonEmpty",
            "NonEmpty(<Set>[, <Set>])",
            "Returns the tuples in the first set that are non-empty when evaluated across the tuples in the second set, or in the context of the current coordinates if not provided.",
            new String[]{"fxx", "fxxx"})
        {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new NonEmptyFunDef(dummyFunDef);
            }
        };

    protected NonEmptyFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc arg1Calc = compiler.compileList(call.getArg(0));
        final ListCalc arg2Calc = call.getArgCount() > 1
            ? compiler.compileList(call.getArg(1))
            : null;
        // determine overridden types
        List<MemberType> typeList = new ArrayList<MemberType>();
        for (Exp arg : call.getArgs()) {
            CrossJoinFunDef.addTypes(arg.getType(), typeList);
        }
        MemberType[] types =
            typeList.toArray(new MemberType[typeList.size()]);
        // must form valid tuples
        TupleType.checkHierarchies(types);
        return new AbstractListCalc(
            call, new Calc[] {arg1Calc, arg2Calc}, false)
        {
            @Override
            public boolean dependsOn(Hierarchy hierarchy) {
                return true;
            }

            public TupleList evaluateList(Evaluator evaluator) {
                final int savepoint = evaluator.savepoint();
                 try {
                     TupleIterable setIterable = null;
                    // empty args can be stripped at the source for some cases
                    // when is it ok to set nonempty?

                    // attempt native
                    NativeEvaluator nativeEvaluator =
                        getNativeEvaluator(evaluator, call, this);
                    if (nativeEvaluator != null) {
                        evaluator.restore(savepoint);
                        return
                            (TupleList) nativeEvaluator.execute(
                                ResultStyle.LIST);
                    } else if (evaluator.nativeEnabled()
                            && MondrianProperties.instance().EnableNativeNonEmptyFunctionDifferentCubes.get())
                    {
                        // This is to address a case when there are measures from different
                        // measure groups in the second arg. The idea is to group measures
                        // and run each measure group separately, and then union results.
                        final boolean tryNonNative =
                            MondrianProperties.instance().EnableNativeNonEmptyFunctionDifferentCubesAndNonNative.get();
                        Exp[] args = call.getArgs();
                        if (args.length == 2) {
                            if (args[1] instanceof ResolvedFunCall) {
                                ResolvedFunCall call = (ResolvedFunCall)args[1];
                                while (("{}".equals(call.getFunName()) || "()".equals(call.getFunName()))
                                    && call.getArgCount() == 1
                                    && call.getArg(0) instanceof ResolvedFunCall)
                                {
                                    call = (ResolvedFunCall)call.getArg(0);
                                }
                                Map<RolapCube, List<Exp>> measureMap =
                                    new HashMap<RolapCube, List<Exp>>();
                                for (Exp arg1 : call.getArgs()) {
                                    if (arg1 instanceof MemberExpr) {
                                        Member member = ((MemberExpr)arg1).getMember();
                                        RolapCube cube = null;
                                        if (member instanceof RolapStoredMeasure) {
                                            cube = ((RolapStoredMeasure) member).getCube();
                                        }
                                        List<Exp> measures = measureMap.get(cube);
                                        if (measures == null) {
                                            measures = new ArrayList<Exp>();
                                            measureMap.put(cube, measures);
                                        }
                                        measures.add(arg1);
                                    }
                                }

                                List<TupleList> nonEmptyTuples;
                                if (measureMap.keySet().size() > 1) {
                                    nonEmptyTuples = new ArrayList<TupleList>();
                                    ExpCompiler expCompiler = evaluator.getQuery().createCompiler();
                                    Validator validator = expCompiler.getValidator();
                                    for (List<Exp> measures : measureMap.values()) {
                                        Exp[] setArgs = measures.toArray(new Exp[measures.size()]);
                                        FunDef set = validator.getDef(setArgs, "{}", Syntax.Braces);
                                        Exp setExp = new ResolvedFunCall(set, setArgs, new SetType(MemberType.Unknown));
                                        Exp[] newArgs = new Exp[] { args[0], setExp};
                                        FunDef nonEmptyFun = validator.getDef(newArgs, "NonEmpty", Syntax.Function);
                                        ResolvedFunCall nonEmpty = new ResolvedFunCall(nonEmptyFun, newArgs, new SetType(MemberType.Unknown));
                                        int save = evaluator.savepoint();
                                        nativeEvaluator = getNativeEvaluator(evaluator, nonEmpty, this);
                                        evaluator.restore(save);
                                        if (nativeEvaluator != null) {
                                            nonEmptyTuples.add((TupleList) nativeEvaluator.execute(ResultStyle.LIST));
                                        } else {
                                            if (!tryNonNative) {
                                                nonEmptyTuples = null;
                                                break;
                                            }
                                            ListCalc arg2Calc2 = expCompiler.compileList(setExp);
                                            if (setIterable == null) {
                                                setIterable = arg1Calc.evaluateIterable(evaluator);
                                            }
                                            TupleIterable auxIterable2 = arg2Calc2.evaluateIterable(evaluator);
                                            nonEmptyTuples.add(
                                                auxIterable2 == null
                                                    ? nonEmpty(evaluator, setIterable)
                                                    : nonEmpty(evaluator, setIterable, auxIterable2));
                                        }
                                    }
                                    if (nonEmptyTuples != null && !nonEmptyTuples.isEmpty()) {
                                        TupleList result = null;
                                        Set<List<Member>> added = new HashSet<List<Member>>();
                                        for (TupleList tuples : nonEmptyTuples) {
                                            if (result == null) {
                                                result = TupleCollections.createList(tuples.getArity());
                                            }
                                            FunUtil.addUnique(result, tuples, added);
                                        }
                                        return result;
                                    }
                                }
                            }
                        }
                    }

                     if (setIterable == null) {
                         setIterable = arg1Calc.evaluateIterable(evaluator);
                     }
                    TupleIterable auxIterable =
                        arg2Calc != null
                            ? arg2Calc.evaluateIterable(evaluator)
                            : null;

                    return (auxIterable == null)
                        ? nonEmpty(evaluator, setIterable)
                        : nonEmpty(evaluator, setIterable, auxIterable);
                } finally {
                    evaluator.restore(savepoint);
                }
            }

            private NativeEvaluator getNativeEvaluator(Evaluator evaluator, ResolvedFunCall call, Calc calc) {
                return
                    evaluator.getSchemaReader().getNativeSetEvaluator(
                        call.getFunDef(), call.getArgs(), evaluator, calc);
            }
        };
    }

    public static TupleList nonEmpty(Evaluator eval, TupleIterable main) {
        TupleList result = TupleCollections.createList(main.getArity());
        final Member[] currentMembers = new Member[main.getArity()];
        final TupleCursor mainCursor = main.tupleCursor();
        while (mainCursor.forward()) {
            mainCursor.currentToArray(currentMembers, 0);
            eval.setContext(currentMembers);
            if (eval.evaluateCurrent() != null) {
                result.add(mainCursor.current());
            }
        }
        return result;
    }

    public static TupleList nonEmpty(
        Evaluator eval,
        TupleIterable main, TupleIterable aux)
    {
        final int arityMain = main.getArity();
        final int arity = arityMain + aux.getArity();
        TupleList result =
            TupleCollections.createList(arityMain);
        final TupleCursor mainCursor = main.tupleCursor();
        TupleCursor auxCursor = null;
        final Member[] currentMembers = new Member[arity];

        final int checkCancelPeriod = MondrianProperties.instance().CancelPhaseInterval.get();
        int rowCount = 0;

        // "crossjoin" iterables and check for nonemptyness
        // only the first tuple is returned
        while (mainCursor.forward()) {
            boolean isNonEmpty = false;
            auxCursor = aux.tupleCursor();
            mainCursor.currentToArray(currentMembers, 0);
            inner : while (auxCursor.forward()) {
                auxCursor.currentToArray(currentMembers, arityMain);
                eval.setContext(currentMembers);
                Object currval = eval.evaluateCurrent();
                if (currval != null) {
                    isNonEmpty = true;
                    // if currval comes back as a Double 0.0, that may mean there was a missed hit in cache.
                    // skip the break, so we can build up all cell requests.
                    if (!(currval instanceof Double) || !(((Double)currval).doubleValue() == 0.0)) {
                        break inner;
                    }
                }
            }
            if (isNonEmpty) {
                result.add(mainCursor.current());
            }

            rowCount++;
            if (checkCancelPeriod > 0 && rowCount % checkCancelPeriod == 0) {
                Locus.peek().execution.checkCancelOrTimeout();
            }
        }
        return result;
    }

}
// End NonEmptyFunDef.java