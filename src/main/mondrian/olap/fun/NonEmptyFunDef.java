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
import mondrian.mdx.MdxVisitorImpl;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.NamedSetExpr;
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
import mondrian.rolap.RolapCalculatedMember;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapMeasure;
import mondrian.rolap.RolapStoredMeasure;
import mondrian.rolap.SqlConstraintUtils;
import mondrian.util.CancellationChecker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

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
                        if (args.length == 2 && args[1] instanceof ResolvedFunCall) {
                            ResolvedFunCall arg2Call = FunUtil.extractResolvedFunCall(args[1]);
                            if (arg2Call != null) {
                                ExpCompiler expCompiler = evaluator.getQuery().createCompiler();
                                Validator validator = expCompiler.getValidator();
                                List<ResolvedFunCall> nonEmptyCalls =
                                    getNewNonEmptyCalls(args[0], arg2Call, validator);
                                if (nonEmptyCalls != null && nonEmptyCalls.size() > 1) {
                                    ArrayList<TupleList> nonEmptyTuples = new ArrayList<TupleList>();
                                    for (ResolvedFunCall nonEmpty : nonEmptyCalls) {
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
                                            ListCalc arg2Calc2 = expCompiler.compileList(nonEmpty.getArg(1));
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

        int currentIteration = 0;
        CancellationChecker cancellationChecker = new CancellationChecker(
            eval.getQuery().getStatement().getCurrentExecution());

        // "crossjoin" iterables and check for nonemptyness
        // only the first tuple is returned
        while (mainCursor.forward()) {
            boolean isNonEmpty = false;
            auxCursor = aux.tupleCursor();
            mainCursor.currentToArray(currentMembers, 0);
            inner : while (auxCursor.forward()) {
                cancellationChecker.check(currentIteration++);

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
        }
        return result;
    }

    private List<ResolvedFunCall> getNewNonEmptyCalls(
        Exp mainArgs, ResolvedFunCall call, Validator validator)
    {
        if (call == null) {
            return null;
        }

        String funName = call.getFunName();
        Map<List<String>, Set<Member>> measureMap = new HashMap<>();
        List<Exp> otherSets = new ArrayList<>();
        extractArguments(call, measureMap, otherSets);
        Exp other = null;
        if (!measureMap.isEmpty()) {
            for (Exp exp : otherSets) {
                if (other == null) {
                    other = exp;
                } else {
                    other = getCrossJoinCall(funName, other, exp, validator);
                }
            }
        }

        return createNewNonEmptyCalls(
            mainArgs, funName, measureMap, other, validator);
    }

    private void extractArguments(
        ResolvedFunCall call,
        Map<List<String>, Set<Member>> measureMap,
        List<Exp> otherSets)
    {
        if ("{}".equals(call.getFunName())) {
            getMeasuresMap(call, measureMap);
        }
        else if ((call.getFunDef() instanceof CrossJoinFunDef
                || call.getFunDef() instanceof NonEmptyCrossJoinFunDef)
            && call.getArgCount() == 2)
        {
            if (!processCrossJoinArgs(call.getArg(0), call.getArg(1), measureMap, otherSets)){
                processCrossJoinArgs(call.getArg(1), call.getArg(0), measureMap, otherSets);
            }
        }
    }

    private boolean processCrossJoinArgs(
        Exp arg1, Exp arg2,
        Map<List<String>, Set<Member>> measureMap,
        List<Exp> otherSets)
    {
        ResolvedFunCall call = FunUtil.extractResolvedFunCall(arg1);
        if (call != null) {
            getMeasuresMap(call, measureMap);
            if (measureMap.isEmpty() && hasMeasures(call)) {
                extractArguments(call, measureMap, otherSets);
            }
            if (!measureMap.isEmpty()) {
                otherSets.add(arg2);
                return true;
            }
        }
        return false;
    }

    private void getMeasuresMap(
        ResolvedFunCall call,
        Map<List<String>, Set<Member>> measureMap)
    {
        if (isSet(call, 2)) {
            List<Exp> exps = new ArrayList<>(Arrays.asList(call.getArgs()));
            for (int i = 0; i < exps.size(); i++) {
                Exp exp = exps.get(i);
                if (exp instanceof ResolvedFunCall && isSet((ResolvedFunCall) exp, 1)) {
                    exps.addAll(Arrays.asList(((ResolvedFunCall) exp).getArgs()));
                } else if (exp instanceof MemberExpr
                    && ((MemberExpr)exp).getMember() instanceof RolapMeasure)
                {
                    Set<String> cubes = new HashSet<String>();
                    Set<Member> foundMeasures = new HashSet<Member>();
                    findMeasures(exp, measureMap, cubes, foundMeasures);
                } else {
                    measureMap.clear();
                    break;
                }
            }
        }
    }

    private boolean isSet(ResolvedFunCall call, int minArgs) {
        return call != null && "{}".equals(call.getFunName()) && call.getArgCount() >= minArgs;
    }

    private boolean hasMeasures(ResolvedFunCall call) {
        if (call == null) {
            return false;
        }
        final AtomicBoolean hasMeasures = new AtomicBoolean(false);
        call.accept(
            new MdxVisitorImpl() {
                public Object visit(MemberExpr memberExpr) {
                    if (memberExpr.getMember().isMeasure()) {
                        hasMeasures.set(true);
                        turnOffVisitChildren();
                        return null;
                    }
                    return super.visit(memberExpr);
                }
            });
        return hasMeasures.get();
    }

    private List<ResolvedFunCall> createNewNonEmptyCalls(
        Exp mainArgs, String funName,
        Map<List<String>, Set<Member>> measureMap,
        Exp otherSet, Validator validator)
    {
        if (measureMap == null || measureMap.size() < 2) {
            return null;
        }
        List<ResolvedFunCall> nonEmptyCalls = new ArrayList<>(measureMap.size());
        for (Set<Member> measures : measureMap.values()) {
            Exp[] setArgs = new Exp[measures.size()];
            int i = 0;
            for (Member measure : measures) {
                setArgs[i++] = new MemberExpr(measure);
            }
            FunDef measureSet = validator.getDef(setArgs, "{}", Syntax.Braces);
            ResolvedFunCall measureExp = new ResolvedFunCall(
                measureSet, setArgs, new SetType(MemberType.Unknown));
            ResolvedFunCall nonEmptyArgs;
            if (otherSet == null) {
                nonEmptyArgs = measureExp;
            } else {
                nonEmptyArgs = getCrossJoinCall(funName, otherSet, measureExp, validator);
            }
            ResolvedFunCall nonEmpty =
                getNonEmptyCall(mainArgs, nonEmptyArgs, validator);
            nonEmptyCalls.add(nonEmpty);
        }
        return nonEmptyCalls;
    }

    private ResolvedFunCall getCrossJoinCall(
        String funName, Exp set1, Exp set2, Validator validator)
    {
        Exp[] args = new Exp[]{ set1, set2 };
        FunDef crossJoinFun = validator.getDef(args, funName, Syntax.Function);
        return new ResolvedFunCall(crossJoinFun, args, new SetType(MemberType.Unknown));
    }

    private ResolvedFunCall getNonEmptyCall(
        Exp mainArgs, Exp setExp, Validator validator)
    {
        Exp[] newArgs = new Exp[] { mainArgs, setExp };
        FunDef nonEmptyFun =
            validator.getDef(newArgs, "NonEmpty", Syntax.Function);
        return new ResolvedFunCall(
            nonEmptyFun, newArgs, new SetType(MemberType.Unknown));
    }

    private void findMeasures(
        Exp exp, Map<List<String>, Set<Member>> measureMap, Set<String> cubes, Set<Member> foundMeasures)
    {
        if (exp instanceof MemberExpr) {
            Member member = ((MemberExpr)exp).getMember();
            if (member.isAll()) {
                return;
            }
            if (member instanceof RolapStoredMeasure) {
                foundMeasures.add(member);
                RolapCube cube = ((RolapStoredMeasure) member).getCube();
                cubes.add(cube.getName());
            } else if (member instanceof RolapCalculatedMember && foundMeasures.add(member)) {
                findMeasures(member.getExpression(), null, cubes, foundMeasures);
                if (!SqlConstraintUtils.isSupportedCalculatedMember(member)) {
                    cubes.add(UUID.randomUUID().toString());
                }
            } else {
                return;
            }

            if (measureMap != null) {
                List<String> cubeList = new ArrayList<String>(cubes);
                Collections.sort(cubeList, String.CASE_INSENSITIVE_ORDER);
                Set<Member> measures = measureMap.get(cubeList);
                if (measures == null) {
                    measures = new LinkedHashSet<>();
                    measureMap.put(cubeList, measures);
                }
                measures.add(member);
            }
        } else if (exp instanceof ResolvedFunCall) {
            ResolvedFunCall funCall = (ResolvedFunCall) exp;
            Exp [] args = funCall.getArgs();
            for (Exp arg : args) {
                findMeasures(arg, measureMap, cubes, foundMeasures);
            }
        } else if (exp instanceof NamedSetExpr) {
            Exp namedSetExp = ((NamedSetExpr)exp).getNamedSet().getExp();
            findMeasures(namedSetExp, measureMap, cubes, foundMeasures);
        }
    }
}
// End NonEmptyFunDef.java