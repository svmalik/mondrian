/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2016 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.HierarchyCalc;
import mondrian.calc.ListCalc;
import mondrian.calc.TupleCollections;
import mondrian.calc.TupleList;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.rolap.RolapEvaluator;
import mondrian.rolap.RolapHierarchy;
import mondrian.rolap.RolapResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Definition of the <code>&lt;Hierarchy&gt;.CurrentMembers</code> MDX
 * builtin function.
 */
public class HierarchyCurrentMembersFunDef extends FunDefBase {
    static final HierarchyCurrentMembersFunDef instance =
            new HierarchyCurrentMembersFunDef();

    private HierarchyCurrentMembersFunDef() {
        super(
            "CurrentMembers",
            "Returns a set of current members along a hierarchy during an iteration.",
            "pxh");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final HierarchyCalc hierarchyCalc =
            compiler.compileHierarchy(call.getArg(0));
        final Hierarchy hierarchy = hierarchyCalc.getType().getHierarchy();
        if (hierarchy != null) {
            return new FixedCalcImpl(call, hierarchy);
        } else {
            return new CalcImpl(call, hierarchyCalc);
        }
    }

    private static TupleList getTupleList(Evaluator evaluator, Member currentMember, Set<Member> checkedMembers) {
        List<Member> members = null;
        if (checkedMembers.add(currentMember)) {
            if (currentMember instanceof RolapResult.CompoundSlicerRolapMember) {
                members = new ArrayList<Member>(expandMember(currentMember, evaluator));
            } else if (currentMember.isCalculated() && evaluator instanceof RolapEvaluator) {
                Exp exp = currentMember.getExpression();
                if (exp instanceof MemberExpr) {
                    return getTupleList(evaluator, ((MemberExpr) exp).getMember(), checkedMembers);
                } else if (currentMember.getExpression() instanceof ResolvedFunCall) {
                    ResolvedFunCall call = (ResolvedFunCall) currentMember.getExpression();
                    call = FunUtil.extractResolvedFunCall(call);
                    if ("Aggregate".equalsIgnoreCase(call.getFunName())) {
                        members = expandNonNative((RolapEvaluator) evaluator, call.getArg(0));
                    }
                }
            }
        }

        if (members == null) {
            members = Collections.singletonList(currentMember);
        }
        return TupleCollections.asTupleList(members);
    }

    private static List<Member> expandNonNative(RolapEvaluator evaluator, Exp exp) {
        ExpCompiler compiler = evaluator.getQuery().createCompiler();
        List<Member> members = null;
        if (MondrianProperties.instance().ExpandNonNative.get()
            && evaluator.getActiveNativeExpansions().add(exp))
        {
            ListCalc listCalc0 = compiler.compileList(exp);
            final TupleList tupleList = listCalc0.evaluateList(evaluator);

            // Prevent the case when the second argument size is too large
            Util.checkCJResultLimit(tupleList.size());

            if (tupleList.getArity() == 1) {
                members = tupleList.slice(0);
            }
            evaluator.getActiveNativeExpansions().remove(exp);
        }
        return members;
    }

    private static Member getContext(Hierarchy hierarchy, Evaluator evaluator) {
        Member context = null;
        if (evaluator instanceof RolapEvaluator) {
            context = ((RolapEvaluator) evaluator).getOriginalContext(hierarchy);
        }
        if (context == null) {
            context = evaluator.getContext(hierarchy);
        }
        return context;
    }

    /**
     * Compiled implementation of the Hierarchy.CurrentMembers function that
     * evaluates the hierarchy expression first.
     */
    public static class CalcImpl extends AbstractListCalc {
        private final HierarchyCalc hierarchyCalc;

        public CalcImpl(Exp exp, HierarchyCalc hierarchyCalc) {
            super(exp, new Calc[] {hierarchyCalc});
            this.hierarchyCalc = hierarchyCalc;
        }

        protected String getName() {
            return "CurrentMembers";
        }

        public TupleList evaluateList(Evaluator evaluator) {
            Hierarchy hierarchy = hierarchyCalc.evaluateHierarchy(evaluator);
            Member currentMember = getContext(hierarchy, evaluator);
            return getTupleList(evaluator, currentMember, new HashSet<Member>());
        }

        public boolean dependsOn(Hierarchy hierarchy) {
            return hierarchyCalc.getType().usesHierarchy(hierarchy, false);
        }
    }

    /**
     * Compiled implementation of the Hierarchy.CurrentMembers function that
     * uses a fixed hierarchy.
     */
    public static class FixedCalcImpl extends AbstractListCalc {
        // getContext works faster if we give RolapHierarchy rather than
        // Hierarchy
        private final RolapHierarchy hierarchy;

        public FixedCalcImpl(Exp exp, Hierarchy hierarchy) {
            super(exp, new Calc[] {});
            assert hierarchy != null;
            this.hierarchy = (RolapHierarchy) hierarchy;
        }

        protected String getName() {
            return "CurrentMembersFixed";
        }

        public TupleList evaluateList(Evaluator evaluator) {
            Member currentMember = getContext(hierarchy, evaluator);
            return getTupleList(evaluator, currentMember, new HashSet<Member>());
        }

        public boolean dependsOn(Hierarchy hierarchy) {
            return this.hierarchy == hierarchy;
        }

        public void collectArguments(Map<String, Object> arguments) {
            arguments.put("hierarchy", hierarchy);
            super.collectArguments(arguments);
        }
    }
}

// End HierarchyCurrentMembersFunDef.java
