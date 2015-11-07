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

import java.util.Set;
import java.util.TreeSet;

import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

/**
 * Computes an Aggregate(set [, numeric expression ]) in SQL if possible.
 */
public class RolapNativeAggregate extends RolapNativeSet {

    public RolapNativeAggregate() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeAggregate.get());
    }

    protected boolean restrictMemberTypes() {
        return true;
    }

    NativeEvaluator createEvaluator(
        RolapEvaluator evaluator,
        FunDef fun,
        Exp[] args)
    {
        if(!isEnabled()) {
            return null;
        }

        // is this "Aggregate(<set>, <numeric expr>)"
        String funName = fun.getName();
        if (!"Aggregate".equalsIgnoreCase(funName)) {
            return null;
        }

        if (args.length == 1 && evaluator.getSlicerMembers().contains(evaluator.getExpanding())) {
            // do not need to evaluate - this is the slicer
            return null;
        }

        if (SqlConstraintUtils.containsCalculatedMember(evaluator.getNonAllMembers(), true)) {
            return null;
        }

        String name = getAggregator(evaluator, args);

        if (name == null) {
            return null;
        }
        SetEvaluator eval = null;
        // check the aggregator is supported
        if (supportsAggregation(name)) {
            // delegate to SUM if there are two args. It will resolve to sum or count
            // for the measure and then sum results. Otherwise use the measure's aggregator.
            eval = getAggregationEvaluator(args.length == 2 ? "SUM" : name, args, evaluator);
        }
        if (eval != null) {
            LOGGER.debug("using native aggregate");
        } 
        return eval;        
    }

    /**
     * Get the name of the aggregator to use.
     *
     * @param evaluator
     * @param args
     * @return
     */
    private String getAggregator(RolapEvaluator evaluator, Exp[] args) {
        if (args.length == 1) {
            Aggregator agg = (Aggregator) evaluator.getProperty(Property.AGGREGATION_TYPE.name, null);
            if (agg != null && agg instanceof RolapAggregator
                && supportsAggregation(((RolapAggregator)agg).getName()))
            {
                Aggregator rollup = agg.getRollup();
                if (rollup != null && rollup instanceof RolapAggregator) {
                    return ((RolapAggregator) rollup).getName();
                }
            }
        } else {
            // call findMeasure
            Set<Member> measures = new TreeSet<Member>();
            findMeasure(args[1], measures);
            if (measures.size() == 1) {
                Member member = measures.iterator().next();
                if (member.isCalculated()) {
                    return null;
                }
                RolapStoredMeasure measure = (RolapStoredMeasure) member;
                RolapAggregator agg = measure.getAggregator();
                String name = agg.getName();
                LOGGER.debug("Measure " + measure.getUniqueName() + " aggregator is " + name);
                return name;
            }
        }
        return null;
    }

    /**
     * Only support COUNT and SUM aggregators for now. DISTINCT-COUNT cannot be supported
     * as things stand. Supporting these two means Aggregator always delegates to SUM because in
     * the
     * case of 1 arg and a COUNT aggregator, the rollup aggregator is SUM, and in the case
     * of 2 args, SUM is called. The logic to determine the aggregator is here so that
     * other aggregators (min, max, avg, etc.) can be added to this method without (hopefully)
     * changing any other code.
     *
     * @param name
     * @return
     */
    private boolean supportsAggregation(String name) {
        return "COUNT".equalsIgnoreCase(name) || "SUM".equalsIgnoreCase(name);
    }

    protected SetEvaluator getAggregationEvaluator(String name, Exp[] args,
                                                   RolapEvaluator evaluator) {
        try {
            FunDef fun = evaluator.getQuery().createValidator().getDef(args, name, Syntax.Function);
            if (fun != null) {
                return (SetEvaluator) evaluator.getSchemaReader()
                                               .getSchema()
                                               .getNativeRegistry()
                                               .createEvaluator(evaluator, fun, args);
            }
        } catch (MondrianException e) {
            //if there is not related function ,go non-native;
            return null;
        }
        return null;

    }
    
    
    /**
     * Extracts the stored measures referenced in an expression
     *
     * @param exp expression
     * 
     * 
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

    private static boolean checkContext(Evaluator evaluator) {
        for (Member member : evaluator.getNonAllMembers()) {
            if (member.isCalculated()) {
                LOGGER.debug("Not supported: " + member + " is calculated");
                return false;
            }
        }
        return true;
    }
}

// End RolapNativeAggregate.java
