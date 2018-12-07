/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2015 Pentaho Corporation..  All rights reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.Role.RollupPolicy;
import mondrian.rolap.ManyToManyUtil;
import mondrian.rolap.RolapAggregator;
import mondrian.rolap.RolapCubeHierarchy;
import mondrian.rolap.RolapEvaluator;
import mondrian.rolap.SqlConstraintUtils;

import org.apache.log4j.Logger;

import org.eigenbase.util.property.IntegerProperty;

import java.util.*;

/**
 * Definition of the <code>AGGREGATE</code> MDX function.
 *
 * @author jhyde
 * @since 2005/8/14
 */
public class AggregateFunDef extends AbstractAggregateFunDef {

    private static final String TIMING_NAME =
        AggregateFunDef.class.getSimpleName();
    private static final Logger LOGGER =
        Logger.getLogger(AggregateFunDef.class);
    static final ReflectiveMultiResolver resolver =
        new ReflectiveMultiResolver(
            "Aggregate", "Aggregate(<Set>[, <Numeric Expression>])",
            "Returns a calculated value using the appropriate aggregate function, based on the context of the query.",
            new String[]{"fnx", "fnxn"},
            AggregateFunDef.class);

    /**
     * Creates an AggregateFunDef.
     *
     * @param dummyFunDef Dummy function
     */
    public AggregateFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc = compiler.compileList(call.getArg(0));
        final Calc calc =
            call.getArgCount() > 1
                ? compiler.compileScalar(call.getArg(1), true)
                : new ValueCalc(call);
        final Exp memberExp =
            call.getArgCount() > 1 ? call.getArg(1) : null;
        return new AggregateCalc(call, listCalc, calc, memberExp);
    }

    public static class AggregateCalc extends GenericCalc {
        private final ListCalc listCalc;
        private final Calc calc;
        private final Exp memberExp;

        public AggregateCalc(
            Exp exp, ListCalc listCalc, Calc calc, Exp memberExp)
        {
            super(exp, new Calc[]{listCalc, calc});
            this.listCalc = listCalc;
            this.calc = calc;
            this.memberExp = memberExp;
        }

        public AggregateCalc(Exp exp, ListCalc listCalc, Calc calc) {
            this(exp, listCalc, calc, null);
        }

        public Object evaluate(Evaluator evaluator) {
            evaluator.getTiming().markStart(TIMING_NAME);
            final int savepoint = evaluator.savepoint();
            try {
                Member member = null;
                Calc memberCalc = this.calc;
                if (memberExp != null) {
                    ExpCompiler compiler = null;
                    if (memberExp instanceof MemberExpr) {
                        member = ((MemberExpr) memberExp).getMember();
                    } else if (memberExp instanceof ResolvedFunCall
                            && ((ResolvedFunCall) memberExp).getFunDef() instanceof StrToMemberFunDef) {
                        compiler = evaluator.getQuery().createCompiler();
                        MemberCalc calc = compiler.compileMember(memberExp);
                        member = calc.evaluateMember(evaluator);
                    }

                    if (member != null && member.isMeasure() && !member.isCalculated()) {
                        evaluator.setContext(member);
                        if (compiler != null) {
                            memberCalc = compiler.compileScalar(new MemberExpr(member), true);
                        }
                    } else {
                        member = null;
                        // Since the expression is not a base measure, we won't
                        // attempt to determine the aggregator and will simply sum.
                        LOGGER.warn(
                            "Unable to determine aggregator for non-base measures "
                            + "in 2nd parameter of Aggregate(), summing: " + memberExp.toString());
                    }
                }

                if(exp instanceof ResolvedFunCall) {
                    ResolvedFunCall call = (ResolvedFunCall) exp;
                    RolapEvaluator manyToManyEval =
                        ManyToManyUtil.getManyToManyEvaluator(
                            (RolapEvaluator)evaluator);
                    SchemaReader schemaReader = evaluator.getSchemaReader();
                    NativeEvaluator nativeEvaluator =
                        schemaReader.getNativeSetEvaluator(
                            call.getFunDef(), call.getArgs(), manyToManyEval, this);
                    if (nativeEvaluator != null) {
                        Double d = (Double) nativeEvaluator.execute(ResultStyle.VALUE);
                        return d == FunUtil.DoubleNull ? null : d;
                    }
                }

                TupleList list = evaluateCurrentList(listCalc, evaluator);
                boolean pushdownAggregation = true;
                if (member == null && exp instanceof ResolvedFunCall
                    && ((ResolvedFunCall)exp).getArgCount() > 1)
                {
                    pushdownAggregation = false;
                } else if (!(exp instanceof DummyExp)) {
                    Set<Member> members = new HashSet<Member>();
                    exp.accept(new MemberExtractingVisitor(members, null, false));
                    checkParents:
                    for (Member m : members) {
                        Member parent = m.getParentMember();
                        while (parent != null && !parent.isAll()) {
                            if (members.contains(parent)) {
                                pushdownAggregation = false;
                                break checkParents;
                            }
                            parent = parent.getParentMember();
                        }
                    }
                }
                return aggregate(memberCalc, evaluator, list, pushdownAggregation);
            } finally {
                evaluator.restore(savepoint);
                evaluator.getTiming().markEnd(TIMING_NAME);
            }
        }

        /**
         * Computes an expression for each element of a list, and aggregates
         * the result according to the evaluation context's current aggregation
         * strategy.
         *
         * @param calc Compiled expression to evaluate a scalar
         * @param evaluator Evaluation context
         * @param tupleList List of members or tuples
         * @return Aggregated result
         */
        public static Object aggregate(
            Calc calc,
            Evaluator evaluator,
            TupleList tupleList,
            boolean pushdownAggregation)
        {
            Aggregator aggregator =
                (Aggregator) evaluator.getProperty(
                    Property.AGGREGATION_TYPE.name, null);
            if (aggregator == null) {
                throw newEvalException(
                    null,
                    "Could not find an aggregator in the current evaluation context");
            }
            Aggregator rollup = aggregator.getRollup();
            if (rollup == null) {
                throw newEvalException(
                    null,
                    "Don't know how to rollup aggregator '" + aggregator + "'");
            }
            if (!shouldPushdownAggregation(pushdownAggregation, aggregator, tupleList, evaluator)) {
                return aggregate(evaluator, calc, rollup, tupleList);
            }

            // All that follows is logic for distinct count and aggregations
            // with many to many members.
            if (tupleList.size() == 0) {
                return DoubleNull;
            }

            // Optimize the list
            // E.g.
            // List consists of:
            //  (Gender.[All Gender], [Product].[All Products]),
            //  (Gender.[F], [Product].[Drink]),
            //  (Gender.[M], [Product].[Food])
            // Can be optimized to:
            //  (Gender.[All Gender], [Product].[All Products])
            //
            // Similar optimization can also be done for list of members.

            boolean unlimitedIn  = false;
            if (evaluator instanceof RolapEvaluator) {
                unlimitedIn =
                    ((RolapEvaluator) evaluator).getDialect()
                        .supportsUnlimitedValueList();
            }
            boolean tupleSizeWithinInListSize =
                tupleList.size()
                    <= MondrianProperties.instance().MaxConstraints.get();
            if (unlimitedIn || tupleSizeWithinInListSize) {
                // If the DBMS does not have an upper limit on IN list
                // predicate size, then don't attempt any list
                // optimization, since the current algorithm is
                // very slow.  May want to revisit this if someone
                // improves the algorithm.
            } else {
                tupleList = optimizeTupleList(evaluator, tupleList);
                if (checkIfAggregationSizeIsTooLarge(
                        tupleList, requiresPushdownAggregation(aggregator))) {
                    return aggregate(evaluator, calc, rollup, tupleList);
                }
            }

            // Can't aggregate distinct-count values in the same way
            // which is used for other types of aggregations. To evaluate a
            // distinct-count across multiple members, we need to gather
            // the members together, then evaluate the collection of
            // members all at once. To do this, we postpone evaluation,
            // and create a lambda function containing the members.

            // TODO: Note, this doesn't support Aggregating calculated Members.
            // We should write tests demonstrating distinct count failing in 
            // this scenario.

            Evaluator evaluator2 =
                evaluator.pushAggregation(tupleList);
            // cancel nonEmpty context
            evaluator2.setNonEmpty(false);
            return evaluator2.evaluateCurrent();
        }

        private static Object aggregate(Evaluator evaluator, Calc calc, Aggregator rollup, TupleList tupleList) {
            tupleList =
                ManyToManyUtil.processManyToManyMembers(
                    evaluator, tupleList);
            final int savepoint = evaluator.savepoint();
            try {
                evaluator.setNonEmpty(false);
                final Object o =
                    rollup.aggregate(
                        evaluator, tupleList, calc);
                return o;
            } finally {
                evaluator.restore(savepoint);
            }
        }

        /**
         * We should push down aggregations for distinct counts and
         * for slicers that contain many to many dimensions to avoid
         * large slicers due to many to many use cases.
         */
        private static boolean shouldPushdownAggregation(
            boolean pushdownAggregation,
            Aggregator aggregator,
            TupleList tupleList,
            Evaluator ev)
        {
            if (requiresPushdownAggregation(aggregator)) {
                return true;
            }
            if (!pushdownAggregation) {
                return false;
            }
            RolapEvaluator rolapEvaluator = null;
            boolean isSlicer = false;
            if (ev instanceof RolapEvaluator) {
                rolapEvaluator = (RolapEvaluator)ev;
                isSlicer = rolapEvaluator.getSlicerTuples() == tupleList;
                if (isSlicer && rolapEvaluator.doesSlicerSupportAggregatePushdown()) {
                    return true;
                }
            }
            Set<Member> argMembers = new HashSet<>();
            for (List<Member> members : tupleList) {
                for (Member member : members) {
                    if (member.isMeasure() || member.isCalculated())
                    {
                        return false;
                    }
                    if (!isSlicer) {
                        argMembers.add(member);
                    }
                }
            }
            for (Member m : argMembers) {
                Member parentMember = m.getParentMember();
                while (parentMember != null) {
                    if (argMembers.contains(parentMember)) {
                        // if tuple list contains parent and its children
                        // Mondrian should not push down the calculation
                        // and has to double sum or count
                        return false;
                    }
                    parentMember = parentMember.getParentMember();
                }
            }

            if (rolapEvaluator != null) {
                for (Member member : rolapEvaluator.getSlicerMembers()) {
                    if (member.isCalculated()
                        && member != rolapEvaluator.getExpanding())
                    {
                        return false;
                    }
                }

                if (isSlicer) {
                    rolapEvaluator.setSlicerSupportAggregatePushdown(true);
                }
            }
            return true;
        }

        private static boolean requiresPushdownAggregation(Aggregator aggregator) {
            return aggregator == RolapAggregator.DistinctCount
                || aggregator == RolapAggregator.Avg;
        }

        /**
         * Analyzes a list of tuples and determines if the list can
         * be safely optimized. If a member of the tuple list is on
         * a hierarchy for which a rollup policy of PARTIAL is set,
         * it is not safe to optimize that list.
         */
        private static boolean canOptimize(
            Evaluator evaluator,
            TupleList tupleList)
        {
            if (SqlConstraintUtils.isDisjointTuple(tupleList)) {
                return false;
            }
            // If members of this hierarchy are controlled by a role which
            // enforces a rollup policy of partial, we cannot safely
            // optimize the tuples list as it might end up rolling up to
            // the parent while not all children are actually accessible.
            if (tupleList.size() > 0) {
                for (Member member : tupleList.get(0)) {
                    final RollupPolicy policy =
                        evaluator.getSchemaReader().getRole()
                            .getAccessDetails(member.getHierarchy())
                            .getRollupPolicy();
                    if (policy == RollupPolicy.PARTIAL) {
                        return false;
                    }
                }
            }
            return true;
        }

        public static TupleList optimizeTupleList(
            Evaluator evaluator, TupleList tupleList)
        {
            if (!canOptimize(evaluator, tupleList)) {
                return tupleList;
            }

            // FIXME: We remove overlapping tuple entries only to pass
            // AggregationOnDistinctCountMeasuresTest
            // .testOptimizeListWithTuplesOfLength3 on Access. Without
            // the optimization, we generate a statement 7000
            // characters long and Access gives "Query is too complex".
            // The optimization is expensive, so we only want to do it
            // if the DBMS can't execute the query otherwise.
            if (false) {
                tupleList = removeOverlappingTupleEntries(tupleList);
            }
            tupleList =
                optimizeChildren(
                    tupleList,
                    evaluator.getSchemaReader(),
                    evaluator.getMeasureCube());
            return tupleList;
        }

        /**
         * In case of distinct count aggregation if a tuple which is a super
         * set of other tuples in the set exists then the child tuples can be
         * ignored.
         *
         * <p>For example. A list consisting of:
         *  (Gender.[All Gender], [Product].[All Products]),
         *  (Gender.[F], [Product].[Drink]),
         *  (Gender.[M], [Product].[Food])
         * Can be optimized to:
         *  (Gender.[All Gender], [Product].[All Products])
         *
         * @param list List of tuples
         */
        public static TupleList removeOverlappingTupleEntries(
            TupleList list)
        {
            TupleList trimmedList = list.cloneList(list.size());
            Member[] tuple1 = new Member[list.getArity()];
            Member[] tuple2 = new Member[list.getArity()];
            final TupleCursor cursor1 = list.tupleCursor();
            while (cursor1.forward()) {
                cursor1.currentToArray(tuple1, 0);
                if (trimmedList.isEmpty()) {
                    trimmedList.addTuple(tuple1);
                } else {
                    boolean ignore = false;
                    final TupleIterator iterator = trimmedList.tupleIterator();
                    while (iterator.forward()) {
                        iterator.currentToArray(tuple2, 0);
                        if (isSuperSet(tuple1, tuple2)) {
                            iterator.remove();
                        } else if (isSuperSet(tuple2, tuple1)
                            || isEqual(tuple1, tuple2))
                        {
                            ignore = true;
                            break;
                        }
                    }
                    if (!ignore) {
                        trimmedList.addTuple(tuple1);
                    }
                }
            }
            return trimmedList;
        }

        /**
         * Returns whether tuple1 is a superset of tuple2.
         *
         * @param tuple1 First tuple
         * @param tuple2 Second tuple
         * @return boolean Whether tuple1 is a superset of tuple2
         */
        public static boolean isSuperSet(Member[] tuple1, Member[] tuple2) {
            int parentLevelCount = 0;
            for (int i = 0; i < tuple1.length; i++) {
                Member member1 = tuple1[i];
                Member member2 = tuple2[i];

                if (!member2.isChildOrEqualTo(member1)) {
                    return false;
                }
                if (member1.getLevel().getDepth()
                    < member2.getLevel().getDepth())
                {
                    parentLevelCount++;
                }
            }
            return parentLevelCount > 0;
        }

        private static boolean checkIfAggregationSizeIsTooLarge(
            List list, boolean fail)
        {
            final IntegerProperty property =
                MondrianProperties.instance().MaxConstraints;
            final int maxConstraints = property.get();
            if (list.size() > maxConstraints) {
                if (fail) {
                    throw newEvalException(
                        null,
                        "Aggregation is not supported over a list"
                        + " with more than " + maxConstraints + " predicates"
                        + " (see property " + property.getPath() + ")");
                }
                return true;
            }
            return false;
        }

        public boolean dependsOn(Hierarchy hierarchy) {
            if (hierarchy.getDimension().isMeasures()) {
                return true;
            }
            return anyDependsButFirst(getCalcs(), hierarchy);
        }

        /**
         * In distinct Count aggregation, if tuple list is a result
         * m.children * n.children then it can be optimized to m * n
         *
         * <p>
         * E.g.
         * List consist of:
         *  (Gender.[F], [Store].[USA]),
         *  (Gender.[F], [Store].[USA].[OR]),
         *  (Gender.[F], [Store].[USA].[CA]),
         *  (Gender.[F], [Store].[USA].[WA]),
         *  (Gender.[F], [Store].[CANADA])
         *  (Gender.[M], [Store].[USA]),
         *  (Gender.[M], [Store].[USA].[OR]),
         *  (Gender.[M], [Store].[USA].[CA]),
         *  (Gender.[M], [Store].[USA].[WA]),
         *  (Gender.[M], [Store].[CANADA])
         * Can be optimized to:
         *  (Gender.[All Gender], [Store].[USA])
         *  (Gender.[All Gender], [Store].[CANADA])
         *
         *
         * @param tuples Tuples
         * @param reader Schema reader
         * @param baseCubeForMeasure Cube
         * @return xxxx
         */
        public static TupleList optimizeChildren(
            TupleList tuples,
            SchemaReader reader,
            Cube baseCubeForMeasure)
        {
            Map<Member, Integer>[] membersOccurencesInTuple =
                membersVersusOccurencesInTuple(tuples);
            int tupleLength = tuples.getArity();

            //noinspection unchecked
            Set<Member>[] sets = new Set[tupleLength];
            boolean optimized = false;
            for (int i = 0; i < tupleLength; i++) {
                if (areOccurencesEqual(membersOccurencesInTuple[i].values())) {
                    Set<Member> members = membersOccurencesInTuple[i].keySet();
                    int originalSize = members.size();
                    sets[i] =
                        optimizeMemberSet(
                            new LinkedHashSet<Member>(members),
                            reader,
                            baseCubeForMeasure);
                    if (sets[i].size() != originalSize) {
                        optimized = true;
                    }
                } else {
                    sets[i] =
                        new LinkedHashSet<Member>(
                            membersOccurencesInTuple[i].keySet());
                }
            }
            if (optimized) {
                return crossProd(sets);
            }
            return tuples;
        }

        /**
         * Finds member occurrences in tuple and generates a map of Members
         * versus their occurrences in tuples.
         *
         * @param tupleList List of tuples
         * @return Map of the number of occurrences of each member in a tuple
         */
        public static Map<Member, Integer>[] membersVersusOccurencesInTuple(
            TupleList tupleList)
        {
            int tupleLength = tupleList.getArity();
            //noinspection unchecked
            Map<Member, Integer>[] counters = new Map[tupleLength];
            for (int i = 0; i < counters.length; i++) {
                counters[i] = new LinkedHashMap<Member, Integer>();
            }
            for (List<Member> tuple : tupleList) {
                for (int i = 0; i < tuple.size(); i++) {
                    Member member = tuple.get(i);
                    Map<Member, Integer> map = counters[i];
                    if (map.containsKey(member)) {
                        Integer count = map.get(member);
                        map.put(member, ++count);
                    } else {
                        map.put(member, 1);
                    }
                }
            }
            return counters;
        }

        private static Set<Member> optimizeMemberSet(
            Set<Member> members,
            SchemaReader reader,
            Cube baseCubeForMeasure)
        {
            boolean didOptimize;
            Set<Member> membersToBeOptimized = new LinkedHashSet<Member>();
            Set<Member> optimizedMembers = new LinkedHashSet<Member>();
            while (members.size() > 0) {
                Iterator<Member> iterator = members.iterator();
                Member first = iterator.next();
                if (first.isAll()) {
                    optimizedMembers.clear();
                    optimizedMembers.add(first);
                    return optimizedMembers;
                }
                membersToBeOptimized.add(first);
                iterator.remove();

                Member firstParentMember = first.getParentMember();
                while (iterator.hasNext()) {
                    Member current =  iterator.next();
                    if (current.isAll()) {
                        optimizedMembers.clear();
                        optimizedMembers.add(current);
                        return optimizedMembers;
                    }

                    Member currentParentMember = current.getParentMember();
                    if (firstParentMember == null
                        && currentParentMember == null
                        || (firstParentMember != null
                        && firstParentMember.equals(currentParentMember)))
                    {
                        membersToBeOptimized.add(current);
                        iterator.remove();
                    }
                }

                int childCountOfParent = -1;
                if (firstParentMember != null) {
                    childCountOfParent =
                        getChildCount(firstParentMember, reader);
                }
                if (childCountOfParent != -1
                    && membersToBeOptimized.size() == childCountOfParent
                    && canOptimize(firstParentMember, baseCubeForMeasure, reader))
                {
                    optimizedMembers.add(firstParentMember);
                    didOptimize = true;
                } else {
                    optimizedMembers.addAll(membersToBeOptimized);
                    didOptimize = false;
                }
                membersToBeOptimized.clear();

                if (members.size() == 0 && didOptimize) {
                    Set temp = members;
                    members = optimizedMembers;
                    optimizedMembers = temp;
                }
            }
            return optimizedMembers;
        }

        /**
         * Returns whether tuples are equal. They must have the same length.
         *
         * @param tuple1 First tuple
         * @param tuple2 Second tuple
         * @return whether tuples are equal
         */
        private static boolean isEqual(Member[] tuple1, Member[] tuple2) {
            for (int i = 0; i < tuple1.length; i++) {
                if (!tuple1[i].getUniqueName().equals(
                        tuple2[i].getUniqueName()))
                {
                    return false;
                }
            }
            return true;
        }

        private static boolean canOptimize(
            Member parentMember,
            Cube baseCube,
            SchemaReader reader)
        {
            if (parentMember.isAll()
                && parentMember.getHierarchy() instanceof RolapCubeHierarchy
                && ((RolapCubeHierarchy)parentMember.getHierarchy()).isManyToMany())
            {
                return false;
            }
            if (reader.getRole().getAccessDetails(parentMember.getHierarchy())
                    .hasInaccessibleDescendants(parentMember))
            {
                return false;
            }
            return dimensionJoinsToBaseCube(
                parentMember.getDimension(), baseCube)
                || !parentMember.isAll();
        }

        private static TupleList crossProd(Set<Member>[] sets) {
            final List<TupleList> tupleLists = new ArrayList<TupleList>();
            for (Set<Member> set : sets) {
                tupleLists.add(
                    new UnaryTupleList(
                        new ArrayList<Member>(set)));
            }
            if (tupleLists.size() == 1) {
                return tupleLists.get(0);
            }
            return CrossJoinFunDef.mutableCrossJoin(tupleLists);
        }

        private static boolean dimensionJoinsToBaseCube(
            Dimension dimension,
            Cube baseCube)
        {
            HashSet<Dimension> dimensions = new HashSet<Dimension>();
            dimensions.add(dimension);
            return baseCube.nonJoiningDimensions(dimensions).size() == 0;
        }

        private static int getChildCount(
            Member parentMember,
            SchemaReader reader)
        {
            int childrenCountFromCache =
                reader.getChildrenCountFromCache(parentMember);
            if (childrenCountFromCache != -1) {
                return childrenCountFromCache;
            }

            // getMemberChildren() is a very expensive operation for large dimensions,
            // use the min threshold for large dims, probably need to define a separate property
            // if this works

            Level childLevel = parentMember.getLevel().getChildLevel();
            final int minThreshold = MondrianProperties.instance()
                .NativizeMinThreshold.get();
            if (getLevelCardinality(reader.getSchema().getSchemaReader(), childLevel) > minThreshold) {
                return -1;
            }
            return reader.getMemberChildren(parentMember).size();
        }

        private static long getLevelCardinality(SchemaReader schema, Level level) {
            if (cardinalityIsKnown(level)) {
                return level.getApproxRowCount();
            }
            return schema.getLevelCardinality(level, false, true);
        }

        private static boolean cardinalityIsKnown(Level level) {
            return level.getApproxRowCount() > 0;
        }
    }
}

// End AggregateFunDef.java
