/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2016 Pentaho and others
// All Rights Reserved.
//
*/
package mondrian.rolap;


import mondrian.calc.TupleIterable;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.Member;
import mondrian.olap.fun.VisualTotalsFunDef;
import mondrian.olap.type.SetType;
import mondrian.olap.type.Type;
import mondrian.resource.MondrianResource;
import mondrian.rolap.agg.AbstractColumnPredicate;
import mondrian.rolap.agg.AndPredicate;
import mondrian.rolap.agg.ListColumnPredicate;
import mondrian.rolap.agg.ListPredicate;
import mondrian.rolap.agg.OrPredicate;
import mondrian.rolap.agg.ValueColumnPredicate;
import mondrian.rolap.sql.SqlQuery;
import mondrian.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Constructs a Pair<BitKey, StarPredicate> based on an tuple list and
 * measure, along with the string representation of the predicate.
 * Also sets the isSatisfiable flag based on whether a predicate
 * is compatible with the measure.
 *
 * This logic was extracted from RolapAggregationManager and AggregationKey.
 */
public class CompoundPredicateInfo {

    private final Pair<BitKey, StarPredicate> predicate;
    private final String predicateString;
    private final RolapMeasure measure;
    private boolean satisfiable = true;
    private boolean isTupleBased = false;

    public CompoundPredicateInfo(
        List<List<Member>> tupleList, RolapMeasure measure, Evaluator evaluator)
    {
        this.measure = measure;
        this.predicate = predicateFromTupleList(tupleList, measure, evaluator);
        this.predicateString = getPredicateString(
            getStar(measure), getPredicate());
        assert measure != null;
    }

    public StarPredicate getPredicate() {
        return predicate == null ? null : predicate.right;
    }

    public BitKey getBitKey() {
        return predicate == null ? null : predicate.left;
    }

    public String getPredicateString() {
        return predicateString;
    }

    public boolean isSatisfiable() {
        return satisfiable;
    }

    public boolean isTupleBased() {
        return isTupleBased;
    }

    public RolapCube getCube() {
        return measure.isCalculated() ? null
            : ((RolapStoredMeasure)measure).getCube();
    }
    /**
     * Returns a string representation of the predicate
     */
    public static String getPredicateString(
        RolapStar star, StarPredicate predicate)
    {
        if (star == null || predicate == null) {
            return null;
        }
        final StringBuilder buf = new StringBuilder();
        SqlQuery query =
            new SqlQuery(
                star.getSqlQueryDialect());
        buf.setLength(0);
        predicate.toSql(query, buf);
        return buf.toString();
    }

    private static RolapStar getStar(RolapMeasure measure) {
        if (measure.isCalculated()) {
            return null;
        }
        final RolapStoredMeasure storedMeasure =
            (RolapStoredMeasure) measure;
        final RolapStar.Measure starMeasure =
            (RolapStar.Measure) storedMeasure.getStarMeasure();
        assert starMeasure != null;
        return starMeasure.getStar();
    }

    private Pair<BitKey, StarPredicate> predicateFromTupleList(
        List<List<Member>> tupleList, RolapMeasure measure, Evaluator evaluator)
    {
        if (measure.isCalculated()) {
            // need a base measure to build predicates
            return null;
        }
        RolapCube cube = ((RolapStoredMeasure)measure).getCube();

        BitKey compoundBitKey;
        StarPredicate compoundPredicate;
        Map<BitKey, List<RolapCubeMember[]>> compoundGroupMap;
        boolean unsatisfiable;
        int starColumnCount = getStar(measure).getColumnCount();

        compoundBitKey = BitKey.Factory.makeBitKey(starColumnCount);
        compoundBitKey.clear();
        compoundGroupMap =
            new LinkedHashMap<BitKey, List<RolapCubeMember[]>>();
        unsatisfiable =
            makeCompoundGroup(
                starColumnCount,
                cube,
                tupleList,
                compoundGroupMap);

        if (unsatisfiable) {
            satisfiable = false;
            return null;
        }
        compoundPredicate =
            makeCompoundPredicate(compoundGroupMap, cube, evaluator);
        if (compoundPredicate != null) {
            for (BitKey bitKey : compoundGroupMap.keySet()) {
                compoundBitKey = compoundBitKey.or(bitKey);
            }
        }
        return  Pair.of(compoundBitKey, compoundPredicate);
    }

    /**
     * Groups members (or tuples) from the same compound (i.e. hierarchy) into
     * groups that are constrained by the same set of columns.
     *
     * <p>E.g.
     *
     * <pre>Members
     *     [USA].[CA],
     *     [Canada].[BC],
     *     [USA].[CA].[San Francisco],
     *     [USA].[OR].[Portland]</pre>
     *
     * will be grouped into
     *
     * <pre>Group 1:
     *     {[USA].[CA], [Canada].[BC]}
     * Group 2:
     *     {[USA].[CA].[San Francisco], [USA].[OR].[Portland]}</pre>
     *
     * <p>This helps with generating optimal form of sql.
     *
     * <p>In case of aggregating over a list of tuples, similar logic also
     * applies.
     *
     * <p>For example:
     *
     * <pre>Tuples:
     *     ([Gender].[M], [Store].[USA].[CA])
     *     ([Gender].[F], [Store].[USA].[CA])
     *     ([Gender].[M], [Store].[USA])
     *     ([Gender].[F], [Store].[Canada])</pre>
     *
     * will be grouped into
     *
     * <pre>Group 1:
     *     {([Gender].[M], [Store].[USA].[CA]),
     *      ([Gender].[F], [Store].[USA].[CA])}
     * Group 2:
     *     {([Gender].[M], [Store].[USA]),
     *      ([Gender].[F], [Store].[Canada])}</pre>
     *
     * <p>This function returns a boolean value indicating if any constraint
     * can be created from the aggregationList. It is possible that only part
     * of the aggregationList can be applied, which still leads to a (partial)
     * constraint that is represented by the compoundGroupMap.
     */
    private boolean makeCompoundGroup(
        int starColumnCount,
        RolapCube baseCube,
        List<List<Member>> aggregationList,
        Map<BitKey, List<RolapCubeMember[]>> compoundGroupMap)
    {
        // The more generalized aggregation as aggregating over tuples.
        // The special case is a tuple defined by only one member.
        int unsatisfiableTupleCount = 0;
        for (List<Member> aggregation : aggregationList) {
            if (!(aggregation.size() > 0
                && (aggregation.get(0) instanceof RolapCubeMember
                || aggregation.get(0) instanceof
                VisualTotalsFunDef.VisualTotalMember)))
            {
                ++unsatisfiableTupleCount;
                continue;
            }

            BitKey bitKey = BitKey.Factory.makeBitKey(starColumnCount);
            RolapCubeMember[] tuple = new RolapCubeMember[aggregation.size()];
            boolean tupleUnsatisfiable = false;
            int i = 0;
            for (Member member : aggregation) {
                RolapCubeMember rolapMember;
                if (member instanceof VisualTotalsFunDef.VisualTotalMember) {
                    rolapMember = (RolapCubeMember)
                        ((VisualTotalsFunDef.VisualTotalMember) member)
                            .getMember();
                } else {
                    rolapMember = (RolapCubeMember)member;
                }

                // Tuple cannot be constrained if any of the member cannot be.
                tupleUnsatisfiable =
                    makeCompoundGroupForMember(rolapMember, baseCube, bitKey);
                if (tupleUnsatisfiable) {
                    // If this tuple is unsatisfiable, skip it and try to
                    // constrain the next tuple.
                    unsatisfiableTupleCount ++;
                    break;
                }

                tuple[i] = rolapMember;
                i++;
            }

            if (!tupleUnsatisfiable && !bitKey.isEmpty()) {
                // Found tuple(columns) to constrain,
                // now add it to the compoundGroupMap
                addTupleToCompoundGroupMap(tuple, bitKey, compoundGroupMap);
            }
        }
        return (unsatisfiableTupleCount == aggregationList.size());
    }

    private void addTupleToCompoundGroupMap(
        RolapCubeMember[] tuple,
        BitKey bitKey,
        Map<BitKey, List<RolapCubeMember[]>> compoundGroupMap)
    {
        List<RolapCubeMember[]> compoundGroup = compoundGroupMap.get(bitKey);
        if (compoundGroup == null) {
            compoundGroup = new ArrayList<RolapCubeMember[]>();
            compoundGroupMap.put(bitKey, compoundGroup);
        }
        compoundGroup.add(tuple);
    }

    private boolean makeCompoundGroupForMember(
        RolapCubeMember member,
        RolapCube baseCube,
        BitKey bitKey)
    {
        RolapCubeMember levelMember = member;
        boolean memberUnsatisfiable = false;
        while (levelMember != null) {
            RolapCubeLevel level = levelMember.getLevel();
            // Only need to constrain the nonAll levels
            if (!level.isAll()) {
                RolapStar.Column column = level.getBaseStarKeyColumn(baseCube);
                if (column != null) {
                    bitKey.set(column.getBitPosition());
                } else {
                    // One level in a member causes the member to be
                    // unsatisfiable.
                    memberUnsatisfiable = true;
                    break;
                }
            }

            levelMember = levelMember.getParentMember();
        }
        return memberUnsatisfiable;
    }

    private StarPredicate makeCompoundPredicate(
        Map<BitKey, List<RolapCubeMember[]>> compoundGroupMap,
        RolapCube baseCube, Evaluator evaluator)
    {
        List<StarPredicate> compoundPredicateList =
            new ArrayList<StarPredicate> ();
        for (List<RolapCubeMember[]> group : compoundGroupMap.values()) {
            if (!SqlConstraintUtils.isDisjointTuples(group)
                && addSimpleGroupPredicate(baseCube, group, compoundPredicateList))
            {
                continue;
            }
            isTupleBased = true;
            // e.g {[USA].[CA], [Canada].[BC]}
            StarPredicate compoundGroupPredicate = null;
            List<StarPredicate> groupPredicates = new ArrayList<>(group.size());
            for (RolapCubeMember[] tuple : group) {
                // [USA].[CA]
                List<StarPredicate> tuplePredicates = new ArrayList<>(tuple.length);
                for (RolapCubeMember member : tuple) {
                    while (member != null) {
                        RolapCubeLevel level = member.getLevel();
                        if (!level.isAll()) {
                            RolapStar.Column column = level.getBaseStarKeyColumn(baseCube);
                            StarPredicate memberPredicate;
                            if (!member.isCalculated()) {
                                memberPredicate = new ValueColumnPredicate(
                                    column, member.getKey());
                            } else {
                                memberPredicate = makeCalculatedMemberPredicate(
                                    member, baseCube, evaluator);
                            }
                            tuplePredicates.add(memberPredicate);
                        }
                        // Don't need to constrain USA if CA is unique
                        if (level.isUnique()) {
                            break;
                        }
                        member = member.getParentMember();
                    }
                }
                if (!tuplePredicates.isEmpty()) {
                    groupPredicates.add(new AndPredicate(tuplePredicates));
                }
            }

            if (groupPredicates.size() == 1) {
                compoundGroupPredicate = groupPredicates.get(0);
            } else if (groupPredicates.size() > 1){
                compoundGroupPredicate = new OrPredicate(groupPredicates);
            }

            if (compoundGroupPredicate != null
                && compoundGroupPredicate instanceof OrPredicate)
            {
                // try to go for a column-based approach if full crossjoin
                compoundGroupPredicate =
                    toColumnPredicates(compoundGroupPredicate, group.size());
            }

            if (compoundGroupPredicate != null) {
                // Sometimes the compound member list does not constrain any
                // columns; for example, if only AllLevel is present.
                compoundPredicateList.add(compoundGroupPredicate);
            }
        }

        StarPredicate compoundPredicate = null;

        if (compoundPredicateList.size() > 1) {
            compoundPredicate = new OrPredicate(compoundPredicateList);
        } else if (compoundPredicateList.size() == 1) {
            compoundPredicate = compoundPredicateList.get(0);
        }

        return compoundPredicate;
    }

    /**
     * Convert a full crossjoin tuple-based predicate to a column-based one.
     * Calculated members are not supported here.
     */
    private static boolean addSimpleGroupPredicate(
        RolapCube baseCube, List<RolapCubeMember[]> group,
        List<StarPredicate> compoundPredicateList)
    {
        int tupleSize = group.size() == 0 ? 0 : group.get(0).length;
        if (tupleSize == 0) {
            return false;
        }
        Set<RolapCubeLevel> levels = new LinkedHashSet<>();
        Map<RolapStar.Column, Set<StarColumnPredicate>> colPredicates = new LinkedHashMap<>(tupleSize);
        Map<RolapCubeLevel, List<StarPredicate>> multiLevelPredicates = new LinkedHashMap<>();
        for (int i = 0; i < group.size(); i++) {
            RolapCubeMember[] tuple = group.get(i);
            if (tupleSize != tuple.length) {
                return false;
            }
            for (RolapCubeMember member : tuple) {
                if (levels.add(member.getLevel()) && i > 0) {
                    return false;
                }
                if (!memberPredicate(member, baseCube, colPredicates, multiLevelPredicates)) {
                    return false;
                }
            }
        }

        int cardinality = 1;
        for (Set<StarColumnPredicate> colPredicate : colPredicates.values()) {
            if (colPredicate != null) {
                cardinality *= colPredicate.size();
            }
        }
        if (cardinality <= group.size()) {
            List<StarPredicate> predicates = new ArrayList<>(colPredicates.size());
            for (Map.Entry<RolapStar.Column, Set<StarColumnPredicate>> entry : colPredicates.entrySet()) {
                predicates.add(new ListColumnPredicate(entry.getKey(), new ArrayList<>(entry.getValue())));
            }

            if (!predicates.isEmpty()) {
                if (!multiLevelPredicates.isEmpty()) {
                    for (RolapCubeLevel level : multiLevelPredicates.keySet()) {
                        predicates.add(new OrPredicate(multiLevelPredicates.get(level)));
                    }
                }
                compoundPredicateList.add(
                    predicates.size() == 1 ? predicates.get(0) : new AndPredicate(predicates));
                return true;
            }
        }
        return false;
    }

    private static boolean memberPredicate(
        RolapCubeMember member, RolapCube baseCube,
        Map<RolapStar.Column, Set<StarColumnPredicate>> singleLevelPredicates,
        Map<RolapCubeLevel, List<StarPredicate>> multiLevelPredicates)
    {
        boolean isFirstLevel = true;
        List<StarPredicate> memberPredicates = new ArrayList<>();
        while (member != null) {
            if (member.isCalculated()) {
                return false;
            }
            RolapCubeLevel level = member.getLevel();
            if (!level.isAll()) {
                RolapStar.Column column = member.getLevel().getBaseStarKeyColumn(baseCube);
                if (isFirstLevel && level.isUnique()) {
                    Set<StarColumnPredicate> predicates =
                        singleLevelPredicates.containsKey(column)
                            ? singleLevelPredicates.get(column)
                            : new LinkedHashSet<StarColumnPredicate>();
                    predicates.add(new ValueColumnPredicate(column, member.getKey()));
                    singleLevelPredicates.put(column, predicates);
                    break;
                } else {
                    memberPredicates.add(
                        new ValueColumnPredicate(column, member.getKey()));
                    if (level.isUnique()) {
                        AndPredicate predicate = new AndPredicate(memberPredicates);
                        boolean exists = false;
                        List<StarPredicate> multiPredicates =
                            multiLevelPredicates.containsKey(level)
                                ? multiLevelPredicates.get(level)
                                : new ArrayList<StarPredicate>();
                        for (StarPredicate starPredicate : multiPredicates) {
                            // simple HashSet will not work for uniqueness here
                            if (starPredicate.equalConstraint(predicate)) {
                                exists = true;
                                break;
                            }
                        }
                        if (!exists) {
                            multiPredicates.add(predicate);
                            multiLevelPredicates.put(level, multiPredicates);
                        }
                        break;
                    }
                }
            }
            member = member.getParentMember();
            isFirstLevel = false;
        }
        return true;
    }

    /**
     * Convert a full crossjoin tuple-based predicate to a column-based one
     */
    private static StarPredicate toColumnPredicates(
            final StarPredicate predicate, final int nbrRows)
    {
        HashMap<RolapStar.Column, Set<StarColumnPredicate>> map =
            new LinkedHashMap<RolapStar.Column, Set<StarColumnPredicate>>();
        extractColumnPredicates(predicate, map);
        List<StarPredicate> predicates = new ArrayList<StarPredicate>(map.size());
        // convert to column in (val0..valn)
        int columnCardinalities = 1;
        for (Map.Entry<RolapStar.Column, Set<StarColumnPredicate>> entry : map.entrySet()) {
            Set<StarColumnPredicate> columnPredicates = entry.getValue();
            columnCardinalities *= columnPredicates.size();
            ArrayList<StarColumnPredicate> list =
                new ArrayList<StarColumnPredicate>(columnPredicates.size());
            list.addAll(columnPredicates);
            predicates.add(new ListColumnPredicate(entry.getKey(), list));
        }
        if (columnCardinalities > nbrRows) {
            // may not be a total crossjoin at column level
            // return original
            return predicate;
        }
        return new AndPredicate(predicates);
    }

    /**
     * extract all distinct ValueColumnPredicate instances and map by column
     */
    private static void extractColumnPredicates(
        StarPredicate predicate,
        HashMap<RolapStar.Column, Set<StarColumnPredicate>> map)
    {
        if (predicate instanceof ListPredicate) {
            ListPredicate listPredicate = (ListPredicate) predicate;
            for (StarPredicate childPredicate : listPredicate.getChildren()) {
                extractColumnPredicates(childPredicate, map);
            }
        } else if (predicate instanceof ValueColumnPredicate
                || predicate instanceof ListColumnPredicate) {
            AbstractColumnPredicate valuePredicate =
                (AbstractColumnPredicate) predicate;
            Set<StarColumnPredicate> list =
                map.get(valuePredicate.getConstrainedColumn());
            if (list == null) {
                list = new LinkedHashSet<StarColumnPredicate>();
                map.put(valuePredicate.getConstrainedColumn(), list);
            }
            list.add(valuePredicate);
        }
    }

    private StarPredicate makeCalculatedMemberPredicate(
        RolapCubeMember member, RolapCube baseCube, Evaluator evaluator)
    {
        assert member.getExpression() instanceof ResolvedFunCall;

        ResolvedFunCall fun = (ResolvedFunCall) member.getExpression();

        final Exp exp = fun.getArg(0);
        final Type type = exp.getType();

        if (type instanceof SetType) {
            return makeSetPredicate(exp, evaluator);
        } else if (type.getArity() == 1) {
            return makeUnaryPredicate(member, baseCube, evaluator);
        } else {
            throw MondrianResource.instance()
                .UnsupportedCalculatedMember.ex(member.getName(), null);
        }
    }

    private StarPredicate makeUnaryPredicate(
        RolapCubeMember member, RolapCube baseCube, Evaluator evaluator)
    {
      TupleConstraintStruct constraint = new TupleConstraintStruct();
      SqlConstraintUtils
          .expandSupportedCalculatedMember(member, evaluator, constraint);
      List<Member> expandedMemberList = constraint.getMembers();
      for (Member checkMember : expandedMemberList) {
          if (checkMember == null
              || checkMember.isCalculated()
              || !(checkMember instanceof RolapCubeMember))
          {
              throw MondrianResource.instance()
                  .UnsupportedCalculatedMember.ex(member.getName(), null);
          }
      }
      List<StarPredicate> predicates =
          new ArrayList<StarPredicate>(expandedMemberList.size());
      for (Member iMember : expandedMemberList) {
          RolapCubeMember iCubeMember = ((RolapCubeMember)iMember);
          RolapCubeLevel iLevel = iCubeMember.getLevel();
          RolapStar.Column iColumn = iLevel.getBaseStarKeyColumn(baseCube);
          Object iKey = iCubeMember.getKey();
          StarPredicate iPredicate = new ValueColumnPredicate(iColumn, iKey);
          predicates.add(iPredicate);
      }
      StarPredicate r = null;
      if (predicates.size() == 1) {
          r = predicates.get(0);
      } else {
          r = new OrPredicate(predicates);
      }
      return r;
    }

    private StarPredicate makeSetPredicate(
        final Exp exp, Evaluator evaluator)
    {
      TupleIterable evaluatedSet =
          evaluator.getSetEvaluator(
              exp, true).evaluateTupleIterable();
      ArrayList<StarPredicate> orList = new ArrayList<StarPredicate>();
      OrPredicate orPredicate = null;
      for (List<Member> complexSetItem : evaluatedSet) {
          List<StarPredicate> andList = new ArrayList<StarPredicate>();
          for (Member singleSetItem : complexSetItem) {
              final List<List<Member>> singleItemList =
                  Collections.singletonList(
                      Collections.singletonList(singleSetItem));
              StarPredicate singlePredicate = predicateFromTupleList(
                  singleItemList,
                  measure, evaluator).getValue();
              andList.add(singlePredicate);
          }
          AndPredicate andPredicate = new AndPredicate(andList);
          orList.add(andPredicate);
          orPredicate  = new OrPredicate(orList);
      }
      return orPredicate;
    }
}

// End CompoundPredicateInfo.java
