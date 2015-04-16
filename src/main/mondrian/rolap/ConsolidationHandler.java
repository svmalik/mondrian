/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2004-2005 TONBELLER AG
 * Copyright (C) 2005-2005 Julian Hyde
 * Copyright (C) 2005-2015 Pentaho and others
 * All Rights Reserved.
 */

package mondrian.rolap;


import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;
import mondrian.olap.MondrianDef;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.DescendantsCrossJoinArg;
import mondrian.rolap.sql.MemberListCrossJoinArg;
import mondrian.rolap.sql.SqlQuery;
import mondrian.spi.Dialect;

/**
 *  A ConsolidationHandler knows how to construct SQL for a given use case, such as Count or Sum
 *  And how to augment constraints with LevelMembers.
 *
 */
public abstract class ConsolidationHandler {

    private ConsolidationMembers members;

    /**
     * Add a level member to the level member object.
     * @param matchedMember the member that has been matched against. This is a 'local' member
     *                      belonging to a particular constraint.
     * @param expression The expression describing the column. This will be a MondrianDef.Column
     * @param member The member from a different constrint that was matched.
     */
    public void addLevelMember(RolapMember matchedMember, MondrianDef.Expression expression,
                               RolapMember member) {
        if (members == null) {
            members = new ConsolidationMembers(matchedMember, expression);
        }
        members.addMember(member);

    }

    /**
     * Add a member that is part of a cross join arg to the level member object.
     *
     * @param matchedMember the member that has been matched against. This is a 'local' member
     *                      belonging to a particular constraint.
     * @param expression    The expression describing the column. This will be a MondrianDef.Column
     * @param member        The member from a different constrint that was matched.
     */
    public void addCrossJoinLevelMember(RolapMember matchedMember,
                                        MondrianDef.Expression expression, RolapMember member) {

        if (members == null) {
            members = new ConsolidationMembers(matchedMember, expression, true);
        }
        if (members.isCrossJoin()) {
            members.addMember(member);
        }
    }

    /**
     * Add a member this is part of a descendant cross join arg to the level member object.
     *
     * @param matchedMember the member that has been matched against. This is a 'local' member
     *                      belonging to a particular constraint.
     * @param expression    The expression describing the column. This will be a MondrianDef.Column
     * @param member        The member from a different constrint that was matched.
     */
    public void addDescendentCrossJoinLevelMember(RolapMember matchedMember,
                                                  MondrianDef.Expression expression,
                                                  RolapMember member) {

        if (members == null) {
            members = new ConsolidationMembers(matchedMember, expression, true, true);
        }
        if (members.isCrossJoin() && members.isDescendantCrossJoin()) {
            members.addMember(member);
        }
    }

    /**
     * Get the LevelMembers objects
     * @return
     */
    public ConsolidationMembers getLevelMembers() {
        return members;
    }

    /**
     * clear the level members.
     */
    public void clearMembers() {
        this.members = null;
    }

    public CrossJoinArg augmentCrossJoinArg(SqlQuery sqlQuery, RolapLevel level,
                                            RolapStar.Column column, RolapEvaluator eval,
                                            AggStar aggStar, CrossJoinArg arg) {
        CrossJoinArg copy = arg;
        ConsolidationMembers members = getLevelMembers();
        if (members != null && members.isCrossJoin() && members.getLevelExpression()
                                                               .equals(column.getExpression())) {
            members.setColumnExpression(
                SqlConstraintUtils.getColumnExpr(sqlQuery, aggStar, column));
            members.setStarColumn(column);
            Set<RolapMember> allMembers = new LinkedHashSet<RolapMember>(arg.getMembers());
            allMembers.addAll(
                members.getMembers()); //don't add matched member-that should be in the arg already
            if (arg instanceof MemberListCrossJoinArg) {
                MemberListCrossJoinArg memArg = (MemberListCrossJoinArg) arg;
                copy = MemberListCrossJoinArg.create(eval, new ArrayList<RolapMember>(allMembers),
                    memArg.isRestrictMemberTypes(), memArg.isExclude());
            } else if (arg instanceof DescendantsCrossJoinArg && members.isDescendantCrossJoin()) {
                copy = MemberListCrossJoinArg.create(eval, new ArrayList<RolapMember>(allMembers),
                    true, false);
            }
        }
        return copy;
    }

    public void configureQuery(SqlQuery sqlQuery, AggStar aggStar, RolapEvaluator evaluator,
                               RolapCube baseCube, boolean restrictMemberTypes,
                               RolapStar.Column column, String columnExpression,
                               String subqueryAlias, String value) {
        ConsolidationMembers members = getLevelMembers();


        if (members != null && members.getLevelExpression().equals(column.getExpression())
            && members.getMembers().size() > 0) {
            List<RolapMember> set = members.getAllMembers();
            final int levelIndex = set.get(0).getHierarchy().getLevels().length - 1;
            RolapLevel levelForWhere = (RolapLevel) set.get(0)
                                                       .getHierarchy()
                                                       .getLevels()[levelIndex];
            // build where constraint
            String where = SqlConstraintUtils.generateSingleValueInExpr(sqlQuery, baseCube, aggStar,
                set, levelForWhere, restrictMemberTypes, false, false);
            if (!where.equals("")) {
                members.setColumnExpression(columnExpression);
                members.setStarColumn(column);
                // M2M
                if (subqueryAlias != null) {
                    SqlQuery q = sqlQuery.getSubQuery(subqueryAlias);
                    if (q != null) {
                        // query must have from already if there is no subquery
                        // otherwise the sub query has it
                        if (!sqlQuery.hasFrom(column.getTable().getRelation(), null)) {
                            sqlQuery.addFrom(column.getTable().getRelation(),
                                column.getTable().getRelation().getAlias(), false);
                        }
                        // ensure the subquery selects the row we want
                        q.addSelect(columnExpression, null);
                        // add the column to the where clause of the outer query
                        List<String> expressions = sqlQuery.subwhereExpr.get(subqueryAlias);
                        if (expressions == null) {
                            expressions = new ArrayList<String>();
                            sqlQuery.subwhereExpr.put(subqueryAlias, expressions);
                            sqlQuery.subwhereExprKeys.add(subqueryAlias);
                        }
                        expressions.add(columnExpression);

                    }

                }
                // The where clause might be null because if the
                // list of members is greater than the limit
                // permitted, we won't constraint.
                sqlQuery.addWhere(where, subqueryAlias);
            }
        } else {
            // TODO: Add Subquery component to this
            // column not constrained by slicer
            SqlConstraintUtils.addSimpleColumnConstraint(sqlQuery, column, columnExpression, value,
                subqueryAlias);
        }
    }

    public SqlQuery wrap(SqlQuery configured, DataSource dataSource, Dialect dialect) {
        return configured;
    }

    public void augmentParentMembers(RolapStar.Column column, SqlQuery sqlQuery, AggStar aggStar,
                                     List<RolapMember> parents) {
        ConsolidationMembers members = getLevelMembers();
        if (members != null && members.getLevelExpression().equals(column.getExpression())) {
            List<RolapMember> matchedMembers = members.getMembers();
            for (RolapMember matchedMember : matchedMembers) {
                parents.add(matchedMember);
            }
            members.setColumnExpression(SqlConstraintUtils.getColumnExpr(sqlQuery, aggStar, column));
            members.setStarColumn(column);
        }
    }
    /**
     * Interface for creating handlers. This allows different implementations to be craeted
     * dependening on the caller.
     */
    public static interface ConsolidationHandlerFactory {

        /**
         * Return a new ConsolidationHandler
         * @return
         */
        public ConsolidationHandler newConsolidationHandler();
    }

}
