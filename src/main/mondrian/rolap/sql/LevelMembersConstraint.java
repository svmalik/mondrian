/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2015 Pentaho Corporation..  All rights reserved.
*/
package mondrian.rolap.sql;

import mondrian.olap.Evaluator;
import mondrian.olap.MondrianDef;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapMember;
import mondrian.rolap.SqlConstraintUtils;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.spi.Dialect;
import mondrian.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Restricts the SQL result set to members where particular columns have
 * particular values.
 *
 * @version $Id$
 */
public class LevelMembersConstraint implements TupleConstraint
{
    private final RolapLevel level;
    private final Pair<RolapLevel, List<List<String>>> cacheKey;
    private final List<List<String>> keyList;

    public LevelMembersConstraint(
        RolapLevel level,
        List<List<String>> keyList)
    {
        this.level = level;
        this.keyList = keyList;
        cacheKey = Pair.of(level, keyList);
    }

    public void addConstraint(
        SqlQuery sqlQuery, RolapCube baseCube, AggStar aggStar)
    {
        List<Dialect.Datatype> datatypeList = new ArrayList<>();
        List<MondrianDef.Expression> columnList = new ArrayList<>();
        for (RolapLevel x = level;; x = (RolapLevel) x.getParentLevel()) {
            if (x.getKeyExp() != null) {
                columnList.add(x.getKeyExp());
                datatypeList.add(x.getDatatype());
            }
            if (x.isUnique()) {
                break;
            }
        }

        for (int i = 0; i < columnList.size(); i++) {
            String[] columnKeys = new String[keyList.size()];
            for (int j = 0; j < keyList.size(); j++) {
                columnKeys[j] = keyList.get(j).get(i);
            }
            sqlQuery.addWhere(
                SqlConstraintUtils.constrainLevel2(
                    sqlQuery,
                    columnList.get(i),
                    datatypeList.get(i),
                    columnKeys));
        }
    }

    public void addLevelConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar,
        RolapLevel level,
        boolean optimize)
    {
    }

    public MemberChildrenConstraint getMemberChildrenConstraint(
        RolapMember parent)
    {
        return null;
    }

    public String toString() {
        return "LevelMembersConstraint";
    }


    public Object getCacheKey() {
        return cacheKey;
    }

    public Evaluator getEvaluator() {
        return null;
    }

    public boolean supportsAggTables() {
        return true;
    }
}

// End LevelMembersConstraint.java
