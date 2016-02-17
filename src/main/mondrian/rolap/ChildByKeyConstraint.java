/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2014-2014 Pentaho
// All rights reserved.
*/
package mondrian.rolap;


import mondrian.olap.Id;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.SqlQuery;

import java.util.Arrays;
import java.util.Collection;

/**
 * Constrains children request by key, for providing ssas-like key access.
 */
class ChildByKeyConstraint extends DefaultMemberChildrenConstraint {

    private final String[] childKeys;
    private final Object cacheKey;

    public ChildByKeyConstraint(Id.KeySegment childKey) {

        this.childKeys = new String[] { getKeyValue(childKey) };
        this.cacheKey = Arrays.asList(ChildByKeyConstraint.class, childKey);
    }

    public ChildByKeyConstraint(Collection<Id.KeySegment> childKeys) {
        this.childKeys = new String[childKeys.size()];
        int i = 0;
        for (Id.KeySegment childKey : childKeys) {
            this.childKeys[i++] = getKeyValue(childKey);
        }
        this.cacheKey = Arrays.asList(ChildByKeyConstraint.class, this.childKeys);
    }

    public void addLevelConstraint(
        SqlQuery query,
        RolapCube baseCube,
        AggStar aggStar,
        RolapLevel level,
        boolean optimize)
    {
        super.addLevelConstraint(query, baseCube, aggStar, level, optimize);
        query.addWhere(SqlConstraintUtils.constrainLevel2(
            query, level.getKeyExp(), level.getDatatype(), childKeys));
    }

    public boolean equals(Object obj) {
        return obj instanceof ChildByKeyConstraint
            && getCacheKey().equals(((ChildByKeyConstraint)obj).getCacheKey());
    }

    public int hashCode() {
        return getCacheKey().hashCode();
    }

    private String getKeyValue(Id.KeySegment key) {
        // assuming only one key segment, discarding the rest
        return key.getKeyParts().get(0).name;
    }

    public String toString() {
        return "ChildByKeyConstraint(" + Arrays.toString(childKeys) + ")";
    }

    public Object getCacheKey() {
        return cacheKey;
    }
}
// End ChildByKeyConstraint.java