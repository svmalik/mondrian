/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2015 Pentaho Corporation.
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.test.FoodMartTestCase;

public class RolapEvaluatorTest extends FoodMartTestCase {

    public void testGetSlicerPredicateInfo() throws Exception {
        RolapResult result = (RolapResult) executeQuery(
            "select  from sales "
            + "WHERE {[Time].[1997].Q1, [Time].[1997].Q2} "
            + "* { Store.[USA].[CA], Store.[USA].[WA]}");
        RolapEvaluator evalulator = (RolapEvaluator) result.getRootEvaluator();
        final CompoundPredicateInfo slicerPredicateInfo =
            evalulator.getSlicerPredicateInfo(evalulator.getMeasureCube());
        assertEquals(
            "(`store`.`store_state` in ('CA', 'WA') and "
            + "(((`time_by_day`.`the_year`, `time_by_day`.`quarter`) in ((1997, 'Q1'), (1997, 'Q2')))))",
            slicerPredicateInfo.getPredicateString());
        assertTrue(slicerPredicateInfo.isSatisfiable());
    }

    public void testSlicerPredicateUnsatisfiable() {
        assertQueryReturns(
            "select measures.[Sales Count] on 0 from [warehouse and sales] "
            + "WHERE {[Time].[1997].Q1, [Time].[1997].Q2} "
            + "*{[Warehouse].[USA].[CA], Warehouse.[USA].[WA]}",
            "Axis #0:\n"
            + "{[Time].[1997].[Q1], [Warehouse].[USA].[CA]}\n"
            + "{[Time].[1997].[Q1], [Warehouse].[USA].[WA]}\n"
            + "{[Time].[1997].[Q2], [Warehouse].[USA].[CA]}\n"
            + "{[Time].[1997].[Q2], [Warehouse].[USA].[WA]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Sales Count]}\n"
            + "Row #0: \n");
        RolapResult result = (RolapResult) executeQuery(
            "select  from [warehouse and sales] "
            + "WHERE {[Time].[1997].Q1, [Time].[1997].Q2} "
            + "* Head([Warehouse].[Country].members, 2)");
        RolapEvaluator evalulator = (RolapEvaluator) result.getRootEvaluator();
        RolapCube cube = evalulator.getMeasureCube();
        assertFalse(evalulator.getSlicerPredicateInfo(cube).isSatisfiable());
        assertNull(evalulator.getSlicerPredicateInfo(cube).getPredicate());
        getTestContext().flushSchemaCache();
    }
}

// End RolapEvaluatorTest.java