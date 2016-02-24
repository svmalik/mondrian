/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2014 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.Cell;
import mondrian.olap.Result;

/**
 * CalculatedCell Work to do:
 *  - implement compilation approach for subcube checks
 *  - review how role based security is applied in codebase for performance ideas
 *  - test scenarios below
 *  
 * Additional Testing Scenarios:
 * - Additional tests for Calculated Cells in virtual cubes
 * - Calculated Cell Solve Order
 * - Calculated Cells with Native Evaluation
 *
 *  - we'll need to decide during native eval if a calculated cell is in possible scope,
 *    and if it's relevant.
 *
 *    for instance, for non empty, a cell calc can transition a cell from non-empty to empty or empty to non-empty.
 *
 *    we can determine if each calculated cell is within scope by comparing the 
 *    cross join args plus the current context with the calculated cell.  The crossjoin args
 *    override the current context, otherwise the current context is appropriate. 
 *
 * - Calculated Cells with Security
 * - Calculated Cells along side more complex calculated measures
 *
 * @author Will Gorman <wgorman@pentaho.com>
 *
 */
public class CalculatedCellTest extends FoodMartTestCase {

    public void testCalculatedCellProperties() {
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            null,
            null,
            null,
            "  <CalculatedCell>\n"
            + "    <SubCube>([Gender].[F])</SubCube>\n"
            + "    <Formula>[Measures].CurrentMember * 1.2</Formula>\n"
            + "    <CalculatedCellProperty name=\"SOLVE_ORDER\" value=\"0\"/>\n"
            + "    <CalculatedCellProperty name=\"FORMAT_STRING\" value=\"|0.0|style=red\"/>\n"
            + "    <CalculatedCellProperty name=\"BACK_COLOR\" expression=\"IIf([Measures].CurrentMember &gt; 78500, 12345,54321)\"/>\n"
            + "  </CalculatedCell>"
        );
  
        Result result = testContext.executeQuery("select {CrossJoin([Gender].[All Gender].Children,[Marital Status].[All Marital Status].Children)} on 0, {[Measures].[Unit Sales]} on 1 from [sales] CELL PROPERTIES BACK_COLOR");
        String desiredResult = 
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Marital Status].[M]}\n"
            + "{[Gender].[F], [Marital Status].[S]}\n"
            + "{[Gender].[M], [Marital Status].[M]}\n"
            + "{[Gender].[M], [Marital Status].[S]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: |78403.2|style=red\n" // Normally would be 65,336
            + "Row #0: |79466.4|style=red\n" // Normally would be 66,222
            + "Row #0: 66,460\n"
            + "Row #0: 68,755\n";
        String resultString = TestContext.toString(result);
        if (desiredResult != null) {
            TestContext.assertEqualsVerbose(
                desiredResult,
                testContext.upgradeActual(resultString));
        }
  
        Cell cell = result.getCell(new int[] {0, 0});
        assertEquals("|0.0|style=red", cell.getPropertyValue("FORMAT_STRING"));
        assertEquals("54321.0", cell.getPropertyValue("BACK_COLOR"));
        
        cell = result.getCell(new int[] {1, 0});
        assertEquals("|0.0|style=red", cell.getPropertyValue("FORMAT_STRING"));
        assertEquals("12345.0", cell.getPropertyValue("BACK_COLOR"));
        
        cell = result.getCell(new int[] {2, 0});
        assertEquals("Standard", cell.getPropertyValue("FORMAT_STRING"));
        assertNull(cell.getPropertyValue("BACK_COLOR"));
    }

    public void testMemberCellCalculation() {
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            null,
            null,
            null,
            "  <CalculatedCell>\n"
            + "    <SubCube>([Gender].[F])</SubCube>\n"
            + "    <Formula>[Measures].CurrentMember * 1.2</Formula>\n"
            + "    <CalculatedCellProperty name=\"SOLVE_ORDER\" value=\"-100\"/>\n"
            + "  </CalculatedCell>"
        );
        testContext.assertQueryReturns(
            "select {CrossJoin([Gender].[All Gender].Children,[Marital Status].[All Marital Status].Children)} on 0, {[Measures].[Unit Sales]} on 1 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Marital Status].[M]}\n"
            + "{[Gender].[F], [Marital Status].[S]}\n"
            + "{[Gender].[M], [Marital Status].[M]}\n"
            + "{[Gender].[M], [Marital Status].[S]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 78,403\n" // Normally would be 65,336
            + "Row #0: 79,466\n" // Normally would be 66,222
            + "Row #0: 66,460\n"
            + "Row #0: 68,755\n");
    }

    public void testTupleCellCalculation() {
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            null,
            null,
            null,
            "  <CalculatedCell>\n"
            + "    <SubCube>([Gender].[F], [Marital Status].[S])</SubCube>\n"
            + "    <Formula>[Measures].CurrentMember * 1.2</Formula>\n"
            + "    <CalculatedCellProperty name=\"SOLVE_ORDER\" value=\"-100\"/>\n"
            + "  </CalculatedCell>"
        );
        testContext.assertQueryReturns(
            "select {CrossJoin([Gender].[All Gender].Children,[Marital Status].[All Marital Status].Children)} on 0, {[Measures].[Unit Sales]} on 1 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Marital Status].[M]}\n"
            + "{[Gender].[F], [Marital Status].[S]}\n"
            + "{[Gender].[M], [Marital Status].[M]}\n"
            + "{[Gender].[M], [Marital Status].[S]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 65,336\n"
            + "Row #0: 79,466\n" // Normally would be 66,222
            + "Row #0: 66,460\n"
            + "Row #0: 68,755\n");
    }

    public void testMembersCellCalculation() {
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            null,
            null,
            null,
            "  <CalculatedCell>\n"
            + "    <SubCube>([Gender].[F], [Marital Status].[Marital Status].Members)</SubCube>\n"
            + "    <Formula>[Measures].CurrentMember * 1.2</Formula>\n"
            + "    <CalculatedCellProperty name=\"SOLVE_ORDER\" value=\"-100\"/>\n"
            + "  </CalculatedCell>"
        );
        testContext.assertQueryReturns(
            "select {CrossJoin([Gender].[All Gender].Children,[Marital Status].Members)} on 0, {[Measures].[Unit Sales]} on 1 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Marital Status].[All Marital Status]}\n"
            + "{[Gender].[F], [Marital Status].[M]}\n"
            + "{[Gender].[F], [Marital Status].[S]}\n"
            + "{[Gender].[M], [Marital Status].[All Marital Status]}\n"
            + "{[Gender].[M], [Marital Status].[M]}\n"
            + "{[Gender].[M], [Marital Status].[S]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 131,558\n"
            + "Row #0: 78,403\n" // 65,336
            + "Row #0: 79,466\n" // 66,222
            + "Row #0: 135,215\n"
            + "Row #0: 66,460\n"
            + "Row #0: 68,755\n");

        // compound slicer
        testContext.assertQueryReturns(
            "select {[Gender].[All Gender].Children} on 0, {[Measures].[Unit Sales]} on 1 from sales where {[Marital Status].[M], [Marital Status].[S]}",
            "Axis #0:\n"
            + "{[Marital Status].[M]}\n"
            + "{[Marital Status].[S]}\n"
            + "Axis #1:\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 157,870\n" // 131,558
            + "Row #0: 135,215\n");

        testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            null,
            null,
            null,
            "  <CalculatedCell>\n"
            + "    <SubCube>([Gender].[F], [Customers].[All Customers], [Measures].[Unit Sales], [Marital Status].[Marital Status].Members)</SubCube>\n"
            + "    <Formula>[Measures].CurrentMember * 1.2</Formula>\n"
            + "    <CalculatedCellProperty name=\"SOLVE_ORDER\" value=\"-100\"/>\n"
            + "  </CalculatedCell>"
        );
        testContext.assertQueryReturns(
            "select {CrossJoin([Gender].[All Gender].Children,[Marital Status].Members)} on 0, {[Measures].[Unit Sales]} on 1 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Marital Status].[All Marital Status]}\n"
            + "{[Gender].[F], [Marital Status].[M]}\n"
            + "{[Gender].[F], [Marital Status].[S]}\n"
            + "{[Gender].[M], [Marital Status].[All Marital Status]}\n"
            + "{[Gender].[M], [Marital Status].[M]}\n"
            + "{[Gender].[M], [Marital Status].[S]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 131,558\n"
            + "Row #0: 78,403\n" // 65,336
            + "Row #0: 79,466\n" // 66,222
            + "Row #0: 135,215\n"
            + "Row #0: 66,460\n"
            + "Row #0: 68,755\n");
    }

    public void testDecendantsCellCalculation() {
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            null,
            null,
            null,
            "  <CalculatedCell>\n"
            + "    <SubCube>([Gender].[F], Descendants([Time].[1997].[Q1]))</SubCube>\n"
            + "    <Formula>[Measures].CurrentMember * 1.2</Formula>\n"
            + "    <CalculatedCellProperty name=\"SOLVE_ORDER\" value=\"-100\"/>\n"
            + "  </CalculatedCell>"
        );
        testContext.assertQueryReturns(
            "select {NonEmptyCrossJoin([Gender].[All Gender].Children, {[Time].[1997].[Q1].[1], [Time].[1997].[Q2].[4]})} on 0, {[Measures].[Unit Sales]} on 1 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Time].[1997].[Q1].[1]}\n"
            + "{[Gender].[F], [Time].[1997].[Q2].[4]}\n"
            + "{[Gender].[M], [Time].[1997].[Q1].[1]}\n"
            + "{[Gender].[M], [Time].[1997].[Q2].[4]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 13,118\n" // 10,932
            + "Row #0: 9,990\n"
            + "Row #0: 10,696\n"
            + "Row #0: 10,189\n");

        // compound slicer
        testContext.assertQueryReturns(
            "select {[Gender].[All Gender].Children } on 0, {[Measures].[Unit Sales]} on 1 from sales where {[Time].[1997].[Q1].[1], [Time].[1997].[Q1].[2]}",
            "Axis #0:\n"
            + "{[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[1997].[Q1].[2]}\n"
            + "Axis #1:\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 25,438\n" // 21,198
            + "Row #0: 21,387\n");

        testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            null,
            null,
            null,
            "  <CalculatedCell>\n"
            + "    <SubCube>([Gender].[F], Descendants([Time].[1997].[Q1], [Time].[Month]))</SubCube>\n"
            + "    <Formula>[Measures].CurrentMember * 1.2</Formula>\n"
            + "    <CalculatedCellProperty name=\"SOLVE_ORDER\" value=\"-100\"/>\n"
            + "  </CalculatedCell>"
        );
        testContext.assertQueryReturns(
            "select {NonEmptyCrossJoin([Gender].[All Gender].Children, {[Time].[1997].[Q1].[1], [Time].[1997].[Q2].[4]})} on 0, {[Measures].[Unit Sales]} on 1 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Time].[1997].[Q1].[1]}\n"
            + "{[Gender].[F], [Time].[1997].[Q2].[4]}\n"
            + "{[Gender].[M], [Time].[1997].[Q1].[1]}\n"
            + "{[Gender].[M], [Time].[1997].[Q2].[4]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 13,118\n" // 10,932
            + "Row #0: 9,990\n"
            + "Row #0: 10,696\n"
            + "Row #0: 10,189\n");

        // Note that this test verifies that the level is having an impact and no cells are changed
        testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            null,
            null,
            null,
            "  <CalculatedCell>\n"
            + "    <SubCube>([Gender].[F], Descendants([Time].[1997].[Q1], [Time].[Year]))</SubCube>\n"
            + "    <Formula>[Measures].CurrentMember * 1.2</Formula>\n"
            + "    <CalculatedCellProperty name=\"SOLVE_ORDER\" value=\"-100\"/>\n"
            + "  </CalculatedCell>"
        );
        testContext.assertQueryReturns(
            "select {NonEmptyCrossJoin([Gender].[All Gender].Children, {[Time].[1997].[Q1].[1], [Time].[1997].[Q2].[4]})} on 0, {[Measures].[Unit Sales]} on 1 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Time].[1997].[Q1].[1]}\n"
            + "{[Gender].[F], [Time].[1997].[Q2].[4]}\n"
            + "{[Gender].[M], [Time].[1997].[Q1].[1]}\n"
            + "{[Gender].[M], [Time].[1997].[Q2].[4]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 10,932\n"
            + "Row #0: 9,990\n"
            + "Row #0: 10,696\n"
            + "Row #0: 10,189\n");

        // this verifies after is taken effect
        testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            null,
            null,
            null,
            "  <CalculatedCell>\n"
            + "    <SubCube>([Gender].[F], Descendants([Time].[1997].[Q1], [Time].[Month], AFTER))</SubCube>\n"
            + "    <Formula>[Measures].CurrentMember * 1.2</Formula>\n"
            + "    <CalculatedCellProperty name=\"SOLVE_ORDER\" value=\"-100\"/>\n"
            + "  </CalculatedCell>"
        );
        testContext.assertQueryReturns(
            "select {NonEmptyCrossJoin([Gender].[All Gender].Children, {[Time].[1997].[Q1].[1], [Time].[1997].[Q2].[4]})} on 0, {[Measures].[Unit Sales]} on 1 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Time].[1997].[Q1].[1]}\n"
            + "{[Gender].[F], [Time].[1997].[Q2].[4]}\n"
            + "{[Gender].[M], [Time].[1997].[Q1].[1]}\n"
            + "{[Gender].[M], [Time].[1997].[Q2].[4]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 10,932\n"
            + "Row #0: 9,990\n"
            + "Row #0: 10,696\n"
            + "Row #0: 10,189\n");
  
        // this verifies after is taken effect
        testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            null,
            null,
            null,
            "  <CalculatedCell>\n"
            + "    <SubCube>([Gender].[F], Descendants([Time].[1997].[Q1], [Time].[Month], SELF_AND_BEFORE))</SubCube>\n"
            + "    <Formula>[Measures].CurrentMember * 1.2</Formula>\n"
            + "    <CalculatedCellProperty name=\"SOLVE_ORDER\" value=\"-100\"/>\n"
            + "  </CalculatedCell>"
        );
        testContext.assertQueryReturns(
            "select {NonEmptyCrossJoin([Gender].[All Gender].Children, {[Time].[1997].[Q1].[1], [Time].[1997].[Q2].[4]})} on 0, {[Measures].[Unit Sales]} on 1 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Time].[1997].[Q1].[1]}\n"
            + "{[Gender].[F], [Time].[1997].[Q2].[4]}\n"
            + "{[Gender].[M], [Time].[1997].[Q1].[1]}\n"
            + "{[Gender].[M], [Time].[1997].[Q2].[4]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 13,118\n" // 10,932\n"
            + "Row #0: 9,990\n"
            + "Row #0: 10,696\n"
            + "Row #0: 10,189\n");

        // compound slicer
        testContext.assertQueryReturns(
            "select {[Gender].[All Gender].Children} on 0, {[Measures].[Unit Sales]} on 1 from sales where {[Time].[1997].[Q1].[1], [Time].[1997].[Q1].[2]}",
            "Axis #0:\n"
            + "{[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[1997].[Q1].[2]}\n"
            + "Axis #1:\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 25,438\n" // 21,198
            + "Row #0: 21,387\n");
    }

    public void testVirtualCube() {
        // this is a very basic test, need to do more in this area
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Warehouse and Sales",
            null,
            null,
            null,
            "  <CalculatedCell>\n"
            + "    <SubCube>([Gender].[F], [Marital Status].[S])</SubCube>\n"
            + "    <Formula>[Measures].CurrentMember * 1.2</Formula>\n"
            + "    <CalculatedCellProperty name=\"SOLVE_ORDER\" value=\"-100\"/>\n"
            + "  </CalculatedCell>"
        );
        testContext.assertQueryReturns(
            "select {CrossJoin([Gender].[All Gender].Children,[Marital Status].[All Marital Status].Children)} on 0, {[Measures].[Unit Sales]} on 1 from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Marital Status].[M]}\n"
            + "{[Gender].[F], [Marital Status].[S]}\n"
            + "{[Gender].[M], [Marital Status].[M]}\n"
            + "{[Gender].[M], [Marital Status].[S]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 65,336\n"
            + "Row #0: 79,466\n" // Normally would be 66,222
            + "Row #0: 66,460\n"
            + "Row #0: 68,755\n");
    }

    public void testCalculatedCellWithOrder() {
      TestContext testContext = TestContext.instance()
      .createSubstitutingCube(
          "Sales",
          null,
          null,
          null,
          "  <CalculatedCell>\n"
          + "    <SubCube>([Gender].[F], Descendants([Time].[1997].[Q1], [Time].[Month], SELF_AND_BEFORE))</SubCube>\n"
          + "    <Formula>[Measures].CurrentMember * 1.2</Formula>\n"
          + "    <CalculatedCellProperty name=\"SOLVE_ORDER\" value=\"-100\"/>\n"
          + "  </CalculatedCell>"
      );
      testContext.assertQueryReturns(
          "select Order([Time].[1997].[Q1].Children, [Measures].[Unit Sales], ASC) on 0 from [Sales] where [Gender].[F]",
          "Axis #0:\n"
          + "{[Gender].[F]}\n"
          + "Axis #1:\n"
          + "{[Time].[1997].[Q1].[2]}\n"
          + "{[Time].[1997].[Q1].[1]}\n"
          + "{[Time].[1997].[Q1].[3]}\n"
          + "Row #0: 12,319\n"
          + "Row #0: 13,118\n"
          + "Row #0: 14,054\n");
    }

    /**
     * This test verifies that native evaluators recognize when active cell
     * calculations are in play.  The first scenario is not nativized, the second is.
     */
    public void testCellCalcWithNativeEval() {
        propSaver.set(propSaver.properties.EnableNativeFilter, false);
        propSaver.set(propSaver.properties.EnableNativeSum, false);
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            null,
            null,
            null,
            "  <CalculatedCell>\n"
            + "    <SubCube>([Gender].[F])</SubCube>\n"
            + "    <Formula>([Measures].CurrentMember, [Gender].[All Gender], [Marital Status].[M])</Formula>\n"
            + "    <CalculatedCellProperty name=\"SOLVE_ORDER\" value=\"-100\"/>\n"
            + "  </CalculatedCell>"
        );

        testContext.assertQueryReturns(
            "with member [Measures].[Calc] as 'Sum(Filter([Time].[1997].Children, [Measures].[Unit Sales] > 10000))', solve_order = 0"
            + "   member [Measures].[MidCalc] as '([Gender].[F], [Measures].[Calc])', solve_order = 100"
            + "select {[Measures].[MidCalc]} on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[MidCalc]}\n"
            + "Row #0: 131,796\n");

        testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            null,
            null,
            null,
            "  <CalculatedCell>\n"
            + "    <SubCube>([Gender].[F])</SubCube>\n"
            + "    <Formula>[Gender].[All Gender]</Formula>\n"
            + "    <CalculatedCellProperty name=\"SOLVE_ORDER\" value=\"-100\"/>\n"
            + "  </CalculatedCell>"
        );

        testContext.assertQueryReturns(
            "with member [Measures].[Calc] as 'Sum(Filter([Time].[1997].Children, [Measures].[Unit Sales] > 10000))', solve_order = 0"
            + "   member [Measures].[MidCalc] as '([Gender].[F], [Measures].[Calc])', solve_order = 100"
            + "select {[Measures].[MidCalc]} on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[MidCalc]}\n"
            + "Row #0: 266,773\n");
    }

    public void _testFilterCellCalc() {
        propSaver.set(propSaver.properties.EnableNativeFilter, false);
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            null,
            null,
            null,
            "  <CalculatedCell>\n"
            + "    <SubCube>([Gender].[F], [Marital Status].[S])</SubCube>\n"
            + "    <Formula>[Measures].CurrentMember * 1.2</Formula>\n"
            + "    <CalculatedCellProperty name=\"SOLVE_ORDER\" value=\"-100\"/>\n"
            + "  </CalculatedCell>"
        );

        testContext.assertQueryReturns(
            "select {Filter(CrossJoin([Gender].[All Gender].Children,[Marital Status].[All Marital Status].Children), [Measures].[Unit Sales] > 67000) } on 0, {[Measures].[Unit Sales]} on 1 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Marital Status].[S]}\n"
            + "{[Gender].[M], [Marital Status].[S]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 79,466\n" // Normally would be 66,222
            + "Row #0: 68,755\n");
    }

    public void _testNonEmptyCellCalc() {
        propSaver.set(propSaver.properties.EnableNativeNonEmpty, false);
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            null,
            null,
            null,
            "  <CalculatedCell>\n"
            + "    <SubCube>([Year].[1998])</SubCube>\n"
            + "    <Formula>100</Formula>\n"
            + "    <CalculatedCellProperty name=\"SOLVE_ORDER\" value=\"-100\"/>\n"
            + "  </CalculatedCell>");

        testContext.assertQueryReturns(
            "select {[Year].Members} on 0 from [sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[1997]}\n"
            + "{[Time].[1998]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 100\n" // normally null
            );

        // the following scenario fails with native evaluation enabled
        testContext.assertQueryReturns(
            "select NON EMPTY {[Year].Members} on 0 from [sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[1997]}\n"
            + "{[Time].[1998]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 100\n" // normally null
            );
    }

    public void _testTopCountCellCalc() {
        propSaver.set(propSaver.properties.EnableNativeTopCount, false);
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            null,
            null,
            null,
            "  <CalculatedCell>\n"
            + "    <SubCube>([Gender].[F], [Marital Status].[S])</SubCube>\n"
            + "    <Formula>[Measures].CurrentMember * 1.2</Formula>\n"
            + "    <CalculatedCellProperty name=\"SOLVE_ORDER\" value=\"-100\"/>\n"
            + "  </CalculatedCell>");

        // for native eval, the topcount returns the wrong order
        testContext.assertQueryReturns(
            "select {TopCount(CrossJoin([Gender].[All Gender].Children,[Marital Status].[All Marital Status].Children), 4, [Measures].[Unit Sales])} on 0, {[Measures].[Unit Sales]} on 1 from [sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Marital Status].[S]}\n"
            + "{[Gender].[M], [Marital Status].[S]}\n"
            + "{[Gender].[M], [Marital Status].[M]}\n"
            + "{[Gender].[F], [Marital Status].[M]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 79,466\n" // Normally would be 66,222
            + "Row #0: 68,755\n"
            + "Row #0: 66,460\n"
            + "Row #0: 65,336\n");

        testContext.assertQueryReturns(
            "select {TopCount(CrossJoin([Gender].[All Gender].Children,[Marital Status].[All Marital Status].Children), 2, [Measures].[Unit Sales]) } on 0, {[Measures].[Unit Sales]} on 1 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Marital Status].[S]}\n"
            + "{[Gender].[M], [Marital Status].[S]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 79,466\n" // Normally would be 66,222
            + "Row #0: 68,755\n");
    }
}
