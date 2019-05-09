/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.MondrianException;
import mondrian.olap.Util;

import org.olap4j.*;

import java.sql.*;

/**
 * Unit test for hanger dimensions.
 */
public class HangerDimensionTest extends FoodMartTestCase {
    /** Unit test for a simple hanger dimension with values true and false. */
    public void testHangerDimension() {
      TestContext testContext = TestContext.instance().createSubstitutingCube(
          "Sales",
          "<Dimension name='Boolean' hanger='true'>\n"
          + "  <Hierarchy hasAll=\"true\">\n"
          + "    <Level name=\"Boolean\"/>\n"
          + "  </Hierarchy>\n"
          + "</Dimension>",
          "<CalculatedMember name='False' hierarchy='[Boolean]' formula='[Marital Status]'/>\n"
          + "<CalculatedMember name='True' hierarchy='[Boolean]' formula='[Marital Status]'/>\n");
      testContext.assertQueryReturns(
          "with member [Measures].[Store Sales2] as\n"
          + "   Iif([Boolean].CurrentMember is [Boolean].[True],\n"
          + "       ([Boolean].[All Booleans], [Measures].[Store Sales]),"
          + "       ([Boolean].[All Booleans], [Measures].[Store Sales]) - ([Boolean].[All Booleans], [Measures].[Store Cost]))\n"
          + "select [Measures].[Store Sales2] on columns,\n"
          + " [Boolean].AllMembers * [Gender].Children on rows\n"
          + "from [Sales]",
          "Axis #0:\n"
          + "{}\n"
          + "Axis #1:\n"
          + "{[Measures].[Store Sales2]}\n"
          + "Axis #2:\n"
          + "{[Boolean].[All Booleans], [Gender].[F]}\n"
          + "{[Boolean].[All Booleans], [Gender].[M]}\n"
          + "{[Boolean].[False], [Gender].[F]}\n"
          + "{[Boolean].[False], [Gender].[M]}\n"
          + "{[Boolean].[True], [Gender].[F]}\n"
          + "{[Boolean].[True], [Gender].[M]}\n"
          + "Row #0: 168,448.73\n"
          + "Row #1: 171,162.17\n"
          + "Row #2: 168,448.73\n"
          + "Row #3: 171,162.17\n"
          + "Row #4: 280,226.21\n"
          + "Row #5: 285,011.92\n");
    }

    public void testHangerWithDescendants() {

      TestContext testContext = TestContext.instance().createSubstitutingCube(
          "Sales",
          "<Dimension name='Boolean' hanger='true'>\n"
          + "  <Hierarchy hasAll=\"true\">\n"
          + "    <Table name=\"store\"/>\n"
          + "    <Level name=\"Boolean\" column=\"store_name\"/>\n"
          + "  </Hierarchy>\n"
          + "</Dimension>",
          "<CalculatedMember name='False' hierarchy='[Boolean]' formula='[Marital Status]'/>\n"
          + "<CalculatedMember name='True' hierarchy='[Boolean]' formula='[Marital Status]'/>\n");

      String mdx =
          "with set [All Measures Fullset] as '{[Measures].[Unit Sales]}'\n"
        + "member [Measures].[(Axis Members Count)] as 'Count(Descendants([Boolean].[All Booleans], 1, SELF_AND_BEFORE))', SOLVE_ORDER = 1000\n"
        + "select {[Measures].[(Axis Members Count)]} ON COLUMNS\n"
        + "from [Sales]";

      testContext.assertQueryReturns( mdx,
          "Axis #0:\n"
          + "{}\n"
          + "Axis #1:\n"
          + "{[Measures].[(Axis Members Count)]}\n"
          + "Row #0: 26\n");
    }

    /**
     * NOTE: In Mondrian 3, hanger dimensions must either be based on a table
     * or have an all member.
     * 
     * This is due to the way we resolve calculated members, we cannot set the
     * default member due to how Query.resolve() behaves in 3.x 
     * (requiring a RootEvaluator)
     * 
     * Unit test that if a hierarchy has no real members, only calculated
     * members, then the default member is the first calculated member. 
     * 
     */
    public void _testHangerDimensionImplicitCalculatedDefaultMember() {
      TestContext testContext = TestContext.instance().createSubstitutingCube(
          "Sales",
          "<Dimension name='Boolean' hanger='true'>\n"
          + "  <Hierarchy hasAll=\"false\">\n"
          + "    <Level name=\"Boolean\"/>\n"
          + "  </Hierarchy>\n"
          + "</Dimension>",
          "<CalculatedMember name='False' hierarchy='[Boolean]' formula='[Marital Status]'/>\n"
          + "<CalculatedMember name='True' hierarchy='[Boolean]' formula='[Marital Status]'/>\n");
       testContext.assertAxisReturns(
          "[Boolean]",
          "[Boolean].[False]");
    }

    /** Tests that it is an error if an attribute has no members.
     * (No all member, no real members, no calculated members.) */
    public void testHangerDimensionEmptyIsError() {
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            "<Dimension name='Boolean' hanger='true'>\n"
            + "  <Hierarchy hasAll=\"false\">\n"
            + "    <Level name=\"Boolean\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>");
        try {
            testContext.getConnection();
            fail();
        } catch (MondrianException e) {
            assertTrue("Missing (has no members): " + e.getCause().getCause().getMessage(), e.getCause().getCause().getMessage().indexOf( "Mondrian Error:Hierarchy '[Boolean]' is invalid (has no members)") >= 0);
        }
    }

    /** Tests that it is an error if an attribute in a hanger dimension has a
     * keyColumn specified. (All other mappings to columns, e.g. nameColumn
     * or included Key element, are illegal too.) */
    public void testHangerDimensionKeyColumnNotAllowed() {
      TestContext testContext = TestContext.instance().createSubstitutingCube(
                "Sales",
                "<Dimension name='Boolean' hanger='true' key='Boolean'>\n"
                + "  <Attributes>\n"
                + "    <Attribute name='Boolean' keyColumn='xxx'/>\n"
                + "  </Attributes>\n"
                + "</Dimension>");
                /*
                 * 
                 * TODO: Figure out the appropriate scenario here
                 * 
            .assertErrorList().containsError(
                "Attribute 'Boolean' in hanger dimension must not map to column \\(in Attribute 'Boolean'\\) \\(at ${pos}\\)",
                "<Attribute name='Boolean' keyColumn='xxx'/>");
                */
    }

    /** Tests drill-through of a query involving a hanger dimension. */
    public void testHangerDimensionDrillThrough() throws SQLException {
        OlapConnection connection = null;
        OlapStatement statement = null;
        CellSet cellSet = null;
        ResultSet resultSet = null;
        try {
          
            TestContext testContext = TestContext.instance().createSubstitutingCube(          
                "Sales",
                "<Dimension name='Boolean' hanger='true'>\n"
                + "  <Hierarchy hasAll=\"true\">\n"
                + "    <Level name=\"Boolean\"/>\n"
                + "  </Hierarchy>\n"
                + "</Dimension>",
                "<CalculatedMember name='False' hierarchy='[Boolean]' formula='[Marital Status]'/>\n"
                + "<CalculatedMember name='True' hierarchy='[Boolean]' formula='[Marital Status]'/>\n");
            
            connection = testContext.getOlap4jConnection();
            statement = connection.createStatement();
            cellSet =
                statement.executeOlapQuery(
                    "select [Gender].Members on 0,\n"
                    + "[Boolean].Members on 1\n"
                    + "from [Sales]");
            resultSet = cellSet.getCell(0).drillThrough();
            int n = 0;
            while (resultSet.next()) {
                ++n;
            }
            assertEquals(86837, n);
        } finally {
            Util.close(resultSet, null, null);
            Util.close(cellSet, statement, connection);
        }
    }

    public void testHangerDimensionDistinctCountAggregation() {
        String schema =
            "<Schema name=\"tiny\">\n"
            + "  <Dimension name=\"Time\" type=\"TimeDimension\">\n"
            + "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\">\n"
            + "      <Table name=\"time_by_day\" />\n"
            + "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\" levelType=\"TimeYears\" />\n"
            + "      <Level name=\"Quarter\" uniqueMembers=\"false\" levelType=\"TimeQuarters\" />\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name=\"Product\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
            + "      <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
            + "        <Table name=\"product\"/>\n"
            + "        <Table name=\"product_class\"/>\n"
            + "      </Join>\n"
            + "      <Level name=\"Product Family\" table=\"product_class\" column=\"product_family\" uniqueMembers=\"true\" />\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name=\"Marital Status\">\n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Marital Status\" primaryKey=\"customer_id\">\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Level name=\"Marital Status\" column=\"marital_status\" uniqueMembers=\"true\" approxRowCount=\"111\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>"
            + "  <Dimension name=\"Boolean\" hanger=\"true\">\n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All\">\n"
            + "      <Level name=\"Boolean\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>"
            + "  <Cube name=\"Sales\">\n"
            + "    <Table name=\"sales_fact_1997\" />\n"
            + "    <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\" />\n"
            + "    <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\" />\n"
            + "    <DimensionUsage name=\"Marital Status\" source=\"Marital Status\" foreignKey=\"customer_id\" />\n"
            + "    <DimensionUsage name=\"Boolean\" source=\"Boolean\" />\n"
            + "    <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\" />\n"
            + "    <Measure name=\"Customer Count\" column=\"customer_id\" aggregator=\"distinct-count\" formatString=\"#,###\" />\n"
            + "    <CalculatedMember name=\"False\" hierarchy=\"[Boolean]\" parent=\"[Boolean].[All]\" formula=\"[Marital Status]\"/>\n"
            + "    <CalculatedMember name=\"True\" hierarchy=\"[Boolean]\" parent=\"[Boolean].[All]\" formula=\"[Marital Status]\"/>\n"
            + "  </Cube>\n"
            + "</Schema>\n";

        TestContext testContext = TestContext.instance().withSchema(schema);
        String desiredResult =
            "Axis #0:\n"
            + "{[Boolean].[All].[True]}\n"
            + "{[Boolean].[All].[False]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 5,581\n";
        testContext.assertQueryReturns(
            "select [Measures].[Customer Count] on columns\n"
            + "from [Sales]\n"
            +"where {[Boolean].[All].[True], [Boolean].[All].[False]}",
            desiredResult);
    }
}

// End HangerDimensionTest.java
