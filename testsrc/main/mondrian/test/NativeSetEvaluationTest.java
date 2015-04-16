/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2014 Pentaho Corporation..  All rights reserved.
*/
package mondrian.test;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Result;
import mondrian.rolap.BatchTestCase;
import mondrian.spi.Dialect;
import mondrian.spi.Dialect.DatabaseProduct;

/**
 * Test native evaluation of supported set operations.
 *
 * <p>
 */
public class NativeSetEvaluationTest extends BatchTestCase {

    /**
     * Checks that a given MDX query results in a particular SQL statement
     * being generated.
     *
     * @param mdxQuery MDX query
     * @param patterns Set of patterns for expected SQL statements
     */
    protected void assertQuerySql(
        String mdxQuery,
        SqlPattern[] patterns)
    {
        assertQuerySqlOrNot(
            getTestContext(), mdxQuery, patterns, false, true, true);
    }

    /**
     *  we'll reuse this in a few variations
     */
    private static final class NativeTopCountWithAgg {
        static final String mysql =
            "select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    `product_class`.`product_department` as `c1`,\n"
            + "    `product_class`.`product_category` as `c2`,\n"
            + "    `product_class`.`product_subcategory` as `c3`,\n"
            + "    `product`.`brand_name` as `c4`,\n"
            + "    `product`.`product_name` as `c5`,\n"
            + "    sum(`sales_fact_1997`.`store_sales`) as `c6`\n"
            + "from\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            // aggregate set
            + "and\n"
            +    "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `time_by_day`.`week_of_year` in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39)\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`,\n"
            + "    `product_class`.`product_category`,\n"
            + "    `product_class`.`product_subcategory`,\n"
            + "    `product`.`brand_name`,\n"
            + "    `product`.`product_name`\n"
            + "order by\n"
            // top count Measures.[Store Sales]
            + "    sum(`sales_fact_1997`.`store_sales`) DESC,\n"
            + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
            + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC,\n"
            + "    ISNULL(`product_class`.`product_category`) ASC, `product_class`.`product_category` ASC,\n"
            + "    ISNULL(`product_class`.`product_subcategory`) ASC, `product_class`.`product_subcategory` ASC,\n"
            + "    ISNULL(`product`.`brand_name`) ASC, `product`.`brand_name` ASC,\n"
            + "    ISNULL(`product`.`product_name`) ASC, `product`.`product_name` ASC";

        static final String mysqlAgg =
            "select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    `product_class`.`product_department` as `c1`,\n"
            + "    `product_class`.`product_category` as `c2`,\n"
            + "    `product_class`.`product_subcategory` as `c3`,\n"
            + "    `product`.`brand_name` as `c4`,\n"
            + "    `product`.`product_name` as `c5`,\n"
            + "    sum(`agg_pl_01_sales_fact_1997`.`store_sales_sum`) as `c6`\n"
            + "from\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `agg_pl_01_sales_fact_1997` as `agg_pl_01_sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `agg_pl_01_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `agg_pl_01_sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `time_by_day`.`week_of_year` in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39)\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`,\n"
            + "    `product_class`.`product_category`,\n"
            + "    `product_class`.`product_subcategory`,\n"
            + "    `product`.`brand_name`,\n"
            + "    `product`.`product_name`\n"
            + "order by\n"
            + "    sum(`agg_pl_01_sales_fact_1997`.`store_sales_sum`) DESC,\n"
            + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
            + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC,\n"
            + "    ISNULL(`product_class`.`product_category`) ASC, `product_class`.`product_category` ASC,\n"
            + "    ISNULL(`product_class`.`product_subcategory`) ASC, `product_class`.`product_subcategory` ASC,\n"
            + "    ISNULL(`product`.`brand_name`) ASC, `product`.`brand_name` ASC,\n"
            + "    ISNULL(`product`.`product_name`) ASC, `product`.`product_name` ASC";
        static final String result =
            "Axis #0:\n"
            + "{[Time].[Weekly].[x]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[x1]}\n"
            + "{[Measures].[x2]}\n"
            + "{[Measures].[x3]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Green Pepper]}\n"
            + "{[Product].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Hilltop].[Hilltop Mint Mouthwash]}\n"
            + "Row #0: 737.26\n"
            + "Row #0: 281.78\n"
            + "Row #0: 165.98\n"
            + "Row #0: 262.48\n"
            + "Row #1: 680.56\n"
            + "Row #1: 264.26\n"
            + "Row #1: 173.76\n"
            + "Row #1: 188.24\n";
    }

    /**
     * Simple enumerated aggregate.
     */
    public void testNativeTopCountWithAggFlatSet() {
        // Note: changed mdx and expected as a part of the fix for MONDRIAN-2202
        // Formerly the aggregate set and measures used a conflicting hierarchy,
        // which is not a safe scenario for nativization.
        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();

        final String mdx =
            "with\n"
            + "member [Time.Weekly].x as Aggregate({[Time.Weekly].[1997].[1] : [Time.Weekly].[1997].[39]}, [Measures].[Store Sales])\n"
            + "member Measures.x1 as ([Time].[1997].[Q1], [Measures].[Store Sales])\n"
            + "member Measures.x2 as ([Time].[1997].[Q2], [Measures].[Store Sales])\n"
            + "member Measures.x3 as ([Time].[1997].[Q3], [Measures].[Store Sales])\n"
            + " set products as TopCount(Product.[Product Name].Members, 2, Measures.[Store Sales])\n"
            + " SELECT NON EMPTY products ON 1,\n"
            + "NON EMPTY {[Measures].[Store Sales], Measures.x1, Measures.x2, Measures.x3} ON 0\n"
            + "FROM [Sales] where [Time.Weekly].x";

        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        SqlPattern mysqlPattern = useAgg?
            new SqlPattern(
                DatabaseProduct.MYSQL,
                NativeTopCountWithAgg.mysqlAgg,
                NativeTopCountWithAgg.mysqlAgg)
            : new SqlPattern(
                DatabaseProduct.MYSQL,
                NativeTopCountWithAgg.mysql,
                NativeTopCountWithAgg.mysql);
        if (MondrianProperties.instance().EnableNativeTopCount.get()) {
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }
        assertQueryReturns(mdx, NativeTopCountWithAgg.result);
    }

    /**
     * Same as above, but using a named set
     */
    public void testNativeTopCountWithAggMemberNamedSet() {
        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();
        final String mdx =
            "with set TO_AGGREGATE as '{[Time.Weekly].[1997].[1] : [Time.Weekly].[1997].[39]}'\n"
            + "member [Time.Weekly].x as Aggregate(TO_AGGREGATE, [Measures].[Store Sales])\n"
            + "member Measures.x1 as ([Time].[1997].[Q1], [Measures].[Store Sales])\n"
            + "member Measures.x2 as ([Time].[1997].[Q2], [Measures].[Store Sales])\n"
            + "member Measures.x3 as ([Time].[1997].[Q3], [Measures].[Store Sales])\n"
            + " set products as TopCount(Product.[Product Name].Members, 2, Measures.[Store Sales])\n"
            + " SELECT NON EMPTY products ON 1,\n"
            + "NON EMPTY {[Measures].[Store Sales], Measures.x1, Measures.x2, Measures.x3} ON 0\n"
            + "FROM [Sales] where [Time.Weekly].x";

        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        SqlPattern mysqlPattern = useAgg?
            new SqlPattern(
                DatabaseProduct.MYSQL,
                NativeTopCountWithAgg.mysqlAgg,
                NativeTopCountWithAgg.mysqlAgg)
            : new SqlPattern(
                DatabaseProduct.MYSQL,
                NativeTopCountWithAgg.mysql,
                NativeTopCountWithAgg.mysql);
        if (propSaver.properties.EnableNativeTopCount.get()
            && propSaver.properties.EnableNativeNonEmpty.get())
        {
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }
        assertQueryReturns(mdx, NativeTopCountWithAgg.result);
    }

    public void testNativeFilterWithAggDescendants() {
      final boolean useAgg =
          MondrianProperties.instance().UseAggregates.get()
          && MondrianProperties.instance().ReadAggregates.get();
      final String mdx =
          "with\n"
          + "  set QUARTERS as Descendants([Time].[1997], [Time].[Time].[Quarter])\n"
          + "  member Time.x as Aggregate(QUARTERS, [Measures].[Store Sales])\n"
          + "  set products as Filter([Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].Children, [Measures].[Store Sales] > 700)\n"
          + "  SELECT NON EMPTY products ON 1,\n"
          + "  NON EMPTY {[Measures].[Store Sales]} ON 0\n"
          + "  FROM [Sales] where Time.x";

        final String mysqlQuery =
            "select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    `product_class`.`product_department` as `c1`,\n"
            + "    `product_class`.`product_category` as `c2`,\n"
            + "    `product_class`.`product_subcategory` as `c3`,\n"
            + "    `product`.`brand_name` as `c4`,\n"
            + "    `product`.`product_name` as `c5`\n"
            + "from\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`,\n"
            + (useAgg?
                "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`\n"
                + "where\n"
                + "    `agg_c_14_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year` = 1997\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`quarter` in ('Q1', 'Q2', 'Q3', 'Q4')\n"
                : "    `sales_fact_1997` as `sales_fact_1997`,\n"
                  + "    `time_by_day` as `time_by_day`\n"
                  + "where\n"
                  + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                  + "and\n"
                  + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                  + "and\n"
                  + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                  + "and\n"
            +    "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
                  + "    `time_by_day`.`quarter` in ('Q1', 'Q2', 'Q3', 'Q4')\n")
            + "and\n"
            + "    (`product`.`brand_name` = 'Hermanos' and `product_class`.`product_subcategory` = 'Fresh Vegetables' and `product_class`.`product_category` = 'Vegetables' and `product_class`.`product_department` = 'Produce' and `product_class`.`product_family` = 'Food')\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`,\n"
            + "    `product_class`.`product_category`,\n"
            + "    `product_class`.`product_subcategory`,\n"
            + "    `product`.`brand_name`,\n"
            + "    `product`.`product_name`\n"
            + "having\n"
            + (useAgg?
                "    (sum(`agg_c_14_sales_fact_1997`.`store_sales`) > 700)\n"
                : "    (sum(`sales_fact_1997`.`store_sales`) > 700)\n")
            + "order by\n"
            + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
            + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC,\n"
            + "    ISNULL(`product_class`.`product_category`) ASC, `product_class`.`product_category` ASC,\n"
            + "    ISNULL(`product_class`.`product_subcategory`) ASC, `product_class`.`product_subcategory` ASC,\n"
            + "    ISNULL(`product`.`brand_name`) ASC, `product`.`brand_name` ASC,\n"
            + "    ISNULL(`product`.`product_name`) ASC, `product`.`product_name` ASC";

        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        SqlPattern mysqlPattern =
            new SqlPattern(
                DatabaseProduct.MYSQL,
                mysqlQuery,
                mysqlQuery);
        if (propSaver.properties.EnableNativeFilter.get()
            && propSaver.properties.EnableNativeNonEmpty.get())
        {
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[x]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Broccoli]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Green Pepper]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos New Potatos]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Prepared Salad]}\n"
            + "Row #0: 742.73\n"
            + "Row #1: 922.54\n"
            + "Row #2: 703.80\n"
            + "Row #3: 718.08\n");
   }



    /**
     * Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-1426">
     * Mondrian-1426:</a> Native top count support for Member expressions
     * in Calculated member slicer
     */
    public void testNativeTopCountWithMemberOnlySlicer() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        final boolean useAggregates =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();
        final String mdx =
            "WITH\n"
            + "  SET TC AS 'TopCount([Product].[Drink].[Alcoholic Beverages].Children, 3, [Measures].[Unit Sales] )'\n"
            + "  MEMBER [Time].[Slicer] as [Time].[1997]\n"
            + "  MEMBER [Store Type].[Slicer] as [Store Type].[Store Type].[Deluxe Supermarket]\n"
            + "\n"
            + "  SELECT NON EMPTY [Measures].[Unit Sales] on 0,\n"
            + "    TC ON 1 \n"
            + "  FROM [Sales] WHERE {([Time].[Slicer], [Store Type].[Slicer])}\n";

        String mysqlQuery =
              "select\n"
              + "    `product_class`.`product_family` as `c0`,\n"
              + "    `product_class`.`product_department` as `c1`,\n"
              + "    `product_class`.`product_category` as `c2`,\n"
              + (useAggregates
              ? ("    sum(`agg_c_14_sales_fact_1997`.`unit_sales`) as `c3`\n"
                + "from\n"
                + "    `product_class` as `product_class`,\n"
                + "    `product` as `product`,\n"
                + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`,\n"
                + "    `store` as `store`\n"
                + "where\n"
                + "    `agg_c_14_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `store`.`store_type` = 'Deluxe Supermarket'\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year` = 1997\n")
              : ("    sum(`sales_fact_1997`.`unit_sales`) as `c3`\n"
                + "from\n"
                + "    `product_class` as `product_class`,\n"
                + "    `product` as `product`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `store` as `store`,\n"
                + "    `time_by_day` as `time_by_day`\n"
                + "where\n"
                + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `store`.`store_type` = `Deluxe Supermarket`\n"
                + "and\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"))
              + "and\n"
              + "    (`product_class`.`product_department` = 'Alcoholic Beverages' and `product_class`.`product_family` = 'Drink')\n"
              + "group by\n"
              + "    `product_class`.`product_family`,\n"
              + "    `product_class`.`product_department`,\n"
              + "    `product_class`.`product_category`\n"
              + "order by\n"
              + "    sum(`"
              + (useAggregates
                  ? "agg_c_14_sales_fact_1997"
                  : "sales_fact_1997")
              + "`.`unit_sales`) DESC,\n"
              + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
              + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC,\n"
              + "    ISNULL(`product_class`.`product_category`) ASC, `product_class`.`product_category` ASC";

        SqlPattern mysqlPattern =
            new SqlPattern(
                DatabaseProduct.MYSQL,
                mysqlQuery,
                mysqlQuery.indexOf("("));
        if (MondrianProperties.instance().EnableNativeTopCount.get()) {
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[Slicer], [Store Type].[Slicer]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]}\n"
            + "Row #0: 1,910\n");
    }

    /**
     * Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-1430">
     * Mondrian-1430:</a> Native top count support for + and
     * tuple (Parentheses) expressions in Calculated member slicer
     */
    public void testNativeTopCountWithParenthesesMemberSlicer() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);

        final boolean useAggregates =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();
        final String mdx =
            "WITH\n"
            + "  SET TC AS 'TopCount([Product].[Drink].[Alcoholic Beverages].Children, 3, [Measures].[Unit Sales] )'\n"
            + "  MEMBER [Time].[Slicer] as [Time].[1997]\n"
            + "  MEMBER [Store Type].[Slicer] as ([Store Type].[Store Type].[Deluxe Supermarket])\n"
            + "\n"
            + "  SELECT NON EMPTY [Measures].[Unit Sales] on 0,\n"
            + "    TC ON 1 \n"
            + "  FROM [Sales] WHERE {([Time].[Slicer], [Store Type].[Slicer])}\n";

        String mysqlQuery =
              "select\n"
              + "    `product_class`.`product_family` as `c0`,\n"
              + "    `product_class`.`product_department` as `c1`,\n"
              + "    `product_class`.`product_category` as `c2`,\n"
              + (useAggregates
              ? ("    sum(`agg_c_14_sales_fact_1997`.`unit_sales`) as `c3`\n"
                + "from\n"
                + "    `product_class` as `product_class`,\n"
                + "    `product` as `product`,\n"
                + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`,\n"
                + "    `store` as `store`\n"
                + "where\n"
                + "    `agg_c_14_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `store`.`store_type` = 'Deluxe Supermarket'\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year` = 1997\n")
              : ("    sum(`sales_fact_1997`.`unit_sales`) as `c3`\n"
                + "from\n"
                + "    `product_class` as `product_class`,\n"
                + "    `product` as `product`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `store` as `store`,\n"
                + "    `time_by_day` as `time_by_day`\n"
                + "where\n"
                + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `store`.`store_type` = `Deluxe Supermarket`\n"
                + "and\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"))
              + "and\n"
              + "    (`product_class`.`product_department` = 'Alcoholic Beverages' and `product_class`.`product_family` = 'Drink')\n"
              + "group by\n"
              + "    `product_class`.`product_family`,\n"
              + "    `product_class`.`product_department`,\n"
              + "    `product_class`.`product_category`\n"
              + "order by\n"
              + "    sum(`"
              + (useAggregates
                  ? "agg_c_14_sales_fact_1997"
                  : "sales_fact_1997")
              + "`.`unit_sales`) DESC,\n"
              + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
              + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC,\n"
              + "    ISNULL(`product_class`.`product_category`) ASC, `product_class`.`product_category` ASC";

        SqlPattern mysqlPattern =
            new SqlPattern(
                DatabaseProduct.MYSQL,
                mysqlQuery,
                mysqlQuery.indexOf("("));
        if (MondrianProperties.instance().EnableNativeTopCount.get()) {
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[Slicer], [Store Type].[Slicer]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]}\n"
            + "Row #0: 1,910\n");
    }

    /**
     * Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-1430">
     * Mondrian-1430:</a> Native top count support for + and
     * tuple (Parentheses) expressions in Calculated member slicer
     */
    public void testNativeTopCountWithMemberSumSlicer() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        final boolean useAggregates =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();
        final String mdx =
            "WITH\n"
            + "  SET TC AS 'TopCount([Product].[Drink].[Alcoholic Beverages].Children, 3, [Measures].[Unit Sales] )'\n"
            + "  MEMBER [Time].[Slicer] as [Time].[1997]\n"
            + "  MEMBER [Store Type].[Slicer] as [Store Type].[Store Type].[Deluxe Supermarket] + [Store Type].[Store Type].[Gourmet Supermarket]\n"
            + "\n"
            + "  SELECT NON EMPTY [Measures].[Unit Sales] on 0,\n"
            + "    TC ON 1 \n"
            + "  FROM [Sales] WHERE {([Time].[Slicer], [Store Type].[Slicer])}\n";

        String mysqlQuery =
              "select\n"
              + "    `product_class`.`product_family` as `c0`,\n"
              + "    `product_class`.`product_department` as `c1`,\n"
              + "    `product_class`.`product_category` as `c2`,\n"
              + (useAggregates
              ? ("    sum(`agg_c_14_sales_fact_1997`.`unit_sales`) as `c3`\n"
                + "from\n"
                + "    `product_class` as `product_class`,\n"
                + "    `product` as `product`,\n"
                + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`,\n"
                + "    `store` as `store`\n"
                + "where\n"
                + "    `agg_c_14_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `store`.`store_type` in ('Deluxe Supermarket', 'Gourmet Supermarket')\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year` = 1997\n")
              : ("    sum(`sales_fact_1997`.`unit_sales`) as `c3`\n"
                + "from\n"
                + "    `product_class` as `product_class`,\n"
                + "    `product` as `product`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `store` as `store`,\n"
                + "    `time_by_day` as `time_by_day`\n"
                + "where\n"
                + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `store`.`store_type` in ('Deluxe Supermarket', 'Gourmet Supermarket')\n"
                + "and\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"))
              + "and\n"
              + "    (`product_class`.`product_department` = 'Alcoholic Beverages' and `product_class`.`product_family` = 'Drink')\n"
              + "group by\n"
              + "    `product_class`.`product_family`,\n"
              + "    `product_class`.`product_department`,\n"
              + "    `product_class`.`product_category`\n"
              + "order by\n"
              + "    sum(`"
              + (useAggregates
                  ? "agg_c_14_sales_fact_1997"
                  : "sales_fact_1997")
              + "`.`unit_sales`) DESC,\n"
              + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
              + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC,\n"
              + "    ISNULL(`product_class`.`product_category`) ASC, `product_class`.`product_category` ASC";

        if (MondrianProperties.instance().EnableNativeTopCount.get()) {
            SqlPattern mysqlPattern =
                new SqlPattern(
                    DatabaseProduct.MYSQL,
                    mysqlQuery,
                    mysqlQuery.indexOf("("));
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[Slicer], [Store Type].[Slicer]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]}\n"
            + "Row #0: 2,435\n");
    }

    /**
     * Aggregate with default measure and TopCount without measure argument.
     */
    public void testAggTCNoExplicitMeasure() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        final String mdx =
            "WITH\n"
            + "  SET TC AS 'TopCount([Product].[Drink].[Alcoholic Beverages].Children, 3)'\n"
            + "  MEMBER [Store Type].[Store Type].[Slicer] as Aggregate([Store Type].[Store Type].Members)\n"
            + "\n"
            + "  SELECT NON EMPTY [Measures].[Unit Sales] on 0,\n"
            + "    TC ON 1 \n"
            + "  FROM [Sales] WHERE [Store Type].[Slicer]\n";
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Store Type].[Slicer]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]}\n"
            + "Row #0: 6,838\n");
    }

    /**
     * Crossjoin that uses same dimension as slicer but is independent from it,
     * evaluated via a named set. No loop should happen here.
     */
    public void testCJSameDimAsSlicerNamedSet() {
        String mdx =
            "WITH\n"
            + "SET ST AS 'TopCount([Store Type].[Store Type].CurrentMember, 5)'\n"
            + "SET TOP_BEV AS 'TopCount([Product].[Drink].Children, 3, [Measures].[Unit Sales])'\n"
            + "SET TC AS TopCount(NonEmptyCrossJoin([Time].[Year].Members, TOP_BEV), 2, [Measures].[Unit Sales])\n"
            + "MEMBER [Product].[Top Drinks] as Aggregate(TC, [Measures].[Unit Sales]) \n"
            + "SET TOP_COUNTRY AS 'TopCount([Customers].[Country].Members, 1, [Measures].[Unit Sales])'\n"
            + "SELECT NON EMPTY [Measures].[Unit Sales] on 0,\n"
            + "  NON EMPTY TOP_COUNTRY ON 1 \n"
            + "FROM [Sales] WHERE [Product].[Top Drinks]";
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Product].[Top Drinks]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customers].[USA]}\n"
            + "Row #0: 20,411\n");
    }

    /**
     * Test evaluation loop detection still works after changes to
     * make it more permissable.
     */
    public void testLoopDetection() {
        if (!MondrianProperties.instance().EnableNativeTopCount.get()) {
            return;
        }
        final String mdx =
            "WITH\n"
            + "  SET CJ AS NonEmptyCrossJoin([Store Type].[Store Type].Members, {[Measures].[Unit Sales]})\n"
            + "  SET TC AS 'TopCount([Store Type].[Store Type].Members, 10, [Measures].[Unit Sales])'\n"
            + "  SET TIME_DEP AS 'Generate(CJ, {[Time].[Time].CurrentMember})' \n"
            + "  MEMBER [Time].[Time].[Slicer] as Aggregate(TIME_DEP)\n"
            + "\n"
            + "  SELECT NON EMPTY [Measures].[Unit Sales] on 0,\n"
            + "    TC ON 1 \n"
            + "  FROM [Sales] where [Time].[Slicer]\n";
        assertQueryThrows(mdx, "evaluating itself");
    }

    /**
     * Partial crossjoin cannot be nativized correctly
     * see MONDRIAN-1187, MONDRIAN-1861
     */
    public void testSlicerTuplesPartialCrossJoin() {
        final String mdx =
            "with\n"
            + "set TSET as {NonEmptyCrossJoin({[Time].[1997].[Q1], [Time].[1997].[Q2]}, {[Store Type].[Supermarket]}),\n"
            + " NonEmptyCrossJoin({[Time].[1997].[Q1]}, {[Store Type].[Deluxe Supermarket], [Store Type].[Gourmet Supermarket]}) }\n"
            + " set products as TopCount(Product.[Product Name].Members, 2, Measures.[Store Sales])\n"
            + " SELECT NON EMPTY products ON 1,\n"
            + "NON EMPTY {[Measures].[Store Sales]} ON 0\n"
            + " FROM [Sales]\n"
            + "where TSET";

        final String result =
            "Axis #0:\n"
            + "{[Time].[1997].[Q1], [Store Type].[Supermarket]}\n"
            + "{[Time].[1997].[Q2], [Store Type].[Supermarket]}\n"
            + "{[Time].[1997].[Q1], [Store Type].[Deluxe Supermarket]}\n"
            + "{[Time].[1997].[Q1], [Store Type].[Gourmet Supermarket]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Raspberry Fruit Roll]}\n"
            + "{[Product].[Food].[Canned Foods].[Canned Soup].[Soup].[Bravo].[Bravo Noodle Soup]}\n"
            + "Row #0: 372.36\n"
            + "Row #1: 365.20\n";

        assertQueryReturns(mdx, result);
    }

    /**
     * Same as before but without combinations missing in the crossjoin
     */
    public void testSlicerTuplesFullCrossJoin() {
        final String mdx =
            "with\n"
            + "set TSET as NonEmptyCrossJoin({[Time].[1997].[Q1], [Time].[1997].[Q2]}, {[Store Type].[Supermarket], [Store Type].[Deluxe Supermarket], [Store Type].[Gourmet Supermarket]})\n"
            + " set products as TopCount(Product.[Product Name].Members, 2, Measures.[Store Sales])\n"
            + " SELECT NON EMPTY products ON 1,\n"
            + "NON EMPTY {[Measures].[Store Sales]} ON 0\n"
            + " FROM [Sales]\n"
            + "where TSET";

        String result =
            "Axis #0:\n"
            + "{[Time].[1997].[Q1], [Store Type].[Deluxe Supermarket]}\n"
            + "{[Time].[1997].[Q1], [Store Type].[Gourmet Supermarket]}\n"
            + "{[Time].[1997].[Q1], [Store Type].[Supermarket]}\n"
            + "{[Time].[1997].[Q2], [Store Type].[Deluxe Supermarket]}\n"
            + "{[Time].[1997].[Q2], [Store Type].[Gourmet Supermarket]}\n"
            + "{[Time].[1997].[Q2], [Store Type].[Supermarket]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Eggs].[Eggs].[Eggs].[Urban].[Urban Small Eggs]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos].[Hermanos Green Pepper]}\n"
            + "Row #0: 460.02\n"
            + "Row #1: 420.74\n";

        assertQueryReturns(mdx, result);
    }

    /**
     * Now that some native evaluation is supporting aggregated members, we
     * need to push that logic down to the AggStar selection
     */
    public void testTopCountWithAggregatedMemberAggStar() {
        propSaver.set(
            propSaver.properties.UseAggregates,
            true);
        propSaver.set(
            propSaver.properties.ReadAggregates,
            true);
        propSaver.set(
            propSaver.properties.GenerateFormattedSql,
            true);

        final String mdx =
            "with member [Time.Weekly].x as Aggregate([Time.Weekly].[1997].Children) "
            + "set products as "
            + "'TopCount([Product].[Product Department].Members, 2, "
            + "[Measures].[Store Sales])' "
            + "select NON EMPTY {[Measures].[Store Sales]} ON COLUMNS, "
            + "NON EMPTY [products] ON ROWS "
            + " from [Sales] where [Time.Weekly].[x]";

        final String mysql =
            "select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    `product_class`.`product_department` as `c1`,\n"
            + "    sum(`agg_pl_01_sales_fact_1997`.`store_sales_sum`) as `c2`\n"
            + "from\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`,\n"
            + "    `agg_pl_01_sales_fact_1997` as `agg_pl_01_sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `agg_pl_01_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `agg_pl_01_sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            +    "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `time_by_day`.`week_of_year` in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52)\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`\n"
            + "order by\n"
            + "    sum(`agg_pl_01_sales_fact_1997`.`store_sales_sum`) DESC,\n"
            + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
            + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC";

        SqlPattern mysqlPattern =
            new SqlPattern(
                DatabaseProduct.MYSQL,
                mysql,
                mysql);

        if (MondrianProperties.instance().EnableNativeTopCount.get()) {
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[Weekly].[x]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Produce]}\n"
            + "{[Product].[Food].[Snack Foods]}\n"
            + "Row #0: 82,248.42\n"
            + "Row #1: 67,609.82\n");
    }

    /**
     * Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-1291">
     * Mondrian-1291:</a> NPE on native set with at least two elements and
     * two all members for same dimension in slicer
     */
    public void testMultipleAllWithInExpr() {
        // set up three hierarchies on same dimension
        final String multiHierarchyCube =
            " <Cube name=\"3StoreHCube\">\n"
            + " <Table name=\"sales_fact_1997\"/>\n"
            + " <Dimension name=\"AltStore\" foreignKey=\"store_id\">\n"
            + " <Hierarchy hasAll=\"true\" primaryKey=\"store_id\" allMemberName=\"All\">\n"
            + " <Table name=\"store\"/>\n"
            + " <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"false\"/>\n"
            + " <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\"/>\n"
            + " </Hierarchy>\n"
            + " <Hierarchy name=\"City\" hasAll=\"true\" primaryKey=\"store_id\" allMemberName=\"All\">\n"
            + " <Table name=\"store\"/>\n"
            + " <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\"/>\n"
            + " <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"false\"/>\n"
            + " </Hierarchy>\n"
            + " <Hierarchy name=\"State\" hasAll=\"true\" primaryKey=\"store_id\" allMemberName=\"All\">\n"
            + " <Table name=\"store\"/>\n"
            + " <Level name=\"Store State\" column=\"store_state\" uniqueMembers=\"false\"/>\n"
            + " <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\"/>\n"
            + " <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"false\"/>\n"
            + " </Hierarchy>\n"
            + " </Dimension>\n"
            + " <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/> \n"
            + " <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n"
            + " <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\" formatString=\"#,###.00\"/>\n"
            + " </Cube>";
        // slicer with multiple elements and two All members
        final String mdx =
            "with member [AltStore].[AllSlicer] as 'Aggregate({[AltStore].[All]})'\n"
            + " member [AltStore.City].[SetSlicer] as 'Aggregate({[AltStore.City].[San Francisco], [AltStore.City].[San Diego]})'\n"
            + " member [AltStore.State].[AllSlicer] as 'Aggregate({[AltStore.State].[All]})'\n"
            + "select {[Time].[1997].[Q1]} ON COLUMNS,\n"
            + " NON EMPTY TopCount(Product.[Product Name].Members, 2, Measures.[Store Sales]) ON ROWS\n"
            + "from [3StoreHCube]\n"
            + "where ([AltStore].[AllSlicer], [AltStore.City].[SetSlicer], [AltStore.State].[AllSlicer])\n";
        String result =
            "Axis #0:\n"
            + "{[AltStore].[AllSlicer], [AltStore.City].[SetSlicer], [AltStore.State].[AllSlicer]}\n"
            + "Axis #1:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Deli].[Meat].[Bologna].[Red Spade].[Red Spade Low Fat Bologna]}\n"
            + "{[Product].[Non-Consumable].[Health and Hygiene].[Bathroom Products].[Mouthwash].[Hilltop].[Hilltop Mint Mouthwash]}\n"
            + "Row #0: 51.60\n"
            + "Row #1: 28.96\n";
        TestContext context =
            getTestContext().create(
                null,
                multiHierarchyCube,
                null,
                null,
                null,
                null);
        context.assertQueryReturns(
            mdx,
            result);
    }


    public void testCompoundSlicerNativeEval() {
        // MONDRIAN-1404
        propSaver.set(
            propSaver.properties.GenerateFormattedSql,
            true);
        propSaver.set(
            propSaver.properties.UseAggregates,
            false);
        final String mdx =
            "select NON EMPTY [Customers].[USA].[CA].[San Francisco].Children ON COLUMNS \n"
            + "from [Sales] \n"
            + "where ([Time].[1997].[Q1] : [Time].[1997].[Q3]) \n";

        final String mysql =
            "select\n"
            + "    `customer`.`customer_id` as `c0`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c1`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c2`,\n"
            + "    `customer`.`gender` as `c3`,\n"
            + "    `customer`.`marital_status` as `c4`,\n"
            + "    `customer`.`education` as `c5`,\n"
            + "    `customer`.`yearly_income` as `c6`\n"
            + "from\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `customer` as `customer`\n"
            + "where\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            +    "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `time_by_day`.`quarter` in ('Q1', 'Q2', 'Q3')\n"
            + "and\n"
            + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
            + "and\n"
            + "    `customer`.`state_province` = 'CA'\n"
            + "and\n"
            + "    `customer`.`city` = 'San Francisco'\n"
            + "group by\n"
            + "    `customer`.`customer_id`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`),\n"
            + "    `customer`.`gender`,\n"
            + "    `customer`.`marital_status`,\n"
            + "    `customer`.`education`,\n"
            + "    `customer`.`yearly_income`\n"
            + "order by\n"
            + "    ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC";
        SqlPattern mysqlPattern =
            new SqlPattern(
                DatabaseProduct.MYSQL,
                mysql,
                mysql);

        if (propSaver.properties.EnableNativeNonEmpty.get()) {
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "{[Time].[1997].[Q2]}\n"
            + "{[Time].[1997].[Q3]}\n"
            + "Axis #1:\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Dennis Messer]}\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Esther Logsdon]}\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Karen Moreland]}\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Kent Brant]}\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Louise Wakefield]}\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Reta Mikalas]}\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Tammy Mihalek]}\n"
            + "Row #0: 8\n"
            + "Row #0: 3\n"
            + "Row #0: 13\n"
            + "Row #0: 5\n"
            + "Row #0: 13\n"
            + "Row #0: 10\n"
            + "Row #0: 1\n");
    }

    public void testSnowflakeDimInSlicerBug1407() {
        // MONDRIAN-1407
        propSaver.set(
            propSaver.properties.GenerateFormattedSql,
            true);
        propSaver.set(
            propSaver.properties.UseAggregates,
            false);
        final String mdx =
            "select TopCount([Customers].[Name].members, 5, measures.[unit sales]) ON COLUMNS \n"
            + "  from sales where \n"
            + " { [Time].[1997]} * {[Product].[All Products].[Drink], [Product].[All Products].[Food] }";

        final String mysql =
            "select\n"
            + "    `customer`.`country` as `c0`,\n"
            + "    `customer`.`state_province` as `c1`,\n"
            + "    `customer`.`city` as `c2`,\n"
            + "    `customer`.`customer_id` as `c3`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c4`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c5`,\n"
            + "    `customer`.`gender` as `c6`,\n"
            + "    `customer`.`marital_status` as `c7`,\n"
            + "    `customer`.`education` as `c8`,\n"
            + "    `customer`.`yearly_income` as `c9`,\n"
            + "    sum(`sales_fact_1997`.`unit_sales`) as `c10`\n"
            + "from\n"
            + "    `customer` as `customer`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`\n"
            + "where\n"
            + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `product_class`.`product_family` in ('Drink', 'Food')\n"
            + "group by\n"
            + "    `customer`.`country`,\n"
            + "    `customer`.`state_province`,\n"
            + "    `customer`.`city`,\n"
            + "    `customer`.`customer_id`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`),\n"
            + "    `customer`.`gender`,\n"
            + "    `customer`.`marital_status`,\n"
            + "    `customer`.`education`,\n"
            + "    `customer`.`yearly_income`\n"
            + "order by\n"
            + "    sum(`sales_fact_1997`.`unit_sales`) DESC,\n"
            + "    ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC,\n"
            + "    ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC,\n"
            + "    ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC,\n"
            + "    ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC";
        SqlPattern mysqlPattern =
            new SqlPattern(
                DatabaseProduct.MYSQL,
                mysql,
                mysql);

        if (MondrianProperties.instance().EnableNativeTopCount.get()) {
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997], [Product].[Drink]}\n"
            + "{[Time].[1997], [Product].[Food]}\n"
            + "Axis #1:\n"
            + "{[Customers].[USA].[WA].[Spokane].[Mary Francis Benigar]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[James Horvat]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Wildon Cameron]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Ida Rodriguez]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Joann Mramor]}\n"
            + "Row #0: 427\n"
            + "Row #0: 384\n"
            + "Row #0: 366\n"
            + "Row #0: 357\n"
            + "Row #0: 324\n");
    }

    public void testCompoundSlicerNonUniqueMemberNames1413() {
        // MONDRIAN-1413
        propSaver.set(
            propSaver.properties.GenerateFormattedSql,
            true);
        propSaver.set(
            propSaver.properties.UseAggregates,
            false);
        final String mdx =
            "select TopCount([Customers].[Name].members, 5, "
            + "measures.[unit sales]) ON COLUMNS \n"
            + "  from sales where \n"
            + "  {[Time.Weekly].[1997].[48].[17] :[Time.Weekly].[1997].[48].[20]} ";

        final String mysql =
            "select\n"
            + "    `customer`.`country` as `c0`,\n"
            + "    `customer`.`state_province` as `c1`,\n"
            + "    `customer`.`city` as `c2`,\n"
            + "    `customer`.`customer_id` as `c3`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c4`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c5`,\n"
            + "    `customer`.`gender` as `c6`,\n"
            + "    `customer`.`marital_status` as `c7`,\n"
            + "    `customer`.`education` as `c8`,\n"
            + "    `customer`.`yearly_income` as `c9`,\n"
            + "    sum(`sales_fact_1997`.`unit_sales`) as `c10`\n"
            + "from\n"
            + "    `customer` as `customer`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `time_by_day`.`week_of_year` = 48\n"
            + "and\n"
            + "    `time_by_day`.`day_of_month` in (17, 18, 19, 20)\n"
            + "group by\n"
            + "    `customer`.`country`,\n"
            + "    `customer`.`state_province`,\n"
            + "    `customer`.`city`,\n"
            + "    `customer`.`customer_id`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`),\n"
            + "    `customer`.`gender`,\n"
            + "    `customer`.`marital_status`,\n"
            + "    `customer`.`education`,\n"
            + "    `customer`.`yearly_income`\n"
            + "order by\n"
            + "    sum(`sales_fact_1997`.`unit_sales`) DESC,\n"
            + "    ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC,\n"
            + "    ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC,\n"
            + "    ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC,\n"
            + "    ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC";
        SqlPattern mysqlPattern =
            new SqlPattern(
                DatabaseProduct.MYSQL,
                mysql,
                mysql);

        if (MondrianProperties.instance().EnableNativeTopCount.get()) {
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[Weekly].[1997].[48].[17]}\n"
            + "{[Time].[Weekly].[1997].[48].[18]}\n"
            + "{[Time].[Weekly].[1997].[48].[19]}\n"
            + "{[Time].[Weekly].[1997].[48].[20]}\n"
            + "Axis #1:\n"
            + "{[Customers].[USA].[WA].[Yakima].[Joanne Skuderna]}\n"
            + "{[Customers].[USA].[WA].[Yakima].[Paula Stevens]}\n"
            + "{[Customers].[USA].[WA].[Everett].[Sarah Miller]}\n"
            + "{[Customers].[USA].[OR].[Albany].[Kathryn Chamberlin]}\n"
            + "{[Customers].[USA].[OR].[Salem].[Scott Pavicich]}\n"
            + "Row #0: 37\n"
            + "Row #0: 32\n"
            + "Row #0: 29\n"
            + "Row #0: 28\n"
            + "Row #0: 28\n");
    }

    public void testConstraintCacheIncludesMultiPositionSlicer() {
        // MONDRIAN-2081
        assertQueryReturns(
            "select non empty [Customers].[USA].[WA].[Spokane].children  on 0, "
            + "Time.[1997].[Q1].[1] * [Store].[USA].[WA].[Spokane] * Gender.F * [Marital Status].M on 1 from sales where\n"
            + "{[Product].[Food].[Snacks].[Candy].[Gum].[Atomic].[Atomic Bubble Gum],\n"
            + "[Product].[Food].[Snacks].[Candy].[Gum].[Choice].[Choice Bubble Gum]}",
            "Axis #0:\n"
            + "{[Product].[Food].[Snacks].[Candy].[Gum].[Atomic].[Atomic Bubble Gum]}\n"
            + "{[Product].[Food].[Snacks].[Candy].[Gum].[Choice].[Choice Bubble Gum]}\n"
            + "Axis #1:\n"
            + "{[Customers].[USA].[WA].[Spokane].[David Cocadiz]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Peter Von Breymann]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q1].[1], [Store].[USA].[WA].[Spokane], [Gender].[F], [Marital Status].[M]}\n"
            + "Row #0: 4\n"
            + "Row #0: 3\n");
        assertQueryReturns(
            "select non empty [Customers].[USA].[WA].[Spokane].children on 0, "
            + "Time.[1997].[Q1].[1] * [Store].[USA].[WA].[Spokane] * Gender.F *"
            + "[Marital Status].M on 1 from sales where "
            + "   { [Product].[Food], [Product].[Drink] }",
            "Axis #0:\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Drink]}\n"
            + "Axis #1:\n"
            + "{[Customers].[USA].[WA].[Spokane].[Abbie Carlbon]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Bob Alexander]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Dauna Barton]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[David Cocadiz]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[David Hassard]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Dawn Laner]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Donna Weisinger]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Fran McEvilly]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[James Horvat]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[John Lenorovitz]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Linda Combs]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Luther Moran]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Martha Griego]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Peter Von Breymann]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Richard Callahan]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Robert Vaughn]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Shirley Gottbehuet]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Stanley Marks]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Suzanne Davis]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Takiko Collins]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Virginia Bell]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q1].[1], [Store].[USA].[WA].[Spokane], [Gender].[F], [Marital Status].[M]}\n"
            + "Row #0: 25\n"
            + "Row #0: 17\n"
            + "Row #0: 17\n"
            + "Row #0: 30\n"
            + "Row #0: 16\n"
            + "Row #0: 9\n"
            + "Row #0: 6\n"
            + "Row #0: 12\n"
            + "Row #0: 61\n"
            + "Row #0: 15\n"
            + "Row #0: 20\n"
            + "Row #0: 27\n"
            + "Row #0: 36\n"
            + "Row #0: 22\n"
            + "Row #0: 32\n"
            + "Row #0: 2\n"
            + "Row #0: 30\n"
            + "Row #0: 19\n"
            + "Row #0: 27\n"
            + "Row #0: 3\n"
            + "Row #0: 7\n");
    }

    /**
     * This is a test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1630">MONDRIAN-1630</a>
     *
     * <p>The baseCube was taken out of the evaluator instead of being passed
     * by the caller, which caused the star column not to be found for the
     * level to evaluate natively as part of the set.
     */
    public void testNativeVirtualRestrictedSet() throws Exception {
        final TestContext ctx = getTestContext().create(
            null, null, null, null, null,
            "  <Role name=\"F-MIS-BE-CLIENT\">\n"
            + "    <SchemaGrant access=\"none\">\n"
            + "      <CubeGrant cube=\"Warehouse and Sales\" access=\"all\">\n"
            + "        <HierarchyGrant hierarchy=\"[Store]\" rollupPolicy=\"partial\" access=\"custom\">\n"
            + "          <MemberGrant member=\"[Store].[All Stores]\" access=\"none\">\n"
            + "          </MemberGrant>\n"
            + "          <MemberGrant member=\"[Store].[USA]\" access=\"all\">\n"
            + "          </MemberGrant>\n"
            + "        </HierarchyGrant>\n"
            + "      </CubeGrant>\n"
            + "      <CubeGrant cube=\"Warehouse\" access=\"all\">\n"
            + "        <HierarchyGrant hierarchy=\"[Store]\" rollupPolicy=\"partial\" access=\"custom\">\n"
            + "          <MemberGrant member=\"[Store].[All Stores]\" access=\"none\">\n"
            + "          </MemberGrant>\n"
            + "          <MemberGrant member=\"[Store].[USA]\" access=\"all\">\n"
            + "          </MemberGrant>\n"
            + "        </HierarchyGrant>\n"
            + "      </CubeGrant>\n"
            + "    </SchemaGrant>\n"
            + "  </Role>\n").withRole("F-MIS-BE-CLIENT");
        ctx.executeQuery(
            "With\n"
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Store],[*BASE_MEMBERS_Warehouse])'\n"
            + "Set [*SORTED_ROW_AXIS] as 'Order([*CJ_ROW_AXIS],[Store].CurrentMember.OrderKey,BASC,[Warehouse].CurrentMember.OrderKey,BASC)'\n"
            + "Set [*BASE_MEMBERS_Warehouse] as '[Warehouse].[Country].Members'\n"
            + "Set [*BASE_MEMBERS_Store] as '[Store].[Store Country].Members'\n"
            + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Store].currentMember,[Warehouse].currentMember)})'\n"
            + "Set [*CJ_COL_AXIS] as '[*NATIVE_CJ_SET]'\n"
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Store Invoice]', FORMAT_STRING = '#,###.00', SOLVE_ORDER=400\n"
            + "Select\n"
            + "[*BASE_MEMBERS_Measures] on columns,\n"
            + "Non Empty [*SORTED_ROW_AXIS] on rows\n"
            + "From [Warehouse and Sales]\n");
    }

    public void testNativeHonorsRoleRestrictions() {
        // NativeSetEvaluation pushes role restrictions to the where clause
        // (see SqlConstraintUtils.addRoleAccessConstraints) by
        // generating an IN expression based on accessible members.
        // If the number of accessible members in a hierarchy w/ CUSTOM
        // access exceeds MaxConstraints, it is not possible to
        // include the full role restriction in the IN clause.
        // This test verifies only permitted members are returned in this
        // case.
        propSaver.set(MondrianProperties.instance().MaxConstraints, 4);
        String roleDef =
            "  <Role name=\"Test\">\n"
            + "    <SchemaGrant access=\"none\">\n"
            + "      <CubeGrant cube=\"Sales\" access=\"all\">\n"
            + "        <HierarchyGrant hierarchy=\"[Product]\" rollupPolicy=\"partial\" access=\"custom\">\n"
            + "          <MemberGrant member=\"[Product].[Non-Consumable].[Household].[Electrical].[Batteries].[Cormorant].[Cormorant AA-Size Batteries]\" access=\"all\" />\n"
            + "          <MemberGrant member=\"[Product].[Non-Consumable].[Household].[Electrical].[Batteries].[Cormorant].[Cormorant AA-Size Batteries]\" access=\"all\"/>\n"
            + "          <MemberGrant member=\"[Product].[Non-Consumable].[Household].[Electrical].[Batteries].[Cormorant].[Cormorant AAA-Size Batteries]\" access=\"all\"/>\n"
            + "          <MemberGrant member=\"[Product].[Non-Consumable].[Household].[Electrical].[Batteries].[Cormorant].[Cormorant C-Size Batteries]\" access=\"all\"/>\n"
            + "          <MemberGrant member=\"[Product].[Non-Consumable].[Household].[Electrical].[Batteries].[Denny].[Denny AA-Size Batteries]\" access=\"all\"/>\n"
            + "          <MemberGrant member=\"[Product].[Non-Consumable].[Household].[Electrical].[Batteries].[Denny].[Denny AAA-Size Batteries]\" access=\"all\"/>\n"
            + "        </HierarchyGrant>\n"
            + "      </CubeGrant>\n"
            + "    </SchemaGrant>\n"
            + "  </Role>";
        // The following queries should not include [Denny C-Size Batteries] or
        // [Denny D-Size Batteries]
        final TestContext ctx = getTestContext().create(
            null, null, null, null, null, roleDef).withRole("Test");
        verifySameNativeAndNot(
            "select non empty crossjoin([Store].[USA],[Product].[Product Name].members) on 0 from sales",
            "Native crossjoin mismatch", ctx);
        verifySameNativeAndNot(
            "select topcount([Product].[Product Name].members, 6, Measures.[Unit Sales]) on 0 from sales",
            "Native topcount mismatch", ctx);
        verifySameNativeAndNot(
            "select filter([Product].[Product Name].members, Measures.[Unit Sales] > 0) on 0 from sales",
            "Native native filter mismatch", ctx);
        propSaver.reset();
    }

    public void testNativeSubsetWithVirtualCube() {
        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();

        propSaver.set(propSaver.properties.GenerateFormattedSql, true);

        final String mdx =
            "WITH\n"
            + "  SET TC AS 'Subset([Product].[Food].Children, 2, 10 )'\n"
            + "\n"
            + "  SELECT NON EMPTY {[Measures].[Unit Sales],[Measures].[Store Invoice]} on 0,\n"
            + "    TC ON 1 \n"
            + "  FROM [Warehouse and Sales] WHERE {([Time].[1997])}\n";

        String mysqlQuery =
            "select\n"
            + "    *\n"
            + "from\n"
            + "    (select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    `product_class`.`product_department` as `c1`\n"
            + "from\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    (`product_class`.`product_family` = 'Food')\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`\n"
            + "union\n"
            + "select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    `product_class`.`product_department` as `c1`\n"
            + "from\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`,\n"
            + "    `inventory_fact_1997` as `inventory_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `inventory_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `inventory_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    (`product_class`.`product_family` = 'Food')\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`) as `unionQuery`\n"
            + "order by\n"
            + "    ISNULL(1) ASC, 1 ASC,\n"
            + "    ISNULL(2) ASC, 2 ASC limit 10 offset 2";

        if (!useAgg && MondrianProperties.instance().EnableNativeSubset.get()) {
            SqlPattern mysqlPattern =
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Invoice]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Breakfast Foods]}\n"
            + "{[Product].[Food].[Canned Foods]}\n"
            + "{[Product].[Food].[Canned Products]}\n"
            + "{[Product].[Food].[Dairy]}\n"
            + "{[Product].[Food].[Deli]}\n"
            + "{[Product].[Food].[Eggs]}\n"
            + "{[Product].[Food].[Frozen Foods]}\n"
            + "{[Product].[Food].[Meat]}\n"
            + "{[Product].[Food].[Produce]}\n"
            + "{[Product].[Food].[Seafood]}\n"
            + "Row #0: 3,317\n"
            + "Row #0: 1,034.384\n"
            + "Row #1: 19,026\n"
            + "Row #1: 6,952.89\n"
            + "Row #2: 1,812\n"
            + "Row #2: 382.504\n"
            + "Row #3: 12,885\n"
            + "Row #3: 6,209.258\n"
            + "Row #4: 12,037\n"
            + "Row #4: 4,279.385\n"
            + "Row #5: 4,132\n"
            + "Row #5: 1,956.589\n"
            + "Row #6: 26,655\n"
            + "Row #6: 9,595.334\n"
            + "Row #7: 1,714\n"
            + "Row #7: 764.79\n"
            + "Row #8: 37,792\n"
            + "Row #8: 14,897.605\n"
            + "Row #9: 1,764\n"
            + "Row #9: 621.922\n");
    }

    public void testNativeSubset() {
        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();

        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        final String mdx =
            "WITH\n"
            + "  SET TC AS 'Subset([Product].[Food].Children, 2, 10 )'\n"
            + "\n"
            + "  SELECT NON EMPTY [Measures].[Unit Sales] on 0,\n"
            + "    TC ON 1 \n"
            + "  FROM [Sales] WHERE {([Time].[1997])}\n";

        String mysqlQuery =
            "select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    `product_class`.`product_department` as `c1`\n"
            + "from\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    (`product_class`.`product_family` = 'Food')\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`\n"
            + "order by\n"
            + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
            + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC limit 10 offset 2";

        if (!useAgg && MondrianProperties.instance().EnableNativeSubset.get()) {
            SqlPattern mysqlPattern =
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Breakfast Foods]}\n"
            + "{[Product].[Food].[Canned Foods]}\n"
            + "{[Product].[Food].[Canned Products]}\n"
            + "{[Product].[Food].[Dairy]}\n"
            + "{[Product].[Food].[Deli]}\n"
            + "{[Product].[Food].[Eggs]}\n"
            + "{[Product].[Food].[Frozen Foods]}\n"
            + "{[Product].[Food].[Meat]}\n"
            + "{[Product].[Food].[Produce]}\n"
            + "{[Product].[Food].[Seafood]}\n"
            + "Row #0: 3,317\n"
            + "Row #1: 19,026\n"
            + "Row #2: 1,812\n"
            + "Row #3: 12,885\n"
            + "Row #4: 12,037\n"
            + "Row #5: 4,132\n"
            + "Row #6: 26,655\n"
            + "Row #7: 1,714\n"
            + "Row #8: 37,792\n"
            + "Row #9: 1,764\n");
    }

    public void testNativeSubsetWithoutLimit() {
        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();

        final String mdx =
            "WITH\n"
            + "  SET TC AS 'Subset([Product].[Food].Children, 10)'\n"
            + "  SELECT NON EMPTY [Measures].[Unit Sales] on 0,\n"
            + "    TC ON 1 \n"
            + "  FROM [Sales] WHERE {([Time].[1997])}\n";

        if (!useAgg && MondrianProperties.instance().EnableNativeSubset.get()) {
            propSaver.set(propSaver.properties.GenerateFormattedSql, true);

            // According to https://dev.mysql.com/doc/refman/5.7/en/select.html:
            // To retrieve all rows from a certain offset up to the end of
            // the result set, you can use some large number for the second parameter.
            final String mysqlQuery =
                "select\n"
                + "    `product_class`.`product_family` as `c0`,\n"
                + "    `product_class`.`product_department` as `c1`\n"
                + "from\n"
                + "    `product_class` as `product_class`,\n"
                + "    `product` as `product`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `time_by_day` as `time_by_day`\n"
                + "where\n"
                + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"
                + "and\n"
                + "    (`product_class`.`product_family` = 'Food')\n"
                + "group by\n"
                + "    `product_class`.`product_family`,\n"
                + "    `product_class`.`product_department`\n"
                + "order by\n"
                + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
                + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC limit 9223372036854775807 offset 10";
            SqlPattern mysqlPattern =
                new SqlPattern(DatabaseProduct.MYSQL, mysqlQuery, null);
            final String verticaQuery =
                "select\n"
                + "    \"product_class\".\"product_family\" as \"c0\",\n"
                + "    \"product_class\".\"product_department\" as \"c1\"\n"
                + "from\n"
                + "    \"product_class\" as \"product_class\",\n"
                + "    \"product\" as \"product\",\n"
                + "    \"sales_fact_1997\" as \"sales_fact_1997\",\n"
                + "    \"time_by_day\" as \"time_by_day\"\n"
                + "where\n"
                + "    \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
                + "and\n"
                + "    \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
                + "and\n"
                + "    \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
                + "and\n"
                + "    \"time_by_day\".\"the_year\" = 1997\n"
                + "and\n"
                + "    (\"product_class\".\"product_family\" = 'Food')\n"
                + "group by\n"
                + "    \"product_class\".\"product_family\",\n"
                + "    \"product_class\".\"product_department\"\n"
                + "order by\n"
                + "    CASE WHEN \"product_class\".\"product_family\" IS NULL THEN 1 ELSE 0 END, \"product_class\".\"product_family\" ASC,\n"
                + "    CASE WHEN \"product_class\".\"product_department\" IS NULL THEN 1 ELSE 0 END, \"product_class\".\"product_department\" ASC offset 10";
            SqlPattern verticaPattern =
                new SqlPattern(DatabaseProduct.VERTICA, verticaQuery, null);
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern, verticaPattern});
        }
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Produce]}\n"
            + "{[Product].[Food].[Seafood]}\n"
            + "{[Product].[Food].[Snack Foods]}\n"
            + "{[Product].[Food].[Snacks]}\n"
            + "{[Product].[Food].[Starchy Foods]}\n"
            + "Row #0: 37,792\n"
            + "Row #1: 1,764\n"
            + "Row #2: 30,545\n"
            + "Row #3: 6,884\n"
            + "Row #4: 5,262\n");
    }

    public void testNativeSubsetWithAllMembers() {
        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();

        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        final String mdx =
            "SELECT SUBSET( [Product].[Product Name].ALLMEMBERS, 0, 3 ) DIMENSION PROPERTIES MEMBER_NAME, MEMBER_TYPE ON 0, {} ON 1 FROM [Sales]";

        String mysqlQuery =
            "select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    `product_class`.`product_department` as `c1`,\n"
            + "    `product_class`.`product_category` as `c2`,\n"
            + "    `product_class`.`product_subcategory` as `c3`,\n"
            + "    `product`.`brand_name` as `c4`,\n"
            + "    `product`.`product_name` as `c5`\n"
            + "from\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`,\n"
            + "    `product_class`.`product_category`,\n"
            + "    `product_class`.`product_subcategory`,\n"
            + "    `product`.`brand_name`,\n"
            + "    `product`.`product_name`\n"
            + "order by\n"
            + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
            + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC,\n"
            + "    ISNULL(`product_class`.`product_category`) ASC, `product_class`.`product_category` ASC,\n"
            + "    ISNULL(`product_class`.`product_subcategory`) ASC, `product_class`.`product_subcategory` ASC,\n"
            + "    ISNULL(`product`.`brand_name`) ASC, `product`.`brand_name` ASC,\n"
            + "    ISNULL(`product`.`product_name`) ASC, `product`.`product_name` ASC limit 3 offset 0";

        if (!useAgg && MondrianProperties.instance().EnableNativeSubset.get()) {
            SqlPattern mysqlPattern =
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl].[Pearl Imported Beer]}\n"
            + "Axis #2:\n");
    }

    public void testNativeNamedSetWithNesting() {
        if (!propSaver.properties.EnableNativeSubset.get()
            || propSaver.properties.UseAggregates.get()
            || propSaver.properties.ReadAggregates.get()) {
            return;
        }
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        final String mdx =
            "WITH\n"
            + "  SET TC AS Subset([Product].[Food].Children, 2, 10)\n"
            + "  SET TC1 AS TC\n"
            + "  SELECT NON EMPTY [Measures].[Unit Sales] on 0,\n"
            + "    TC1 ON 1 \n"
            + "  FROM [Sales] WHERE {([Time].[1997])}\n";

        String mysqlQuery =
            "select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    `product_class`.`product_department` as `c1`\n"
            + "from\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    (`product_class`.`product_family` = 'Food')\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`\n"
            + "order by\n"
            + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
            + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC limit 10 offset 2";
        SqlPattern mysqlPattern =
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
        assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Breakfast Foods]}\n"
            + "{[Product].[Food].[Canned Foods]}\n"
            + "{[Product].[Food].[Canned Products]}\n"
            + "{[Product].[Food].[Dairy]}\n"
            + "{[Product].[Food].[Deli]}\n"
            + "{[Product].[Food].[Eggs]}\n"
            + "{[Product].[Food].[Frozen Foods]}\n"
            + "{[Product].[Food].[Meat]}\n"
            + "{[Product].[Food].[Produce]}\n"
            + "{[Product].[Food].[Seafood]}\n"
            + "Row #0: 3,317\n"
            + "Row #1: 19,026\n"
            + "Row #2: 1,812\n"
            + "Row #3: 12,885\n"
            + "Row #4: 12,037\n"
            + "Row #5: 4,132\n"
            + "Row #6: 26,655\n"
            + "Row #7: 1,714\n"
            + "Row #8: 37,792\n"
            + "Row #9: 1,764\n");
    }

    public void testNativeOrder() {
        if (!propSaver.properties.EnableNativeOrder.get()
            || !propSaver.properties.EnableNativeNonEmptyFunction.get()
            || propSaver.properties.UseAggregates.get()
            || propSaver.properties.ReadAggregates.get()) {
            return;
        }
        propSaver.set(propSaver.properties.ExpandNonNative, false);
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx =
            "WITH\n"
            + "  SET [PRODUCT_SET] as 'NonEmpty([Product].[Product Family].Members, [Measures].[Store Cost])'\n"
            + "  SET [PRODUCT_SET_SORTED] as 'Order([PRODUCT_SET], [Measures].[Store Cost], BDESC)' \n"
            + "SELECT\n"
            + "  [Measures].[Store Cost] ON COLUMNS,\n"
            + "  [PRODUCT_SET_SORTED] ON ROWS\n"
            + "FROM [Sales]";
        String mysqlQuery =
            "select\n"
            + "    `time_by_day`.`the_year` as `c0`,\n"
            + "    `product_class`.`product_family` as `c1`,\n"
            + "    sum(`sales_fact_1997`.`store_cost`) as `m0`\n"
            + "from\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`\n"
            + "where\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "group by\n"
            + "    `time_by_day`.`the_year`,\n"
            + "    `product_class`.`product_family`";
        SqlPattern mysqlPattern =
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
        assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Cost]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "{[Product].[Drink]}\n"
            + "Row #0: 163,270.72\n"
            + "Row #1: 42,879.28\n"
            + "Row #2: 19,477.23\n");
    }

    public void testNativeNonEmptyWithBasicCalcMeasure() {
        final String mdx =
            "WITH MEMBER [Measures].[Calc] AS '[Measures].[Store Sales]'\n"
            + "SELECT\n"
            + "NonEmpty([Store].[Store City].Members, {[Measures].[Calc]}) ON 0\n"
            + "FROM [Sales]";

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[CA].[San Francisco]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA].[Bellingham]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "{[Store].[USA].[WA].[Walla Walla]}\n"
            + "{[Store].[USA].[WA].[Yakima]}\n"
            + "Row #0: 21,333\n"
            + "Row #0: 25,663\n"
            + "Row #0: 25,635\n"
            + "Row #0: 2,117\n"
            + "Row #0: 26,079\n"
            + "Row #0: 41,580\n"
            + "Row #0: 2,237\n"
            + "Row #0: 24,576\n"
            + "Row #0: 25,011\n"
            + "Row #0: 23,591\n"
            + "Row #0: 35,257\n"
            + "Row #0: 2,203\n"
            + "Row #0: 11,491\n");

        if (!MondrianProperties.instance().EnableNativeNonEmptyFunction.get()) {
            return;
        }
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mysqlQuery = isUseAgg()
            ? "select\n"
              + "    `store`.`store_country` as `c0`,\n"
              + "    `store`.`store_state` as `c1`,\n"
              + "    `store`.`store_city` as `c2`\n"
              + "from\n"
              + "    `store` as `store`,\n"
              + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`\n"
              + "where\n"
              + "    `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id`\n"
              + "and\n"
              + "    `agg_c_14_sales_fact_1997`.`the_year` = 1997\n"
              + "and\n"
              + "    `agg_c_14_sales_fact_1997`.`store_sales` is not null\n"
              + "group by\n"
              + "    `store`.`store_country`,\n"
              + "    `store`.`store_state`,\n"
              + "    `store`.`store_city`\n"
              + "order by\n"
              + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
              + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
              + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC"
            : "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`,\n"
            + "    `store`.`store_city` as `c2`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`store_sales` is not null\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`,\n"
            + "    `store`.`store_city`\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
            + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
            + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC";
        SqlPattern mysqlPattern =
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
        assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
    }

    public void testNativeNonEmptyWithMeasure() {
        final String mdx =
            "SELECT\n"
            + "NonEmpty([Store].[Store City].Members, [Measures].[Store Sales]) ON 0\n"
            + "FROM [Sales]";

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[CA].[San Francisco]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA].[Bellingham]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "{[Store].[USA].[WA].[Walla Walla]}\n"
            + "{[Store].[USA].[WA].[Yakima]}\n"
            + "Row #0: 21,333\n"
            + "Row #0: 25,663\n"
            + "Row #0: 25,635\n"
            + "Row #0: 2,117\n"
            + "Row #0: 26,079\n"
            + "Row #0: 41,580\n"
            + "Row #0: 2,237\n"
            + "Row #0: 24,576\n"
            + "Row #0: 25,011\n"
            + "Row #0: 23,591\n"
            + "Row #0: 35,257\n"
            + "Row #0: 2,203\n"
            + "Row #0: 11,491\n");

        if (!MondrianProperties.instance().EnableNativeNonEmptyFunction.get()) {
            return;
        }
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mysqlQuery = isUseAgg()
            ? "select\n"
              + "    `store`.`store_country` as `c0`,\n"
              + "    `store`.`store_state` as `c1`,\n"
              + "    `store`.`store_city` as `c2`\n"
              + "from\n"
              + "    `store` as `store`,\n"
              + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`\n"
              + "where\n"
              + "    `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id`\n"
              + "and\n"
              + "    `agg_c_14_sales_fact_1997`.`the_year` = 1997\n"
              + "and\n"
              + "    `agg_c_14_sales_fact_1997`.`store_sales` is not null\n"
              + "group by\n"
              + "    `store`.`store_country`,\n"
              + "    `store`.`store_state`,\n"
              + "    `store`.`store_city`\n"
              + "order by\n"
              + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
              + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
              + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC"
            : "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`,\n"
            + "    `store`.`store_city` as `c2`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`store_sales` is not null\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`,\n"
            + "    `store`.`store_city`\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
            + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
            + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC";
        SqlPattern mysqlPattern =
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
        assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
    }

    public void testNativeNonEmptyFuncMeasureAndMembers() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx = "SELECT { Measures.[Store Sales] } ON COLUMNS,\n"
            + "       NonEmpty( [Customers].[USA].[CA].[San Francisco].Children, CrossJoin({Gender.[F]}, CrossJoin( {[Measures].[Store Sales]}, {[Marital Status].[S]}) ) ) ON ROWS\n"
            + "FROM [Sales]";
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Customers].[USA].[CA].[San Francisco].[Gladys Evans]}\n"
            + "Row #0: 17.20\n");
        if (!MondrianProperties.instance().EnableNativeNonEmptyFunction.get()) {
            return;
        }
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mySql = isUseAgg()
            ? "select\n"
            + "    `customer`.`country` as `c0`,\n"
            + "    `customer`.`state_province` as `c1`,\n"
            + "    `customer`.`city` as `c2`,\n"
            + "    `customer`.`customer_id` as `c3`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c4`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c5`,\n"
            + "    `customer`.`gender` as `c6`,\n"
            + "    `customer`.`marital_status` as `c7`,\n"
            + "    `customer`.`education` as `c8`,\n"
            + "    `customer`.`yearly_income` as `c9`\n"
            + "from\n"
            + "    `customer` as `customer`,\n"
            + "    `agg_l_03_sales_fact_1997` as `agg_l_03_sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `agg_l_03_sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
            + "and\n"
            + "    `agg_l_03_sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    (`customer`.`city` = 'San Francisco' and `customer`.`state_province` = 'CA')\n"
            + "and\n"
            + "    (`customer`.`gender` = 'F')\n"
            + "and\n"
            + "    (`customer`.`marital_status` = 'S')\n"
            + "and\n"
            + "    `agg_l_03_sales_fact_1997`.`store_sales` is not null\n"
            + "group by\n"
            + "    `customer`.`country`,\n"
            + "    `customer`.`state_province`,\n"
            + "    `customer`.`city`,\n"
            + "    `customer`.`customer_id`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`),\n"
            + "    `customer`.`gender`,\n"
            + "    `customer`.`marital_status`,\n"
            + "    `customer`.`education`,\n"
            + "    `customer`.`yearly_income`\n"
            + "order by\n"
            + "    ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC,\n"
            + "    ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC,\n"
            + "    ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC,\n"
            + "    ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC"
            : "select\n"
            + "    `customer`.`country` as `c0`,\n"
            + "    `customer`.`state_province` as `c1`,\n"
            + "    `customer`.`city` as `c2`,\n"
            + "    `customer`.`customer_id` as `c3`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c4`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) as `c5`,\n"
            + "    `customer`.`gender` as `c6`,\n"
            + "    `customer`.`marital_status` as `c7`,\n"
            + "    `customer`.`education` as `c8`,\n"
            + "    `customer`.`yearly_income` as `c9`\n"
            + "from\n"
            + "    `customer` as `customer`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    (`customer`.`city` = 'San Francisco' and `customer`.`state_province` = 'CA')\n"
            + "and\n"
            + "    (`customer`.`gender` = 'F')\n"
            + "and\n"
            + "    (`customer`.`marital_status` = 'S')\n"
            + "and\n"
            + "    `sales_fact_1997`.`store_sales` is not null\n"
            + "group by\n"
            + "    `customer`.`country`,\n"
            + "    `customer`.`state_province`,\n"
            + "    `customer`.`city`,\n"
            + "    `customer`.`customer_id`,\n"
            + "    CONCAT(`customer`.`fname`, ' ', `customer`.`lname`),\n"
            + "    `customer`.`gender`,\n"
            + "    `customer`.`marital_status`,\n"
            + "    `customer`.`education`,\n"
            + "    `customer`.`yearly_income`\n"
            + "order by\n"
            + "    ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC,\n"
            + "    ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC,\n"
            + "    ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC,\n"
            + "    ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC";
        SqlPattern mysqlPattern =
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, mySql, null);
        assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
    }

    public void testNativeNonEmptyFunc() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        final String mdx =
            "SELECT\n"
            + "  { Measures.[Store Sales] } ON COLUMNS,\n"
            + "NonEmpty( [Store].[Store City].Members, [Product].[Food].Children ) ON ROWS\n"
            + "FROM [Sales]";

        if (MondrianProperties.instance().EnableNativeNonEmptyFunction.get()) {
            final boolean useAgg =
                MondrianProperties.instance().UseAggregates.get()
                && MondrianProperties.instance().ReadAggregates.get();
            SqlPattern mysqlPattern =
                ((useAgg)
                    ? new SqlPattern(
                        Dialect.DatabaseProduct.MYSQL,
                        "select\n"
                        + "    `store`.`store_country` as `c0`,\n"
                        + "    `store`.`store_state` as `c1`,\n"
                        + "    `store`.`store_city` as `c2`\n"
                        + "from\n"
                        + "    `store` as `store`,\n"
                        + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`,\n"
                        + "    `product_class` as `product_class`,\n"
                        + "    `product` as `product`\n"
                        + "where\n"
                        + "    `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                        + "and\n"
                        + "    `agg_c_14_sales_fact_1997`.`the_year` = 1997\n"
                        + "and\n"
                        + "    `agg_c_14_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                        + "and\n"
                        + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                        + "and\n"
                        + "    (`product_class`.`product_family` = 'Food')\n"
                        + "group by\n"
                        + "    `store`.`store_country`,\n"
                        + "    `store`.`store_state`,\n"
                        + "    `store`.`store_city`\n"
                        + "order by\n"
                        + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
                        + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
                        + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC",
                        null)
                    : new SqlPattern(
                        Dialect.DatabaseProduct.MYSQL,
                        "select\n"
                        + "    `store`.`store_country` as `c0`,\n"
                        + "    `store`.`store_state` as `c1`,\n"
                        + "    `store`.`store_city` as `c2`\n"
                        + "from\n"
                        + "    `store` as `store`,\n"
                        + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                        + "    `time_by_day` as `time_by_day`,\n"
                        + "    `product_class` as `product_class`,\n"
                        + "    `product` as `product`\n"
                        + "where\n"
                        + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                        + "and\n"
                        + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                        + "and\n"
                        + "    `time_by_day`.`the_year` = 1997\n"
                        + "and\n"
                        + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                        + "and\n"
                        + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                        + "and\n"
                        + "    (`product_class`.`product_family` = 'Food')\n"
                        + "group by\n"
                        + "    `store`.`store_country`,\n"
                        + "    `store`.`store_state`,\n"
                        + "    `store`.`store_city`\n"
                        + "order by\n"
                        + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
                        + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
                        + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC",
                        null));
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[CA].[San Francisco]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA].[Bellingham]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "{[Store].[USA].[WA].[Walla Walla]}\n"
            + "{[Store].[USA].[WA].[Yakima]}\n"
            + "Row #0: 45,750.24\n"
            + "Row #1: 54,545.28\n"
            + "Row #2: 54,431.14\n"
            + "Row #3: 4,441.18\n"
            + "Row #4: 55,058.79\n"
            + "Row #5: 87,218.28\n"
            + "Row #6: 4,739.23\n"
            + "Row #7: 52,896.30\n"
            + "Row #8: 52,644.07\n"
            + "Row #9: 49,634.46\n"
            + "Row #10: 74,843.96\n"
            + "Row #11: 4,705.97\n"
            + "Row #12: 24,329.23\n");
    }

    public void testNativeCountWithoutMeasuresInVirtualCube() {
        propSaver.set(propSaver.properties.UseAggregates, false);
        String mdx =
            "WITH MEMBER [Measures].[MyMeasure] AS 'Count([Product].[All Products].Children)'\n"
            + "SELECT [Measures].[MyMeasure] ON 0 FROM [Warehouse and Sales]";

        if (MondrianProperties.instance().EnableNativeCount.get()) {
            propSaver.set(propSaver.properties.GenerateFormattedSql, true);
            String mysqlQuery =
                "select\n" +
                "    COUNT(*)\n" +
                "from\n" +
                "    (select\n" +
                "    `product_class`.`product_family` as `c0`\n" +
                "from\n" +
                "    `product` as `product`,\n" +
                "    `product_class` as `product_class`\n" +
                "where\n" +
                "    `product`.`product_class_id` = `product_class`.`product_class_id`\n" +
                "group by\n" +
                "    `product_class`.`product_family`) as `countQuery`";
            SqlPattern mysqlPattern =
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[MyMeasure]}\n"
            + "Row #0: 3\n");
    }

    public void testNativeCountWithChildren() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);

        TestContext testContext = TestContext.instance()
            .createSubstitutingCube(
                "Sales",
                "  <Dimension name=\"Customer IDs\" foreignKey=\"customer_id\">\n"
                + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Customer IDs\" primaryKey=\"customer_id\">\n"
                + "      <Table name=\"customer\"/>\n"
                + "      <Level name=\"ID\" column=\"customer_id\" uniqueMembers=\"true\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>");

        String mdx =
            "with member [Measures].[Cust Count] as 'Count(AddCalculatedMembers([Customer IDs].[All Customer IDs].Children))'\n"
            + "select {[Measures].[Cust Count]} on 0 from [Sales]";

        String mysqlQuery =
            "select\n"
            + "    COUNT(*)\n"
            + "from\n"
            + "    (select\n"
            + "    `customer`.`customer_id` as `c0`\n"
            + "from\n"
            + "    `customer` as `customer`\n"
            + "group by\n"
            + "    `customer`.`customer_id`) as `countQuery`";

        if (MondrianProperties.instance().EnableNativeCount.get()) {
            SqlPattern mysqlPattern =
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(testContext, mdx, new SqlPattern[]{mysqlPattern});
        }
        testContext.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Cust Count]}\n"
            + "Row #0: 10,281\n");
    }

    public void testNativeCountWithAllMembers() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);

        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();

        TestContext testContext = TestContext.instance()
            .createSubstitutingCube(
                "Sales",
                "  <Dimension name=\"Customer IDs\" foreignKey=\"customer_id\">\n"
                + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Customer IDs\" primaryKey=\"customer_id\">\n"
                + "      <Table name=\"customer\"/>\n"
                + "      <Level name=\"ID\" column=\"customer_id\" uniqueMembers=\"true\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>");

        String mdx =
            "with member [Measures].[Cust Count] as 'Count([Customer IDs].AllMembers)'\n"
            + "select {[Measures].[Cust Count]} on 0 from [Sales]";

        String mysqlQuery =
            "select\n"
            + "    COUNT(*)\n"
            + "from\n"
            + "    (select\n"
            + "    `customer`.`customer_id` as `c0`\n"
            + "from\n"
            + "    `customer` as `customer`\n"
            + "group by\n"
            + "    `customer`.`customer_id`) as `countQuery`";

        if (!useAgg && MondrianProperties.instance().EnableNativeCount.get()) {
            SqlPattern mysqlPattern =
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(testContext, mdx, new SqlPattern[]{mysqlPattern});
        }
        testContext.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Cust Count]}\n"
            + "Row #0: 10,282\n");

        // Test Level, the same SQL above should apply.
        mdx = "with member [Measures].[Cust ID Count] as 'Count([Customer IDs].[ID].AllMembers)'\n"
            + "select {[Measures].[Cust ID Count]} on 0 from [Sales]";

        testContext.flushSchemaCache();

        if (!useAgg && MondrianProperties.instance().EnableNativeCount.get()) {
            SqlPattern mysqlPattern =
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(testContext, mdx, new SqlPattern[]{mysqlPattern});
        }
        testContext.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Cust ID Count]}\n"
            + "Row #0: 10,281\n");

        // Test EXCLUDEEMPTY
        mdx = "with member [Measures].[Cust ID Count] as 'Count([Customer IDs].[ID].AllMembers, EXCLUDEEMPTY)'\n"
            + "select {[Measures].[Cust ID Count]} on 0 from [Sales]";

        mysqlQuery =
            "select\n"
            + "    COUNT(*)\n"
            + "from\n"
            + "    (select\n"
            + "    `sales_fact_1997`.`customer_id` as `c0`\n"
            + "from\n"
            + "    `sales_fact_1997` as `sales_fact_1997`\n"
            + "group by\n"
            + "    `sales_fact_1997`.`customer_id`) as `countQuery`";

        testContext.flushSchemaCache();

        if (!useAgg && MondrianProperties.instance().EnableNativeCount.get()) {
            SqlPattern mysqlPattern =
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(testContext, mdx, new SqlPattern[]{mysqlPattern});
        }
        testContext.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Cust ID Count]}\n"
            + "Row #0: 5,581\n");
    }

    public void testNativeCountWithHanger() {
        TestContext testContext = TestContext.instance()
            .createSubstitutingCube(
                "Sales",
                "<Dimension name=\"GenderHanger\" foreignKey=\"customer_id\" hanger=\"true\">"
                + "<Hierarchy hasAll=\"true\" allMemberName=\"All GenderHanger\" primaryKey=\"customer_id\">\n"
                + "  <Table name=\"customer\"/>\n"
                + "  <Level name=\"GenderHanger\" column=\"gender\" uniqueMembers=\"true\"/>\n"
                + "</Hierarchy>\n"
                + "</Dimension>\n");
        String mdx =
            "with member [Measures].[Count] as 'Count([GenderHanger].AllMembers))'\n"
            + "select {[Measures].[Count]} on 0 from [Sales]";
        getTestContext().flushSchemaCache();

        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();
        if (MondrianProperties.instance().EnableNativeCount.get() && !useAgg) {
            propSaver.set(propSaver.properties.GenerateFormattedSql, true);
            String mysqlQuery =
                "select\n"
                + "    COUNT(*)\n"
                + "from\n"
                + "    (select\n"
                + "    `customer`.`gender` as `c0`\n"
                + "from\n"
                + "    `customer` as `customer`\n"
                + "group by\n"
                + "    `customer`.`gender`) as `countQuery`";
            SqlPattern mysqlPattern =
                    new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(testContext, mdx, new SqlPattern[]{mysqlPattern});
        }

        testContext.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Count]}\n"
            + "Row #0: 3\n");
    }

    /**
     * Found an issue with Native Filter NonEmpty behavior... Need to log a JIRA
     */
    public void testFilterNonEmptyCrossJoin() {
        String mdx =
            "select Filter(NonEmptyCrossJoin([Time].[Year].Members, [Product].[All Products].Children), 1=1) on 0 from [Sales]";
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[1997], [Product].[Drink]}\n"
            + "{[Time].[1997], [Product].[Food]}\n"
            + "{[Time].[1997], [Product].[Non-Consumable]}\n"
            + "Row #0: 24,597\n"
            + "Row #0: 191,940\n"
            + "Row #0: 50,236\n");
    }

    /**
     * Validation for MONDRIAN-1972, this MDX was generating an empty SQL request
     */
    public void testNativeAllLevel() {
        assertQueryReturns(
            "SELECT NON EMPTY CrossJoin([Gender].[(All)].Members, [Product].[(All)].Members) on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[All Gender], [Product].[All Products]}\n"
            + "Row #0: 266,773\n");
    }

    /**
     * Validation for MONDRIAN-1972, the first two MDX statements were generating an empty SQL request
     */
    public void testNativeAllLevelSubset() {
      propSaver.set(propSaver.properties.GenerateFormattedSql, true);

      final boolean useAgg =
          MondrianProperties.instance().UseAggregates.get()
          && MondrianProperties.instance().ReadAggregates.get();

      assertQueryReturns(
          "SELECT Subset([Gender].[(All)].Members, 0, 1) on 0 from [Sales]",
          "Axis #0:\n"
          + "{}\n"
          + "Axis #1:\n"
          + "{[Gender].[All Gender]}\n"
          + "Row #0: 266,773\n");
      assertQueryReturns(
          "SELECT Subset({[Gender].[All Gender]}, 0, 1) on 0 from [Sales]",
          "Axis #0:\n"
          + "{}\n"
          + "Axis #1:\n"
          + "{[Gender].[All Gender]}\n"
          + "Row #0: 266,773\n");
      // Verify this still executes natively
      String mdx = "SELECT Subset({[Gender].[All Gender].Children}, 0, 1) on 0 from [Sales]";
      String mysqlQuery =
          "select\n"
          + "    `customer`.`gender` as `c0`\n"
          + "from\n"
          + "    `customer` as `customer`,\n"
          + "    `sales_fact_1997` as `sales_fact_1997`,\n"
          + "    `time_by_day` as `time_by_day`\n"
          + "where\n"
          + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
          + "and\n"
          + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
          + "and\n"
          + "    `time_by_day`.`the_year` = 1997\n"
          + "group by\n"
          + "    `customer`.`gender`\n"
          + "order by\n"
          + "    ISNULL(`customer`.`gender`) ASC, `customer`.`gender` ASC limit 1 offset 0";

      getTestContext().flushSchemaCache();

      if (!useAgg && MondrianProperties.instance().EnableNativeCount.get()) {
          SqlPattern mysqlPattern =
              new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
          assertQuerySql(getTestContext(), mdx, new SqlPattern[]{mysqlPattern});
      }
      assertQueryReturns(
          mdx,
          "Axis #0:\n"
          + "{}\n"
          + "Axis #1:\n"
          + "{[Gender].[F]}\n"
          + "Row #0: 131,558\n");
    }

    /**
     * This test demonstrates sharing of the key cache between queries.
     */
    public void testSharedLookupByKeyCache() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);

        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();
        String mdx =
            "SELECT TopCount(Filter(NonEmpty([Product].[Product Family].Members), [Measures].[Unit Sales] > 5000), 2, [Measures].[Unit Sales]) on 0 FROM [Sales] WHERE ([Time].[1997].[Q1])";

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 47,809\n"
            + "Row #0: 12,506\n");

        mdx =
            "SELECT {[Product].[Product Family].&[Non-Consumable],\n"
            + " [Product].[Product Family].&[Food]} on 0 FROM [Sales] WHERE ([Time].[1997].[Q1])";

        // Make sure this SQL does not occur!
        String sql =
            "select\n"
            + "    `product_class`.`product_family` as `c0`\n"
            + "from\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`\n"
            + "where\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `product_class`.`product_family` = 'Food'\n"
            + "group by\n"
            + "    `product_class`.`product_family`\n"
            + "order by\n"
            + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC";

        if (!useAgg) {
            SqlPattern mysqlPattern =
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, sql, null);
            assertNoQuerySql(mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Product].[Non-Consumable]}\n"
            + "{[Product].[Food]}\n"
            + "Row #0: 12,506\n"
            + "Row #0: 47,809\n");
    }

    public void testNativeFilterWithTupleAndVal() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);

        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();

        String mdx =
            "WITH MEMBER [Gender].[All Gender].[60] AS [Gender].[All Gender], solve_order=10\n"
            + "   MEMBER [Measures].[Sales Filtered] AS SUM(Filter(EXISTING [Product].[Product Name].Members, ([Gender].[All Gender], [Measures].[Unit Sales]) > Val([Gender].CurrentMember.Name)), [Measures].[Unit Sales]), solve_order=20\n"
            + "SELECT {[Measures].[Sales Filtered]} on 0 FROM [Sales] WHERE ([Time].[1997].[Q1], [Gender].[All Gender].[60])";

        String mysqlQuery =
            "select\n"
            + "    sum(`m1`)\n"
            + "from\n"
            + "    (select\n"
            + "    sum(`sales_fact_1997`.`unit_sales`) as `m1`\n"
            + "from\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `time_by_day`.`quarter` = 'Q1'\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`,\n"
            + "    `product_class`.`product_category`,\n"
            + "    `product_class`.`product_subcategory`,\n"
            + "    `product`.`brand_name`,\n"
            + "    `product`.`product_name`\n"
            + "having\n"
            + "    (sum(`sales_fact_1997`.`unit_sales`) > 60.0)) as `sumQuery`";

        if (!useAgg && MondrianProperties.instance().EnableNativeCount.get()) {
            SqlPattern mysqlPattern =
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(getTestContext(), mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997].[Q1], [Gender].[All Gender].[60]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Sales Filtered]}\n"
            + "Row #0: 8,050\n");
    }

    /**
     * This tests a scenario where a calculated member still allows nonempty
     * native evaluation with a literal.
     */
    public void testNonEmptyNativeWithLiteralCalcMember() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);

        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();

        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Customer IDs\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Customer IDs\" primaryKey=\"customer_id\">\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Level name=\"ID\" column=\"customer_id\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n",
            "  <CalculatedMember visible=\"true\" name=\"20\" hierarchy=\"[Gender]\" parent=\"[Gender].[All Gender]\">\n"
            + "    <Formula>NULL</Formula>\n"
            + "  </CalculatedMember>\n"
            + "  <CalculatedMember dimension=\"Measures\" visible=\"true\" name=\"Calc\">\n"
            + "    <Formula>IIf(([Gender].[M], [Measures].[Unit Sales])>=val([Gender].currentMember.Name), ([Gender].[M], [Measures].[Unit Sales]), NULL)</Formula>\n"
            + "  </CalculatedMember>\n"
            + "  <CalculatedMember dimension=\"Measures\" visible=\"true\" name=\"Calc3\">\n"
            + "    <Formula>Count(Filter(NonEmpty([Customer IDs].[All Customer IDs].Children), NOT(ISEMPTY([Measures].[Calc]))))</Formula>\n"
            + "  </CalculatedMember>\n");

        String mdx = "with member [Measures].[Calc2] as '([Gender].[All Gender].[20], [Measures].[Calc3]) ', solve_order = -1\n"
            + "select {[Measures].[Calc2]} on 0,\n" +
            "{[Product].[All Products].Children} on 1" +
            "from [Sales] where (StrToMember(\"[Marital Status].[S]\"))";

        String mysqlQuery =
            "select\n"
            + "    `customer`.`customer_id` as `c0`\n"
            + "from\n"
            + "    `customer` as `customer`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`\n"
            + "where\n"
            + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `product_class`.`product_family` = 'Non-Consumable'\n"
            + "and\n"
            + "    `customer`.`marital_status` = 'S'\n"
            + "and\n"
            + "    `sales_fact_1997`.`unit_sales` is not null\n"
            + "group by\n"
            + "    `customer`.`customer_id`\n"
            + "order by\n"
            + "    ISNULL(`customer`.`customer_id`) ASC, `customer`.`customer_id` ASC";

        getTestContext().flushSchemaCache();

        if (!useAgg && MondrianProperties.instance().EnableNativeCount.get()) {
            SqlPattern mysqlPattern =
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(testContext, mdx, new SqlPattern[]{mysqlPattern});
        }

        testContext.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Marital Status].[S]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Calc2]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 40\n"
            + "Row #1: 820\n"
            + "Row #2: 169\n");
    }

    /**
     * This tests a scenario where a hanger dimension still allows nonempty
     * native evaluation with a calculated member.
     *
     * Note that the results are different from above because the gender
     * member is never in context as a hanger dimension. 
     */
    public void testNonEmptyNativeWithLiteralCalcMemberAndHanger() {
      propSaver.set(propSaver.properties.GenerateFormattedSql, true);

      final boolean useAgg =
          MondrianProperties.instance().UseAggregates.get()
          && MondrianProperties.instance().ReadAggregates.get();

      TestContext testContext = TestContext.instance().createSubstitutingCube(
          "Sales",
          "  <Dimension name=\"Customer IDs\" foreignKey=\"customer_id\">\n"
          + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Customer IDs\" primaryKey=\"customer_id\">\n"
          + "      <Table name=\"customer\"/>\n"
          + "      <Level name=\"ID\" column=\"customer_id\" uniqueMembers=\"true\"/>\n"
          + "    </Hierarchy>\n"
          + "  </Dimension>\n"
          + "<Dimension name=\"GenderHanger\" foreignKey=\"customer_id\" hanger=\"true\">"
          + "<Hierarchy hasAll=\"true\" allMemberName=\"All GenderHanger\" primaryKey=\"customer_id\">\n"
          + "  <Table name=\"customer\"/>\n"
          + "  <Level name=\"GenderHanger\" column=\"gender\" uniqueMembers=\"true\"/>\n"
          + "</Hierarchy>\n"
          + "</Dimension>\n",
          "  <CalculatedMember visible=\"true\" name=\"20\" hierarchy=\"[GenderHanger]\" parent=\"[GenderHanger].[All GenderHanger]\">\n"
          + "    <Formula>NULL</Formula>\n"
          + "  </CalculatedMember>\n"
          + "  <CalculatedMember dimension=\"Measures\" visible=\"true\" name=\"Calc\">\n"
          + "    <Formula>IIf(([GenderHanger].[M], [Measures].[Unit Sales])>=val([GenderHanger].currentMember.Name), ([GenderHanger].[M], [Measures].[Unit Sales]), NULL)</Formula>\n"
          + "  </CalculatedMember>\n"
          + "  <CalculatedMember dimension=\"Measures\" visible=\"true\" name=\"Calc3\">\n"
          + "    <Formula>Count(Filter(NonEmpty([Customer IDs].[All Customer IDs].Children), NOT(ISEMPTY([Measures].[Calc]))))</Formula>\n"
          + "  </CalculatedMember>\n");

      String mdx = "with member [Measures].[Calc2] as '([GenderHanger].[All GenderHanger].[20], [Measures].[Calc3]) ', solve_order = -1\n"
          + "select {[Measures].[Calc2]} on 0,\n" +
          "{[Product].[All Products].Children} on 1" +
          "from [Sales] where (StrToMember(\"[Marital Status].[S]\"))";

      String mysqlQuery =
          "select\n"
          + "    `customer`.`customer_id` as `c0`\n"
          + "from\n"
          + "    `customer` as `customer`,\n"
          + "    `sales_fact_1997` as `sales_fact_1997`,\n"
          + "    `time_by_day` as `time_by_day`,\n"
          + "    `product_class` as `product_class`,\n"
          + "    `product` as `product`\n"
          + "where\n"
          + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
          + "and\n"
          + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
          + "and\n"
          + "    `time_by_day`.`the_year` = 1997\n"
          + "and\n"
          + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
          + "and\n"
          + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
          + "and\n"
          + "    `product_class`.`product_family` = 'Non-Consumable'\n"
          + "and\n"
          + "    `customer`.`marital_status` = 'S'\n"
          + "and\n"
          + "    `sales_fact_1997`.`unit_sales` is not null\n"
          + "group by\n"
          + "    `customer`.`customer_id`\n"
          + "order by\n"
          + "    ISNULL(`customer`.`customer_id`) ASC, `customer`.`customer_id` ASC";

      getTestContext().flushSchemaCache();

      if (!useAgg && MondrianProperties.instance().EnableNativeCount.get()) {
          SqlPattern mysqlPattern =
              new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
          assertQuerySql(testContext, mdx, new SqlPattern[]{mysqlPattern});
      }

      testContext.assertQueryReturns(
          mdx,
          "Axis #0:\n"
          + "{[Marital Status].[S]}\n"
          + "Axis #1:\n"
          + "{[Measures].[Calc2]}\n"
          + "Axis #2:\n"
          + "{[Product].[Drink]}\n"
          + "{[Product].[Food]}\n"
          + "{[Product].[Non-Consumable]}\n"
          + "Row #0: 86\n"
          + "Row #1: 1,576\n"
          + "Row #2: 353\n");
    }

    /**
     * This issue would appear during validation of nonempty() native evaluation
     */
    public void testSelfReferenceWithNativeEval() {
        String mdx =
            "WITH\n"
            + "member [Measures].[Ref] AS '[Measures].[SelfRef]'\n"
            + "member [Measures].[SelfRef] as '\n"
            + "CASE WHEN\n"
            + "([Gender].CurrentMember IS [Gender].[M])\n"
            + "THEN\n"
            + "0\n"
            + "WHEN\n"
            + "([Gender].CurrentMember IS [Gender].[F])\n"
            + "THEN\n"
            + "1\n"
            + "WHEN\n"
            + "([Gender].CurrentMember IS [Gender].[All Gender])\n"
            + "THEN\n"
            + "SUM([Gender].[F], [Measures].[SelfRef])\n"
            + "END'"
            + "SELECT {NonEmpty([Measures].[Ref])} on 0 from [Warehouse and Sales]";
        assertQueryReturns(mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Ref]}\n"
            + "Row #0: 1\n");
    }

    public void testNativeExisting() {
        String mdx =
            "with \n"
            + "  member measures.drinkSum as\n"
            + "  aggregate(Existing [Product].[Product Name].Members)\n"
            + "  select {measures.drinkSum} on 0,\n"
            + "  [Product].[Product Family].[Drink] on 1\n"
            + "  from [Sales]";
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[drinkSum]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "Row #0: 24,597\n");

        if (!MondrianProperties.instance().EnableNativeExisting.get()) {
            return;
        }
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mysqlQuery = isUseAgg()
            ? "select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    `product_class`.`product_department` as `c1`,\n"
            + "    `product_class`.`product_category` as `c2`,\n"
            + "    `product_class`.`product_subcategory` as `c3`,\n"
            + "    `product`.`brand_name` as `c4`,\n"
            + "    `product`.`product_name` as `c5`\n"
            + "from\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`,\n"
            + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`\n"
            + "where\n"
            + "    `agg_c_14_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    (`product_class`.`product_family` = 'Drink')\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`,\n"
            + "    `product_class`.`product_category`,\n"
            + "    `product_class`.`product_subcategory`,\n"
            + "    `product`.`brand_name`,\n"
            + "    `product`.`product_name`\n"
            + "order by\n"
            + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
            + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC,\n"
            + "    ISNULL(`product_class`.`product_category`) ASC, `product_class`.`product_category` ASC,\n"
            + "    ISNULL(`product_class`.`product_subcategory`) ASC, `product_class`.`product_subcategory` ASC,\n"
            + "    ISNULL(`product`.`brand_name`) ASC, `product`.`brand_name` ASC,\n"
            + "    ISNULL(`product`.`product_name`) ASC, `product`.`product_name` ASC"
            : "select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    `product_class`.`product_department` as `c1`,\n"
            + "    `product_class`.`product_category` as `c2`,\n"
            + "    `product_class`.`product_subcategory` as `c3`,\n"
            + "    `product`.`brand_name` as `c4`,\n"
            + "    `product`.`product_name` as `c5`\n"
            + "from\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`\n"
            + "where\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    (`product_class`.`product_family` = 'Drink')\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`,\n"
            + "    `product_class`.`product_category`,\n"
            + "    `product_class`.`product_subcategory`,\n"
            + "    `product`.`brand_name`,\n"
            + "    `product`.`product_name`\n"
            + "order by\n"
            + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
            + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC,\n"
            + "    ISNULL(`product_class`.`product_category`) ASC, `product_class`.`product_category` ASC,\n"
            + "    ISNULL(`product_class`.`product_subcategory`) ASC, `product_class`.`product_subcategory` ASC,\n"
            + "    ISNULL(`product`.`brand_name`) ASC, `product`.`brand_name` ASC,\n"
            + "    ISNULL(`product`.`product_name`) ASC, `product`.`product_name` ASC";
        SqlPattern mysqlPattern =
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery,
              mysqlQuery.substring(0, mysqlQuery.indexOf("where")));
        assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
    }

    private static final boolean isUseAgg() {
        return
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();
    }

    public void testNativeSumScenarios() {

      propSaver.set(propSaver.properties.GenerateFormattedSql, true);

      final boolean useAgg =
          MondrianProperties.instance().UseAggregates.get()
          && MondrianProperties.instance().ReadAggregates.get();

      String mdx =
          "WITH MEMBER [Measures].[TotalVal] AS 'SUM({[Store].[Store City].members})'\n"
          + "SELECT [Measures].[TotalVal] ON 0 FROM [Sales]";

      String mysqlQuery =
          "select\n"
          + "    sum(`m1`)\n"
          + "from\n"
          + "    (select\n"
          + "    sum(`sales_fact_1997`.`unit_sales`) as `m1`\n"
          + "from\n"
          + "    `store` as `store`,\n"
          + "    `sales_fact_1997` as `sales_fact_1997`\n"
          + "where\n"
          + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
          + "group by\n"
          + "    `store`.`store_country`,\n"
          + "    `store`.`store_state`,\n"
          + "    `store`.`store_city`) as `sumQuery`";

      if (!useAgg && MondrianProperties.instance().EnableNativeSum.get()) {
          getTestContext().flushSchemaCache();
          SqlPattern mysqlPattern =
              new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
          assertQuerySql(TestContext.instance(), mdx, new SqlPattern[]{mysqlPattern});
      }

      assertQueryReturns(
          mdx,
          "Axis #0:\n"
          + "{}\n"
          + "Axis #1:\n"
          + "{[Measures].[TotalVal]}\n"
          + "Row #0: 266,773\n");

      mdx =
          "WITH MEMBER [Measures].[TotalVal] AS 'SUM(Filter({[Store].[Store City].members},[Measures].[Unit Sales] > 20000))'\n"
          + "SELECT [Measures].[TotalVal] ON 0 FROM [Sales]";

      mysqlQuery =
          "select\n"
          + "    sum(`m1`)\n"
          + "from\n"
          + "    (select\n"
          + "    sum(`sales_fact_1997`.`unit_sales`) as `m1`\n"
          + "from\n"
          + "    `store` as `store`,\n"
          + "    `sales_fact_1997` as `sales_fact_1997`,\n"
          + "    `time_by_day` as `time_by_day`\n"
          + "where\n"
          + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
          + "and\n"
          + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
          + "and\n"
          + "    `time_by_day`.`the_year` = 1997\n"
          + "group by\n"
          + "    `store`.`store_country`,\n"
          + "    `store`.`store_state`,\n"
          + "    `store`.`store_city`\n"
          + "having\n"
          + "    (sum(`sales_fact_1997`.`unit_sales`) > 20000)) as `sumQuery`";

      if (!useAgg && MondrianProperties.instance().EnableNativeSum.get()) {
          getTestContext().flushSchemaCache();
          SqlPattern mysqlPattern =
              new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
          assertQuerySql(TestContext.instance(), mdx, new SqlPattern[]{mysqlPattern});
      }

      assertQueryReturns(
          mdx,
          "Axis #0:\n"
          + "{}\n"
          + "Axis #1:\n"
          + "{[Measures].[TotalVal]}\n"
          + "Row #0: 248,725\n");

      mdx =
          "WITH MEMBER [Measures].[TotalVal] AS 'SUM(Filter({[Store].[Store City].members},[Measures].[Unit Sales] > 20000))'\n"
          + "SELECT [Measures].[TotalVal] ON 0 FROM [Sales] where {[Product].[Food]}";

      mysqlQuery =
          "select\n"
          + "    sum(`m1`)\n"
          + "from\n"
          + "    (select\n"
          + "    sum(`sales_fact_1997`.`unit_sales`) as `m1`\n"
          + "from\n"
          + "    `store` as `store`,\n"
          + "    `sales_fact_1997` as `sales_fact_1997`,\n"
          + "    `time_by_day` as `time_by_day`,\n"
          + "    `product_class` as `product_class`,\n"
          + "    `product` as `product`\n"
          + "where\n"
          + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
          + "and\n"
          + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
          + "and\n"
          + "    `time_by_day`.`the_year` = 1997\n"
          + "and\n"
          + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
          + "and\n"
          + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
          + "and\n"
          + "    `product_class`.`product_family` = 'Food'\n"
          + "group by\n"
          + "    `store`.`store_country`,\n"
          + "    `store`.`store_state`,\n"
          + "    `store`.`store_city`\n"
          + "having\n"
          + "    (sum(`sales_fact_1997`.`unit_sales`) > 20000)) as `sumQuery`";

      if (!useAgg && MondrianProperties.instance().EnableNativeSum.get()) {
          getTestContext().flushSchemaCache();
          SqlPattern mysqlPattern =
              new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
          assertQuerySql(TestContext.instance(), mdx, new SqlPattern[]{mysqlPattern});
      }

      assertQueryReturns(
          mdx,
          "Axis #0:\n"
          + "{[Product].[Food]}\n"
          + "Axis #1:\n"
          + "{[Measures].[TotalVal]}\n"
          + "Row #0: 55,358\n");

      mdx =
          "WITH MEMBER [Measures].[TotalVal] AS 'SUM(Filter({[Store].[Store City].members},[Measures].[Unit Sales] > 3000))'\n"
          + "SELECT [Measures].[TotalVal] ON 0, [Product].[All Products].Children on 1 FROM [Sales]";

      mysqlQuery =
          "select\n"
              + "    sum(case when `m2` = 'Drink' then `m1` else 0 end) as `sum0`,\n"
              + "    sum(case when `m2` = 'Food' then `m1` else 0 end) as `sum1`,\n"
              + "    sum(case when `m2` = 'Non-Consumable' then `m1` else 0 end) as `sum2`\n"
              + "from\n"
              + "    (select\n"
              + "    sum(`sales_fact_1997`.`unit_sales`) as `m1`,\n"
              + "    `product_class`.`product_family` as `m2`\n"
              + "from\n"
              + "    `store` as `store`,\n"
              + "    `sales_fact_1997` as `sales_fact_1997`,\n"
              + "    `time_by_day` as `time_by_day`,\n"
              + "    `product_class` as `product_class`,\n"
              + "    `product` as `product`\n"
              + "where\n"
              + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
              + "and\n"
              + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
              + "and\n"
              + "    `time_by_day`.`the_year` = 1997\n"
              + "and\n"
              + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
              + "and\n"
              + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
              + "and\n"
              + "    `product_class`.`product_family` in ('Drink', 'Food', 'Non-Consumable')\n"
              + "group by\n"
              + "    `store`.`store_country`,\n"
              + "    `store`.`store_state`,\n"
              + "    `store`.`store_city`,\n"
              + "    `product_class`.`product_family`\n"
              + "having\n"
              + "    (sum(`sales_fact_1997`.`unit_sales`) > 3000)) as `sumQuery`";

      if (!useAgg && MondrianProperties.instance().EnableNativeSum.get()) {
          getTestContext().flushSchemaCache();
          SqlPattern mysqlPattern =
              new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
          assertQuerySql(TestContext.instance(), mdx, new SqlPattern[]{mysqlPattern});
      }

      assertQueryReturns(
          mdx,
          "Axis #0:\n"
          + "{}\n"
          + "Axis #1:\n"
          + "{[Measures].[TotalVal]}\n"
          + "Axis #2:\n"
          + "{[Product].[Drink]}\n"
          + "{[Product].[Food]}\n"
          + "{[Product].[Non-Consumable]}\n"
          + "Row #0: 6,827\n"
          + "Row #1: 187,176\n"
          + "Row #2: 46,877\n");

      mdx =
          "WITH MEMBER [Measures].[TotalVal] AS 'SUM(Filter({[Store].[Store City].members},[Measures].[Unit Sales] > 1000))'\n"
          + "SELECT [Measures].[TotalVal] ON 0, [Product].[All Products].Children on 1 FROM [Sales] WHERE [Time].[1997].[Q1]";

      mysqlQuery =
          "select\n"
              + "    sum(case when `m2` = 'Drink' then `m1` else 0 end) as `sum0`,\n"
              + "    sum(case when `m2` = 'Food' then `m1` else 0 end) as `sum1`,\n"
              + "    sum(case when `m2` = 'Non-Consumable' then `m1` else 0 end) as `sum2`\n"
              + "from\n"
              + "    (select\n"
              + "    sum(`sales_fact_1997`.`unit_sales`) as `m1`,\n"
              + "    `product_class`.`product_family` as `m2`\n"
              + "from\n"
              + "    `store` as `store`,\n"
              + "    `sales_fact_1997` as `sales_fact_1997`,\n"
              + "    `time_by_day` as `time_by_day`,\n"
              + "    `product_class` as `product_class`,\n"
              + "    `product` as `product`\n"
              + "where\n"
              + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
              + "and\n"
              + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
              + "and\n"
              + "    `time_by_day`.`the_year` = 1997\n"
              + "and\n"
              + "    `time_by_day`.`quarter` = 'Q1'\n"
              + "and\n"
              + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
              + "and\n"
              + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
              + "and\n"
              + "    `product_class`.`product_family` in ('Drink', 'Food', 'Non-Consumable')\n"
              + "group by\n"
              + "    `store`.`store_country`,\n"
              + "    `store`.`store_state`,\n"
              + "    `store`.`store_city`,\n"
              + "    `product_class`.`product_family`\n"
              + "having\n"
              + "    (sum(`sales_fact_1997`.`unit_sales`) > 1000)) as `sumQuery`";

      if (!useAgg && MondrianProperties.instance().EnableNativeSum.get()) {
          getTestContext().flushSchemaCache();
          SqlPattern mysqlPattern =
              new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
          assertQuerySql(TestContext.instance(), mdx, new SqlPattern[]{mysqlPattern});
      }

      assertQueryReturns(
          mdx,
          "Axis #0:\n"
          + "{[Time].[1997].[Q1]}\n"
          + "Axis #1:\n"
          + "{[Measures].[TotalVal]}\n"
          + "Axis #2:\n"
          + "{[Product].[Drink]}\n"
          + "{[Product].[Food]}\n"
          + "{[Product].[Non-Consumable]}\n"
          + "Row #0: 1,063\n"
          + "Row #1: 46,759\n"
          + "Row #2: 9,983\n");
    }

    public void testNativeSumWithSecondParam() {
        propSaver.set(propSaver.properties.EnableNativeSum, true);
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);

        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();

        String mdx =
            "WITH MEMBER [Measures].[TotalCount] AS 'SUM(Filter({[Store].[Store City].members},[Measures].[Unit Sales] > 1000), [Measures].[Sales Count])'\n"
            + "SELECT [Measures].[TotalCount] ON 0, [Product].[All Products].Children on 1 FROM [Sales] WHERE [Time].[1997].[Q1]";

        String mysqlQuery =
            "select\n"
                + "    sum(case when `m2` = 'Drink' then `m1` else 0 end) as `sum0`,\n"
                + "    sum(case when `m2` = 'Food' then `m1` else 0 end) as `sum1`,\n"
                + "    sum(case when `m2` = 'Non-Consumable' then `m1` else 0 end) as `sum2`\n"
                + "from\n"
                + "    (select\n"
                + "    count(`sales_fact_1997`.`product_id`) as `m1`,\n"
                + "    `product_class`.`product_family` as `m2`\n"
                + "from\n"
                + "    `store` as `store`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `time_by_day` as `time_by_day`,\n"
                + "    `product_class` as `product_class`,\n"
                + "    `product` as `product`\n"
                + "where\n"
                + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"
                + "and\n"
                + "    `time_by_day`.`quarter` = 'Q1'\n"
                + "and\n"
                + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `product_class`.`product_family` in ('Drink', 'Food', 'Non-Consumable')\n"
                + "group by\n"
                + "    `store`.`store_country`,\n"
                + "    `store`.`store_state`,\n"
                + "    `store`.`store_city`,\n"
                + "    `product_class`.`product_family`\n"
                + "having\n"
                + "    (sum(`sales_fact_1997`.`unit_sales`) > 1000)) as `sumQuery`";

        if (!useAgg && MondrianProperties.instance().EnableNativeSum.get()) {
            getTestContext().flushSchemaCache();
            SqlPattern mysqlPattern =
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(TestContext.instance(), mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[TotalCount]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 347\n"
            + "Row #1: 14,898\n"
            + "Row #2: 3,201\n");
    }

    public void testNativeSumWithCalcSecondParam() {
        propSaver.set(propSaver.properties.EnableNativeSum, true);
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);

        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();

        String mdx =
            "WITH MEMBER [Measures].[Calc] AS '[Measures].[Store Cost]'\n"
            + "MEMBER [Measures].[TotalCount] AS 'SUM(Filter({[Store].[Store City].members},[Measures].[Unit Sales] > 1000), [Measures].[Calc])'\n"
            + "SELECT [Measures].[TotalCount] ON 0, [Product].[All Products].Children on 1 FROM [Sales] WHERE [Time].[1997].[Q1]";

        String mysqlQuery =
            "select\n"
                + "    sum(case when `m2` = 'Drink' then `m1` else 0 end) as `sum0`,\n"
                + "    sum(case when `m2` = 'Food' then `m1` else 0 end) as `sum1`,\n"
                + "    sum(case when `m2` = 'Non-Consumable' then `m1` else 0 end) as `sum2`\n"
                + "from\n"
                + "    (select\n"
                + "    sum(`sales_fact_1997`.`store_cost`) as `m1`,\n"
                + "    `product_class`.`product_family` as `m2`\n"
                + "from\n"
                + "    `store` as `store`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `time_by_day` as `time_by_day`,\n"
                + "    `product_class` as `product_class`,\n"
                + "    `product` as `product`\n"
                + "where\n"
                + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"
                + "and\n"
                + "    `time_by_day`.`quarter` = 'Q1'\n"
                + "and\n"
                + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `product_class`.`product_family` in ('Drink', 'Food', 'Non-Consumable')\n"
                + "group by\n"
                + "    `store`.`store_country`,\n"
                + "    `store`.`store_state`,\n"
                + "    `store`.`store_city`,\n"
                + "    `product_class`.`product_family`\n"
                + "having\n"
                + "    (sum(`sales_fact_1997`.`unit_sales`) > 1000)) as `sumQuery`";

        if (!useAgg && MondrianProperties.instance().EnableNativeSum.get()) {
            getTestContext().flushSchemaCache();
            SqlPattern mysqlPattern =
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(TestContext.instance(), mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[TotalCount]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 817\n"
            + "Row #1: 39,572\n"
            + "Row #2: 8,603\n");
    }

    public void testNativeFilterWithVirtualCube() {

      propSaver.set(propSaver.properties.GenerateFormattedSql, true);

      final boolean useAgg =
          MondrianProperties.instance().UseAggregates.get()
          && MondrianProperties.instance().ReadAggregates.get();

      String mdx =
          "WITH MEMBER [Measures].[TotalCount] AS 'Aggregate(Filter({[Store].[Store City].members},[Measures].[Unit Sales] > 1000), [Measures].[Warehouse Sales])'\n"
          + "SELECT [Measures].[TotalCount] ON 0, [Product].[All Products].Children on 1 FROM [Warehouse and Sales] WHERE [Time].[1997].[Q1]";

      String mysqlQuery =
          "select\n"
          + "    `store`.`store_country` as `c0`,\n"
          + "    `store`.`store_state` as `c1`,\n"
          + "    `store`.`store_city` as `c2`\n"
          + "from\n"
          + "    `store` as `store`,\n"
          + "    `sales_fact_1997` as `sales_fact_1997`,\n"
          + "    `time_by_day` as `time_by_day`,\n"
          + "    `product_class` as `product_class`,\n"
          + "    `product` as `product`\n"
          + "where\n"
          + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
          + "and\n"
          + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
          + "and\n"
          + "    `time_by_day`.`the_year` = 1997\n"
          + "and\n"
          + "    `time_by_day`.`quarter` = 'Q1'\n"
          + "and\n"
          + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
          + "and\n"
          + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
          + "and\n"
          + "    `product_class`.`product_family` = 'Drink'\n"
          + "group by\n"
          + "    `store`.`store_country`,\n"
          + "    `store`.`store_state`,\n"
          + "    `store`.`store_city`\n"
          + "having\n"
          + "    (sum(`sales_fact_1997`.`unit_sales`) > 1000)\n"
          + "order by\n"
          + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
          + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
          + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC";

      if (!useAgg && MondrianProperties.instance().EnableNativeFilter.get()) {
          getTestContext().flushSchemaCache();
          SqlPattern mysqlPattern =
              new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
          assertQuerySql(TestContext.instance().withFreshConnection(), mdx,
              new SqlPattern[]{mysqlPattern});
      }

      assertQueryReturns(
          mdx,
          "Axis #0:\n"
          + "{[Time].[1997].[Q1]}\n"
          + "Axis #1:\n"
          + "{[Measures].[TotalCount]}\n"
          + "Axis #2:\n"
          + "{[Product].[Drink]}\n"
          + "{[Product].[Food]}\n"
          + "{[Product].[Non-Consumable]}\n"
          + "Row #0: 745\n"
          + "Row #1: 31,059\n"
          + "Row #2: 6,235\n");
    }


    // test a scenario where the members are not related to base cube
    public void testNativeSumInVirtCubeWithMultipleBaseCubes() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();

        String mdx =
            "WITH MEMBER [Measures].[TotalCount] AS 'SUM(Filter({[Product].[All Products].Children},[Measures].[Unit Sales] > 10000), [Measures].[Warehouse Sales])'\n"
            + "SELECT [Measures].[TotalCount] ON 0, NON EMPTY [Store].[Store State].members on 1 FROM [Warehouse and Sales] WHERE [Time].[1997].[Q1]";

        String mysqlQuery =
            "select\n"
                + "    sum(case when `m2` = 'CA' then `m1` else 0 end) as `sum0`,\n"
                + "    sum(case when `m2` = 'OR' then `m1` else 0 end) as `sum1`,\n"
                + "    sum(case when `m2` = 'WA' then `m1` else 0 end) as `sum2`\n"
                + "from\n"
                + "    (select\n"
                + "    sum(`inventory_fact_1997`.`warehouse_sales`) as `m1`,\n"
                + "    `store`.`store_state` as `m2`\n"
                + "from\n"
                + "    `product_class` as `product_class`,\n"
                + "    `product` as `product`,\n"
                + "    `inventory_fact_1997` as `inventory_fact_1997`,\n"
                + "    `store` as `store`,\n"
                + "    `time_by_day` as `time_by_day`,\n"
                + "    (select\n"
                + "    `product_class`.`product_family` as `c0`,\n"
                + "    `store`.`store_state` as `c1`\n"
                + "from\n"
                + "    `product_class` as `product_class`,\n"
                + "    `product` as `product`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `store` as `store`,\n"
                + "    `time_by_day` as `time_by_day`\n"
                + "where\n"
                + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `store`.`store_state` in ('CA', 'OR', 'WA')\n"
                + "and\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"
                + "and\n"
                + "    `time_by_day`.`quarter` = 'Q1'\n"
                + "group by\n"
                + "    `product_class`.`product_family`,\n"
                + "    `store`.`store_state`\n"
                + "having\n"
                + "    (sum(`sales_fact_1997`.`unit_sales`) > 10000)) as `tbl001`\n"
                + "where\n"
                + "    `inventory_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `inventory_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `store`.`store_state` in ('CA', 'OR', 'WA')\n"
                + "and\n"
                + "    `inventory_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"
                + "and\n"
                + "    `time_by_day`.`quarter` = 'Q1'\n"
                + "and\n"
                + "    `product_class`.`product_family` = `tbl001`.`c0`\n"
                + "and\n"
                + "    `store`.`store_state` = `tbl001`.`c1`\n"
                + "group by\n"
                + "    `product_class`.`product_family`,\n"
                + "    `store`.`store_state`) as `sumQuery`";

        if (!useAgg && MondrianProperties.instance().EnableNativeFilter.get()) {
            getTestContext().flushSchemaCache();
            SqlPattern mysqlPattern =
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(TestContext.instance(), mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
                + "{[Time].[1997].[Q1]}\n"
                + "Axis #1:\n"
                + "{[Measures].[TotalCount]}\n"
                + "Axis #2:\n"
                + "{[Store].[USA].[CA]}\n"
                + "{[Store].[USA].[OR]}\n"
                + "{[Store].[USA].[WA]}\n"
                + "Row #0: 6,121\n"
                + "Row #1: 5,315\n"
                + "Row #2: 24,075\n");

        // In the second scenario, the promotion is not related to the warehouse
        //  MondrianProperties.instance().EnableNativeSum.set( false );
        mdx =
            "WITH MEMBER [Measures].[TotalCount] AS 'SUM(Filter({[Promotions].[All Promotions].Children},[Measures].[Unit Sales] > 10000), [Measures].[Warehouse Sales])'\n"
            + "SELECT [Measures].[TotalCount] ON 0, NON EMPTY [Store].[Store State].members on 1 FROM [Warehouse and Sales] WHERE [Time].[1997].[Q1]";

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
                + "{[Time].[1997].[Q1]}\n"
                + "Axis #1:\n"
                + "{[Measures].[TotalCount]}\n"
                + "Axis #2:\n");
    }

    public void testNativeSumInVirtualCube() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();

        String mdx =
            "WITH MEMBER [Measures].[TotalCount] AS 'SUM(Filter({[Store].[Store City].members},[Measures].[Unit Sales] > 1000), [Measures].[Sales Count])'\n"
            + "SELECT [Measures].[TotalCount] ON 0, [Product].[All Products].Children on 1 FROM [Warehouse and Sales] WHERE [Time].[1997].[Q1]";

        String mysqlQuery =
            "select\n"
                + "    sum(case when `m2` = 'Drink' then `m1` else 0 end) as `sum0`,\n"
                + "    sum(case when `m2` = 'Food' then `m1` else 0 end) as `sum1`,\n"
                + "    sum(case when `m2` = 'Non-Consumable' then `m1` else 0 end) as `sum2`\n"
                + "from\n"
                + "    (select\n"
                + "    count(`sales_fact_1997`.`product_id`) as `m1`,\n"
                + "    `product_class`.`product_family` as `m2`\n"
                + "from\n"
                + "    `store` as `store`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `time_by_day` as `time_by_day`,\n"
                + "    `product_class` as `product_class`,\n"
                + "    `product` as `product`\n"
                + "where\n"
                + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"
                + "and\n"
                + "    `time_by_day`.`quarter` = 'Q1'\n"
                + "and\n"
                + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `product_class`.`product_family` in ('Drink', 'Food', 'Non-Consumable')\n"
                + "group by\n"
                + "    `store`.`store_country`,\n"
                + "    `store`.`store_state`,\n"
                + "    `store`.`store_city`,\n"
                + "    `product_class`.`product_family`\n"
                + "having\n"
                + "    (sum(`sales_fact_1997`.`unit_sales`) > 1000)) as `sumQuery`";

        if (!useAgg && MondrianProperties.instance().EnableNativeFilter.get()) {
            getTestContext().flushSchemaCache();
            SqlPattern mysqlPattern =
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(TestContext.instance(), mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[TotalCount]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 347\n"
            + "Row #1: 14,898\n"
            + "Row #2: 3,201\n");

        // the following should pass but is non native, multiple fact tables are in play here.
        mdx =
            "WITH MEMBER [Measures].[TotalCount] AS 'SUM(Filter({[Store].[Store City].members},[Measures].[Unit Sales] > 1000), [Measures].[Warehouse Sales])'\n"
            + "SELECT [Measures].[TotalCount] ON 0, [Product].[All Products].Children on 1 FROM [Warehouse and Sales] WHERE [Time].[1997].[Q1]";

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[TotalCount]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 745\n"
            + "Row #1: 31,059\n"
            + "Row #2: 6,235\n");
    }

    /**
     * This test verifies the removal of unnecessary select clause members for sums
     */
    public void testNativeSumInVirtualCubeSqlOptimization() {

        propSaver.set(propSaver.properties.GenerateFormattedSql, true);

        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();

        String mdx =
            "WITH MEMBER [Measures].[TotalCount] AS 'SUM(Filter({[Customers].[Name].members},[Measures].[Unit Sales] > 10), [Measures].[Sales Count])'\n"
            + "SELECT [Measures].[TotalCount] ON 0, [Product].[All Products].Children on 1 FROM [Warehouse and Sales] WHERE [Time].[1997].[Q1]";

        String mysqlQuery =
            "select\n"
                + "    sum(case when `m2` = 'Drink' then `m1` else 0 end) as `sum0`,\n"
                + "    sum(case when `m2` = 'Food' then `m1` else 0 end) as `sum1`,\n"
                + "    sum(case when `m2` = 'Non-Consumable' then `m1` else 0 end) as `sum2`\n"
                + "from\n"
                + "    (select\n"
                + "    count(`sales_fact_1997`.`product_id`) as `m1`,\n"
                + "    `product_class`.`product_family` as `m2`\n"
                + "from\n"
                + "    `customer` as `customer`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `time_by_day` as `time_by_day`,\n"
                + "    `product_class` as `product_class`,\n"
                + "    `product` as `product`\n"
                + "where\n"
                + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"
                + "and\n"
                + "    `time_by_day`.`quarter` = 'Q1'\n"
                + "and\n"
                + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `product_class`.`product_family` in ('Drink', 'Food', 'Non-Consumable')\n"
                + "group by\n"
                + "    `customer`.`country`,\n"
                + "    `customer`.`state_province`,\n"
                + "    `customer`.`city`,\n"
                + "    `sales_fact_1997`.`customer_id`,\n"
                + "    `product_class`.`product_family`\n"
                + "having\n"
                + "    (sum(`sales_fact_1997`.`unit_sales`) > 10)) as `sumQuery`";

        if (!useAgg && MondrianProperties.instance().EnableNativeFilter.get()) {
            getTestContext().flushSchemaCache();
            SqlPattern mysqlPattern =
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(TestContext.instance(), mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[TotalCount]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 242\n"
            + "Row #1: 12,888\n"
            + "Row #2: 1,433\n");
    }

    public void testNativeFilterWithCompoundSlicer() {
        String mdx =
            "WITH MEMBER [Measures].[TotalVal] AS 'Aggregate(Filter({[Store].[Store City].members},[Measures].[Unit Sales] > 1000))'\n"
            + "SELECT [Measures].[TotalVal] ON 0, [Product].[All Products].Children on 1 \n"
            + "FROM [Sales] WHERE {[Time].[1997].[Q1],[Time].[1997].[Q2]}";
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "{[Time].[1997].[Q2]}\n"
            + "Axis #1:\n"
            + "{[Measures].[TotalVal]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 10,152\n"
            + "Row #1: 90,413\n"
            + "Row #2: 23,813\n");
        if (!MondrianProperties.instance().EnableNativeFilter.get()) {
            return;
        }
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        final String mysql = !isUseAgg()
            ? "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`,\n"
            + "    `store`.`store_city` as `c2`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`\n"
            + "where\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `time_by_day`.`quarter` in ('Q1', 'Q2')\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `product_class`.`product_family` = 'Drink'\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`,\n"
            + "    `store`.`store_city`\n"
            + "having\n"
            + "    (sum(`sales_fact_1997`.`unit_sales`) > 1000)\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
            + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
            + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC"
            : "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`,\n"
            + "    `store`.`store_city` as `c2`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`\n"
            + "where\n"
            + "    `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `agg_c_14_sales_fact_1997`.`the_year` = 1997\n"
            + "and\n"
            + "    `agg_c_14_sales_fact_1997`.`quarter` in ('Q1', 'Q2')\n"
            + "and\n"
            + "    `agg_c_14_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `product_class`.`product_family` = 'Drink'\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`,\n"
            + "    `store`.`store_city`\n"
            + "having\n"
            + "    (sum(`agg_c_14_sales_fact_1997`.`unit_sales`) > 1000)\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
            + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
            + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC";
        SqlPattern mysqlPattern =
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysql, null);
        assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
    }

    public void testOverridingNonDisjointCompoundFilter() {
        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx =
            "WITH MEMBER [Gender].[All Gender].[NoSlicer] AS '([Product].[Drink], [Time].[1997])', solve_order=1000\n "
            + "MEMBER [Measures].[TotalVal] AS 'Aggregate(Filter({[Store].[Store City].members},[Measures].[Unit Sales] < 2300)), solve_order=900'\n"
            + "SELECT {[Measures].[TotalVal], [Measures].[Unit Sales]} on 0, {[Gender].[All Gender], [Gender].[All Gender].[NoSlicer]} on 1 from [Sales]\n"
            + "WHERE {([Product].[Non-Consumable], [Time].[1997].[Q1]),([Product].[Drink], [Time].[1997].[Q1])}";

        String mysqlQuery = "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`,\n"
            + "    `store`.`store_city` as `c2`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`\n"
            + "where\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `product_class`.`product_family` = 'Drink'\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`,\n"
            + "    `store`.`store_city`\n"
            + "having\n"
            + "    (sum(`sales_fact_1997`.`unit_sales`) < 2300)\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
            + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
            + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC";

        if (!useAgg && MondrianProperties.instance().EnableNativeFilter.get()) {
            getTestContext().flushSchemaCache();
            SqlPattern mysqlPattern =
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(TestContext.instance(), mdx, new SqlPattern[]{mysqlPattern});
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Product].[Non-Consumable], [Time].[1997].[Q1]}\n"
            + "{[Product].[Drink], [Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[TotalVal]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[All Gender].[NoSlicer]}\n"
            + "Row #0: 14,865\n"
            + "Row #0: 18,482\n"
            + "Row #1: 10,417\n"
            + "Row #1: 24,597\n");
    }

    /**
     * This test demonstrates complex interaction between member calcs
     * and a compound slicer
     */
    public void testOverridingCompoundFilter() {
        String mdx =
            "WITH MEMBER [Gender].[All Gender].[NoSlicer] AS '([Product].[All Products], [Time].[1997])', solve_order=1000\n "
            + "MEMBER [Measures].[TotalVal] AS 'Aggregate(Filter({[Store].[Store City].members},[Measures].[Unit Sales] < 2300)), solve_order=900'\n"
            + "SELECT {[Measures].[TotalVal], [Measures].[Unit Sales]} on 0, {[Gender].[All Gender], [Gender].[All Gender].[NoSlicer]} on 1 from [Sales]\n"
            + "WHERE {([Product].[Non-Consumable], [Time].[1997].[Q1]),([Product].[Drink], [Time].[1997].[Q2])}";

        TestContext context = getTestContext().withFreshConnection();
        context.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Product].[Non-Consumable], [Time].[1997].[Q1]}\n"
            + "{[Product].[Drink], [Time].[1997].[Q2]}\n"
            + "Axis #1:\n"
            + "{[Measures].[TotalVal]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[All Gender].[NoSlicer]}\n"
            + "Row #0: 12,730\n"
            + "Row #0: 18,401\n"
            + "Row #1: 6,557\n"
            + "Row #1: 266,773\n");

        mdx =
            "WITH MEMBER [Gender].[All Gender].[SomeSlicer] AS '([Product].[All Products])', solve_order=1000\n "
            + "MEMBER [Measures].[TotalVal] AS 'Aggregate(Filter({[Store].[Store City].members},[Measures].[Unit Sales] < 2700)), solve_order=900'\n"
            + "SELECT {[Measures].[TotalVal], [Measures].[Unit Sales]} on 0, {[Gender].[All Gender], [Gender].[All Gender].[SomeSlicer]} on 1 from [Sales]\n"
            + "WHERE {([Product].[Non-Consumable], [Time].[1997].[Q1]),([Product].[Drink], [Time].[1997].[Q2])}";

        context.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Product].[Non-Consumable], [Time].[1997].[Q1]}\n"
            + "{[Product].[Drink], [Time].[1997].[Q2]}\n"
            + "Axis #1:\n"
            + "{[Measures].[TotalVal]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[All Gender].[SomeSlicer]}\n"
            + "Row #0: 15,056\n"
            + "Row #0: 18,401\n"
            + "Row #1: 3,045\n"
            + "Row #1: 128,901\n");
    }

    public void testNativeFilterWithCompoundSlicerCJ() {
        String mdx =
            "WITH MEMBER [Measures].[TotalVal] AS 'Aggregate(Filter( {[Store].[Store City].members},[Measures].[Unit Sales] > 1000))'\n"
            + "SELECT [Measures].[TotalVal] ON 0, [Gender].[All Gender].Children on 1 \n"
            + "FROM [Sales]\n"
            + "WHERE CrossJoin({ [Product].[Non-Consumable], [Product].[Drink] }, {[Time].[1997].[Q1],[Time].[1997].[Q2]})";
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Product].[Non-Consumable], [Time].[1997].[Q1]}\n"
            + "{[Product].[Non-Consumable], [Time].[1997].[Q2]}\n"
            + "{[Product].[Drink], [Time].[1997].[Q1]}\n"
            + "{[Product].[Drink], [Time].[1997].[Q2]}\n"
            + "Axis #1:\n"
            + "{[Measures].[TotalVal]}\n"
            + "Axis #2:\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "Row #0: 16,729\n"
            + "Row #1: 17,044\n");
    }

    public void testFilterWithDiffLevelCompoundSlicer() {
        // not supported in native, but detected
        // and skipped to regular evaluation
        String mdx =
            "SELECT [Measures].[Unit Sales] ON 0,\n"
            + " Filter({[Store].[Store City].members},[Measures].[Unit Sales] > 10000) on 1 \n"
            + "FROM [Sales] WHERE {[Time].[1997].[Q1], [Time].[1997].[Q2].[4]}";
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "{[Time].[1997].[Q2].[4]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "Row #0: 14,683\n"
            + "Row #1: 10,950\n");
    }

    public void testNativeFilterWithCompoundSlicer2049() {
        assertQueryReturns(
            "with member measures.avgQtrs as 'avg( filter( time.quarter.members, measures.[unit sales] < 200))' "
            + "select measures.avgQtrs * gender.members on 0 from sales where head( product.[product name].members, 3)",
            "Axis #0:\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl].[Pearl Imported Beer]}\n"
            + "Axis #1:\n"
            + "{[Measures].[avgQtrs], [Gender].[All Gender]}\n"
            + "{[Measures].[avgQtrs], [Gender].[F]}\n"
            + "{[Measures].[avgQtrs], [Gender].[M]}\n"
            + "Row #0: 111\n"
            + "Row #0: 58\n"
            + "Row #0: 53\n");
    }

    public void testNativeFilterTupleCompoundSlicer1861() {
        // Using a slicer list instead of tuples causes slicers with
        // tuples where not all combinations of their members are present to
        // fail when nativized.
        // MondrianProperties.instance().EnableNativeFilter.set(true);
        assertQueryReturns(
            "select [Measures].[Unit Sales] on columns, Filter([Time].[1997].Children, [Measures].[Unit Sales] < 12335) on rows from [Sales] where {([Product].[Drink],[Store].[USA].[CA]),([Product].[Food],[Store].[USA].[OR])}",
            "Axis #0:\n"
            + "{[Product].[Drink], [Store].[USA].[CA]}\n"
            + "{[Product].[Food], [Store].[USA].[OR]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q2]}\n"
            + "Row #0: 12,334\n");
    }

    public void testNativeCountNonEmptyExisting() {
        String mdx =
            "WITH MEMBER [Measures].[WithExisting] AS Count(NonEmpty(Existing [Product].[Product Name].Members))\n"
            + " MEMBER [Measures].[WithoutExisting] AS Count(NonEmpty([Product].[Product Name].Members))\n"
            + "SELECT {[Measures].[WithExisting], [Measures].[WithoutExisting]} ON 0,\n"
            + " { [Time].[1997].[Q4].[12], [Time].[1997]} ON 1 FROM [Sales] WHERE [Product].[Non-Consumable]";

        String mysqlQuery =
            "select\n"
                + "    COUNT(*)\n"
                + "from\n"
                + "    (select\n"
                + "    `product_class`.`product_family` as `c0`,\n"
                + "    `product_class`.`product_department` as `c1`,\n"
                + "    `product_class`.`product_category` as `c2`,\n"
                + "    `product_class`.`product_subcategory` as `c3`,\n"
                + "    `product`.`brand_name` as `c4`,\n"
                + "    `product`.`product_name` as `c5`\n"
                + "from\n"
                + "    `product_class` as `product_class`,\n"
                + "    `product` as `product`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `time_by_day` as `time_by_day`\n"
                + "where\n"
                + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"
                + "and\n"
                + "    `time_by_day`.`quarter` = 'Q4'\n"
                + "and\n"
                + "    `time_by_day`.`month_of_year` = 12\n"
                + "and\n"
                + "    (`product_class`.`product_family` = 'Non-Consumable')\n"
                + "and\n"
                + "    `sales_fact_1997`.`unit_sales` is not null\n"
                + "group by\n"
                + "    `product_class`.`product_family`,\n"
                + "    `product_class`.`product_department`,\n"
                + "    `product_class`.`product_category`,\n"
                + "    `product_class`.`product_subcategory`,\n"
                + "    `product`.`brand_name`,\n"
                + "    `product`.`product_name` ) as `count0`\n"
                + "union all\n"
                + "select\n"
                + "    COUNT(*)\n"
                + "from\n"
                + "    (select\n"
                + "    `product_class`.`product_family` as `c0`,\n"
                + "    `product_class`.`product_department` as `c1`,\n"
                + "    `product_class`.`product_category` as `c2`,\n"
                + "    `product_class`.`product_subcategory` as `c3`,\n"
                + "    `product`.`brand_name` as `c4`,\n"
                + "    `product`.`product_name` as `c5`\n"
                + "from\n"
                + "    `product_class` as `product_class`,\n"
                + "    `product` as `product`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `time_by_day` as `time_by_day`\n"
                + "where\n"
                + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"
                + "and\n"
                + "    `time_by_day`.`quarter` = 'Q4'\n"
                + "and\n"
                + "    `time_by_day`.`month_of_year` = 12\n"
                + "and\n"
                + "    `sales_fact_1997`.`unit_sales` is not null\n"
                + "group by\n"
                + "    `product_class`.`product_family`,\n"
                + "    `product_class`.`product_department`,\n"
                + "    `product_class`.`product_category`,\n"
                + "    `product_class`.`product_subcategory`,\n"
                + "    `product`.`brand_name`,\n"
                + "    `product`.`product_name` ) as `count1`\n"
                + "union all\n"
                + "select\n"
                + "    COUNT(*)\n"
                + "from\n"
                + "    (select\n"
                + "    `product_class`.`product_family` as `c0`,\n"
                + "    `product_class`.`product_department` as `c1`,\n"
                + "    `product_class`.`product_category` as `c2`,\n"
                + "    `product_class`.`product_subcategory` as `c3`,\n"
                + "    `product`.`brand_name` as `c4`,\n"
                + "    `product`.`product_name` as `c5`\n"
                + "from\n"
                + "    `product_class` as `product_class`,\n"
                + "    `product` as `product`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `time_by_day` as `time_by_day`\n"
                + "where\n"
                + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"
                + "and\n"
                + "    (`product_class`.`product_family` = 'Non-Consumable')\n"
                + "and\n"
                + "    `sales_fact_1997`.`unit_sales` is not null\n"
                + "group by\n"
                + "    `product_class`.`product_family`,\n"
                + "    `product_class`.`product_department`,\n"
                + "    `product_class`.`product_category`,\n"
                + "    `product_class`.`product_subcategory`,\n"
                + "    `product`.`brand_name`,\n"
                + "    `product`.`product_name` ) as `count2`\n"
                + "union all\n"
                + "select\n"
                + "    COUNT(*)\n"
                + "from\n"
                + "    (select\n"
                + "    `product_class`.`product_family` as `c0`,\n"
                + "    `product_class`.`product_department` as `c1`,\n"
                + "    `product_class`.`product_category` as `c2`,\n"
                + "    `product_class`.`product_subcategory` as `c3`,\n"
                + "    `product`.`brand_name` as `c4`,\n"
                + "    `product`.`product_name` as `c5`\n"
                + "from\n"
                + "    `product_class` as `product_class`,\n"
                + "    `product` as `product`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `time_by_day` as `time_by_day`\n"
                + "where\n"
                + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
                + "and\n"
                + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"
                + "and\n"
                + "    `sales_fact_1997`.`unit_sales` is not null\n"
                + "group by\n"
                + "    `product_class`.`product_family`,\n"
                + "    `product_class`.`product_department`,\n"
                + "    `product_class`.`product_category`,\n"
                + "    `product_class`.`product_subcategory`,\n"
                + "    `product`.`brand_name`,\n"
                + "    `product`.`product_name` ) as `count3`";
        if (!isUseAgg() && (MondrianProperties.instance().EnableNativeCount.get()
            && MondrianProperties.instance().EnableNativeFilter.get()
            && MondrianProperties.instance().EnableNativeExisting.get()))
        {
            propSaver.set(propSaver.properties.GenerateFormattedSql, true);
            getTestContext().flushSchemaCache();
            SqlPattern mysqlPattern =
                new SqlPattern(
                    Dialect.DatabaseProduct.MYSQL,
                    mysqlQuery,
                    null);
            assertQuerySql(TestContext.instance(), mdx, new SqlPattern[]{mysqlPattern});
        }
        assertQueryReturns(mdx,
            "Axis #0:\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Axis #1:\n"
            + "{[Measures].[WithExisting]}\n"
            + "{[Measures].[WithoutExisting]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q4].[12]}\n"
            + "{[Time].[1997]}\n"
            + "Row #0: 294\n"
            + "Row #0: 1,558\n"
            + "Row #1: 295\n"
            + "Row #1: 1,559\n");
  }

    public void testNativeCountFilterExistingScenario() {
//        final boolean useAgg =
//            MondrianProperties.instance().UseAggregates.get()
//            && MondrianProperties.instance().ReadAggregates.get();
//        propSaver.set(propSaver.properties.GenerateFormattedSql, true);

        String mdx =
            "WITH MEMBER [Measures].[WithExisting] AS Count(Filter(Existing [Product].[Product Name].Members, 1=1))\n"
            + "MEMBER [Measures].[WithoutExisting] AS Count(Filter([Product].[Product Name].Members, 1=1))\n"
            + "MEMBER [Measures].[ExistingNonEmpty] AS Count(Filter(NonEmpty( Existing [Product].[Product Name].Members), 1=1))\n"
            + "SELECT {[Measures].[WithExisting], [Measures].[WithoutExisting], [Measures].[ExistingNonEmpty]} ON 0, {[Time].[1997], [Time].[1997].[Q4].[12]} ON 1 FROM [Sales] WHERE [Product].[Non-Consumable]";
        //TODO sql
//        "WITH MEMBER [Measures].[WithExisting] AS Count(Filter(NonEmpty(Existing [Product].[Product Name].Members), 1=1))\n"
//        + "MEMBER [Measures].[WithoutExisting] AS Count(Filter([Product].[Product Name].Members, 1=1))\n"
//        + "SELECT {[Measures].[WithExisting], [Measures].[WithoutExisting]} ON 0, {[Time].[1997], [Time].[1997].[Q4].[12]} ON 1 FROM [Sales] WHERE [Product].[Non-Consumable]";
//        String mysqlQuery =
//            "select\n"
//            + "    COUNT(*)\n"
//            + "from\n"
//            + "    (select\n"
//            + "    `product_class`.`product_family` as `c0`,\n"
//            + "    `product_class`.`product_department` as `c1`,\n"
//            + "    `product_class`.`product_category` as `c2`,\n"
//            + "    `product_class`.`product_subcategory` as `c3`,\n"
//            + "    `product`.`brand_name` as `c4`,\n"
//            + "    `product`.`product_name` as `c5`\n"
//            + "from\n"
//            + "    `product` as `product`,\n"
//            + "    `product_class` as `product_class`,\n"
//            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
//            + "    `time_by_day` as `time_by_day`\n"
//            + "where\n"
//            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
//            + "and\n"
//            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
//            + "and\n"
//            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
//            + "and\n"
//            + "    `time_by_day`.`the_year` = 1997\n"
//            + "and\n"
//            + "    `time_by_day`.`quarter` = 'Q4'\n"
//            + "and\n"
//            + "    `time_by_day`.`month_of_year` = 12\n"
//            + "and\n"
//            + "    `product_class`.`product_family` = 'Non-Consumable'\n"
//            + "group by\n"
//            + "    `product_class`.`product_family`,\n"
//            + "    `product_class`.`product_department`,\n"
//            + "    `product_class`.`product_category`,\n"
//            + "    `product_class`.`product_subcategory`,\n"
//            + "    `product`.`brand_name`,\n"
//            + "    `product`.`product_name`\n"
//            + "having\n"
//            + "    (1 = 1)\n"
//            + "order by\n"
//            + "    ISNULL(`product_class`.`product_family`) ASC, `product_class`.`product_family` ASC,\n"
//            + "    ISNULL(`product_class`.`product_department`) ASC, `product_class`.`product_department` ASC,\n"
//            + "    ISNULL(`product_class`.`product_category`) ASC, `product_class`.`product_category` ASC,\n"
//            + "    ISNULL(`product_class`.`product_subcategory`) ASC, `product_class`.`product_subcategory` ASC,\n"
//            + "    ISNULL(`product`.`brand_name`) ASC, `product`.`brand_name` ASC,\n"
//            + "    ISNULL(`product`.`product_name`) ASC, `product`.`product_name` ASC) as `countQuery`";
        // filter no longer nativizable
//        if (!useAgg && (MondrianProperties.instance().EnableNativeCount.get()
//            && MondrianProperties.instance().EnableNativeFilter.get()
//            && MondrianProperties.instance().EnableNativeExisting.get()))
//        {
//            getTestContext().flushSchemaCache();
//            SqlPattern mysqlPattern =
//                new SqlPattern(
//                    Dialect.DatabaseProduct.MYSQL,
//                    mysqlQuery,
////                    null);
//                    mysqlQuery.substring(0, mysqlQuery.indexOf("from")));
//            assertQuerySql(TestContext.instance(), mdx, new SqlPattern[]{mysqlPattern});
//        }

        assertQueryReturns(mdx,
          "Axis #0:\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Axis #1:\n"
            + "{[Measures].[WithExisting]}\n"
            + "{[Measures].[WithoutExisting]}\n"
            + "{[Measures].[ExistingNonEmpty]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997]}\n"
            + "{[Time].[1997].[Q4].[12]}\n"
            + "Row #0: 295\n"
            + "Row #0: 1,560\n"
            + "Row #0: 295\n"
            + "Row #1: 295\n"
            + "Row #1: 1,560\n"
            + "Row #1: 294\n"); //existing+nonempty
    }

    public void testNativeCountFilterExistingScenarioSimple() {
        String mdx =
            "WITH MEMBER [Measures].[ExistingNonEmpty] AS Count(Filter(NonEmpty(Existing [Product].[Product Name].Members), 1=1))\n"
            + "SELECT {[Measures].[ExistingNonEmpty]} ON 0, {[Time].[1997].[Q4].[12]} ON 1 FROM [Sales] WHERE [Product].[Non-Consumable]";

        assertQueryReturns(mdx,
          "Axis #0:\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Axis #1:\n"
            + "{[Measures].[ExistingNonEmpty]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q4].[12]}\n"
            + "Row #0: 294\n"); //existing+nonempty
    }

    public void testNativeCountExistingEmptyScenario() {
        propSaver.set(propSaver.properties.SsasCompatibleNaming, true);
        TestContext testContext = TestContext.instance()
            .create(
                "  <Dimension name=\"Customer IDs\">\n"
                + "    <Hierarchy name=\"Customer IDs\" hasAll=\"true\" allMemberName=\"All\" primaryKey=\"customer_id\">\n"
                + "      <Table name=\"customer\"/>\n"
                + "      <Level name=\"ID\" column=\"customer_id\" nameColumn=\"fullname\" uniqueMembers=\"true\"/>"
                + "    </Hierarchy>\n"
                + "    <Hierarchy name=\"Customer City\" hasAll=\"true\" allMemberName=\"All\" primaryKey=\"customer_id\">\n"
                + "      <Table name=\"customer\"/>\n"
                + "      <Level name=\"City\" column=\"city\" uniqueMembers=\"true\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n",
                "<Cube name=\"Customers\">\n"
                + "  <Table name=\"sales_fact_1997\"/>\n"
                + "  <DimensionUsage name=\"Customer IDs\" source=\"Customer IDs\" foreignKey=\"customer_id\"/>\n"
                + "  <Measure name=\"Customer Count\" column=\"customer_id\" aggregator=\"count\"/>\n"
                + "</Cube>\n"
                + "<VirtualCube name=\"VCustomers\">\n"
                + "  <VirtualCubeDimension cubeName=\"Customers\" name=\"Customer IDs\"/>\n"
                + "  <VirtualCubeMeasure cubeName=\"Customers\" name=\"[Measures].[Customer Count]\"/>\n"
                + "</VirtualCube>\n"
                , null, null, null, null);
        String mdx =
            "WITH MEMBER [Measures].[City Count] AS Count(Existing [Customer IDs].[Customer City].[City].members)\n"
            + "SELECT {[Measures].[City Count]} ON 0, {[Customer IDs].[ID].&[55]} ON 1\n"
            + "FROM [VCustomers]\n";
        if (!isUseAgg() && propSaver.properties.EnableNativeCount.get()) {
            propSaver.set(propSaver.properties.GenerateFormattedSql, true);
            String mysqlQuery =
                "select\n"
                + "    COUNT(*)\n"
                + "from\n"
                + "    (select\n"
                + "    `customer`.`city` as `c0`\n"
                + "from\n"
                + "    `customer` as `customer`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`\n"
                + "where\n"
                + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
                + "and\n"
                + "    `customer`.`customer_id` = '55'\n"
                + "group by\n"
                + "    `customer`.`city`) as `countQuery`";
            SqlPattern mysqlPattern =
                new SqlPattern(DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(testContext, mdx, new SqlPattern[]{mysqlPattern});
        }

        testContext.assertQueryReturns(mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[City Count]}\n"
            + "Axis #2:\n"
            + "{[Customer IDs].[Gary Dumin]}\n"
            + "Row #0: 1\n");
    }

    public void testNativeFilterNoMeasureUnconstrained() {
        // Native filter would return wrong results when no measure in filter
        // and its arguments need member constraints
        String mdx =
          "WITH MEMBER [Measures].[AggNC] AS Aggregate( [Product].[Non-Consumable].Children )\n"
          + "MEMBER [Measures].[AggNCFilter] AS Aggregate(Filter( [Product].[Non-Consumable].Children, 1=1 ))\n"
          + "MEMBER [Measures].[AggNCFilterCJ] AS Aggregate(Filter( CrossJoin({[Product].[Non-Consumable]}, {[Gender].[All Gender].Children}), 1=1 ))\n"
          + "SELECT {[Measures].[AggNC],[Measures].[AggNCFilter], [Measures].[AggNCFilterCJ]} ON 0,"
          + " {[Time].[1997].[Q4].[12]} ON 1 FROM [Sales]";
        assertQueryReturns(mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[AggNC]}\n"
            + "{[Measures].[AggNCFilter]}\n"
            + "{[Measures].[AggNCFilterCJ]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q4].[12]}\n"
            + "Row #0: 5,021\n"
            + "Row #0: 5,021\n"
            + "Row #0: 5,021\n");
    }

    // Similar to MDX found in this article: http://www.bp-msbi.com/2012/02/what-exists-and-what-is-empty-in-mdx/
    public void testExistingNonEmptyScenario() {
        //        propSaver.set(MondrianProperties.instance().EnableNativeCount, false);
        //propSaver.set(MondrianProperties.instance().EnableNativeNonEmptyFunction, true);
        String mdx =
            "WITH MEMBER [Measures].[Existing Test 1] AS Count(NonEmpty([Time].[Month].Members))\n"
            + "MEMBER [Measures].[Existing Test 2] AS Count(Existing NonEmpty([Time].[Month].Members))\n"
            + "SELECT {[Measures].[Existing Test 1],[Measures].[Existing Test 2]} ON 0, {[Time].[1997].[Q1]} ON 1 FROM [Sales]";
        assertQueryReturns(mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Existing Test 1]}\n"
            + "{[Measures].[Existing Test 2]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "Row #0: 12\n"
            + "Row #0: 3\n");
        // a slight variation on the NonEmpty function
        mdx =
            "WITH MEMBER [Measures].[Existing Test 1] AS Count(NonEmpty([Time].[Month].Members, {[Measures].[Unit Sales], [Measures].[Store Sales]}))\n"
            + "MEMBER [Measures].[Existing Test 2] AS Count(Existing NonEmpty([Time].[Month].Members, {[Measures].[Unit Sales], [Measures].[Store Sales]}))\n"
            + "SELECT {[Measures].[Existing Test 1],[Measures].[Existing Test 2]} ON 0, {[Time].[1997].[Q1]} ON 1 FROM [Sales]";
        assertQueryReturns(mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Existing Test 1]}\n"
            + "{[Measures].[Existing Test 2]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "Row #0: 12\n"
            + "Row #0: 3\n");
        propSaver.set(MondrianProperties.instance().GenerateFormattedSql, true);
        String sql = isUseAgg()
            ? "select\n"
            + "    `agg_c_10_sales_fact_1997`.`the_year` as `c0`,\n"
            + "    `agg_c_10_sales_fact_1997`.`quarter` as `c1`,\n"
            + "    `agg_c_10_sales_fact_1997`.`month_of_year` as `c2`\n"
            + "from\n"
            + "    `agg_c_10_sales_fact_1997` as `agg_c_10_sales_fact_1997`\n"
            + "where\n"
            + "    (`agg_c_10_sales_fact_1997`.`unit_sales` is not null or `agg_c_10_sales_fact_1997`.`store_sales` is not null)\n"
            + "group by\n"
            + "    `agg_c_10_sales_fact_1997`.`the_year`,\n"
            + "    `agg_c_10_sales_fact_1997`.`quarter`,\n"
            + "    `agg_c_10_sales_fact_1997`.`month_of_year`\n"
            + "order by\n"
            + "    ISNULL(`agg_c_10_sales_fact_1997`.`the_year`) ASC, `agg_c_10_sales_fact_1997`.`the_year` ASC,\n"
            + "    ISNULL(`agg_c_10_sales_fact_1997`.`quarter`) ASC, `agg_c_10_sales_fact_1997`.`quarter` ASC,\n"
            + "    ISNULL(`agg_c_10_sales_fact_1997`.`month_of_year`) ASC, `agg_c_10_sales_fact_1997`.`month_of_year` ASC"
            :"select\n"
            + "    `time_by_day`.`the_year` as `c0`,\n"
            + "    `time_by_day`.`quarter` as `c1`,\n"
            + "    `time_by_day`.`month_of_year` as `c2`\n"
            + "from\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`\n"
            + "where\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    (`sales_fact_1997`.`unit_sales` is not null or `sales_fact_1997`.`store_sales` is not null)\n"
            + "group by\n"
            + "    `time_by_day`.`the_year`,\n"
            + "    `time_by_day`.`quarter`,\n"
            + "    `time_by_day`.`month_of_year`\n"
            + "order by\n"
            + "    ISNULL(`time_by_day`.`the_year`) ASC, `time_by_day`.`the_year` ASC,\n"
            + "    ISNULL(`time_by_day`.`quarter`) ASC, `time_by_day`.`quarter` ASC,\n"
            + "    ISNULL(`time_by_day`.`month_of_year`) ASC, `time_by_day`.`month_of_year` ASC";
        SqlPattern mysqlPattern =
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, sql,
                sql.substring(0, sql.indexOf("where")));
        assertQuerySql(mdx, new SqlPattern[]{mysqlPattern});
    }

    public void testExistingCrossJoin() {
        String mdx = "WITH MEMBER [Measures].[ProductMonthSum]"
            + " AS Sum(Existing CrossJoin([Product].[Product Name].Members, [Time].[Month].Members))\n"
            + "SELECT {[Measures].[ProductMonthSum]} ON 0,\n"
            + " {([Product].[Food].[Snacks].[Candy],[Time].[1997].[Q1].[1])} ON 1\n"
            + " FROM [Sales]";
        String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ProductMonthSum]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Snacks].[Candy], [Time].[1997].[Q1].[1]}\n"
            + "Row #0: 549\n";
        assertQueryReturns(mdx, result);
        mdx = "WITH MEMBER [Measures].[ProductMonthSum]"
            + " AS Sum( CrossJoin( Existing[Product].[Product Name].Members, Existing [Time].[Month].Members))\n"
            + "SELECT {[Measures].[ProductMonthSum]} ON 0,\n"
            + " {([Product].[Food].[Snacks].[Candy],[Time].[1997].[Q1].[1])} ON 1\n"
            + " FROM [Sales]";
        assertQueryReturns(mdx, result);
        mdx = "WITH MEMBER [Measures].[ProductMonthSum]"
            + " AS Sum( CrossJoin( Existing [Product].[Product Name].Members, Existing [Time].[Month].Members))\n"
            + "SELECT {[Measures].[ProductMonthSum]} ON 0,\n"
            + " {([Product].[Food].[Snacks].[Candy])} ON 1\n"
            + " FROM [Sales] where [Time].[1997].[Q1].[1]";
        result = "Axis #0:\n"
            + "{[Time].[1997].[Q1].[1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[ProductMonthSum]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Snacks].[Candy]}\n"
            + "Row #0: 549\n";
        assertQueryReturns(mdx, result);
    }

    public void _testExistingEmptyCount() {
      // the 2 empty candy in q1 are not counted
      String mdx = "WITH MEMBER [Measures].[Count Existing]"
          + " AS Count( existing CrossJoin( [Product].[Product Name].Members, [Time].[Month].Members))\n"
          + " MEMBER [Measures].[With Exists] AS Count(Exists( CrossJoin([Product].[Product Name].Members, [Time].[Month].Members) , ([Product].CurrentMember, [Time].CurrentMember)) )"
          + "SELECT {[Measures].[Count Existing], [Measures].[With Exists]} ON 0,\n"
          + " {([Product].[Food].[Snacks].[Candy],[Time].[1997].[Q1].[1]), ([Product].[Food].[Snacks].[Candy],[Time].[1997].[Q1].[2])} ON 1\n"
          + " FROM [Sales]";
      assertQueryReturns(mdx,
          "Axis #0:\n"
          + "{}\n"
          + "Axis #1:\n"
          + "{[Measures].[Count Existing]}\n"
          + "{[Measures].[With Exists]}\n"
          + "Axis #2:\n"
          + "{[Product].[Food].[Snacks].[Candy], [Time].[1997].[Q1].[1]}\n"
          + "{[Product].[Food].[Snacks].[Candy], [Time].[1997].[Q1].[2]}\n"
          + "Row #0: 40\n"
          + "Row #0: 40\n"
          + "Row #1: 40\n"
          + "Row #1: 40\n");
    }

    public void testExistingDiffHierarchySameDim() {
        // test existing constraining a set by a member in a different
        // hierarchy in the same dimension
        String mdx = "WITH MEMBER [Measures].[Count Existing]"
          + " AS Count( existing [Time.Weekly].[Week].Members)\n"
          + "SELECT {[Measures].[Count Existing]} ON 0,\n"
          + " {[Time].[1997].[Q2]} ON 1\n"
          + " FROM [Sales]";
        // 14 weeks overlap with Q2, from week 15 (30/Mar - 05/Apr)
        // to week 28 (29/Jun - 05/Jul)
        assertQueryReturns(mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Count Existing]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q2]}\n"
            + "Row #0: 14\n");

        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();
        if (!useAgg && MondrianProperties.instance().EnableNativeExisting.get()) {
            propSaver.set(MondrianProperties.instance().GenerateFormattedSql, true);
            String mySql =
                "select\n"
                + "    COUNT(*)\n"
                + "from\n"
                + "    (select\n"
                + "    `time_by_day`.`the_year` as `c0`,\n"
                + "    `time_by_day`.`week_of_year` as `c1`\n"
                + "from\n"
                + "    `time_by_day` as `time_by_day`,\n"
                + "    `sales_fact_1997` as `sales_fact_1997`\n"
                + "where\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    (`time_by_day`.`quarter` = 'Q2' and `time_by_day`.`the_year` = 1997)\n"
                + "group by\n"
                + "    `time_by_day`.`the_year`,\n"
                + "    `time_by_day`.`week_of_year`) as `countQuery`";
            SqlPattern mysqlPattern =
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, mySql, null);
            assertQuerySql(mdx, new SqlPattern[]{ mysqlPattern });
        }
    }

    public void testExistingWithCompoundSlicer() {
        String mdx =
            "WITH MEMBER [Measures].[Count Existing]"
            + " AS Count(existing [Time].[Month].Members)\n"
            + "SELECT {[Measures].[Count Existing]} ON 0\n"
            + "FROM [Sales]\n"
            + "WHERE {[Time].[1997].[Q2], [Time].[1997].[Q3]}";
        assertQueryReturns(mdx,
            "Axis #0:\n"
            + "{[Time].[1997].[Q2]}\n"
            + "{[Time].[1997].[Q3]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Count Existing]}\n"
            + "Row #0: 6\n");

        if (propSaver.properties.EnableNativeExisting.get()) {
            verifySameNativeAndNot(mdx, "Existing with compound slicer", getTestContext());

            final boolean useAgg =
                propSaver.properties.UseAggregates.get()
                && propSaver.properties.ReadAggregates.get();
            if (!useAgg) {
                propSaver.set(propSaver.properties.GenerateFormattedSql, true);
                String mySql =
                    "select\n"
                    + "    COUNT(*)\n"
                    + "from\n"
                    + "    (select\n"
                    + "    `time_by_day`.`the_year` as `c0`,\n"
                    + "    `time_by_day`.`quarter` as `c1`,\n"
                    + "    `time_by_day`.`month_of_year` as `c2`\n"
                    + "from\n"
                    + "    `time_by_day` as `time_by_day`,\n"
                    + "    `sales_fact_1997` as `sales_fact_1997`\n"
                    + "where\n"
                    + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                    + "and\n"
                    + "    (`time_by_day`.`quarter` in ('Q2', 'Q3') and `time_by_day`.`the_year` = 1997)\n"
                    + "group by\n"
                    + "    `time_by_day`.`the_year`,\n"
                    + "    `time_by_day`.`quarter`,\n"
                    + "    `time_by_day`.`month_of_year`) as `countQuery`";
                SqlPattern mysqlPattern =
                    new SqlPattern(Dialect.DatabaseProduct.MYSQL, mySql, null);
                assertQuerySql(mdx, new SqlPattern[]{ mysqlPattern });
            }
        }
    }

    // tests creating consolidated count with descendant cross join arg instances
    // i.e. mondrian.rolap.sql.DescendantsCrossJoinArg instances in the constraint
    public void testNativeCombinedCountWithDescendantsCrossJoinArg() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);

        String mdx =
            "WITH MEMBER [Measures].[C1] AS 'Count([Product].[Product Family].&[Non-Consumable].children)' "  +
                "MEMBER [Measures].[C2] AS 'Count([Product].[Product Family].&[Food].children)' " +
                "MEMBER [Measures].[C3] AS 'Count([Product].[Product Family].&[Drink].children)' " +
                "SELECT {[Measures].[C1],[Measures].[C3], [Measures].[C2]} on 0 FROM [Sales] WHERE ([Time].[1997].[Q1])";

        String mysql = "select\n"
            + "    count(case when `c0` = 'Non-Consumable' then 1 else null end) as `count0`,\n"
            + "    count(case when `c0` = 'Drink' then 1 else null end) as `count1`,\n"
            + "    count(case when `c0` = 'Food' then 1 else null end) as `count2`\n"
            + "from\n"
            + "    (select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    `product_class`.`product_department` as `c1`\n"
            + "from\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`\n"
            + "where\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    (`product_class`.`product_family` in ('Drink', 'Non-Consumable', 'Food'))\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`) as `countQuery`";

        SqlPattern mysqlPattern = new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysql, null);
        assertQuerySql(mdx, new SqlPattern[] {mysqlPattern});

        assertQueryReturns(mdx, "Axis #0:\n" +
                "{[Time].[1997].[Q1]}\n" +
                "Axis #1:\n" +
                "{[Measures].[C1]}\n" +
                "{[Measures].[C3]}\n" +
                "{[Measures].[C2]}\n" +
                "Row #0: 5\n" +
                "Row #0: 3\n" +
                "Row #0: 15\n");
        propSaver.reset();
    }

    // tests creating consolidated count with descendant cross join arg instances
    // i.e. mondrian.rolap.sql.DescendantsCrossJoinArg instances in the constraint
    public void testNativeCombinedCountWithDescendantsCrossJoinArgDisjoint() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);

        String mdx =
            "WITH MEMBER [Measures].[C1] AS 'Count([Product].[Product Family].&[Non-Consumable].children)' "
                +
                "MEMBER [Measures].[C2] AS 'Count([Product].[Product Family].&[Food].children)' "
                +
                "MEMBER [Measures].[C3] AS 'Count([Product].[Product Family].&[Drink].children)' "
                +
                "SELECT {[Measures].[C1],[Measures].[C3], [Measures].[C2]} on 0 FROM [Sales] "
                + "WHERE {([Time].[1997].[Q1], [Store].[USA].[CA]),([Time].[1997].[Q2], [Store].[USA].[OR])}";

        String mysql = "select\n"
            + "    count(case when `c0` = 'Non-Consumable' then 1 else null end) as `count0`,\n"
            + "    count(case when `c0` = 'Drink' then 1 else null end) as `count1`,\n"
            + "    count(case when `c0` = 'Food' then 1 else null end) as `count2`\n"
            + "from\n"
            + "    (select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    `product_class`.`product_department` as `c1`\n"
            + "from\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`\n"
            + "where\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    (`product_class`.`product_family` in ('Drink', 'Non-Consumable', 'Food'))\n"
            + "group by\n"
            + "    `product_class`.`product_family`,\n"
            + "    `product_class`.`product_department`) as `countQuery`";

        SqlPattern mysqlPattern = new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysql, null);
        assertQuerySql(mdx, new SqlPattern[] {mysqlPattern});

        assertQueryReturns(mdx, "Axis #0:\n"
            + "{[Time].[1997].[Q1], [Store].[USA].[CA]}\n"
            + "{[Time].[1997].[Q2], [Store].[USA].[OR]}\n"
            + "Axis #1:\n"
            + "{[Measures].[C1]}\n"
            + "{[Measures].[C3]}\n"
            + "{[Measures].[C2]}\n"
            + "Row #0: 5\n"
            + "Row #0: 3\n"
            + "Row #0: 15\n");
        propSaver.reset();
    }



    // tests creating consolidated count with member list cross join arg instances
    // i.e. mondrian.rolap.sql.MemberListCrossJoinArgs in the constraint
    public void testNativeCombinedCountWithMemberListCrossJoinArgs() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);

        String mdx =
            "WITH MEMBER [Measures].[C1] AS 'Count([Product].[Product Family].&[Non-Consumable])' " +
                "MEMBER [Measures].[C2] AS 'Count([Product].[Product Family].&[Food])' " +
                "MEMBER [Measures].[C3] AS 'Count([Product].[Product Family].&[Drink])' " +
                "SELECT {[Measures].[C1],[Measures].[C2], [Measures].[C3]} on 0 FROM [Sales] WHERE ([Time].[1997].[Q1])";

        String mysql = "select\n"
            + "    count(case when `c0` = 'Non-Consumable' then 1 else null end) as `count0`,\n"
            + "    count(case when `c0` = 'Food' then 1 else null end) as `count1`,\n"
            + "    count(case when `c0` = 'Drink' then 1 else null end) as `count2`\n"
            + "from\n"
            + "    (select\n"
            + "    `product_class`.`product_family` as `c0`\n"
            + "from\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`\n"
            + "where\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    (`product_class`.`product_family` in ('Drink', 'Non-Consumable', 'Food'))\n"
            + "group by\n"
            + "    `product_class`.`product_family`) as `countQuery`";

        SqlPattern mysqlPattern = new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysql, null);
        assertQuerySql(mdx, new SqlPattern[] {mysqlPattern});

        String result = "Axis #0:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[C1]}\n"
            + "{[Measures].[C2]}\n"
            + "{[Measures].[C3]}\n"
            + "Row #0: 1\n"
            + "Row #0: 1\n"
            + "Row #0: 1\n";

        assertQueryReturns(mdx, result);

        propSaver.reset();
    }

    public void testNativeSimpleConsolidatedCount() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx =""
                + "WITH MEMBER [Measures].[C1] as 'Count([Customers].[USA].[WA].[Spokane].children)'\n"
                + "MEMBER [Measures].[C2] as 'Count([Store].[USA].[WA].[Spokane].children)'\n"
                + "SELECT "
                + "{[Measures].[C1], [Measures].[C2]} on 0 "
                + "FROM [Sales]";

        String mysql = "select\n"
            + "    COUNT(*)\n"
            + "from\n"
            + "    (select\n"
            + "    `customer`.`country` as `c0`,\n"
            + "    `customer`.`state_province` as `c1`,\n"
            + "    `customer`.`city` as `c2`,\n"
            + "    `customer`.`customer_id` as `c3`\n"
            + "from\n"
            + "    `customer` as `customer`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`\n"
            + "where\n"
            + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
            + "and\n"
            + "    (`customer`.`city` = 'Spokane' and `customer`.`state_province` = 'WA')\n"
            + "group by\n"
            + "    `customer`.`country`,\n"
            + "    `customer`.`state_province`,\n"
            + "    `customer`.`city`,\n"
            + "    `customer`.`customer_id` ) as `count0`\n"
            + "union all\n"
            + "select\n"
            + "    COUNT(*)\n"
            + "from\n"
            + "    (select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`,\n"
            + "    `store`.`store_city` as `c2`,\n"
            + "    `store`.`store_name` as `c3`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`\n"
            + "where\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    (`store`.`store_city` = 'Spokane' and `store`.`store_state` = 'WA')\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`,\n"
            + "    `store`.`store_city`,\n"
            + "    `store`.`store_name` ) as `count1`";

        SqlPattern mysqlPattern = new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysql, null);
        assertQuerySql(mdx, new SqlPattern[] {mysqlPattern});


        String result = "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[C1]}\n"
            + "{[Measures].[C2]}\n"
            + "Row #0: 84\n"
            + "Row #0: 1\n";
        assertQueryReturns(mdx, result);

        propSaver.reset();
    }

    public void testStrToMemberMeasure() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx =
            "with member [Measures].[Profit] AS [Measures].[Store Sales] - [Measures].[Store Cost]\n"
                + "member [Measures].[SalesNum] as StrToMember('[Measures].[Sales Count]')\n"
                + "member [Measures].[ProfitPerSale] as [Measures].[Profit] / [Measures].[SalesNum]\n"
                + "set StoreSet as 'Filter(NonEmpty([Store].[Store City].Members, {[Measures].[Unit Sales]}), [Measures].[ProfitPerSale] > 3.00)'\n"
                + "select [StoreSet] on 0, {[Measures].[ProfitPerSale]} on 1 from [Sales]";


        String mysql = "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`,\n"
            + "    `store`.`store_city` as `c2`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`unit_sales` is not null\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`,\n"
            + "    `store`.`store_city`\n"
            + "having\n"
            + "    (((sum(`sales_fact_1997`.`store_sales`) - sum(`sales_fact_1997`.`store_cost`)) / count(`sales_fact_1997`.`product_id`)) > 3.00)\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
            + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
            + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC";

        SqlPattern mysqlPattern = new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysql, null);
        assertQuerySql(mdx, new SqlPattern[] {mysqlPattern});
        String result = "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "{[Store].[USA].[WA].[Yakima]}\n"
            + "Axis #2:\n"
            + "{[Measures].[ProfitPerSale]}\n"
            + "Row #0: 4.03\n"
            + "Row #0: 3.99\n"
            + "Row #0: 4.04\n"
            + "Row #0: 4.01\n"
            + "Row #0: 3.93\n"
            + "Row #0: 4.03\n"
            + "Row #0: 3.98\n"
            + "Row #0: 4.03\n"
            + "Row #0: 4.01\n"
            + "Row #0: 4.00\n";
        assertQueryReturns(mdx, result);
        propSaver.reset();
    }
    

    public void testStrToMemberConstrainedMeasure() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx =
            "with member [Measures].[Profit] AS [Measures].[Store Sales] - [Measures].[Store Cost]\n"
                + "member [Measures].[SalesNum] as StrToMember('[Measures].[Sales Count]', CONSTRAINED)\n"
                + "member [Measures].[ProfitPerSale] as [Measures].[Profit] / [Measures].[SalesNum]\n"
                + "set StoreSet as 'Filter(NonEmpty([Store].[Store City].Members, {[Measures].[Unit Sales]}), [Measures].[ProfitPerSale] > 3.00)'\n"
                + "select [StoreSet] on 0, {[Measures].[ProfitPerSale]} on 1 from [Sales]";

        String mysql = "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`,\n"
            + "    `store`.`store_city` as `c2`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`unit_sales` is not null\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`,\n"
            + "    `store`.`store_city`\n"
            + "having\n"
            + "    (((sum(`sales_fact_1997`.`store_sales`) - sum(`sales_fact_1997`.`store_cost`)) / count(`sales_fact_1997`.`product_id`)) > 3.00)\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
            + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
            + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC";

        SqlPattern mysqlPattern = new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysql, null);
        assertQuerySql(mdx, new SqlPattern[] {mysqlPattern});
        String result = "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "{[Store].[USA].[WA].[Yakima]}\n"
            + "Axis #2:\n"
            + "{[Measures].[ProfitPerSale]}\n"
            + "Row #0: 4.03\n"
            + "Row #0: 3.99\n"
            + "Row #0: 4.04\n"
            + "Row #0: 4.01\n"
            + "Row #0: 3.93\n"
            + "Row #0: 4.03\n"
            + "Row #0: 3.98\n"
            + "Row #0: 4.03\n"
            + "Row #0: 4.01\n"
            + "Row #0: 4.00\n";
        assertQueryReturns(mdx, result);

        propSaver.reset();
    }

    public void testStrToMemberConstrainedParameterMeasure() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx =
            "with member [Measures].[Profit] AS [Measures].[Store Sales] - [Measures].[Store Cost]\n"
                + "member [Measures].[SalesNum] as StrToMember(Parameter(\"SN\", STRING, '[Measures].[Sales Count]'), CONSTRAINED)\n"
                + "member [Measures].[ProfitPerSale] as [Measures].[Profit] / [Measures].[SalesNum]\n"
                + "set StoreSet as 'Filter(NonEmpty([Store].[Store City].Members, {[Measures].[Unit Sales]}), [Measures].[ProfitPerSale] > 3.00)'\n"
                + "select [StoreSet] on 0, {[Measures].[ProfitPerSale]} on 1 from [Sales]";

        String mysql = "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`,\n"
            + "    `store`.`store_city` as `c2`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`unit_sales` is not null\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`,\n"
            + "    `store`.`store_city`\n"
            + "having\n"
            + "    (((sum(`sales_fact_1997`.`store_sales`) - sum(`sales_fact_1997`.`store_cost`)) / count(`sales_fact_1997`.`product_id`)) > 3.00)\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
            + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
            + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC";

        SqlPattern mysqlPattern = new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysql, null);
        assertQuerySql(mdx, new SqlPattern[] {mysqlPattern});
        String result = "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "{[Store].[USA].[WA].[Yakima]}\n"
            + "Axis #2:\n"
            + "{[Measures].[ProfitPerSale]}\n"
            + "Row #0: 4.03\n"
            + "Row #0: 3.99\n"
            + "Row #0: 4.04\n"
            + "Row #0: 4.01\n"
            + "Row #0: 3.93\n"
            + "Row #0: 4.03\n"
            + "Row #0: 3.98\n"
            + "Row #0: 4.03\n"
            + "Row #0: 4.01\n"
            + "Row #0: 4.00\n";
        assertQueryReturns(mdx, result);

        propSaver.reset();
    }
    

    public void testValStrToMemberParameterNameMember() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx =
            "with member [Measures].[Profit] AS [Measures].[Store Sales] - [Measures].[Store Cost]\n"
                + "member [Measures].[ProfitByMinSqft] as [Measures].[Profit] / Val(StrToMember(Parameter(\"SQ\", STRING, '[Store Size in SQFT].[Store Sqft].[20319]')).Name)\n"
                + "set StoreSet as 'Filter(NonEmpty([Store].[Store City].Members, {[Measures].[Unit Sales]}), [Measures].[ProfitByMinSqft] > 1.00)'\n"
                + "select [StoreSet] on 0, {[Measures].[ProfitByMinSqft]} on 1 from [Sales]";

        String mysql = "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`,\n"
            + "    `store`.`store_city` as `c2`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`unit_sales` is not null\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`,\n"
            + "    `store`.`store_city`\n"
            + "having\n"
            + "    (((sum(`sales_fact_1997`.`store_sales`) - sum(`sales_fact_1997`.`store_cost`)) / 20319.0) > 1.00)\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
            + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
            + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC";

        SqlPattern mysqlPattern = new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysql, null);
        assertQuerySql(mdx, new SqlPattern[] {mysqlPattern});
        String result = "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "Axis #2:\n"
            + "{[Measures].[ProfitByMinSqft]}\n"
            + "Row #0: 1.35\n"
            + "Row #0: 1.61\n"
            + "Row #0: 1.61\n"
            + "Row #0: 1.63\n"
            + "Row #0: 2.58\n"
            + "Row #0: 1.56\n"
            + "Row #0: 1.56\n"
            + "Row #0: 1.47\n"
            + "Row #0: 2.21\n";
        assertQueryReturns(mdx, result);

        propSaver.reset();
    }

    public void testValStrToMemberParameterPropertyMember() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx =
            "with member [Measures].[Profit] AS [Measures].[Store Sales] - [Measures].[Store Cost]\n"
                + "member [Measures].[ProfitByMinSqft] as [Measures].[Profit] / Val(StrToMember(Parameter(\"SQ\", STRING, '[Store Size in SQFT].[Store Sqft].[20319]')).Properties('Name'))\n"
                + "set StoreSet as 'Filter(NonEmpty([Store].[Store City].Members, {[Measures].[Unit Sales]}), [Measures].[ProfitByMinSqft] > 1.00)'\n"
                + "select [StoreSet] on 0, {[Measures].[ProfitByMinSqft]} on 1 from [Sales]";

        String mysql = "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`,\n"
            + "    `store`.`store_city` as `c2`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`unit_sales` is not null\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`,\n"
            + "    `store`.`store_city`\n"
            + "having\n"
            + "    (((sum(`sales_fact_1997`.`store_sales`) - sum(`sales_fact_1997`.`store_cost`)) / 20319.0) > 1.00)\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
            + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
            + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC";

        SqlPattern mysqlPattern = new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysql, null);
        assertQuerySql(mdx, new SqlPattern[] {mysqlPattern});
        String result = "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "Axis #2:\n"
            + "{[Measures].[ProfitByMinSqft]}\n"
            + "Row #0: 1.35\n"
            + "Row #0: 1.61\n"
            + "Row #0: 1.61\n"
            + "Row #0: 1.63\n"
            + "Row #0: 2.58\n"
            + "Row #0: 1.56\n"
            + "Row #0: 1.56\n"
            + "Row #0: 1.47\n"
            + "Row #0: 2.21\n";
        assertQueryReturns(mdx, result);

        propSaver.reset();
    }

    public void testValNameMember() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx =
            "with member [Measures].[Profit] AS [Measures].[Store Sales] - [Measures].[Store Cost]\n"
                + "member [Measures].[ProfitByMinSqft] as [Measures].[Profit] / Val([Store Size in SQFT].[Store Sqft].[20319].Name)\n"
                + "set StoreSet as 'Filter(NonEmpty([Store].[Store City].Members, {[Measures].[Unit Sales]}), [Measures].[ProfitByMinSqft] > 1.00)'\n"
                + "select [StoreSet] on 0, {[Measures].[ProfitByMinSqft]} on 1 from [Sales]";

        String mysql = "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`,\n"
            + "    `store`.`store_city` as `c2`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`unit_sales` is not null\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`,\n"
            + "    `store`.`store_city`\n"
            + "having\n"
            + "    (((sum(`sales_fact_1997`.`store_sales`) - sum(`sales_fact_1997`.`store_cost`)) / 20319.0) > 1.00)\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
            + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
            + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC";

        SqlPattern mysqlPattern = new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysql, null);
        assertQuerySql(mdx, new SqlPattern[] {mysqlPattern});
        String result = "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "Axis #2:\n"
            + "{[Measures].[ProfitByMinSqft]}\n"
            + "Row #0: 1.35\n"
            + "Row #0: 1.61\n"
            + "Row #0: 1.61\n"
            + "Row #0: 1.63\n"
            + "Row #0: 2.58\n"
            + "Row #0: 1.56\n"
            + "Row #0: 1.56\n"
            + "Row #0: 1.47\n"
            + "Row #0: 2.21\n";
        assertQueryReturns(mdx, result);

        propSaver.reset();
    }

    public void testValStrToMemberNameMember() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx =
            "with member [Measures].[Profit] AS [Measures].[Store Sales] - [Measures].[Store Cost]\n"
                + "member [Measures].[ProfitByMinSqft] as [Measures].[Profit] / Val(StrToMember('[Store Size in SQFT].[Store Sqft].[20319]').Name)\n"
                + "set StoreSet as 'Filter(NonEmpty([Store].[Store City].Members, {[Measures].[Unit Sales]}), [Measures].[ProfitByMinSqft] > 1.00)'\n"
                + "select [StoreSet] on 0, {[Measures].[ProfitByMinSqft]} on 1 from [Sales]";

        String mysql = "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`,\n"
            + "    `store`.`store_city` as `c2`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`unit_sales` is not null\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`,\n"
            + "    `store`.`store_city`\n"
            + "having\n"
            + "    (((sum(`sales_fact_1997`.`store_sales`) - sum(`sales_fact_1997`.`store_cost`)) / 20319.0) > 1.00)\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
            + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
            + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC";

        SqlPattern mysqlPattern = new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysql, null);
        assertQuerySql(mdx, new SqlPattern[] {mysqlPattern});
        String result = "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "Axis #2:\n"
            + "{[Measures].[ProfitByMinSqft]}\n"
            + "Row #0: 1.35\n"
            + "Row #0: 1.61\n"
            + "Row #0: 1.61\n"
            + "Row #0: 1.63\n"
            + "Row #0: 2.58\n"
            + "Row #0: 1.56\n"
            + "Row #0: 1.56\n"
            + "Row #0: 1.47\n"
            + "Row #0: 2.21\n";
        assertQueryReturns(mdx, result);

        propSaver.reset();
    }

    public void testValPropertiesMember() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx =
            "with member [Measures].[Profit] AS [Measures].[Store Sales] - [Measures].[Store Cost]\n"
                + "member [Measures].[ProfitByMinSqft] as [Measures].[Profit] / Val([Store Size in SQFT].[Store Sqft].[20319].Properties('Name'))\n"
                + "set StoreSet as 'Filter(NonEmpty([Store].[Store City].Members, {[Measures].[Unit Sales]}), [Measures].[ProfitByMinSqft] > 1.00)'\n"
                + "select [StoreSet] on 0, {[Measures].[ProfitByMinSqft]} on 1 from [Sales]";

        String mysql = "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`,\n"
            + "    `store`.`store_city` as `c2`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`unit_sales` is not null\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`,\n"
            + "    `store`.`store_city`\n"
            + "having\n"
            + "    (((sum(`sales_fact_1997`.`store_sales`) - sum(`sales_fact_1997`.`store_cost`)) / 20319.0) > 1.00)\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
            + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
            + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC";

        SqlPattern mysqlPattern = new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysql, null);
        assertQuerySql(mdx, new SqlPattern[] {mysqlPattern});
        String result = "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "Axis #2:\n"
            + "{[Measures].[ProfitByMinSqft]}\n"
            + "Row #0: 1.35\n"
            + "Row #0: 1.61\n"
            + "Row #0: 1.61\n"
            + "Row #0: 1.63\n"
            + "Row #0: 2.58\n"
            + "Row #0: 1.56\n"
            + "Row #0: 1.56\n"
            + "Row #0: 1.47\n"
            + "Row #0: 2.21\n";
        assertQueryReturns(mdx, result);

        propSaver.reset();
    }

    public void testValStrToMemberPropertiesMember() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx =
            "with member [Measures].[Profit] AS [Measures].[Store Sales] - [Measures].[Store Cost]\n"
                + "member [Measures].[ProfitByMinSqft] as [Measures].[Profit] / Val(StrToMember('[Store Size in SQFT].[Store Sqft].[20319]').Properties('Name'))\n"
                + "set StoreSet as 'Filter(NonEmpty([Store].[Store City].Members, {[Measures].[Unit Sales]}), [Measures].[ProfitByMinSqft] > 1.00)'\n"
                + "select [StoreSet] on 0, {[Measures].[ProfitByMinSqft]} on 1 from [Sales]";

        String mysql = "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`,\n"
            + "    `store`.`store_city` as `c2`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`unit_sales` is not null\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`,\n"
            + "    `store`.`store_city`\n"
            + "having\n"
            + "    (((sum(`sales_fact_1997`.`store_sales`) - sum(`sales_fact_1997`.`store_cost`)) / 20319.0) > 1.00)\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
            + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
            + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC";

        SqlPattern mysqlPattern = new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysql, null);
        assertQuerySql(mdx, new SqlPattern[] {mysqlPattern});
        String result = "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "Axis #2:\n"
            + "{[Measures].[ProfitByMinSqft]}\n"
            + "Row #0: 1.35\n"
            + "Row #0: 1.61\n"
            + "Row #0: 1.61\n"
            + "Row #0: 1.63\n"
            + "Row #0: 2.58\n"
            + "Row #0: 1.56\n"
            + "Row #0: 1.56\n"
            + "Row #0: 1.47\n"
            + "Row #0: 2.21\n";
        assertQueryReturns(mdx, result);

        propSaver.reset();
    }

    public void testTupleSqlCompilerWithFunction() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);

        final boolean useAgg = MondrianProperties.instance().UseAggregates.get() && MondrianProperties.instance().ReadAggregates.get();

        TestContext testContext
            = TestContext.instance()
           .createSubstitutingCube("Sales",
               "  <Dimension name=\"Customer IDs\" foreignKey=\"customer_id\">\n"
                   + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Customer IDs\" primaryKey=\"customer_id\">\n"
                   + "      <Table name=\"customer\"/>\n"
                   + "      <Level name=\"ID\" column=\"customer_id\" uniqueMembers=\"true\"/>\n"
                   + "    </Hierarchy>\n"
                   + "  </Dimension>\n",
               "  <CalculatedMember visible=\"true\" name=\"20\" hierarchy=\"[Gender]\" parent=\"[Gender].[All Gender]\">\n"
                   + "    <Formula>NULL</Formula>\n"
                   + "  </CalculatedMember>\n"
                   + "  <CalculatedMember dimension=\"Measures\" visible=\"true\" name=\"Calc\">\n"
                   + "    <Formula>IIf(([Gender].[M], [Measures].[Unit Sales])>=val([Gender].currentMember.Name), (StrToMember(Parameter(\"Blokes\", STRING, '[Gender].[M]')), [Measures].[Unit Sales]), NULL)</Formula>\n"
                   + "  </CalculatedMember>\n"
                   + "  <CalculatedMember dimension=\"Measures\" visible=\"true\" name=\"Calc3\">\n"
                   + "    <Formula>Count(Filter(NonEmpty([Customer IDs].[All Customer IDs].Children), NOT(ISEMPTY([Measures].[Calc]))))</Formula>\n"
                   + "  </CalculatedMember>\n");

        String mdx =
            "with member [Measures].[Calc2] as '([Gender].[All Gender].[20], [Measures].[Calc3]) ', solve_order = -1\n"
                + "select {[Measures].[Calc2]} on 0,\n"
                +
                "{[Product].[All Products].Children} on 1"
                +
                "from [Sales] where (StrToMember(\"[Marital Status].[S]\"))";

        String mysqlQuery = "select\n"
            + "    `customer`.`customer_id` as `c0`\n"
            + "from\n"
            + "    `customer` as `customer`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`\n"
            + "where\n"
            + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `product_class`.`product_family` = 'Non-Consumable'\n"
            + "and\n"
            + "    `customer`.`marital_status` = 'S'\n"
            + "and\n"
            + "    `sales_fact_1997`.`unit_sales` is not null\n"
            + "group by\n"
            + "    `customer`.`customer_id`\n"
            + "order by\n"
            + "    ISNULL(`customer`.`customer_id`) ASC, `customer`.`customer_id` ASC";

        getTestContext().flushSchemaCache();

        if (!useAgg && MondrianProperties.instance().EnableNativeCount.get()) {
            SqlPattern mysqlPattern = new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(testContext, mdx, new SqlPattern[] {mysqlPattern});
        }

        testContext.assertQueryReturns(mdx, "Axis #0:\n"
                + "{[Marital Status].[S]}\n"
                + "Axis #1:\n"
                + "{[Measures].[Calc2]}\n"
                + "Axis #2:\n"
                + "{[Product].[Drink]}\n"
                + "{[Product].[Food]}\n"
                + "{[Product].[Non-Consumable]}\n"
                + "Row #0: 40\n"
                + "Row #1: 820\n"
                + "Row #2: 169\n");
    }

    public void testHangerTupleSqlCompilerWithFunction() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);

        final boolean useAgg = MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();

        TestContext testContext
            = TestContext.instance()
         .createSubstitutingCube("Sales",
             "  <Dimension name=\"Customer IDs\" foreignKey=\"customer_id\">\n"
                 + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Customer IDs\" primaryKey=\"customer_id\">\n"
                 + "      <Table name=\"customer\"/>\n"
                 + "      <Level name=\"ID\" column=\"customer_id\" uniqueMembers=\"true\"/>\n"
                 + "    </Hierarchy>\n"
                 + "  </Dimension>\n"
                 + "<Dimension name=\"GenderHanger\" foreignKey=\"customer_id\" hanger=\"true\">"
                 + "<Hierarchy hasAll=\"true\" allMemberName=\"All GenderHanger\" primaryKey=\"customer_id\">\n"
                 + "  <Table name=\"customer\"/>\n"
                 + "  <Level name=\"GenderHanger\" column=\"gender\" uniqueMembers=\"true\"/>\n"
                 + "</Hierarchy>\n"
                 + "</Dimension>\n",
             "  <CalculatedMember visible=\"true\" name=\"20\" hierarchy=\"[GenderHanger]\" parent=\"[GenderHanger].[All GenderHanger]\">\n"
                 + "    <Formula>NULL</Formula>\n"
                 + "  </CalculatedMember>\n"
                 + "  <CalculatedMember dimension=\"Measures\" visible=\"true\" name=\"Calc\">\n"
                 + "    <Formula>IIf(([GenderHanger].[M], [Measures].[Unit Sales])>=val([GenderHanger].currentMember.Name), (StrToMember('[GenderHanger].[M]'), [Measures].[Unit Sales]), NULL)</Formula>\n"
                 + "  </CalculatedMember>\n"
                 + "  <CalculatedMember dimension=\"Measures\" visible=\"true\" name=\"Calc3\">\n"
                 + "    <Formula>Count(Filter(NonEmpty([Customer IDs].[All Customer IDs].Children), NOT(ISEMPTY([Measures].[Calc]))))</Formula>\n"
                 + "  </CalculatedMember>\n");

        String mdx =
            "with member [Measures].[Calc2] as '([GenderHanger].[All GenderHanger].[20], [Measures].[Calc3]) ', solve_order = -1\n"
                + "select {[Measures].[Calc2]} on 0,\n"
                +
                "{[Product].[All Products].Children} on 1"
                +
                "from [Sales] where (StrToMember(\"[Marital Status].[S]\"))";

        String mysqlQuery = "select\n"
            + "    `customer`.`customer_id` as `c0`\n"
            + "from\n"
            + "    `customer` as `customer`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `product` as `product`\n"
            + "where\n"
            + "    `sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `product_class`.`product_family` = 'Non-Consumable'\n"
            + "and\n"
            + "    `customer`.`marital_status` = 'S'\n"
            + "and\n"
            + "    `sales_fact_1997`.`unit_sales` is not null\n"
            + "group by\n"
            + "    `customer`.`customer_id`\n"
            + "order by\n"
            + "    ISNULL(`customer`.`customer_id`) ASC, `customer`.`customer_id` ASC";

        getTestContext().flushSchemaCache();

        if (!useAgg && MondrianProperties.instance().EnableNativeCount.get()) {
            SqlPattern mysqlPattern = new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlQuery, null);
            assertQuerySql(testContext, mdx, new SqlPattern[] {mysqlPattern});
        }

        testContext.assertQueryReturns(mdx, "Axis #0:\n"
            + "{[Marital Status].[S]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Calc2]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 86\n"
            + "Row #1: 1,576\n"
            + "Row #2: 353\n");
    }

    public void testValWithAddress() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx =
            "with member [Measures].[Profit] AS [Measures].[Store Sales] - [Measures].[Store Cost]\n"
                + "member [Measures].[ProfitByAddress] as [Measures].[Profit] / Val(StrToMember('[Store].[Store Name].[Store 10]').Properties('Street address'))\n"
                + "set StoreSet as 'Filter(NonEmpty([Store].[Store City].Members, {[Measures].[Unit Sales]}), [Measures].[ProfitByAddress] > 4.0)'\n"
                + "select [StoreSet] on 0, {[Measures].[ProfitByAddress]} on 1 from [Sales]";
        
        String mysql = "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`,\n"
            + "    `store`.`store_city` as `c2`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`unit_sales` is not null\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`,\n"
            + "    `store`.`store_city`\n"
            + "having\n"
            + "    (((sum(`sales_fact_1997`.`store_sales`) - sum(`sales_fact_1997`.`store_cost`)) / 7894.0) > 4.0)\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
            + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
            + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC";

        SqlPattern mysqlPattern = new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysql, null);
        assertQuerySql(mdx, new SqlPattern[] {mysqlPattern});
        String result = "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "Axis #2:\n"
            + "{[Measures].[ProfitByAddress]}\n"
            + "Row #0: 4.15\n"
            + "Row #0: 4.14\n"
            + "Row #0: 4.19\n"
            + "Row #0: 6.64\n"
            + "Row #0: 4.03\n"
            + "Row #0: 4.01\n"
            + "Row #0: 5.69\n";
        assertQueryReturns(mdx, result);

        propSaver.reset();
    }

    public void testStrToMemberParameterMeasure() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        String mdx =
            "with member [Measures].[Profit] AS [Measures].[Store Sales] - [Measures].[Store Cost]\n"
                + "member [Measures].[SalesNum] as StrToMember(Parameter(\"NumberOfSales\", STRING, '[Measures].[Sales Count]')), FORMAT_STRING = \"$#,0\"\n"
                + "member [Measures].[ProfitPerSale] as [Measures].[Profit] / [Measures].[SalesNum]\n"
                + "set StoreSet as 'Filter(NonEmpty([Store].[Store City].Members, {[Measures].[Unit Sales]}), [Measures].[ProfitPerSale] > 3.00)'\n"
                + "select [StoreSet] on 0, {[Measures].[ProfitPerSale]} on 1 from [Sales]";

        String mysql = "select\n"
            + "    `store`.`store_country` as `c0`,\n"
            + "    `store`.`store_state` as `c1`,\n"
            + "    `store`.`store_city` as `c2`\n"
            + "from\n"
            + "    `store` as `store`,\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`unit_sales` is not null\n"
            + "group by\n"
            + "    `store`.`store_country`,\n"
            + "    `store`.`store_state`,\n"
            + "    `store`.`store_city`\n"
            + "having\n"
            + "    (((sum(`sales_fact_1997`.`store_sales`) - sum(`sales_fact_1997`.`store_cost`)) / count(`sales_fact_1997`.`product_id`)) > 3.00)\n"
            + "order by\n"
            + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
            + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
            + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC";

        SqlPattern mysqlPattern = new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysql, null);
        assertQuerySql(mdx, new SqlPattern[] {mysqlPattern});
        String result = "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "{[Store].[USA].[WA].[Yakima]}\n"
            + "Axis #2:\n"
            + "{[Measures].[ProfitPerSale]}\n"
            + "Row #0: 4.03\n"
            + "Row #0: 3.99\n"
            + "Row #0: 4.04\n"
            + "Row #0: 4.01\n"
            + "Row #0: 3.93\n"
            + "Row #0: 4.03\n"
            + "Row #0: 3.98\n"
            + "Row #0: 4.03\n"
            + "Row #0: 4.01\n"
            + "Row #0: 4.00\n";
        assertQueryReturns(mdx, result);

        propSaver.reset();
    }

}
// End NativeSetEvaluationTest.java
