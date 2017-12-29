/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2016 Pentaho and others
// All Rights Reserved.
 */
package mondrian.rolap;

import mondrian.olap.Connection;
import mondrian.olap.MondrianProperties;
import mondrian.rolap.sql.MemberListCrossJoinArg;
import mondrian.spi.Dialect;
import mondrian.test.SqlPattern;
import mondrian.test.TestContext;

/**
 * Tests for Filter and native Filters.
 *
 * @author Rushan Chen
 * @since April 28, 2009
 */
public class FilterTest extends BatchTestCase {
    public FilterTest() {
        super();
    }

    public FilterTest(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        propSaver.set(
            MondrianProperties.instance().EnableNativeCrossJoin, true);
    }

    public void testInFilterSimple() throws Exception {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, false);
        propSaver.set(MondrianProperties.instance().EnableNativeFilter, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;

        String query =
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Customers],[*BASE_MEMBERS_Product])' "
            + "Set [*BASE_MEMBERS_Customers] as 'Filter([Customers].[City].Members,Ancestor([Customers].CurrentMember, [Customers].[State Province]) In {[Customers].[All Customers].[USA].[CA]})' "
            + "Set [*BASE_MEMBERS_Product] as 'Filter([Product].[Product Family].Members,[Product].CurrentMember In {[Product].[All Products].[Drink]})' "
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Customers].currentMember,[Product].currentMember)})' "
            + "Select "
            + "{[Measures].[Customer Count]} on columns, "
            + "Non Empty [*CJ_ROW_AXIS] on rows "
            + "From [Sales]";

        checkNative(100, 45, query, null, requestFreshConnection);
    }

    public void testNotInFilterSimple() throws Exception {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, false);
        propSaver.set(MondrianProperties.instance().EnableNativeFilter, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;

        String query =
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Customers],[*BASE_MEMBERS_Product])' "
            + "Set [*BASE_MEMBERS_Customers] as 'Filter([Customers].[City].Members,Ancestor([Customers].CurrentMember, [Customers].[State Province]) Not In {[Customers].[All Customers].[USA].[CA]})' "
            + "Set [*BASE_MEMBERS_Product] as 'Filter([Product].[Product Family].Members,[Product].CurrentMember Not In {[Product].[All Products].[Drink]})' "
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Customers].currentMember,[Product].currentMember)})' "
            + "Select "
            + "{[Measures].[Customer Count]} on columns, "
            + "Non Empty [*CJ_ROW_AXIS] on rows "
            + "From [Sales]";

        checkNative(100, 66, query, null, requestFreshConnection);
    }

    public void testInFilterAND() throws Exception {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, false);
        propSaver.set(MondrianProperties.instance().EnableNativeFilter, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;

        String query =
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Customers],[*BASE_MEMBERS_Product])' "
            + "Set [*BASE_MEMBERS_Customers] as 'Filter([Customers].[City].Members,"
            + "((Ancestor([Customers].CurrentMember, [Customers].[State Province]) In {[Customers].[All Customers].[USA].[CA]}) "
            + "AND ([Customers].CurrentMember Not In {[Customers].[All Customers].[USA].[CA].[Altadena]})))' "
            + "Set [*BASE_MEMBERS_Product] as 'Filter([Product].[Product Family].Members,[Product].CurrentMember Not In {[Product].[All Products].[Drink]})' "
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Customers].currentMember,[Product].currentMember)})' "
            + "Select "
            + "{[Measures].[Customer Count]} on columns, "
            + "Non Empty [*CJ_ROW_AXIS] on rows "
            + "From [Sales]";

        checkNative(200, 88, query, null, requestFreshConnection);
    }

    public void testIsFilterSimple() throws Exception {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, false);
        propSaver.set(MondrianProperties.instance().EnableNativeFilter, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;

        String query =
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Customers],[*BASE_MEMBERS_Product])' "
            + "Set [*BASE_MEMBERS_Customers] as 'Filter([Customers].[City].Members,Ancestor([Customers].CurrentMember, [Customers].[State Province]) Is [Customers].[All Customers].[USA].[CA])' "
            + "Set [*BASE_MEMBERS_Product] as 'Filter([Product].[Product Family].Members,[Product].CurrentMember Is [Product].[All Products].[Drink])' "
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Customers].currentMember,[Product].currentMember)})' "
            + "Select "
            + "{[Measures].[Customer Count]} on columns, "
            + "Non Empty [*CJ_ROW_AXIS] on rows "
            + "From [Sales]";

        checkNative(100, 45, query, null, requestFreshConnection);
    }

    public void testNotIsFilterSimple() throws Exception {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, false);
        propSaver.set(MondrianProperties.instance().EnableNativeFilter, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;

        String query =
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Customers],[*BASE_MEMBERS_Product])' "
            + "Set [*BASE_MEMBERS_Customers] as 'Filter([Customers].[City].Members, not (Ancestor([Customers].CurrentMember, [Customers].[State Province]) Is [Customers].[All Customers].[USA].[CA]))' "
            + "Set [*BASE_MEMBERS_Product] as 'Filter([Product].[Product Family].Members,not ([Product].CurrentMember Is [Product].[All Products].[Drink]))' "
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Customers].currentMember,[Product].currentMember)})' "
            + "Select "
            + "{[Measures].[Customer Count]} on columns, "
            + "Non Empty [*CJ_ROW_AXIS] on rows "
            + "From [Sales]";

        checkNative(100, 66, query, null, requestFreshConnection);
    }

    public void testMixedInIsFilters() throws Exception {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, false);
        propSaver.set(MondrianProperties.instance().EnableNativeFilter, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;

        String query =
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Customers],[*BASE_MEMBERS_Product])' "
            + "Set [*BASE_MEMBERS_Customers] as 'Filter([Customers].[City].Members,"
            + "((Ancestor([Customers].CurrentMember, [Customers].[State Province]) Is [Customers].[All Customers].[USA].[CA]) "
            + "AND ([Customers].CurrentMember Not In {[Customers].[All Customers].[USA].[CA].[Altadena]})))' "
            + "Set [*BASE_MEMBERS_Product] as 'Filter([Product].[Product Family].Members, not ([Product].CurrentMember Is [Product].[All Products].[Drink]))' "
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Customers].currentMember,[Product].currentMember)})' "
            + "Select "
            + "{[Measures].[Customer Count]} on columns, "
            + "Non Empty [*CJ_ROW_AXIS] on rows "
            + "From [Sales]";

        checkNative(200, 88, query, null, requestFreshConnection);
    }

    /**
     * Here the filter is above (rather than as inputs to) the NECJ.  These
     * types of filters are currently not natively evaluated.
     *
     * <p>To expand on this case, RolapNativeFilter needs to be improved so it
     * knows how to represent the dimension filter constraint.  Currently the
     * FilterConstraint is only used for filters on measures.
     *
     * @throws Exception
     */
    public void testInFilterNonNative() throws Exception {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, false);
        propSaver.set(MondrianProperties.instance().EnableNativeFilter, true);

        String query =
            "With "
            + "Set [*BASE_CJ_SET] as 'CrossJoin([Customers].[City].Members,[Product].[Product Family].Members)' "
            + "Set [*NATIVE_CJ_SET] as 'Filter([*BASE_CJ_SET], "
            + "(Ancestor([Customers].CurrentMember,[Customers].[State Province]) In {[Customers].[All Customers].[USA].[CA]}) AND ([Product].CurrentMember In {[Product].[All Products].[Drink]}))' "
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Customers].currentMember,[Product].currentMember)})' "
            + "Select "
            + "{[Measures].[Customer Count]} on columns, "
            + "Non Empty [*CJ_ROW_AXIS] on rows "
            + "From [Sales]";

        checkNotNative(45, query);
    }

    public void testTopCountOverInFilter() throws Exception {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, false);
        propSaver.set(MondrianProperties.instance().EnableNativeFilter, true);
        propSaver.set(MondrianProperties.instance().EnableNativeTopCount, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;

        String query =
            "With "
            + "Set [*NATIVE_TOP_SET] as 'TopCount([*BASE_MEMBERS_Customers], 3, [Measures].[Customer Count])' "
            + "Set [*BASE_MEMBERS_Customers] as 'Filter([Customers].[City].Members,Ancestor([Customers].CurrentMember, [Customers].[State Province]) In {[Customers].[All Customers].[USA].[CA]})' "
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_TOP_SET], {([Customers].currentMember,[Product].currentMember)})' "
            + "Select "
            + "{[Measures].[Customer Count]} on columns, "
            + "Non Empty [*CJ_ROW_AXIS] on rows "
            + "From [Sales]";

        checkNative(100, 3, query, null, requestFreshConnection);
    }

    /**
     * Test that if Null member is not explicitly excluded, then the native
     * filter SQL should not filter out null members.
     *
     * @throws Exception
     */
    public void testNotInFilterKeepNullMember() throws Exception {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, false);
        propSaver.set(MondrianProperties.instance().EnableNativeFilter, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;

        String query =
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Customers],[*BASE_MEMBERS_SQFT])' "
            + "Set [*BASE_MEMBERS_Customers] as 'Filter([Customers].[Country].Members, [Customers].CurrentMember In {[Customers].[All Customers].[USA]})' "
            + "Set [*BASE_MEMBERS_SQFT] as 'Filter([Store Size in SQFT].[Store Sqft].Members, [Store Size in SQFT].currentMember not in {[Store Size in SQFT].[All Store Size in SQFTs].[39696]})' "
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Customers].currentMember,[Store Size in SQFT].currentMember)})' "
            + "Set [*ORDERED_CJ_ROW_AXIS] as 'Order([*CJ_ROW_AXIS], [Store Size in SQFT].currentmember.OrderKey, BASC)' "
            + "Select "
            + "{[Measures].[Customer Count]} on columns, "
            + "Non Empty [*ORDERED_CJ_ROW_AXIS] on rows "
            + "From [Sales]";

        String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Customers].[USA], [Store Size in SQFT].[#null]}\n"
            + "{[Customers].[USA], [Store Size in SQFT].[20319]}\n"
            + "{[Customers].[USA], [Store Size in SQFT].[21215]}\n"
            + "{[Customers].[USA], [Store Size in SQFT].[22478]}\n"
            + "{[Customers].[USA], [Store Size in SQFT].[23598]}\n"
            + "{[Customers].[USA], [Store Size in SQFT].[23688]}\n"
            + "{[Customers].[USA], [Store Size in SQFT].[27694]}\n"
            + "{[Customers].[USA], [Store Size in SQFT].[28206]}\n"
            + "{[Customers].[USA], [Store Size in SQFT].[30268]}\n"
            + "{[Customers].[USA], [Store Size in SQFT].[33858]}\n"
            + "Row #0: 1,153\n"
            + "Row #1: 563\n"
            + "Row #2: 906\n"
            + "Row #3: 296\n"
            + "Row #4: 1,147\n"
            + "Row #5: 1,059\n"
            + "Row #6: 474\n"
            + "Row #7: 190\n"
            + "Row #8: 84\n"
            + "Row #9: 278\n";

        checkNative(0, 10, query, result, requestFreshConnection);
    }

    /**
     * Test that if Null member is explicitly excluded, then the native filter
     * SQL should filter out null members.
     *
     * @throws Exception
     */
    public void testNotInFilterExcludeNullMember() throws Exception {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, false);
        propSaver.set(MondrianProperties.instance().EnableNativeFilter, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;

        String query =
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Customers],[*BASE_MEMBERS_SQFT])' "
            + "Set [*BASE_MEMBERS_Customers] as 'Filter([Customers].[Country].Members, [Customers].CurrentMember In {[Customers].[All Customers].[USA]})' "
            + "Set [*BASE_MEMBERS_SQFT] as 'Filter([Store Size in SQFT].[Store Sqft].Members, "
            + "[Store Size in SQFT].currentMember not in {[Store Size in SQFT].[All Store Size in SQFTs].[#null], [Store Size in SQFT].[All Store Size in SQFTs].[39696]})' "
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Customers].currentMember,[Store Size in SQFT].currentMember)})' "
            + "Set [*ORDERED_CJ_ROW_AXIS] as 'Order([*CJ_ROW_AXIS], [Store Size in SQFT].currentmember.OrderKey, BASC)' "
            + "Select "
            + "{[Measures].[Customer Count]} on columns, "
            + "Non Empty [*ORDERED_CJ_ROW_AXIS] on rows "
            + "From [Sales]";

        String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Customers].[USA], [Store Size in SQFT].[20319]}\n"
            + "{[Customers].[USA], [Store Size in SQFT].[21215]}\n"
            + "{[Customers].[USA], [Store Size in SQFT].[22478]}\n"
            + "{[Customers].[USA], [Store Size in SQFT].[23598]}\n"
            + "{[Customers].[USA], [Store Size in SQFT].[23688]}\n"
            + "{[Customers].[USA], [Store Size in SQFT].[27694]}\n"
            + "{[Customers].[USA], [Store Size in SQFT].[28206]}\n"
            + "{[Customers].[USA], [Store Size in SQFT].[30268]}\n"
            + "{[Customers].[USA], [Store Size in SQFT].[33858]}\n"
            + "Row #0: 563\n"
            + "Row #1: 906\n"
            + "Row #2: 296\n"
            + "Row #3: 1,147\n"
            + "Row #4: 1,059\n"
            + "Row #5: 474\n"
            + "Row #6: 190\n"
            + "Row #7: 84\n"
            + "Row #8: 278\n";

        checkNative(0, 9, query, result, requestFreshConnection);
    }

    /**
     * Test that null members are included when the filter excludes members
     * that contain multiple levels, but none being null.
     */
    public void testNotInMultiLevelMemberConstraintNonNullParent() {
        if (MondrianProperties.instance().ReadAggregates.get()) {
            // If aggregate tables are enabled, generates similar SQL involving
            // agg tables.
            return;
        }
        String query =
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Customers],[*BASE_MEMBERS_Quarters])' "
            + "Set [*BASE_MEMBERS_Customers] as 'Filter([Customers].[Country].Members, [Customers].CurrentMember In {[Customers].[All Customers].[USA]})' "
            + "Set [*BASE_MEMBERS_Quarters] as 'Filter([Time].[Quarter].Members, "
            + "[Time].currentMember not in {[Time].[1997].[Q1], [Time].[1998].[Q3]})' "
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Customers].currentMember,[Time].currentMember)})' "
            + "Set [*ORDERED_CJ_ROW_AXIS] as 'Order([*CJ_ROW_AXIS], [Time].currentmember.OrderKey, BASC)' "
            + "Select "
            + "{[Measures].[Customer Count]} on columns, "
            + "Non Empty [*ORDERED_CJ_ROW_AXIS] on rows "
            + "From [Sales]";

        String necjSqlDerby =
            "select \"customer\".\"country\", \"time_by_day\".\"the_year\", "
            + "\"time_by_day\".\"quarter\" from \"customer\" as \"customer\", "
            + "\"sales_fact_1997\" as \"sales_fact_1997\", \"time_by_day\" as "
            + "\"time_by_day\" where \"sales_fact_1997\".\"customer_id\" = "
            + "\"customer\".\"customer_id\" and \"sales_fact_1997\".\"time_id\" = "
            + "\"time_by_day\".\"time_id\" and (\"customer\".\"country\" = 'USA') and "
            + "(not ((\"time_by_day\".\"the_year\" = 1997 and \"time_by_day\".\"quarter\" "
            + "= 'Q1') or (\"time_by_day\".\"the_year\" = 1998 and "
            + "\"time_by_day\".\"quarter\" = 'Q3')) or ((\"time_by_day\".\"quarter\" is "
            + "null or \"time_by_day\".\"the_year\" is null) and "
            + "not((\"time_by_day\".\"the_year\" = 1997 and \"time_by_day\".\"quarter\" "
            + "= 'Q1') or (\"time_by_day\".\"the_year\" = 1998 and "
            + "\"time_by_day\".\"quarter\" = 'Q3')))) group by \"customer\".\"country\", "
            + "\"time_by_day\".\"the_year\", \"time_by_day\".\"quarter\" "
            + "order by CASE WHEN \"customer\".\"country\" IS NULL THEN 1 ELSE 0 END, \"customer\".\"country\" ASC, CASE WHEN \"time_by_day\".\"the_year\" IS NULL THEN 1 ELSE 0 END, \"time_by_day\".\"the_year\" ASC, CASE WHEN \"time_by_day\".\"quarter\" IS NULL THEN 1 ELSE 0 END, \"time_by_day\".\"quarter\" ASC";

        String necjSqlMySql =
            "select `customer`.`country` as `c0`, `time_by_day`.`the_year` as `c1`, "
            + "`time_by_day`.`quarter` as `c2` from `customer` as `customer`, "
            + "`sales_fact_1997` as `sales_fact_1997`, `time_by_day` as `time_by_day` "
            + "where `sales_fact_1997`.`customer_id` = `customer`.`customer_id` "
            + "and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and "
            + "(`customer`.`country` = 'USA') and "
            + "(not ((`time_by_day`.`quarter`, `time_by_day`.`the_year`) in "
            + "(('Q1', 1997), ('Q3', 1998))) or (`time_by_day`.`quarter` is null or "
            + "`time_by_day`.`the_year` is null)) "
            + "group by `customer`.`country`, `time_by_day`.`the_year`, `time_by_day`.`quarter` "
            + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "order by ISNULL(`c0`) ASC, "
                + "`c0` ASC, ISNULL(`c1`) ASC, "
                + "`c1` ASC, ISNULL(`c2`) ASC, "
                + "`c2` ASC"
                : "order by ISNULL(`customer`.`country`) ASC, "
                + "`customer`.`country` ASC, ISNULL(`time_by_day`.`the_year`) ASC, "
                + "`time_by_day`.`the_year` ASC, ISNULL(`time_by_day`.`quarter`) ASC, "
                + "`time_by_day`.`quarter` ASC");

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.DERBY, necjSqlDerby, necjSqlDerby),
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL, necjSqlMySql, necjSqlMySql)
        };

        assertQuerySql(query, patterns);
    }

    /**
     * Test that null members are included when the filter excludes members
     * that contain multiple levels, but none being null.  The members have
     * the same parent.
     */
    public void testNotInMultiLevelMemberConstraintNonNullSameParent() {
        if (MondrianProperties.instance().ReadAggregates.get()) {
            // If aggregate tables are enabled, generates similar SQL involving
            // agg tables.
            return;
        }
        String query =
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Customers],[*BASE_MEMBERS_Quarters])' "
            + "Set [*BASE_MEMBERS_Customers] as 'Filter([Customers].[Country].Members, [Customers].CurrentMember In {[Customers].[All Customers].[USA]})' "
            + "Set [*BASE_MEMBERS_Quarters] as 'Filter([Time].[Quarter].Members, "
            + "[Time].currentMember not in {[Time].[1997].[Q1], [Time].[1997].[Q3]})' "
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Customers].currentMember,[Time].currentMember)})' "
            + "Set [*ORDERED_CJ_ROW_AXIS] as 'Order([*CJ_ROW_AXIS], [Time].currentmember.OrderKey, BASC)' "
            + "Select "
            + "{[Measures].[Customer Count]} on columns, "
            + "Non Empty [*ORDERED_CJ_ROW_AXIS] on rows "
            + "From [Sales]";

        String necjSqlDerby =
            "select \"customer\".\"country\", \"time_by_day\".\"the_year\", "
            + "\"time_by_day\".\"quarter\" from \"customer\" as \"customer\", "
            + "\"sales_fact_1997\" as \"sales_fact_1997\", \"time_by_day\" as "
            + "\"time_by_day\" where \"sales_fact_1997\".\"customer_id\" = "
            + "\"customer\".\"customer_id\" and \"sales_fact_1997\".\"time_id\" = "
            + "\"time_by_day\".\"time_id\" and (\"customer\".\"country\" = 'USA') and "
            + "((not (\"time_by_day\".\"quarter\" in ('Q1', 'Q3')) or "
            + "(\"time_by_day\".\"quarter\" is null)) or (not "
            + "(\"time_by_day\".\"the_year\" = 1997) or (\"time_by_day\".\"the_year\" is "
            + "null))) group by \"customer\".\"country\", \"time_by_day\".\"the_year\", "
            + "\"time_by_day\".\"quarter\" "
            + "order by CASE WHEN \"customer\".\"country\" IS NULL THEN 1 ELSE 0 END, \"customer\".\"country\" ASC, CASE WHEN \"time_by_day\".\"the_year\" IS NULL THEN 1 ELSE 0 END, \"time_by_day\".\"the_year\" ASC, CASE WHEN \"time_by_day\".\"quarter\" IS NULL THEN 1 ELSE 0 END, \"time_by_day\".\"quarter\" ASC";

        String necjSqlMySql =
            "select `customer`.`country` as `c0`, `time_by_day`.`the_year` as "
            + "`c1`, `time_by_day`.`quarter` as `c2` from `customer` as "
            + "`customer`, `sales_fact_1997` as `sales_fact_1997`, `time_by_day` "
            + "as `time_by_day` where `sales_fact_1997`.`customer_id` = "
            + "`customer`.`customer_id` and `sales_fact_1997`.`time_id` = "
            + "`time_by_day`.`time_id` and (`customer`.`country` = 'USA') and "
            + "((not (`time_by_day`.`quarter` in ('Q1', 'Q3')) or "
            + "(`time_by_day`.`quarter` is null)) or (not "
            + "(`time_by_day`.`the_year` = 1997) or (`time_by_day`.`the_year` "
            + "is null))) group by `customer`.`country`, `time_by_day`.`the_year`, `time_by_day`.`quarter` "
            + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "order by ISNULL(`c0`) ASC, "
                + "`c0` ASC, ISNULL(`c1`) ASC, "
                + "`c1` ASC, ISNULL(`c2`) ASC, "
                + "`c2` ASC"
                : "order by ISNULL(`customer`.`country`) ASC, "
                + "`customer`.`country` ASC, ISNULL(`time_by_day`.`the_year`) ASC, "
                + "`time_by_day`.`the_year` ASC, ISNULL(`time_by_day`.`quarter`) ASC, "
                + "`time_by_day`.`quarter` ASC");

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.DERBY, necjSqlDerby, necjSqlDerby),
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL, necjSqlMySql, necjSqlMySql)
        };

        assertQuerySql(query, patterns);
    }

    /**
     * Test that null members are included when the filter explicitly excludes
     * certain members that contain nulls.  The members span multiple levels.
     */
    public void testNotInMultiLevelMemberConstraintMixedNullNonNullParent() {
        if (!isDefaultNullMemberRepresentation()) {
            return;
        }
        if (MondrianProperties.instance().FilterChildlessSnowflakeMembers.get())
        {
            return;
        }

        String dimension =
            "<Dimension name=\"Warehouse2\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"warehouse_id\">\n"
            + "    <Table name=\"warehouse\"/>\n"
            + "    <Level name=\"fax\" column=\"warehouse_fax\" uniqueMembers=\"true\"/>\n"
            + "    <Level name=\"address1\" column=\"wa_address1\" uniqueMembers=\"false\"/>\n"
            + "    <Level name=\"name\" column=\"warehouse_name\" uniqueMembers=\"false\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>\n";

        String cube =
            "<Cube name=\"Warehouse2\">\n"
            + "  <Table name=\"inventory_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n"
            + "  <DimensionUsage name=\"Warehouse2\" source=\"Warehouse2\" foreignKey=\"warehouse_id\"/>\n"
            + "  <Measure name=\"Warehouse Cost\" column=\"warehouse_cost\" aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Warehouse Sales\" column=\"warehouse_sales\" aggregator=\"sum\"/>\n"
            + "</Cube>";

        String query =
            "with\n"
            + "set [Filtered Warehouse Set] as 'Filter([Warehouse2].[name].Members, [Warehouse2].CurrentMember Not In"
            + "{[Warehouse2].[#null].[234 West Covina Pkwy].[Freeman And Co],"
            + " [Warehouse2].[971-555-6213].[3377 Coachman Place].[Jones International]})' "
            + "set [NECJ] as NonEmptyCrossJoin([Filtered Warehouse Set], {[Product].[Product Family].Food}) "
            + "select [NECJ] on 0 from [Warehouse2]";

        String necjSqlDerby =
            "select \"warehouse\".\"warehouse_fax\", \"warehouse\".\"wa_address1\", "
            + "\"warehouse\".\"warehouse_name\", \"product_class\".\"product_family\" "
            + "from \"warehouse\" as \"warehouse\", \"inventory_fact_1997\" as "
            + "\"inventory_fact_1997\", \"product\" as \"product\", \"product_class\" as "
            + "\"product_class\" where \"inventory_fact_1997\".\"warehouse_id\" = "
            + "\"warehouse\".\"warehouse_id\" and \"product\".\"product_class_id\" = "
            + "\"product_class\".\"product_class_id\" and "
            + "\"inventory_fact_1997\".\"product_id\" = \"product\".\"product_id\" and "
            + "(\"product_class\".\"product_family\" = 'Food') and "
            + "(not ((\"warehouse\".\"wa_address1\" = '234 West Covina Pkwy' and "
            + "\"warehouse\".\"warehouse_fax\" is null and "
            + "\"warehouse\".\"warehouse_name\" = 'Freeman And Co') or "
            + "(\"warehouse\".\"wa_address1\" = '3377 Coachman Place' and "
            + "\"warehouse\".\"warehouse_fax\" = '971-555-6213' and "
            + "\"warehouse\".\"warehouse_name\" = 'Jones International')) or "
            + "((\"warehouse\".\"warehouse_name\" is null or "
            + "\"warehouse\".\"wa_address1\" is null or \"warehouse\".\"warehouse_fax\" "
            + "is null) and not((\"warehouse\".\"wa_address1\" = "
            + "'234 West Covina Pkwy' and \"warehouse\".\"warehouse_fax\" is null "
            + "and \"warehouse\".\"warehouse_name\" = 'Freeman And Co') or "
            + "(\"warehouse\".\"wa_address1\" = '3377 Coachman Place' and "
            + "\"warehouse\".\"warehouse_fax\" = '971-555-6213' and "
            + "\"warehouse\".\"warehouse_name\" = 'Jones International')))) "
            + "group by \"warehouse\".\"warehouse_fax\", \"warehouse\".\"wa_address1\", "
            + "\"warehouse\".\"warehouse_name\", \"product_class\".\"product_family\" "
            + "order by \"warehouse\".\"warehouse_fax\" ASC, "
            + "\"warehouse\".\"wa_address1\" ASC, \"warehouse\".\"warehouse_name\" ASC, "
            + "\"product_class\".\"product_family\" ASC";

        String necjSqlMySql =
            "select `warehouse`.`warehouse_fax` as `c0`, `warehouse`.`wa_address1` as `c1`, "
            + "`warehouse`.`warehouse_name` as `c2`, `product_class`.`product_family` as `c3` "
            + "from `warehouse` as `warehouse`, `inventory_fact_1997` as `inventory_fact_1997`, "
            + "`product` as `product`, `product_class` as `product_class` where "
            + "`inventory_fact_1997`.`warehouse_id` = `warehouse`.`warehouse_id` "
            + "and `product`.`product_class_id` = `product_class`.`product_class_id` "
            + "and `inventory_fact_1997`.`product_id` = `product`.`product_id` "
            + "and (`product_class`.`product_family` = 'Food') and "
            + "(not ((`warehouse`.`warehouse_name`, `warehouse`.`wa_address1`, `warehouse`.`warehouse_fax`) "
            + "in (('Jones International', '3377 Coachman Place', '971-555-6213')) "
            + "or (`warehouse`.`warehouse_fax` is null and (`warehouse`.`warehouse_name`, `warehouse`.`wa_address1`) "
            + "in (('Freeman And Co', '234 West Covina Pkwy')))) or "
            + "((`warehouse`.`warehouse_name` is null or `warehouse`.`wa_address1` is null "
            + "or `warehouse`.`warehouse_fax` is null) and not((`warehouse`.`warehouse_fax` is null "
            + "and (`warehouse`.`warehouse_name`, `warehouse`.`wa_address1`) in "
            + "(('Freeman And Co', '234 West Covina Pkwy')))))) "
            + "group by `warehouse`.`warehouse_fax`, `warehouse`.`wa_address1`, "
            + "`warehouse`.`warehouse_name`, `product_class`.`product_family` "
            + "order by ISNULL(`warehouse`.`warehouse_fax`), `warehouse`.`warehouse_fax` ASC, "
            + "ISNULL(`warehouse`.`wa_address1`), `warehouse`.`wa_address1` ASC, "
            + "ISNULL(`warehouse`.`warehouse_name`), `warehouse`.`warehouse_name` ASC, "
            + "ISNULL(`product_class`.`product_family`), `product_class`.`product_family` ASC";

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.DERBY, necjSqlDerby, necjSqlDerby),
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL, necjSqlMySql, necjSqlMySql)
        };

        TestContext testContext =
            TestContext.instance().create(
                dimension,
                cube,
                null,
                null,
                null,
                null);

        assertQuerySql(testContext, query, patterns);
    }

    /**
     * Test that null members are included when the filter explicitly excludes
     * a single member that has a null.  The members span multiple levels.
     */
    public void testNotInMultiLevelMemberConstraintSingleNullParent() {
        if (!isDefaultNullMemberRepresentation()) {
            return;
        }
        if (MondrianProperties.instance().FilterChildlessSnowflakeMembers.get())
        {
            return;
        }

        String dimension =
            "<Dimension name=\"Warehouse2\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"warehouse_id\">\n"
            + "    <Table name=\"warehouse\"/>\n"
            + "    <Level name=\"fax\" column=\"warehouse_fax\" uniqueMembers=\"true\"/>\n"
            + "    <Level name=\"address1\" column=\"wa_address1\" uniqueMembers=\"false\"/>\n"
            + "    <Level name=\"name\" column=\"warehouse_name\" uniqueMembers=\"false\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>\n";

        String cube =
            "<Cube name=\"Warehouse2\">\n"
            + "  <Table name=\"inventory_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n"
            + "  <DimensionUsage name=\"Warehouse2\" source=\"Warehouse2\" foreignKey=\"warehouse_id\"/>\n"
            + "  <Measure name=\"Warehouse Cost\" column=\"warehouse_cost\" aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Warehouse Sales\" column=\"warehouse_sales\" aggregator=\"sum\"/>\n"
            + "</Cube>";

        String query =
            "with\n"
            + "set [Filtered Warehouse Set] as 'Filter([Warehouse2].[name].Members, [Warehouse2].CurrentMember Not In"
            + "{[Warehouse2].[#null].[234 West Covina Pkwy].[Freeman And Co]})' "
            + "set [NECJ] as NonEmptyCrossJoin([Filtered Warehouse Set], {[Product].[Product Family].Food}) "
            + "select [NECJ] on 0 from [Warehouse2]";

        String necjSqlDerby =
            "select \"warehouse\".\"warehouse_fax\", \"warehouse\".\"wa_address1\", "
            + "\"warehouse\".\"warehouse_name\", \"product_class\".\"product_family\" "
            + "from \"warehouse\" as \"warehouse\", \"inventory_fact_1997\" as "
            + "\"inventory_fact_1997\", \"product\" as \"product\", \"product_class\" "
            + "as \"product_class\" where \"inventory_fact_1997\".\"warehouse_id\" = "
            + "\"warehouse\".\"warehouse_id\" and \"product\".\"product_class_id\" = "
            + "\"product_class\".\"product_class_id\" and "
            + "\"inventory_fact_1997\".\"product_id\" = \"product\".\"product_id\" and "
            + "(\"product_class\".\"product_family\" = 'Food') and ((not "
            + "(\"warehouse\".\"warehouse_name\" = 'Freeman And Co') or "
            + "(\"warehouse\".\"warehouse_name\" is null)) or (not "
            + "(\"warehouse\".\"wa_address1\" = '234 West Covina Pkwy') or "
            + "(\"warehouse\".\"wa_address1\" is null)) or not "
            + "(\"warehouse\".\"warehouse_fax\" is null)) group by "
            + "\"warehouse\".\"warehouse_fax\", \"warehouse\".\"wa_address1\", "
            + "\"warehouse\".\"warehouse_name\", \"product_class\".\"product_family\" "
            + "order by \"warehouse\".\"warehouse_fax\" ASC, "
            + "\"warehouse\".\"wa_address1\" ASC, \"warehouse\".\"warehouse_name\" ASC, "
            + "\"product_class\".\"product_family\" ASC";

        String necjSqlMySql =
            "select `warehouse`.`warehouse_fax` as `c0`, "
            + "`warehouse`.`wa_address1` as `c1`, `warehouse`.`warehouse_name` "
            + "as `c2`, `product_class`.`product_family` as `c3` from "
            + "`warehouse` as `warehouse`, `inventory_fact_1997` as "
            + "`inventory_fact_1997`, `product` as `product`, `product_class` "
            + "as `product_class` where `inventory_fact_1997`.`warehouse_id` = "
            + "`warehouse`.`warehouse_id` and `product`.`product_class_id` = "
            + "`product_class`.`product_class_id` and "
            + "`inventory_fact_1997`.`product_id` = `product`.`product_id` and "
            + "(`product_class`.`product_family` = 'Food') and "
            + "((not (`warehouse`.`warehouse_name` = 'Freeman And Co') or "
            + "(`warehouse`.`warehouse_name` is null)) or (not "
            + "(`warehouse`.`wa_address1` = '234 West Covina Pkwy') or "
            + "(`warehouse`.`wa_address1` is null)) or not "
            + "(`warehouse`.`warehouse_fax` is null)) group by "
            + "`warehouse`.`warehouse_fax`, `warehouse`.`wa_address1`, "
            + "`warehouse`.`warehouse_name`, `product_class`.`product_family` "
            + "order by ISNULL(`warehouse`.`warehouse_fax`), "
            + "`warehouse`.`warehouse_fax` ASC, "
            + "ISNULL(`warehouse`.`wa_address1`), `warehouse`.`wa_address1` ASC, "
            + "ISNULL(`warehouse`.`warehouse_name`), "
            + "`warehouse`.`warehouse_name` ASC, "
            + "ISNULL(`product_class`.`product_family`), "
            + "`product_class`.`product_family` ASC";

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.DERBY, necjSqlDerby, necjSqlDerby),
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL, necjSqlMySql, necjSqlMySql)
        };

        TestContext testContext =
            TestContext.instance().create(
                dimension,
                cube,
                null,
                null,
                null,
                null);

        assertQuerySql(testContext, query, patterns);
    }

    public void testCachedNativeSetUsingFilters() throws Exception {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, false);
        propSaver.set(MondrianProperties.instance().EnableNativeFilter, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;

        String query1 =
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Customers],[*BASE_MEMBERS_Product])' "
            + "Set [*BASE_MEMBERS_Customers] as 'Filter([Customers].[City].Members,Ancestor([Customers].CurrentMember, [Customers].[State Province]) In {[Customers].[All Customers].[USA].[CA]})' "
            + "Set [*BASE_MEMBERS_Product] as 'Filter([Product].[Product Family].Members,[Product].CurrentMember In {[Product].[All Products].[Drink]})' "
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Customers].currentMember,[Product].currentMember)})' "
            + "Select "
            + "{[Measures].[Customer Count]} on columns, "
            + "Non Empty [*CJ_ROW_AXIS] on rows "
            + "From [Sales]";

        checkNative(100, 45, query1, null, requestFreshConnection);

        // query2 has different filters; it should not reuse the result from
        // query1.
        String query2 =
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Customers],[*BASE_MEMBERS_Product])' "
            + "Set [*BASE_MEMBERS_Customers] as 'Filter([Customers].[City].Members,Ancestor([Customers].CurrentMember, [Customers].[State Province]) In {[Customers].[All Customers].[USA].[OR]})' "
            + "Set [*BASE_MEMBERS_Product] as 'Filter([Product].[Product Family].Members,[Product].CurrentMember In {[Product].[All Products].[Drink]})' "
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Customers].currentMember,[Product].currentMember)})' "
            + "Select "
            + "{[Measures].[Customer Count]} on columns, "
            + "Non Empty [*CJ_ROW_AXIS] on rows "
            + "From [Sales]";

        checkNative(100, 11, query2, null, requestFreshConnection);
    }

    public void testNativeFilter() {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, false);
        propSaver.set(MondrianProperties.instance().EnableNativeFilter, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;
        checkNative(
            32,
            18,
            "select {[Measures].[Store Sales]} ON COLUMNS, "
            + "Order(Filter(Descendants([Customers].[All Customers].[USA].[CA], [Customers].[Name]), ([Measures].[Store Sales] > 200.0)), [Measures].[Store Sales], DESC) ON ROWS "
            + "from [Sales] "
            + "where ([Time].[1997])",
            null,
            requestFreshConnection);
    }

    /**
     * Executes a Filter() whose condition contains a calculated member.
     */
    public void testCmNativeFilter() {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, false);
        propSaver.set(MondrianProperties.instance().EnableNativeFilter, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;
        checkNative(
            0,
            8,
            "with member [Measures].[Rendite] as '([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Cost]' "
            + "select NON EMPTY {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Rendite], [Measures].[Store Sales]} ON COLUMNS, "
            + "NON EMPTY Order(Filter([Product].[Product Name].Members, ([Measures].[Rendite] > 1.8)), [Measures].[Rendite], BDESC) ON ROWS "
            + "from [Sales] "
            + "where ([Store].[All Stores].[USA].[CA], [Time].[1997])",
            "Axis #0:\n"
            + "{[Store].[USA].[CA], [Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Cost]}\n"
            + "{[Measures].[Rendite]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[Plato].[Plato Extra Chunky Peanut Butter]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Popcorn].[Horatio].[Horatio Buttered Popcorn]}\n"
            + "{[Product].[Food].[Canned Foods].[Canned Tuna].[Tuna].[Better].[Better Canned Tuna in Oil]}\n"
            + "{[Product].[Food].[Produce].[Fruit].[Fresh Fruit].[High Top].[High Top Cantelope]}\n"
            + "{[Product].[Non-Consumable].[Household].[Electrical].[Lightbulbs].[Denny].[Denny 75 Watt Lightbulb]}\n"
            + "{[Product].[Food].[Breakfast Foods].[Breakfast Foods].[Cereal].[Johnson].[Johnson Oatmeal]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Portsmouth].[Portsmouth Light Wine]}\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony].[Ebony Squash]}\n"
            + "Row #0: 42\n"
            + "Row #0: 24.06\n"
            + "Row #0: 1.93\n"
            + "Row #0: 70.56\n"
            + "Row #1: 36\n"
            + "Row #1: 29.02\n"
            + "Row #1: 1.91\n"
            + "Row #1: 84.60\n"
            + "Row #2: 39\n"
            + "Row #2: 20.55\n"
            + "Row #2: 1.85\n"
            + "Row #2: 58.50\n"
            + "Row #3: 25\n"
            + "Row #3: 21.76\n"
            + "Row #3: 1.84\n"
            + "Row #3: 61.75\n"
            + "Row #4: 43\n"
            + "Row #4: 59.62\n"
            + "Row #4: 1.83\n"
            + "Row #4: 168.99\n"
            + "Row #5: 34\n"
            + "Row #5: 7.20\n"
            + "Row #5: 1.83\n"
            + "Row #5: 20.40\n"
            + "Row #6: 36\n"
            + "Row #6: 33.10\n"
            + "Row #6: 1.83\n"
            + "Row #6: 93.60\n"
            + "Row #7: 46\n"
            + "Row #7: 28.34\n"
            + "Row #7: 1.81\n"
            + "Row #7: 79.58\n",
            requestFreshConnection);
    }

    public void testNonNativeFilterWithNullMeasure() {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, false);
        propSaver.set(MondrianProperties.instance().EnableNativeFilter, false);
        checkNotNative(
            9,
            "select Filter([Store].[Store Name].members, "
            + "              Not ([Measures].[Store Sqft] - [Measures].[Grocery Sqft] < 10000)) on rows, "
            + "{[Measures].[Store Sqft], [Measures].[Grocery Sqft]} on columns "
            + "from [Store]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sqft]}\n"
            + "{[Measures].[Grocery Sqft]}\n"
            + "Axis #2:\n"
            + "{[Store].[Mexico].[DF].[Mexico City].[Store 9]}\n"
            + "{[Store].[Mexico].[DF].[San Andres].[Store 21]}\n"
            + "{[Store].[Mexico].[Yucatan].[Merida].[Store 8]}\n"
            + "{[Store].[USA].[CA].[Alameda].[HQ]}\n"
            + "{[Store].[USA].[CA].[San Diego].[Store 24]}\n"
            + "{[Store].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[USA].[WA].[Tacoma].[Store 17]}\n"
            + "{[Store].[USA].[WA].[Walla Walla].[Store 22]}\n"
            + "{[Store].[USA].[WA].[Yakima].[Store 23]}\n"
            + "Row #0: 36,509\n"
            + "Row #0: 22,450\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #2: 30,797\n"
            + "Row #2: 20,141\n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #4: \n"
            + "Row #4: \n"
            + "Row #5: 39,696\n"
            + "Row #5: 24,390\n"
            + "Row #6: 33,858\n"
            + "Row #6: 22,123\n"
            + "Row #7: \n"
            + "Row #7: \n"
            + "Row #8: \n"
            + "Row #8: \n");
    }

    public void testNativeFilterWithNullMeasure() {
        // Currently this behaves differently from the non-native evaluation.
        propSaver.set(MondrianProperties.instance().EnableNativeFilter, true);
        propSaver.set(MondrianProperties.instance().ExpandNonNative, false);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        final TestContext context = getTestContext().withFreshConnection();
        try {
            context.assertQueryReturns(
                "select Filter([Store].[Store Name].members, "
                + "              Not ([Measures].[Store Sqft] - [Measures].[Grocery Sqft] < 10000)) on rows, "
                + "{[Measures].[Store Sqft], [Measures].[Grocery Sqft]} on columns "
                + "from [Store]", "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Measures].[Store Sqft]}\n"
                + "{[Measures].[Grocery Sqft]}\n"
                + "Axis #2:\n"
                + "{[Store].[Mexico].[DF].[Mexico City].[Store 9]}\n"
                + "{[Store].[Mexico].[Yucatan].[Merida].[Store 8]}\n"
                + "{[Store].[USA].[WA].[Bremerton].[Store 3]}\n"
                + "{[Store].[USA].[WA].[Tacoma].[Store 17]}\n"
                + "Row #0: 36,509\n"
                + "Row #0: 22,450\n"
                + "Row #1: 30,797\n"
                + "Row #1: 20,141\n"
                + "Row #2: 39,696\n"
                + "Row #2: 24,390\n"
                + "Row #3: 33,858\n"
                + "Row #3: 22,123\n");
        } finally {
            context.close();
        }
    }

    public void testNonNativeFilterWithCalcMember() {
        // Currently this query cannot run natively
        propSaver.set(MondrianProperties.instance().EnableNativeFilter, false);
        propSaver.set(MondrianProperties.instance().ExpandNonNative, false);
        checkNotNative(
            3,
            "with\n"
            + "member [Time].[Time].[Date Range] as 'Aggregate({[Time].[1997].[Q1]:[Time].[1997].[Q4]})'\n"
            + "select\n"
            + "{[Measures].[Unit Sales]} ON columns,\n"
            + "Filter ([Store].[Store State].members, [Measures].[Store Cost] > 100) ON rows\n"
            + "from [Sales]\n"
            + "where [Time].[Date Range]\n",
            "Axis #0:\n"
            + "{[Time].[Date Range]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[USA].[WA]}\n"
            + "Row #0: 74,748\n"
            + "Row #1: 67,659\n"
            + "Row #2: 124,366\n");
    }

    /**
     * Verify that filter with Not IsEmpty(storedMeasure) can be natively
     * evaluated.
     */
    public void testNativeFilterNonEmpty() {
        propSaver.set(MondrianProperties.instance().ExpandNonNative, false);
        propSaver.set(MondrianProperties.instance().EnableNativeFilter, true);

        // Get a fresh connection; Otherwise the mondrian property setting
        // is not refreshed for this parameter.
        boolean requestFreshConnection = true;
        checkNative(
            0,
            20,
            "select Filter(CrossJoin([Store].[Store Name].members, "
            + "                        "
            + TestContext.hierarchyName("Store Type", "Store Type")
            + ".[Store Type].members), "
            + "                        Not IsEmpty([Measures].[Store Sqft])) on rows, "
            + "{[Measures].[Store Sqft]} on columns "
            + "from [Store]",
            null,
            requestFreshConnection);
    }

    /**
     * Testcase for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-706">bug MONDRIAN-706,
     * "SQL using hierarchy attribute 'Column Name' instead of 'Column' in the
     * filter"</a>.
     */
    public void testBugMondrian706() {
        propSaver.set(
            MondrianProperties.instance().UseAggregates,
            false);
        propSaver.set(
            MondrianProperties.instance().ReadAggregates,
            false);
        propSaver.set(
            MondrianProperties.instance().DisableCaching,
            false);
        propSaver.set(
            MondrianProperties.instance().EnableNativeNonEmpty,
            true);
        propSaver.set(
            MondrianProperties.instance().CompareSiblingsByOrderKey,
            true);
        propSaver.set(
            MondrianProperties.instance().NullDenominatorProducesNull,
            true);
        propSaver.set(
            MondrianProperties.instance().ExpandNonNative,
            true);
        propSaver.set(
            MondrianProperties.instance().EnableNativeFilter,
            true);
        // With bug MONDRIAN-706, would generate
        //
        // ((`store`.`store_name`, `store`.`store_city`, `store`.`store_state`)
        //   in (('11', 'Portland', 'OR'), ('14', 'San Francisco', 'CA'))
        //
        // Notice that the '11' and '14' store ID is applied on the store_name
        // instead of the store_id. So it would return no rows.
        final String badMysqlSQL =
            "select `store`.`store_country` as `c0`, `store`.`store_state` as `c1`, `store`.`store_city` as `c2`, `store`.`store_id` as `c3`, `store`.`store_name` as `c4`, `store`.`store_type` as `c5`, `store`.`store_manager` as `c6`, `store`.`store_sqft` as `c7`, `store`.`grocery_sqft` as `c8`, `store`.`frozen_sqft` as `c9`, `store`.`meat_sqft` as `c10`, `store`.`coffee_bar` as `c11`, `store`.`store_street_address` as `c12` from `FOODMART`.`store` as `store` where (`store`.`store_state` in ('CA', 'OR')) and ((`store`.`store_name`,`store`.`store_city`,`store`.`store_state`) in (('11','Portland','OR'),('14','San Francisco','CA'))) group by `store`.`store_country`, `store`.`store_state`, `store`.`store_city`, `store`.`store_id`, `store`.`store_name`, `store`.`store_type`, `store`.`store_manager`, `store`.`store_sqft`, `store`.`grocery_sqft`, `store`.`frozen_sqft`, `store`.`meat_sqft`, `store`.`coffee_bar`, `store`.`store_street_address` having NOT((sum(`store`.`store_sqft`) is null)) "
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? "order by ISNULL(`c0`) ASC, `c0` ASC, "
                + "ISNULL(`c1`) ASC, `c1` ASC, "
                + "ISNULL(`c2`) ASC, `c2` ASC, "
                + "ISNULL(`c3`) ASC, `c3` ASC"
                : "order by ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC, "
                + "ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC, "
                + "ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC, "
                + "ISNULL(`store`.`store_id`) ASC, `store`.`store_id` ASC");
        final String goodMysqlSQL =
            "select `store`.`store_country` as `c0`, `store`.`store_state` as `c1`, `store`.`store_city` as `c2`, `store`.`store_id` as `c3`, `store`.`store_name` as `c4` from `store` as `store` where (`store`.`store_state` in ('CA', 'OR')) and ((`store`.`store_id`, `store`.`store_city`, `store`.`store_state`) in ((11, 'Portland', 'OR'), (14, 'San Francisco', 'CA'))) group by `store`.`store_country`, `store`.`store_state`, `store`.`store_city`, `store`.`store_id`, `store`.`store_name` having NOT((sum(`store`.`store_sqft`) is null)) "
                + (TestContext.instance().getDialect().requiresOrderByAlias()
                ? " order by ISNULL(`c0`) ASC, `c0` ASC, "
                + "ISNULL(`c1`) ASC, `c1` ASC, "
                + "ISNULL(`c2`) ASC, `c2` ASC, "
                + "ISNULL(`c3`) ASC, `c3` ASC"
                : " order by ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC, "
                + "ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC, "
                + "ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC, "
                + "ISNULL(`store`.`store_id`) ASC, `store`.`store_id` ASC");
        final String mdx =
            "With\n"
            + "Set [*NATIVE_CJ_SET] as 'Filter([*BASE_MEMBERS_Store], Not IsEmpty ([Measures].[Store Sqft]))'\n"
            + "Set [*SORTED_ROW_AXIS] as 'Order([*CJ_ROW_AXIS],Ancestor([Store].CurrentMember, [Store].[Store Country]).OrderKey,BASC,Ancestor([Store].CurrentMember, [Store].[Store State]).OrderKey,BASC,Ancestor([Store].CurrentMember,\n"
            + "[Store].[Store City]).OrderKey,BASC,[Store].CurrentMember.OrderKey,BASC)'\n"
            + "Set [*NATIVE_MEMBERS_Store] as 'Generate([*NATIVE_CJ_SET], {[Store].CurrentMember})'\n"
            + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
            + "Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Store].currentMember)})'\n"
            + "Set [*BASE_MEMBERS_Store] as 'Filter([Store].[Store Name].Members,(Ancestor([Store].CurrentMember, [Store].[Store State]) In {[Store].[All Stores].[USA].[CA],[Store].[All Stores].[USA].[OR]}) AND ([Store].CurrentMember In\n"
            + "{[Store].[All Stores].[USA].[OR].[Portland].[Store 11],[Store].[All Stores].[USA].[CA].[San Francisco].[Store 14]}))'\n"
            + "Set [*CJ_COL_AXIS] as '[*NATIVE_CJ_SET]'\n"
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Store Sqft]', FORMAT_STRING = '#,###', SOLVE_ORDER=400\n"
            + "Select\n"
            + "[*BASE_MEMBERS_Measures] on columns,\n"
            + "[*SORTED_ROW_AXIS] on rows\n"
            + "From [Store] \n";
        final SqlPattern[] badPatterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                badMysqlSQL,
                null)
        };
        final SqlPattern[] goodPatterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                goodMysqlSQL,
                null)
        };
        final TestContext testContext =
            TestContext.instance().createSubstitutingCube(
                "Store",
                "<Dimension name='Store Type'>\n"
                + "    <Hierarchy name='Store Types Hierarchy' allMemberName='All Store Types Member Name' hasAll='true'>\n"
                + "      <Level name='Store Type' column='store_type' uniqueMembers='true'/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Dimension name='Store'>\n"
                + "    <Hierarchy hasAll='true' primaryKey='store_id'>\n"
                + "      <Table name='store'/>\n"
                + "      <Level name='Store Country' column='store_country' uniqueMembers='true'/>\n"
                + "      <Level name='Store State' column='store_state' uniqueMembers='true'/>\n"
                + "      <Level name='Store City' column='store_city' uniqueMembers='false'/>\n"
                + "      <Level name='Store Name' column='store_id' type='Numeric' nameColumn='store_name' uniqueMembers='false'/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n");
        assertQuerySqlOrNot(testContext, mdx, badPatterns, true, true, true);
        assertQuerySqlOrNot(testContext, mdx, goodPatterns, false, true, true);
        testContext.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA].[San Francisco].[Store 14]}\n"
            + "{[Store].[USA].[OR].[Portland].[Store 11]}\n"
            + "Row #0: 22,478\n"
            + "Row #1: 20,319\n");
    }

    /**
     * Tests the bug MONDRIAN-779. The {@link MemberListCrossJoinArg}
     * was not considering the 'exclude' attribute in its hash code.
     * This resulted in two filters being chained within two different
     * named sets to register a cache element with the same key, even
     * though they were the different because of the added "NOT" keyword.
     */
    public void testBug779() {
        final String query1 =
            "With Set [*NATIVE_CJ_SET] as 'Filter([*BASE_MEMBERS_Product], Not IsEmpty ([Measures].[Unit Sales]))' Set [*BASE_MEMBERS_Product] as 'Filter([Product].[Product Department].Members,(Ancestor([Product].CurrentMember, [Product].[Product Family]) In {[Product].[Drink],[Product].[Food]}) AND ([Product].CurrentMember In {[Product].[Drink].[Dairy]}))' Select [Measures].[Unit Sales] on columns, [*NATIVE_CJ_SET] on rows From [Sales]";
        final String query2 =
            "With Set [*NATIVE_CJ_SET] as 'Filter([*BASE_MEMBERS_Product], Not IsEmpty ([Measures].[Unit Sales]))' Set [*BASE_MEMBERS_Product] as 'Filter([Product].[Product Department].Members,(Ancestor([Product].CurrentMember, [Product].[Product Family]) In {[Product].[Drink],[Product].[Food]}) AND ([Product].CurrentMember Not In {[Product].[Drink].[Dairy]}))' Select [Measures].[Unit Sales] on columns, [*NATIVE_CJ_SET] on rows From [Sales]";

        final String expectedResult1 =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink].[Dairy]}\n"
            + "Row #0: 4,186\n";

        final String expectedResult2 =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink].[Alcoholic Beverages]}\n"
            + "{[Product].[Drink].[Beverages]}\n"
            + "{[Product].[Food].[Baked Goods]}\n"
            + "{[Product].[Food].[Baking Goods]}\n"
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
            + "{[Product].[Food].[Snack Foods]}\n"
            + "{[Product].[Food].[Snacks]}\n"
            + "{[Product].[Food].[Starchy Foods]}\n"
            + "Row #0: 6,838\n"
            + "Row #1: 13,573\n"
            + "Row #2: 7,870\n"
            + "Row #3: 20,245\n"
            + "Row #4: 3,317\n"
            + "Row #5: 19,026\n"
            + "Row #6: 1,812\n"
            + "Row #7: 12,885\n"
            + "Row #8: 12,037\n"
            + "Row #9: 4,132\n"
            + "Row #10: 26,655\n"
            + "Row #11: 1,714\n"
            + "Row #12: 37,792\n"
            + "Row #13: 1,764\n"
            + "Row #14: 30,545\n"
            + "Row #15: 6,884\n"
            + "Row #16: 5,262\n";

        assertQueryReturns(query1, expectedResult1);
        assertQueryReturns(query2, expectedResult2);
    }

    /**
     * http://jira.pentaho.com/browse/MONDRIAN-1458
     * When using a multi value IN clause which includes null values
     * against a collapsed field on an aggregate table, the dimension table
     * field was referenced as the column expression, causing sql errors.
     */
    public void testMultiValueInWithNullVals() {
        // MONDRIAN-1458 - Native exclusion predicate doesn't use agg table
        // when checking for nulls
        TestContext context = getTestContext();
        if (!propSaver.properties.EnableNativeCrossJoin.get()
            || !propSaver.properties.ReadAggregates.get()
            || !propSaver.properties.UseAggregates.get())
        {
            return;
        }

        String sql;
        if (!context.getDialect().supportsMultiValueInExpr()) {
            sql = "select `agg_g_ms_pcat_sales_fact_1997`.`product_family` "
                + "as `c0`,"
                + " `agg_g_ms_pcat_sales_fact_1997`.`product_department` as "
                + "`c1`,"
                + " `agg_g_ms_pcat_sales_fact_1997`.`gender` as `c2` "
                + "from `agg_g_ms_pcat_sales_fact_1997` as "
                + "`agg_g_ms_pcat_sales_fact_1997` "
                + "where (not ((`agg_g_ms_pcat_sales_fact_1997`."
                + "`product_family` = 'Food'"
                + " and `agg_g_ms_pcat_sales_fact_1997`."
                + "`product_department` = 'Baked Goods') "
                + "or (`agg_g_ms_pcat_sales_fact_1997`.`product_family` "
                + "= 'Drink' "
                + "and `agg_g_ms_pcat_sales_fact_1997`."
                + "`product_department` = 'Dairy')) "
                + "or ((`agg_g_ms_pcat_sales_fact_1997`."
                + "`product_department` is null "
                + "or `agg_g_ms_pcat_sales_fact_1997`."
                + "`product_family` is null) "
                + "and not((`agg_g_ms_pcat_sales_fact_1997`.`product_family`"
                + " = 'Food' "
                + "and `agg_g_ms_pcat_sales_fact_1997`.`product_department` "
                + "= 'Baked Goods') "
                + "or (`agg_g_ms_pcat_sales_fact_1997`.`product_family` = "
                + "'Drink' "
                + "and `agg_g_ms_pcat_sales_fact_1997`.`product_department` "
                + "= 'Dairy')))) "
                + "group by `agg_g_ms_pcat_sales_fact_1997`.`product_family`,"
                + " `agg_g_ms_pcat_sales_fact_1997`.`product_department`,"
                + " `agg_g_ms_pcat_sales_fact_1997`.`gender` "
                + "order by ISNULL(`agg_g_ms_pcat_sales_fact_1997`."
                + "`product_family`) ASC,"
                + " `agg_g_ms_pcat_sales_fact_1997`.`product_family` ASC,"
                + " ISNULL(`agg_g_ms_pcat_sales_fact_1997`."
                + "`product_department`) ASC,"
                + " `agg_g_ms_pcat_sales_fact_1997`.`product_department` ASC,"
                + " ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`gender`) ASC,"
                + " `agg_g_ms_pcat_sales_fact_1997`.`gender` ASC";
        } else {
                sql = "select `agg_g_ms_pcat_sales_fact_1997`."
                + "`product_family` as `c0`,"
                + " `agg_g_ms_pcat_sales_fact_1997`.`product_department` as `c1`,"
                + " `agg_g_ms_pcat_sales_fact_1997`.`gender` as `c2` "
                + "from `agg_g_ms_pcat_sales_fact_1997` as "
                + "`agg_g_ms_pcat_sales_fact_1997` "
                + "where (not ((`agg_g_ms_pcat_sales_fact_1997`.`product_department`,"
                + " `agg_g_ms_pcat_sales_fact_1997`.`product_family`) in "
                + "(('Baked Goods',"
                + " 'Food'),"
                + " ('Dairy',"
                + " 'Drink'))) or (`agg_g_ms_pcat_sales_fact_1997`."
                + "`product_department` "
                + "is null or `agg_g_ms_pcat_sales_fact_1997`.`product_family` "
                + "is null)) "
                + "group by `agg_g_ms_pcat_sales_fact_1997`.`product_family`,"
                + " `agg_g_ms_pcat_sales_fact_1997`.`product_department`,"
                + " `agg_g_ms_pcat_sales_fact_1997`.`gender` order by "
                + "ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`product_family`) ASC,"
                + " `agg_g_ms_pcat_sales_fact_1997`.`product_family` ASC,"
                + " ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`product_department`) ASC,"
                + " `agg_g_ms_pcat_sales_fact_1997`.`product_department` ASC,"
                + " ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`gender`) ASC,"
                + " `agg_g_ms_pcat_sales_fact_1997`.`gender` ASC";
        }
        String mdx =  "select NonEmptyCrossjoin( \n"+
            "   filter ( product.[product department].members,\n"+
            "      NOT ([Product].CurrentMember IN  \n"+
            "  { [Product].[Food].[Baked Goods], Product.Drink.Dairy})),\n"+
            "   gender.gender.members\n"+
            ")\n"+
            "on 0 from sales\n";
        assertQuerySql(
            mdx,
            new SqlPattern[] {
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, sql, null)
            });
    }

    public void testNativeTopCountFilter() {
        TestContext context = getTestContext().withFreshConnection();
          final boolean useAgg =
              MondrianProperties.instance().UseAggregates.get()
              && MondrianProperties.instance().ReadAggregates.get();
          // should nativize with separate measures, but not when they are both referenced.
          // peanut butter! 3 items in sales, 2 in warehouse
          String mdx =
              "SELECT TopCount(Filter(NonEmpty([Product].[All Products].[Food].[Baking Goods].[Baking Goods].Children,{[Measures].[Unit Sales]}), [Measures].[Sales Count] > 300), 2, [Measures].[Sales Count]) "
              + " on 0, {[Measures].[Unit Sales], [Measures].[Sales Count]} on 1 from [Sales]";

          // verify SQL
          String sql =
              "select `product_class`.`product_family` as `c0`, "
              + "`product_class`.`product_department` as `c1`, "
              + "`product_class`.`product_category` as `c2`, "
              + "`product_class`.`product_subcategory` as `c3`, "
              + "count(`sales_fact_1997`.`product_id`) as `c4` "
              + "from `product_class` as `product_class`, "
              + "`product` as `product`, `sales_fact_1997` as `sales_fact_1997`, "
              + "`time_by_day` as `time_by_day` where "
              + "`sales_fact_1997`.`product_id` = `product`.`product_id` "
              + "and `product`.`product_class_id` = `product_class`.`product_class_id` "
              + "and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` "
              + "and `time_by_day`.`the_year` = 1997 "
              + "and (`product_class`.`product_category` = 'Baking Goods' "
              + "and `product_class`.`product_department` = 'Baking Goods' "
              + "and `product_class`.`product_family` = 'Food') "
              + "and `sales_fact_1997`.`unit_sales` is not null "
              + "group by `product_class`.`product_family`, "
              + "`product_class`.`product_department`, "
              + "`product_class`.`product_category`, "
              + "`product_class`.`product_subcategory` "
              + "having (count(`sales_fact_1997`.`product_id`) > 300) "
              + "order by count(`sales_fact_1997`.`product_id`) DESC, "
              + "ISNULL(`product_class`.`product_family`) ASC, "
              + "`product_class`.`product_family` ASC, "
              + "ISNULL(`product_class`.`product_department`) ASC, "
              + "`product_class`.`product_department` ASC, "
              + "ISNULL(`product_class`.`product_category`) ASC, "
              + "`product_class`.`product_category` ASC, "
              + "ISNULL(`product_class`.`product_subcategory`) ASC, "
              + "`product_class`.`product_subcategory` ASC limit 2";

          if (!useAgg && propSaver.properties.EnableNativeFilter.get()) {
              assertQuerySql(
                  context,
                  mdx,
                  new SqlPattern[] {
                      new SqlPattern(Dialect.DatabaseProduct.MYSQL, sql, null)
                  });
          }

          context.assertQueryReturns(
              mdx,
              "Axis #0:\n"
              + "{}\n"
              + "Axis #1:\n"
              + "{[Product].[Food].[Baking Goods].[Baking Goods].[Cooking Oil]}\n"
              + "{[Product].[Food].[Baking Goods].[Baking Goods].[Spices]}\n"
              + "Axis #2:\n"
              + "{[Measures].[Unit Sales]}\n"
              + "{[Measures].[Sales Count]}\n"
              + "Row #0: 3,277\n"
              + "Row #0: 2,574\n"
              + "Row #1: 1,067\n"
              + "Row #1: 827\n");
    }

    /**
     * This tests Filter(,Count(,EXCLUDEEMPTY)) scenarios in a virtual cube context.
     */
    public void testVirtualCubeNativeCountFilter() {
      TestContext context = getTestContext().withFreshConnection();
        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();
        // should nativize with separate measures, but not when they are both referenced.
        // peanut butter! 3 items in sales, 2 in warehouse
        String mdx =
            "SELECT Filter("
            + "   [Product].[All Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[CDR].Children, "
            + "   Count(Crossjoin({[Product].CurrentMember}, {[Measures].[Units Ordered]}), ExcludeEmpty)) on 0, {[Measures].[Unit Sales], [Measures].[Units Ordered]} on 1 from [Warehouse and Sales]";
        // verify SQL
        String sql =
            "select `product_class`.`product_family` as `c0`, "
            + "`product_class`.`product_department` as `c1`, "
            + "`product_class`.`product_category` as `c2`, "
            + "`product_class`.`product_subcategory` as `c3`, "
            + "`product`.`brand_name` as `c4`, `product`.`product_name` as `c5` "
            + "from `product_class` as `product_class`, `product` as `product`, "
            + "`inventory_fact_1997` as `inventory_fact_1997`, "
            + "`time_by_day` as `time_by_day` where "
            + "`inventory_fact_1997`.`product_id` = `product`.`product_id` and "
            + "`product`.`product_class_id` = `product_class`.`product_class_id` and "
            + "`inventory_fact_1997`.`time_id` = `time_by_day`.`time_id` and "
            + "`time_by_day`.`the_year` = 1997 and (`product`.`brand_name` = 'CDR' "
            + "and `product_class`.`product_subcategory` = 'Peanut Butter' and "
            + "`product_class`.`product_category` = 'Jams and Jellies' and "
            + "`product_class`.`product_department` = 'Baking Goods' and "
            + "`product_class`.`product_family` = 'Food') group by "
            + "`product_class`.`product_family`, `product_class`.`product_department`, "
            + "`product_class`.`product_category`, `product_class`.`product_subcategory`, "
            + "`product`.`brand_name`, `product`.`product_name` order by "
            + "ISNULL(`product_class`.`product_family`) ASC, "
            + "`product_class`.`product_family` ASC, "
            + "ISNULL(`product_class`.`product_department`) ASC, "
            + "`product_class`.`product_department` ASC, "
            + "ISNULL(`product_class`.`product_category`) ASC, "
            + "`product_class`.`product_category` ASC, "
            + "ISNULL(`product_class`.`product_subcategory`) ASC, "
            + "`product_class`.`product_subcategory` ASC, "
            + "ISNULL(`product`.`brand_name`) ASC, `product`.`brand_name` ASC, "
            + "ISNULL(`product`.`product_name`) ASC, `product`.`product_name` ASC";
        if (!useAgg && propSaver.properties.EnableNativeFilter.get()) {
            assertQuerySql(
                context,
                mdx,
                new SqlPattern[] {
                    new SqlPattern(Dialect.DatabaseProduct.MYSQL, sql, null)
                });
        }

        context.assertQueryReturns(
            mdx, 
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[CDR].[CDR Chunky Peanut Butter]}\n"
            + "{[Product].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[CDR].[CDR Creamy Peanut Butter]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Units Ordered]}\n"
            + "Row #0: 182\n"
            + "Row #0: 178\n"
            + "Row #1: 222.0\n"
            + "Row #1: 164.0\n");

        mdx = 
            "SELECT Filter("
            + "   [Product].[All Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[CDR].Children, "
            + "   Count(Crossjoin({[Product].CurrentMember}, {[Measures].[Unit Sales]}), ExcludeEmpty)) on 0, {[Measures].[Unit Sales], [Measures].[Units Ordered]} on 1 from [Warehouse and Sales]";

        // verify SQL
        sql = 
            "select `product_class`.`product_family` as `c0`, "
            + "`product_class`.`product_department` as `c1`, "
            + "`product_class`.`product_category` as `c2`, "
            + "`product_class`.`product_subcategory` as `c3`, "
            + "`product`.`brand_name` as `c4`, `product`.`product_name` as `c5` "
            + "from `product_class` as `product_class`, `product` as `product`, "
            + "`sales_fact_1997` as `sales_fact_1997`, `time_by_day` as `time_by_day` "
            + "where `sales_fact_1997`.`product_id` = `product`.`product_id` and "
            + "`product`.`product_class_id` = `product_class`.`product_class_id` and "
            + "`sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and "
            + "`time_by_day`.`the_year` = 1997 and (`product`.`brand_name` = 'CDR' and "
            + "`product_class`.`product_subcategory` = 'Peanut Butter' and "
            + "`product_class`.`product_category` = 'Jams and Jellies' and "
            + "`product_class`.`product_department` = 'Baking Goods' and "
            + "`product_class`.`product_family` = 'Food') group by "
            + "`product_class`.`product_family`, `product_class`.`product_department`, "
            + "`product_class`.`product_category`, `product_class`.`product_subcategory`, "
            + "`product`.`brand_name`, `product`.`product_name` order by "
            + "ISNULL(`product_class`.`product_family`) ASC, "
            + "`product_class`.`product_family` ASC, "
            + "ISNULL(`product_class`.`product_department`) ASC, "
            + "`product_class`.`product_department` ASC, "
            + "ISNULL(`product_class`.`product_category`) ASC, "
            + "`product_class`.`product_category` ASC, "
            + "ISNULL(`product_class`.`product_subcategory`) ASC, "
            + "`product_class`.`product_subcategory` ASC, "
            + "ISNULL(`product`.`brand_name`) ASC, `product`.`brand_name` ASC, "
            + "ISNULL(`product`.`product_name`) ASC, `product`.`product_name` ASC";
        if (!useAgg && propSaver.properties.EnableNativeFilter.get()) {
            assertQuerySql(
                context,
                mdx,
                new SqlPattern[] {
                    new SqlPattern(Dialect.DatabaseProduct.MYSQL, sql, null)
                });
        }

        context.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[CDR].[CDR Chunky Peanut Butter]}\n"
            + "{[Product].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[CDR].[CDR Creamy Peanut Butter]}\n"
            + "{[Product].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[CDR].[CDR Extra Chunky Peanut Butter]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Units Ordered]}\n"
            + "Row #0: 182\n"
            + "Row #0: 178\n"
            + "Row #0: 185\n"
            + "Row #1: 222.0\n"
            + "Row #1: 164.0\n"
            + "Row #1: \n");

        mdx = 
            "SELECT Filter("
            + "   [Product].[All Products].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[CDR].Children, "
            + "   Count(Crossjoin({[Product].CurrentMember}, {[Measures].[Unit Sales], [Measures].[Units Ordered]}), ExcludeEmpty)) on 0, {[Measures].[Unit Sales], [Measures].[Units Ordered]} on 1 from [Warehouse and Sales]";

        // verify SQL
        sql =
            "select `product`.`product_name` as `c0` from `product` as `product`, "
            + "`product_class` as `product_class` where "
            + "`product`.`product_class_id` = `product_class`.`product_class_id` "
            + "and (`product`.`brand_name` = 'CDR' "
            + "and `product_class`.`product_subcategory` = 'Peanut Butter' "
            + "and `product_class`.`product_category` = 'Jams and Jellies' "
            + "and `product_class`.`product_department` = 'Baking Goods' "
            + "and `product_class`.`product_family` = 'Food') group by "
            + "`product`.`product_name` order by ISNULL(`product`.`product_name`) ASC, "
            + "`product`.`product_name` ASC";
        if (!useAgg && propSaver.properties.EnableNativeFilter.get()) {
            assertQuerySql(
                context,
                mdx,
                new SqlPattern[] {
                    new SqlPattern(Dialect.DatabaseProduct.MYSQL, sql, null)
                });
        }

        context.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[CDR].[CDR Chunky Peanut Butter]}\n"
            + "{[Product].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[CDR].[CDR Creamy Peanut Butter]}\n"
            + "{[Product].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter].[CDR].[CDR Extra Chunky Peanut Butter]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Units Ordered]}\n"
            + "Row #0: 182\n"
            + "Row #0: 178\n"
            + "Row #0: 185\n"
            + "Row #1: 222.0\n"
            + "Row #1: 164.0\n"
            + "Row #1: \n");
    }

    /**
     * This verifies that a native filter embedded in a subset generates the correct SQL
     */
    public void testNativeFilterInSubset() {
        final boolean useAgg =
            MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get();
        String mdx =
            "SELECT\n"
            + "Subset(Filter([Product].[Food].[Baking Goods].[Jams and Jellies].Children, [Measures].[Unit Sales] > 2564), 1, 2) ON 0,\n"
            + "{[Measures].[Unit Sales]} ON 1\n"
            + "FROM [Sales]";

        String sql =
            "select `product_class`.`product_family` as `c0`, "
            + "`product_class`.`product_department` as `c1`, "
            + "`product_class`.`product_category` as `c2`, "
            + "`product_class`.`product_subcategory` as `c3` "
            + "from `product_class` as `product_class`, "
            + "`product` as `product`, `sales_fact_1997` as `sales_fact_1997`, "
            + "`time_by_day` as `time_by_day` where "
            + "`sales_fact_1997`.`product_id` = `product`.`product_id` and "
            + "`product`.`product_class_id` = `product_class`.`product_class_id` and "
            + "`sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and "
            + "`time_by_day`.`the_year` = 1997 and "
            + "(`product_class`.`product_category` = 'Jams and Jellies' and "
            + "`product_class`.`product_department` = 'Baking Goods' and "
            + "`product_class`.`product_family` = 'Food') group by "
            + "`product_class`.`product_family`, `product_class`.`product_department`, "
            + "`product_class`.`product_category`, `product_class`.`product_subcategory` "
            + "having (sum(`sales_fact_1997`.`unit_sales`) > 2564) order by "
            + "ISNULL(`product_class`.`product_family`) ASC, "
            + "`product_class`.`product_family` ASC, "
            + "ISNULL(`product_class`.`product_department`) ASC, "
            + "`product_class`.`product_department` ASC, "
            + "ISNULL(`product_class`.`product_category`) ASC, "
            + "`product_class`.`product_category` ASC, "
            + "ISNULL(`product_class`.`product_subcategory`) ASC, "
            + "`product_class`.`product_subcategory` ASC limit 2 offset 1";

        if (!useAgg && propSaver.properties.EnableNativeFilter.get() 
            && propSaver.properties.EnableNativeSubset.get()) 
        {
            assertQuerySql(
                mdx,
                new SqlPattern[] {
                    new SqlPattern(Dialect.DatabaseProduct.MYSQL, sql, null)
                });
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Food].[Baking Goods].[Jams and Jellies].[Peanut Butter]}\n"
            + "{[Product].[Food].[Baking Goods].[Jams and Jellies].[Preserves]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 2,667\n"
            + "Row #0: 4,100\n");
    }

    /**
     * Test for Native Count Filter Scenario
     *
     * TODO: Test with Calculated Members that match scenario
     * TODO: Add support for more than just related All members
     */
    public void testNativeCountFilter() {  
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
            + "  </Dimension>"
            + "  <Dimension name=\"Store IDs\" foreignKey=\"store_id\">\n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Store IDs\" primaryKey=\"store_id\">\n"
            + "      <Table name=\"store\"/>\n"
            + "      <Level name=\"ID\" column=\"store_id\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>");

        // expected native filter with a join against the fact table
        String sql = 
            "select `sales_fact_1997`.`customer_id` as `c0` "
            + "from `sales_fact_1997` as `sales_fact_1997`, "
            + "`time_by_day` as `time_by_day`, `promotion` as `promotion`, "
            + "`customer` as `customer` "
            + "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` "
            + "and `time_by_day`.`the_year` = 1997 "
            + "and `sales_fact_1997`.`promotion_id` = `promotion`.`promotion_id` "
            + "and `promotion`.`promotion_name` = 'Big Promo' "
            + "and `sales_fact_1997`.`customer_id` = `customer`.`customer_id` "
            + "and `customer`.`gender` = 'F' "
            + "group by `sales_fact_1997`.`customer_id` "
            + "order by ISNULL(`sales_fact_1997`.`customer_id`) ASC, `sales_fact_1997`.`customer_id` ASC "
            + "limit 25 offset 0";
        String mdx = 
            "WITH\n"
            + "     SET [Customer IDs Fullset] AS 'Filter({AddCalculatedMembers([Customer IDs].[All Customer IDs].Children)}, (Count(CrossJoin({[Customer IDs].CurrentMember},CrossJoin({AddCalculatedMembers({[Store IDs].[All Store IDs].Children, [Store IDs].[All Store IDs]})},{[Measures].[Unit Sales]})), ExcludeEmpty)))'\n"
            + "     SET [Customer IDs Subset] AS 'Subset([Customer IDs Fullset], 0, 25)'\n"
            + "SELECT\n"
            + "     {[Store IDs].[All Store IDs]} ON COLUMNS,\n"
            + "     {[Customer IDs Subset], Ascendants([Customer IDs].[All Customer IDs])} ON ROWS\n"
            + "FROM\n"
            + "      [Sales]\n"
            + "WHERE\n"
            + "      ([Promotions].[Big Promo], [Time].[1997], [Gender].[F], [Measures].[Unit Sales] )\n"
            + "CELL PROPERTIES\n"
            + "      VALUE, FORMATTED_VALUE, CELL_ORDINAL, FORE_COLOR, BACK_COLOR, UPDATEABLE, FORMAT_STRING";

        if (!useAgg && propSaver.properties.EnableNativeFilter.get()) {
            assertQuerySql(
                testContext,
                mdx,
                new SqlPattern[] {
                    new SqlPattern(Dialect.DatabaseProduct.MYSQL, sql, null)
                });
        }

        testContext.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Promotions].[Big Promo], [Time].[1997], [Gender].[F], [Measures].[Unit Sales]}\n"
            + "Axis #1:\n"
            + "{[Store IDs].[All Store IDs]}\n"
            + "Axis #2:\n"
            + "{[Customer IDs].[158]}\n"
            + "{[Customer IDs].[229]}\n"
            + "{[Customer IDs].[485]}\n"
            + "{[Customer IDs].[617]}\n"
            + "{[Customer IDs].[1024]}\n"
            + "{[Customer IDs].[1057]}\n"
            + "{[Customer IDs].[1607]}\n"
            + "{[Customer IDs].[1652]}\n"
            + "{[Customer IDs].[1813]}\n"
            + "{[Customer IDs].[1965]}\n"
            + "{[Customer IDs].[2035]}\n"
            + "{[Customer IDs].[2244]}\n"
            + "{[Customer IDs].[2390]}\n"
            + "{[Customer IDs].[2459]}\n"
            + "{[Customer IDs].[2664]}\n"
            + "{[Customer IDs].[2918]}\n"
            + "{[Customer IDs].[2942]}\n"
            + "{[Customer IDs].[3015]}\n"
            + "{[Customer IDs].[3160]}\n"
            + "{[Customer IDs].[3245]}\n"
            + "{[Customer IDs].[3310]}\n"
            + "{[Customer IDs].[4269]}\n"
            + "{[Customer IDs].[4287]}\n"
            + "{[Customer IDs].[4322]}\n"
            + "{[Customer IDs].[4340]}\n"
            + "{[Customer IDs].[All Customer IDs]}\n"
            + "Row #0: 18\n"
            + "Row #1: 12\n"
            + "Row #2: 20\n"
            + "Row #3: 16\n"
            + "Row #4: 9\n"
            + "Row #5: 8\n"
            + "Row #6: 12\n"
            + "Row #7: 8\n"
            + "Row #8: 9\n"
            + "Row #9: 20\n"
            + "Row #10: 17\n"
            + "Row #11: 25\n"
            + "Row #12: 23\n"
            + "Row #13: 10\n"
            + "Row #14: 10\n"
            + "Row #15: 24\n"
            + "Row #16: 41\n"
            + "Row #17: 12\n"
            + "Row #18: 31\n"
            + "Row #19: 16\n"
            + "Row #20: 14\n"
            + "Row #21: 23\n"
            + "Row #22: 9\n"
            + "Row #23: 21\n"
            + "Row #24: 21\n"
            + "Row #25: 936\n");
    }

    /**
     * This verifies the MemberExpr portion of MONDRIAN-2017, allowing expandMembers to
     * expand Member expressions.
     */
    public void testNativeFilterWithComplexCalc() {
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
            + "  </Dimension>"
            + "  <Dimension name=\"Store IDs\" foreignKey=\"store_id\">\n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Store IDs\" primaryKey=\"store_id\">\n"
            + "      <Table name=\"store\"/>\n"
            + "      <Level name=\"ID\" column=\"store_id\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>");

        // expected native filter with a join against the fact table
        String sql =
            "select `sales_fact_1997`.`customer_id` as `c0` "
            + "from `sales_fact_1997` as `sales_fact_1997`, "
            + "`time_by_day` as `time_by_day`, `promotion` as `promotion`, "
            + "`customer` as `customer` "
            + "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` "
            + "and `time_by_day`.`the_year` = 1997 "
            + "and `sales_fact_1997`.`promotion_id` = `promotion`.`promotion_id` "
            + "and `promotion`.`promotion_name` = 'Big Promo' "
            + "and `sales_fact_1997`.`customer_id` = `customer`.`customer_id` "
            + "and `customer`.`gender` = 'F' "
            + "group by `sales_fact_1997`.`customer_id` "
            + "order by ISNULL(`sales_fact_1997`.`customer_id`) ASC, `sales_fact_1997`.`customer_id` ASC "
            + "limit 25 offset 0";
        String mdx =
            "WITH\n"
            + "     MEMBER [Gender].[Filter 0] as Aggregate(Filter([Gender].[F], 1=1))\n"
            + "     MEMBER [Gender].[Filter] as [Gender].[Filter 0]\n"
            + "     SET [Customer IDs Fullset] AS 'Filter({AddCalculatedMembers([Customer IDs].[All Customer IDs].Children)}, (Count(CrossJoin({[Customer IDs].CurrentMember},CrossJoin({AddCalculatedMembers({[Store IDs].[All Store IDs].Children, [Store IDs].[All Store IDs]})},{[Measures].[Unit Sales]})), ExcludeEmpty)))'\n"
            + "     SET [Customer IDs Subset] AS 'Subset([Customer IDs Fullset], 0, 25)'\n"
            + "SELECT\n"
            + "     {[Store IDs].[All Store IDs]} ON COLUMNS,\n"
            + "     {[Customer IDs Subset], Ascendants([Customer IDs].[All Customer IDs])} ON ROWS\n"
            + "FROM\n"
            + "      [Sales]\n"
            + "WHERE\n"
            + "      ([Promotions].[Big Promo], [Time].[1997], [Gender].[Filter], [Measures].[Unit Sales] )\n"
            + "CELL PROPERTIES\n"
            + "      VALUE, FORMATTED_VALUE, CELL_ORDINAL, FORE_COLOR, BACK_COLOR, UPDATEABLE, FORMAT_STRING";

        if (!useAgg && propSaver.properties.EnableNativeFilter.get()) {
            assertQuerySql(
                testContext,
                mdx,
                new SqlPattern[] {
                    new SqlPattern(Dialect.DatabaseProduct.MYSQL, sql, null)
                });
        }

        testContext.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Promotions].[Big Promo], [Time].[1997], [Gender].[Filter], [Measures].[Unit Sales]}\n"
            + "Axis #1:\n"
            + "{[Store IDs].[All Store IDs]}\n"
            + "Axis #2:\n"
            + "{[Customer IDs].[158]}\n"
            + "{[Customer IDs].[229]}\n"
            + "{[Customer IDs].[485]}\n"
            + "{[Customer IDs].[617]}\n"
            + "{[Customer IDs].[1024]}\n"
            + "{[Customer IDs].[1057]}\n"
            + "{[Customer IDs].[1607]}\n"
            + "{[Customer IDs].[1652]}\n"
            + "{[Customer IDs].[1813]}\n"
            + "{[Customer IDs].[1965]}\n"
            + "{[Customer IDs].[2035]}\n"
            + "{[Customer IDs].[2244]}\n"
            + "{[Customer IDs].[2390]}\n"
            + "{[Customer IDs].[2459]}\n"
            + "{[Customer IDs].[2664]}\n"
            + "{[Customer IDs].[2918]}\n"
            + "{[Customer IDs].[2942]}\n"
            + "{[Customer IDs].[3015]}\n"
            + "{[Customer IDs].[3160]}\n"
            + "{[Customer IDs].[3245]}\n"
            + "{[Customer IDs].[3310]}\n"
            + "{[Customer IDs].[4269]}\n"
            + "{[Customer IDs].[4287]}\n"
            + "{[Customer IDs].[4322]}\n"
            + "{[Customer IDs].[4340]}\n"
            + "{[Customer IDs].[All Customer IDs]}\n"
            + "Row #0: 18\n"
            + "Row #1: 12\n"
            + "Row #2: 20\n"
            + "Row #3: 16\n"
            + "Row #4: 9\n"
            + "Row #5: 8\n"
            + "Row #6: 12\n"
            + "Row #7: 8\n"
            + "Row #8: 9\n"
            + "Row #9: 20\n"
            + "Row #10: 17\n"
            + "Row #11: 25\n"
            + "Row #12: 23\n"
            + "Row #13: 10\n"
            + "Row #14: 10\n"
            + "Row #15: 24\n"
            + "Row #16: 41\n"
            + "Row #17: 12\n"
            + "Row #18: 31\n"
            + "Row #19: 16\n"
            + "Row #20: 14\n"
            + "Row #21: 23\n"
            + "Row #22: 9\n"
            + "Row #23: 21\n"
            + "Row #24: 21\n"
            + "Row #25: 936\n");
    }

    public void testNativeFilterInStr() {
        propSaver.set(MondrianProperties.instance().SsasCompatibleNaming, true);
        String mdx =
            "WITH MEMBER [Measures].[(Ancestors)] AS "
            + "Generate(Ascendants([Store].CurrentMember), [Store].CurrentMember.Name, \"^$^\")\n"
            + "SELECT {[Measures].[(Ancestors)]} ON COLUMNS,\n"
            + "Filter([Store].[Store State].AllMembers,"
            + "InStr(\"CA, PA, WA\", [Store].CurrentMember.Name) > 0) ON ROWS\n"
            + "FROM [Sales]";
        if (!isUseAgg() && MondrianProperties.instance().EnableNativeFilter.get())
        {
            propSaver.set(MondrianProperties.instance().GenerateFormattedSql, true);
            String sql =
                "select\n"
                + "    `store`.`store_country` as `c0`,\n"
                + "    `store`.`store_state` as `c1`\n"
                + "from\n"
                + "    `store` as `store`\n"
                + "group by\n"
                + "    `store`.`store_country`,\n"
                + "    `store`.`store_state`\n"
                + "having\n"
                + "    (INSTR('CA, PA, WA', c1) > 0)\n"
                + "order by\n"
                + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
                + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC";
            assertQuerySql(
                mdx,
                new SqlPattern[] {
                    new SqlPattern(Dialect.DatabaseProduct.MYSQL, sql, null)
                });
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[(Ancestors)]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[WA]}\n"
            + "Row #0: CA^$^USA^$^All Stores\n"
            + "Row #1: WA^$^USA^$^All Stores\n");
    }

    public void testNativeFilterMatchNoJoin() {
        if (!getTestContext().getDialect().allowsRegularExpressionInWhereClause()){
            return;
        }
        propSaver.set(MondrianProperties.instance().SsasCompatibleNaming, true);

        String mdx =
            "WITH MEMBER [Measures].[(order)] AS "
            + "InStr(LCase([Customers].CurrentMember.Caption), \"oliv\")\n"
            + "MEMBER [Measures].[(Ancestors)] AS "
            + "Generate(Ascendants([Customers].CurrentMember), [Customers].CurrentMember.Caption, \"^$^\")\n"
            + "SELECT {[Measures].[(Ancestors)]} ON COLUMNS,\n"
            + "Order(Filter([Customers].[Customers].[Name].AllMembers, ([Customers].CurrentMember.Caption "
            + "MATCHES \"(?i).*oliv.*\")), [Measures].[(order)], BASC) ON ROWS\n"
            + "FROM [Sales]";
        if (!isUseAgg() && MondrianProperties.instance().EnableNativeFilter.get())
        {
            propSaver.set(MondrianProperties.instance().GenerateFormattedSql, true);
            String sql =
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
                + "    INSTR(LOWER(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)), 'oliv') as `c10`\n"
                + "from\n"
                + "    `customer` as `customer`\n"
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
                + "having\n"
                + "    (UPPER(c5) REGEXP '.*OLIV.*') \n"
                + "order by\n"
                + "    ISNULL(INSTR(LOWER(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)), 'oliv')) ASC, INSTR(LOWER(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)), 'oliv') ASC,\n"
                + "    ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC,\n"
                + "    ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC,\n"
                + "    ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC,\n"
                + "    ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC";
            assertQuerySql(mdx, mysqlPattern(sql));
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[(Ancestors)]}\n"
            + "Axis #2:\n"
            + "{[Customers].[USA].[CA].[Pomona].[Olive Magan]}\n"
            + "{[Customers].[USA].[OR].[Milwaukie].[Olivia Gardner]}\n"
            + "{[Customers].[Canada].[BC].[Oak Bay].[Mike Olivares]}\n"
            + "{[Customers].[USA].[CA].[Arcadia].[Olga Oliver]}\n"
            + "{[Customers].[USA].[CA].[Lemon Grove].[Paula Oliveto]}\n"
            + "{[Customers].[USA].[WA].[Bellingham].[Glenn Olivera]}\n"
            + "{[Customers].[USA].[CA].[Oakland].[Barbara Olivera]}\n"
            + "{[Customers].[USA].[CA].[Lemon Grove].[Kathleen Oliver]}\n"
            + "{[Customers].[USA].[CA].[Lemon Grove].[Kimberly Oliver]}\n"
            + "{[Customers].[USA].[CA].[La Mesa].[Jacqueline Oliver]}\n"
            + "Row #0: Olive Magan^$^Pomona^$^CA^$^USA^$^All Customers\n"
            + "Row #1: Olivia Gardner^$^Milwaukie^$^OR^$^USA^$^All Customers\n"
            + "Row #2: Mike Olivares^$^Oak Bay^$^BC^$^Canada^$^All Customers\n"
            + "Row #3: Olga Oliver^$^Arcadia^$^CA^$^USA^$^All Customers\n"
            + "Row #4: Paula Oliveto^$^Lemon Grove^$^CA^$^USA^$^All Customers\n"
            + "Row #5: Glenn Olivera^$^Bellingham^$^WA^$^USA^$^All Customers\n"
            + "Row #6: Barbara Olivera^$^Oakland^$^CA^$^USA^$^All Customers\n"
            + "Row #7: Kathleen Oliver^$^Lemon Grove^$^CA^$^USA^$^All Customers\n"
            + "Row #8: Kimberly Oliver^$^Lemon Grove^$^CA^$^USA^$^All Customers\n"
            + "Row #9: Jacqueline Oliver^$^La Mesa^$^CA^$^USA^$^All Customers\n");

        // search by numeric value
        mdx = "SELECT {} ON COLUMNS,\n"
            + "Filter([Store].[Store].[Store Name].AllMembers,"
            + " INSTR([Store].CurrentMember.Caption, \"5\") > 0) ON ROWS\n"
            + "FROM [Sales]";
        if (!isUseAgg()) {
            String sql =
                "select\n"
                + "    `store`.`store_country` as `c0`,\n"
                + "    `store`.`store_state` as `c1`,\n"
                + "    `store`.`store_city` as `c2`,\n"
                + "    `store`.`store_name` as `c3`,\n"
                + "    `store`.`store_type` as `c4`,\n"
                + "    `store`.`store_manager` as `c5`,\n"
                + "    `store`.`store_sqft` as `c6`,\n"
                + "    `store`.`grocery_sqft` as `c7`,\n"
                + "    `store`.`frozen_sqft` as `c8`,\n"
                + "    `store`.`meat_sqft` as `c9`,\n"
                + "    `store`.`coffee_bar` as `c10`,\n"
                + "    `store`.`store_street_address` as `c11`\n"
                + "from\n"
                + "    `store` as `store`\n"
                + "group by\n"
                + "    `store`.`store_country`,\n"
                + "    `store`.`store_state`,\n"
                + "    `store`.`store_city`,\n"
                + "    `store`.`store_name`,\n"
                + "    `store`.`store_type`,\n"
                + "    `store`.`store_manager`,\n"
                + "    `store`.`store_sqft`,\n"
                + "    `store`.`grocery_sqft`,\n"
                + "    `store`.`frozen_sqft`,\n"
                + "    `store`.`meat_sqft`,\n"
                + "    `store`.`coffee_bar`,\n"
                + "    `store`.`store_street_address`\n"
                + "having\n"
                + "    (INSTR(c3, '5') > 0)\n"
                + "order by\n"
                + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC,\n"
                + "    ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC,\n"
                + "    ISNULL(`store`.`store_city`) ASC, `store`.`store_city` ASC,\n"
                + "    ISNULL(`store`.`store_name`) ASC, `store`.`store_name` ASC";
            assertQuerySql(mdx, mysqlPattern(sql));
        }
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "Axis #2:\n"
                + "{[Store].[Mexico].[Jalisco].[Guadalajara].[Store 5]}\n"
                + "{[Store].[USA].[WA].[Seattle].[Store 15]}\n");
    }

    public void testNativeNonEmptyFilterMatchOptimization() {
        if (!getTestContext().getDialect().allowsRegularExpressionInWhereClause()) {
            return;
        }
        propSaver.set(MondrianProperties.instance().SsasCompatibleNaming, true);
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
            "WITH MEMBER [Measures].[(Ancestors)] AS "
            + "Generate(Ascendants([Customer IDs].CurrentMember), [Customer IDs].CurrentMember.Caption, \"^$^\")\n"
            + "SELECT {[Measures].[(Ancestors)]} ON COLUMNS,\n"
            + "Subset(NonEmpty(Filter([Customer IDs].[Customer IDs].[ID].AllMembers, ([Customer IDs].CurrentMember.Caption "
            + "MATCHES \"(?i)^(1771|1493|10198|123)$\")), [Measures].[Sales Count]), 0, 10) ON ROWS\n"
            + "FROM [Sales]";
        if (!isUseAgg() && MondrianProperties.instance().EnableNativeFilter.get()
            && MondrianProperties.instance().EnableNativeNonEmpty.get())
        {
            propSaver.set(MondrianProperties.instance().GenerateFormattedSql, true);
            String sql =
                "select\n"
                + "    `sales_fact_1997`.`customer_id` as `c0`\n"
                + "from\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `time_by_day` as `time_by_day`\n"
                + "where\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"
                + "and\n"
                + "    `sales_fact_1997`.`product_id` is not null\n"
                + "group by\n"
                + "    `sales_fact_1997`.`customer_id`\n"
                + "having\n"
                + "    (UPPER(c0) REGEXP '^(1771|1493|10198|123)$') \n"
                + "order by\n"
                + "    ISNULL(`sales_fact_1997`.`customer_id`) ASC, `sales_fact_1997`.`customer_id` ASC limit 10 offset 0";
            assertQuerySql(testContext, mdx, mysqlPattern(sql));
        }

        testContext.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[(Ancestors)]}\n"
            + "Axis #2:\n"
            + "{[Customer IDs].[1493]}\n"
            + "{[Customer IDs].[1771]}\n"
            + "{[Customer IDs].[10198]}\n"
            + "Row #0: 1493^$^All Customer IDs\n"
            + "Row #1: 1771^$^All Customer IDs\n"
            + "Row #2: 10198^$^All Customer IDs\n");
    }

    public void testNativeFilterMatchNoJoinWithHanger() {
        if (!getTestContext().getDialect().allowsRegularExpressionInWhereClause()){
            return;
        }
        propSaver.set(MondrianProperties.instance().SsasCompatibleNaming, true);
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            "<Dimension name=\"GenderHanger\" foreignKey=\"customer_id\" hanger=\"true\">"
            + "<Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
            + "  <Table name=\"customer\"/>\n"
            + "  <Level name=\"GenderHanger\" column=\"gender\" uniqueMembers=\"true\"/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n");

        String mdx =
            "WITH MEMBER [Measures].[(order)] AS "
            + "InStr(LCase([Customers].CurrentMember.Caption), \"oliv\")\n"
            + "MEMBER [Measures].[(Ancestors)] AS "
            + "Generate(Ascendants([Customers].CurrentMember), [Customers].CurrentMember.Caption, \"^$^\")\n"
            + "SELECT {[Measures].[(Ancestors)]} ON COLUMNS,\n"
            + "Order(Filter([Customers].[Customers].[Name].AllMembers, ([Customers].CurrentMember.Caption "
            + "MATCHES \"(?i).*oliv.*\")), [Measures].[(order)], BASC) ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE [GenderHanger].[M]";
        if (!isUseAgg() && MondrianProperties.instance().EnableNativeFilter.get())
        {
            propSaver.set(MondrianProperties.instance().GenerateFormattedSql, true);
            String sql =
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
                + "    INSTR(LOWER(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)), 'oliv') as `c10`\n"
                + "from\n"
                + "    `customer` as `customer`\n"
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
                + "having\n"
                + "    (UPPER(c5) REGEXP '.*OLIV.*') \n"
                + "order by\n"
                + "    ISNULL(INSTR(LOWER(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)), 'oliv')) ASC, INSTR(LOWER(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)), 'oliv') ASC,\n"
                + "    ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC,\n"
                + "    ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC,\n"
                + "    ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC,\n"
                + "    ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC";
            assertQuerySql(testContext, mdx, mysqlPattern(sql));
        }

        testContext.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[GenderHanger].[M]}\n"
            + "Axis #1:\n"
            + "{[Measures].[(Ancestors)]}\n"
            + "Axis #2:\n"
            + "{[Customers].[USA].[CA].[Pomona].[Olive Magan]}\n"
            + "{[Customers].[USA].[OR].[Milwaukie].[Olivia Gardner]}\n"
            + "{[Customers].[Canada].[BC].[Oak Bay].[Mike Olivares]}\n"
            + "{[Customers].[USA].[CA].[Arcadia].[Olga Oliver]}\n"
            + "{[Customers].[USA].[CA].[Lemon Grove].[Paula Oliveto]}\n"
            + "{[Customers].[USA].[WA].[Bellingham].[Glenn Olivera]}\n"
            + "{[Customers].[USA].[CA].[Oakland].[Barbara Olivera]}\n"
            + "{[Customers].[USA].[CA].[Lemon Grove].[Kathleen Oliver]}\n"
            + "{[Customers].[USA].[CA].[Lemon Grove].[Kimberly Oliver]}\n"
            + "{[Customers].[USA].[CA].[La Mesa].[Jacqueline Oliver]}\n"
            + "Row #0: Olive Magan^$^Pomona^$^CA^$^USA^$^All Customers\n"
            + "Row #1: Olivia Gardner^$^Milwaukie^$^OR^$^USA^$^All Customers\n"
            + "Row #2: Mike Olivares^$^Oak Bay^$^BC^$^Canada^$^All Customers\n"
            + "Row #3: Olga Oliver^$^Arcadia^$^CA^$^USA^$^All Customers\n"
            + "Row #4: Paula Oliveto^$^Lemon Grove^$^CA^$^USA^$^All Customers\n"
            + "Row #5: Glenn Olivera^$^Bellingham^$^WA^$^USA^$^All Customers\n"
            + "Row #6: Barbara Olivera^$^Oakland^$^CA^$^USA^$^All Customers\n"
            + "Row #7: Kathleen Oliver^$^Lemon Grove^$^CA^$^USA^$^All Customers\n"
            + "Row #8: Kimberly Oliver^$^Lemon Grove^$^CA^$^USA^$^All Customers\n"
            + "Row #9: Jacqueline Oliver^$^La Mesa^$^CA^$^USA^$^All Customers\n");
    }

    public void testNativeFilterMatchAndSubsetNoJoin() {
        if (!getTestContext().getDialect().allowsRegularExpressionInWhereClause()){
            return;
        }
        propSaver.set(MondrianProperties.instance().SsasCompatibleNaming, true);

        String mdx =
            "WITH MEMBER [Measures].[(order)] AS "
            + "InStr(LCase([Customers].CurrentMember.Caption), \"oliv\")\n"
            + "MEMBER [Measures].[(Ancestors)] AS "
            + "Generate(Ascendants([Customers].CurrentMember), [Customers].CurrentMember.Caption, \"^$^\")\n"
            + "SELECT "
            + "Subset(Filter([Customers].[Customers].[Name].AllMembers, ([Customers].CurrentMember.Caption "
            + "MATCHES \"(?i).*oliv.*\")), 0, 10) ON 0,\n"
            + "{[Measures].[(Ancestors)]} ON 1\n"
            + "FROM [Sales]";
        if (!isUseAgg() && MondrianProperties.instance().EnableNativeFilter.get()
            && MondrianProperties.instance().EnableNativeSubset.get())
        {
            propSaver.set(MondrianProperties.instance().GenerateFormattedSql, true);
            String sql =
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
                + "    `customer`.`yearly_income` as `c9`\n"
                + "from\n"
                + "    `customer` as `customer`\n"
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
                + "having\n"
                + "    (UPPER(c5) REGEXP '.*OLIV.*') \n"
                + "order by\n"
                + "    ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC,\n"
                + "    ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC,\n"
                + "    ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC,\n"
                + "    ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC limit 10 offset 0";
            assertQuerySql(
                mdx,
                new SqlPattern[] {
                    new SqlPattern(Dialect.DatabaseProduct.MYSQL, sql, null)
                });
        }

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customers].[Canada].[BC].[Oak Bay].[Mike Olivares]}\n"
            + "{[Customers].[USA].[CA].[Arcadia].[Olga Oliver]}\n"
            + "{[Customers].[USA].[CA].[La Mesa].[Jacqueline Oliver]}\n"
            + "{[Customers].[USA].[CA].[Lemon Grove].[Kathleen Oliver]}\n"
            + "{[Customers].[USA].[CA].[Lemon Grove].[Kimberly Oliver]}\n"
            + "{[Customers].[USA].[CA].[Lemon Grove].[Paula Oliveto]}\n"
            + "{[Customers].[USA].[CA].[Oakland].[Barbara Olivera]}\n"
            + "{[Customers].[USA].[CA].[Pomona].[Olive Magan]}\n"
            + "{[Customers].[USA].[OR].[Milwaukie].[Olivia Gardner]}\n"
            + "{[Customers].[USA].[WA].[Bellingham].[Glenn Olivera]}\n"
            + "Axis #2:\n"
            + "{[Measures].[(Ancestors)]}\n"
            + "Row #0: Mike Olivares^$^Oak Bay^$^BC^$^Canada^$^All Customers\n"
            + "Row #0: Olga Oliver^$^Arcadia^$^CA^$^USA^$^All Customers\n"
            + "Row #0: Jacqueline Oliver^$^La Mesa^$^CA^$^USA^$^All Customers\n"
            + "Row #0: Kathleen Oliver^$^Lemon Grove^$^CA^$^USA^$^All Customers\n"
            + "Row #0: Kimberly Oliver^$^Lemon Grove^$^CA^$^USA^$^All Customers\n"
            + "Row #0: Paula Oliveto^$^Lemon Grove^$^CA^$^USA^$^All Customers\n"
            + "Row #0: Barbara Olivera^$^Oakland^$^CA^$^USA^$^All Customers\n"
            + "Row #0: Olive Magan^$^Pomona^$^CA^$^USA^$^All Customers\n"
            + "Row #0: Olivia Gardner^$^Milwaukie^$^OR^$^USA^$^All Customers\n"
            + "Row #0: Glenn Olivera^$^Bellingham^$^WA^$^USA^$^All Customers\n");
    }

    public void testNativeFilterUCaseWithEmptyString() {
        String mdx =
            "Select Filter([Store].Members, UCase(\"\") = \"\" "
            + "and [Store].CurrentMember.Name = \"Bellingham\") "
            + "on 0 from [Sales]";
        if (MondrianProperties.instance().EnableNativeFilter.get())
        {
            String mysql =
                "select\n"
                + "    \"store\".\"store_country\" as \"c0\"\n"
                + "from\n"
                + "    \"store\" as \"store\"\n"
                + "group by\n"
                + "    \"store\".\"store_country\"\n"
                + "having\n"
                + "    ((UPPER(\"\") = \"\") AND (\"store\".\"store_country\" = \"Bellingham\"))\n"
                + "order by\n"
                + "    ISNULL(\"store\".\"store_country\") ASC, \"store\".\"store_country\" ASC";
            propSaver.set(MondrianProperties.instance().GenerateFormattedSql, true);
            assertQuerySql(mdx, mysqlPattern(mysql));
            verifySameNativeAndNot(
                mdx, "Filtering with UCase and empty string", getTestContext());
        }
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[WA].[Bellingham]}\n"
            + "Row #0: 2,237\n");
    }

    public void testNativeFilterMatchNoJoinOnMultiLevelHierarchy() {
        if (!getTestContext().getDialect().allowsRegularExpressionInWhereClause()) {
            return;
        }
        propSaver.set(MondrianProperties.instance().SsasCompatibleNaming, true);

        // test <Hierarchy>.AllMembers
        String mdx =
            "WITH MEMBER [Measures].[(order)] AS "
            + "InStr(LCase([Customers].CurrentMember.Caption), \"oliv\")\n"
            + "MEMBER [Measures].[(Ancestors)] AS "
            + "Generate(Ascendants([Customers].CurrentMember), [Customers].CurrentMember.Caption, \"^$^\")\n"
            + "SELECT {[Measures].[(Ancestors)]} ON COLUMNS,\n"
            + "Order(Filter([Customers].AllMembers, ([Customers].CurrentMember.Caption "
            + "MATCHES \"(?i).*oliv.*\")), [Measures].[(order)], BASC) ON ROWS\n"
            + "FROM [Sales]";
        String mysql =
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
            + "    `customer`.`yearly_income` as `c9`\n"
            + "from\n"
            + "    `customer` as `customer`\n"
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
            + "having\n"
            + "    (UPPER(c5) REGEXP '.*OLIV.*') \n"
            + "order by\n"
            + "    ISNULL(`customer`.`country`) ASC, `customer`.`country` ASC,\n"
            + "    ISNULL(`customer`.`state_province`) ASC, `customer`.`state_province` ASC,\n"
            + "    ISNULL(`customer`.`city`) ASC, `customer`.`city` ASC,\n"
            + "    ISNULL(CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)) ASC, CONCAT(`customer`.`fname`, ' ', `customer`.`lname`) ASC";
        String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[(Ancestors)]}\n"
            + "Axis #2:\n"
            + "{[Customers].[USA].[CA].[Pomona].[Olive Magan]}\n"
            + "{[Customers].[USA].[OR].[Milwaukie].[Olivia Gardner]}\n"
            + "{[Customers].[Canada].[BC].[Oak Bay].[Mike Olivares]}\n"
            + "{[Customers].[USA].[CA].[Arcadia].[Olga Oliver]}\n"
            + "{[Customers].[USA].[CA].[Lemon Grove].[Paula Oliveto]}\n"
            + "{[Customers].[USA].[WA].[Bellingham].[Glenn Olivera]}\n"
            + "{[Customers].[USA].[CA].[Oakland].[Barbara Olivera]}\n"
            + "{[Customers].[USA].[CA].[Lemon Grove].[Kathleen Oliver]}\n"
            + "{[Customers].[USA].[CA].[Lemon Grove].[Kimberly Oliver]}\n"
            + "{[Customers].[USA].[CA].[La Mesa].[Jacqueline Oliver]}\n"
            + "Row #0: Olive Magan^$^Pomona^$^CA^$^USA^$^All Customers\n"
            + "Row #1: Olivia Gardner^$^Milwaukie^$^OR^$^USA^$^All Customers\n"
            + "Row #2: Mike Olivares^$^Oak Bay^$^BC^$^Canada^$^All Customers\n"
            + "Row #3: Olga Oliver^$^Arcadia^$^CA^$^USA^$^All Customers\n"
            + "Row #4: Paula Oliveto^$^Lemon Grove^$^CA^$^USA^$^All Customers\n"
            + "Row #5: Glenn Olivera^$^Bellingham^$^WA^$^USA^$^All Customers\n"
            + "Row #6: Barbara Olivera^$^Oakland^$^CA^$^USA^$^All Customers\n"
            + "Row #7: Kathleen Oliver^$^Lemon Grove^$^CA^$^USA^$^All Customers\n"
            + "Row #8: Kimberly Oliver^$^Lemon Grove^$^CA^$^USA^$^All Customers\n"
            + "Row #9: Jacqueline Oliver^$^La Mesa^$^CA^$^USA^$^All Customers\n";

        if (MondrianProperties.instance().EnableNativeFilter.get())
        {
            propSaver.set(MondrianProperties.instance().GenerateFormattedSql, true);
            assertQuerySql(mdx, mysqlPattern(mysql));
            verifySameNativeAndNot(
                mdx, "Filtering AllMembers on a multi-level hierarchy", getTestContext());
        }
        assertQueryReturns(mdx, result);

        // test <Hierarchy>.Members
        mdx =
            "WITH MEMBER [Measures].[(order)] AS "
            + "InStr(LCase([Customers].CurrentMember.Caption), \"oliv\")\n"
            + "MEMBER [Measures].[(Ancestors)] AS "
            + "Generate(Ascendants([Customers].CurrentMember), [Customers].CurrentMember.Caption, \"^$^\")\n"
            + "SELECT {[Measures].[(Ancestors)]} ON COLUMNS,\n"
            + "Order(Filter([Customers].Members, ([Customers].CurrentMember.Caption "
            + "MATCHES \"(?i).*oliv.*\")), [Measures].[(order)], BASC) ON ROWS\n"
            + "FROM [Sales]";
        if (MondrianProperties.instance().EnableNativeFilter.get())
        {
            propSaver.set(MondrianProperties.instance().GenerateFormattedSql, true);
            assertQuerySql(getTestContext().withFreshConnection(), mdx, mysqlPattern(mysql));
            verifySameNativeAndNot(
                mdx, "Filtering AllMembers on a multi-level hierarchy", getTestContext());
        }
        assertQueryReturns(mdx, result);
    }
}

// End FilterTest.java
