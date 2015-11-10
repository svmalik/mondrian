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
import java.util.List;

import javax.sql.DataSource;
import mondrian.olap.Evaluator;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.rolap.sql.SqlQuery;
import mondrian.spi.Dialect;
import mondrian.spi.DialectManager;
import mondrian.util.Pair;

/**
 *  This class contains classes to create consolidated requests for count and sum use cases.
 *  The factory classes (SumConsolidatorFactory and CountConsolidatorFactory) create
 *  NativeRequestConsolidator instances. The request consolidators can take multiple sum or
 *  count requests (RolapNativeSet.NativeSumRequest and RolapNativeSet.NativeCountRequest) and
 *  combine them into a single SQL query. The factories are invoked by the FastBatchingCellReader
 *  just before it sends out requests to the DB.
 *  <p/>
 *  The particular use case that consolidation related code addresses, is the situation where
 *  there are multiple members that inhabit the same level in the same cube that are being
 *  requested individually and sequentially. This can be time consuming if the sum or
 *  count requests are complex.
 *  <p/>
 *  Constraints that are candidates for consolidation are identical in all respects except that
 *  either a single member referenced by the evaluator, or a single member in a CrossJoinArg has
 *  a different identity, but inhabits the same level, dimension, hierarchy and cube. All other
 *  members must be the same.
 *  <p/>
 *  The ConsolidationUtils class contains utilities for determining whether two constraints can
 *  be consolidated into a single request based on the cache key of the native request
 *  (the getLevelMatch match method), as well
 *  as grouping requests into consolidated groups or simple UNION requests (the groupPairs method).
 *  <p/>
 *  As part of the process of grouping candiadtes, a ConsolidationHandler is added to the first
 *  constraint in the list and matching constraints have their single non-matching member
 *  added to this consolidation handler.
 *
 *  Given a list of grouped native requests, i.e., consolidated request candidates, the
 *  consolidator classes defined here can create SQL that combines multiple sum or count queries
 *  into a single query. They do this by using the first request constraint in each list as the
 *  constraint to construct the query with, i.e., the constarint with the consolidation handler
 *  set. During the process of SQL generation, the constraint
 *  is asked to create the SQL constraints and this in turn invokes its consolidation handler,
 *  either directly to add constaints to cross join args, or indirectly by passing the handler
 *  to SqlConstraintUtils.addContextConstraint. The handler ensures the members from the
 *  other constraints are exposed to the SQL generation process, resulting in a SQL
 *  WHERE x IN (a, b, c) clause
 *  containing the constraint's own member and the members from the other constraints, rather than
 *  a simple 'WHERE x = a'.
 *
 *  The metadata in the ConsolidatedMembers object maintained by the
 *  ConsolidationHandler is used by the NativeRequestConsolidator instances
 *  defined in this class to construct the wrapper SQL which uses COUNT clauses combined
 *  with CASE WHEN THEN clauses based on the identities of all members contained in the
 *  ConsolidatedMembers.
 *
 *  @see mondrian.rolap.NativeRequestConsolidator
 *  @see mondrian.rolap.ConsolidationUtils
 *  @see mondrian.rolap.ConsolidationHandler
 *  @see mondrian.rolap.ConsolidationMembers
 *  @see mondrian.rolap.RolapNativeSet.SetConstraint
 *  @see mondrian.rolap.SqlConstraintUtils
 *
 *
 */
public class ConsolidationQuery {

    public static final int MAX_UNION_QUERIES = 4;
    public static final int MAX_CONSOLIDATED_QUERIES = 512;


    static class NativeSumRequestConsolidator
        implements NativeRequestConsolidator, ConsolidationHandler.ConsolidationHandlerFactory {

        private List<RolapNativeSet.NativeSumRequest> requests
            = new ArrayList<RolapNativeSet.NativeSumRequest>();
        private List<RolapNativeRequest> consolidatedRequests
            = new ArrayList<RolapNativeRequest>();

        @Override
        public boolean addRequest(RolapNativeRequest request) {
            if (request instanceof RolapNativeSet.NativeSumRequest) {
                RolapNativeSet.NativeSumRequest counter = (RolapNativeSet.NativeSumRequest) request;
                requests.add(counter);
                return true;
            }
            return false;
        }

        @Override
        public List<RolapNativeRequest> getConsolidatedRequests() {
            buildRequests();
            return consolidatedRequests;
        }

        private void buildRequests() {
            if (requests.size() == 0) {
                ConsolidationUtils.CONSOLIDATION_LOGGER.debug(
                    "ConsolidatedNativeSumRequest execute no requests");
                return;
            }

            int maxSingle = MAX_UNION_QUERIES;
            int maxMulti = MAX_CONSOLIDATED_QUERIES;
            Dialect dialect;
            Evaluator eval = requests.get(0).getSetEvaluator().getConstraint().getEvaluator();
            if (eval instanceof RolapEvaluator) {
                dialect = ((RolapEvaluator) eval).getDialect();
            } else {
                dialect = DialectManager.createDialect(
                    requests.get(0).getSetEvaluator().getSchemaReader().getDataSource(), null);
            }
            if (!dialect.supportsUnlimitedValueList()) {
                maxMulti = Math.min(MondrianProperties.instance().MaxConstraints.get(), maxMulti);
            }
            Pair<List<List<Pair<RolapNativeSet.NativeSumRequest,
                ConsolidationUtils.LevelMatch>>>,
                List<List<Pair<RolapNativeSet.NativeSumRequest, ConsolidationUtils.LevelMatch>>>>
                orderedRequestsPair = ConsolidationUtils.groupPairs(this, requests, maxSingle,
                maxMulti);
            List<Pair<RolapNativeSet.NativeSumRequest, ConsolidationUtils.LevelMatch>> currSingles
                = new ArrayList<Pair<RolapNativeSet.NativeSumRequest,
                ConsolidationUtils.LevelMatch>>();

            List<List<Pair<RolapNativeSet.NativeSumRequest,
                ConsolidationUtils.LevelMatch>>> singleRequests
                = orderedRequestsPair.left;
            List<List<Pair<RolapNativeSet.NativeSumRequest,
                ConsolidationUtils.LevelMatch>>> orderedRequests
                = orderedRequestsPair.right;


            for (List<Pair<RolapNativeSet.NativeSumRequest,
                ConsolidationUtils.LevelMatch>> orderedRequest : orderedRequests) {

                int singles = 0;
                int multiples = 0;
                int unsatisfiedMultiples = 0;

                multiples++;
                Pair<RolapNativeSet.NativeSumRequest,
                    ConsolidationUtils.LevelMatch> pair = orderedRequest.get(0);
                RolapNativeSet.NativeSumRequest ncr = pair.left;
                RolapNativeSet.SetConstraint constraint
                    = (RolapNativeSet.SetConstraint) ncr.getSetEvaluator().getConstraint();
                // this needs to execute before we grab the column expression
                // because this is determined in the context of a cube and agg star
                // we just get level members so we don't screw up the sum constraint
                // if there are multiple base cubes in virtual cube.
                SqlQuery root = ncr.getSetEvaluator().getLevelMembersSql(ncr.getTupleReader(),
                    true).left;

                ConsolidationHandler handler = constraint.getConsolidationHandler();
                if (handler == null) {
                    ConsolidationUtils.CONSOLIDATION_LOGGER.debug(
                        "No consolidation handler found:");
                    for (int i = 0; i < orderedRequest.size(); i++) {
                        currSingles.add(orderedRequest.get(i));
                        if (currSingles.size() >= maxSingle) {
                            singleRequests.add(currSingles);
                            currSingles
                                = new ArrayList<Pair<RolapNativeSet.NativeSumRequest,
                                ConsolidationUtils.LevelMatch>>();
                        }
                    }
                    unsatisfiedMultiples++;
                } else {
                    ConsolidationMembers mm = handler.getLevelMembers();
                    if (mm == null
                        || mm.getColumnExpression() == null
                        || mm.getStarColumn() == null) {
                        ConsolidationUtils.CONSOLIDATION_LOGGER.debug(
                            "LevelMambers are null or no column Expression:" + mm);
                        // clear constaint even though we can't actually find the members
                        handler.clearMembers();
                        for (int i = 0; i < orderedRequest.size(); i++) {
                            currSingles.add(orderedRequest.get(i));
                            if (currSingles.size() >= maxSingle) {
                                singleRequests.add(currSingles);
                                currSingles
                                    = new ArrayList<Pair<RolapNativeSet.NativeSumRequest,
                                    ConsolidationUtils.LevelMatch>>();
                            }
                        }
                        unsatisfiedMultiples++;
                    } else {
                        List<RolapMember> allMembers = mm.getAllMembers();
                        if (allMembers.size() != orderedRequest.size()) {
                            ConsolidationUtils.CONSOLIDATION_LOGGER.debug(
                                "LevelMambers size does not match request list size" + mm);
                            // something has gone pear shaped - remove the level members
                            // and fall back to simple consolidation
                            handler.clearMembers();
                            for (int i = 0; i < orderedRequest.size(); i++) {
                                currSingles.add(orderedRequest.get(i));
                                if (currSingles.size() >= maxSingle) {
                                    singleRequests.add(currSingles);
                                    currSingles
                                        = new ArrayList<Pair<RolapNativeSet.NativeSumRequest,
                                        ConsolidationUtils.LevelMatch>>();
                                }
                            }
                            unsatisfiedMultiples++;
                        } else {
                            List<CacheKey> keys = new ArrayList<CacheKey>();
                            for (Pair<RolapNativeSet.NativeSumRequest,
                                ConsolidationUtils.LevelMatch> matchPair : orderedRequest) {
                                keys.add(matchPair.left.getCacheKey());
                            }
                            if (((RolapNativeSum.SumConstraint) constraint)
                                .isVirtualCubeQueryMode()) {
                                // if we're in virtual cube mode, we need to rebuild the root.
                                root = ncr.getSetEvaluator().getUnwrappedSumSql(ncr
                                    .getTupleReader());
                            }
                            SqlQuery wrapper = handler.wrap(root,
                                ncr.getSetEvaluator().getSchemaReader().getDataSource(), dialect);
                            ConsolidationUtils.CONSOLIDATION_LOGGER.debug(
                                "SENDING CONSOLIDATED SQL:" + wrapper);
                            consolidatedRequests.add(
                                new SumRequest(ncr, wrapper.toString(), keys, true));
                        }
                    }
                }
                ConsolidationUtils.CONSOLIDATION_LOGGER.debug("Requests contain " +
                    singles + " single requests and " + multiples + " multi requests. " +
                    unsatisfiedMultiples + " had to get combined in a simple way.");

            }
            if (currSingles.size() > 0) {
                singleRequests.add(currSingles);
            }
            if (singleRequests.size() > 0) {
                for (List<Pair<RolapNativeSet.NativeSumRequest,
                    ConsolidationUtils.LevelMatch>> singleRequest : singleRequests) {
                    executeSimpleGroup(singleRequest);
                }
            }
        }

        /**
         * Group the requests together by simply wrapping the whole query in a count and unioning
         * with the other queries.
         *
         * @param singleRequests
         */
        private void executeSimpleGroup(List<Pair<RolapNativeSet.NativeSumRequest,
            ConsolidationUtils.LevelMatch>> singleRequests) {
            // mostly so that tests will pass unchanged.
            ConsolidationUtils.CONSOLIDATION_LOGGER.debug(
                "ConsolidatedNativeCountRequest executing simple group of size "
                    + singleRequests.size());
            if (singleRequests.size() == 0) {
                return;
            }
            if (singleRequests.size() == 1) {
                RolapNativeSet.NativeSumRequest request = singleRequests.get(0).left;
                consolidatedRequests.add(new SumRequest(request, null, null, false));
            } else {
                boolean pretty = MondrianProperties.instance().GenerateFormattedSql.get();
                String spc = pretty ? Util.nl : " ";
                StringBuilder sql = new StringBuilder();
                List<CacheKey> keys = new ArrayList<CacheKey>();
                RolapNativeSet.NativeSumRequest request = null;
                int cnt = 0;
                for (Pair<RolapNativeSet.NativeSumRequest,
                    ConsolidationUtils.LevelMatch> pair : singleRequests) {
                    RolapNativeSet.NativeSumRequest ncr = pair.left;
                    sql.append(ncr.getSetEvaluator().getSumSql(ncr.getTupleReader()));
                    cnt++;
                    if (cnt < singleRequests.size()) {
                        sql.append(spc).append("union all").append(spc);
                    }
                    if (request == null) {
                        request = ncr;
                    }
                    keys.add(ncr.getCacheKey());
                }
                consolidatedRequests.add(new SumRequest(request, sql.toString(), keys, true));
            }
        }

        @Override
        public ConsolidationHandler newConsolidationHandler() {
            return new SumConsolidationHandler();
        }

        private static class SumRequest implements RolapNativeRequest {

            private RolapNativeSet.NativeSumRequest request;
            private String sql;
            private List<CacheKey> keys;
            private boolean isMulti;

            public SumRequest(RolapNativeSet.NativeSumRequest request, String sql,
                              List<CacheKey> keys,
                              boolean isMulti) {
                this.request = request;
                this.sql = sql;
                this.keys = keys;
                this.isMulti = isMulti;
            }

            @Override
            public void execute() {
                if (isMulti) {
                    request.getSetEvaluator().populateMultiSumCache(request.getTupleReader(),
                        sql, keys);
                } else {
                    request.getSetEvaluator().populateSumCache(request.getTupleReader(),
                        request.getCacheKey());
                }
            }
        }
    }

    static class SumConsolidationHandler extends ConsolidationHandler {

        @Override
        public SqlQuery wrap(SqlQuery configured, DataSource dataSource, Dialect dialect) {

            ConsolidationMembers members = getLevelMembers();
            if (members != null && members.getMembers().size() > 0) {
                String columnExpr = members.getColumnExpression();
                String colAlias = configured.getAlias(columnExpr);
                if (colAlias == null) {
                    // if there is no alias, then create one and add a select and group by
                    colAlias = "m2";
                    configured.addSelect(columnExpr, null, colAlias);
                    configured.addGroupBy(columnExpr, colAlias);
                }
                RolapStar.Column column = members.getStarColumn();
                if (column.isOptimized()) {
                    column = column.optimize();
                }

                configured.addFrom(column.getTable().getRelation(),
                    column.getTable().getRelation().getAlias(), false);
                int cnt = 0;
                SqlQuery wrapper = SqlQuery.newQuery(dataSource, "foobar");
                Dialect.Datatype dt = null;
                List<RolapMember> allMembers = members.getAllMembers();
                for (int i = 0; i < allMembers.size(); i++) {
                    RolapMember allMember = allMembers.get(i);
                    if (dt == null) {
                        dt = allMember.getLevel().getDatatype();
                    }
                    StringBuilder sb = new StringBuilder();
                    dt.quoteValue(sb, dialect, allMember.getKey().toString());
                    String v1 = sb.toString();
                    sb.setLength(0);

                    // return null, not 0, if there are no matching rows. 
                    // e.g.: case when count( case when `m2` ='Drink' then 1 else null end) = 0
                    // then NULL
                    // else sum(case when `m2` = 'Drink' then `m1` else 0 end) end as `sum0`,
                    StringBuilder select = new StringBuilder("case when count( case when ");
                    select.append(dialect.quoteIdentifier(colAlias))
                          .append(" = ")
                          .append(v1)
                          .append(" then 1 else null end) = 0 then null else sum( case when ")
                          .append(dialect.quoteIdentifier(colAlias))
                          .append(" = ")
                          .append(v1)
                          .append(" then ")
                          .append(dialect.quoteIdentifier("m1"))
                          .append(" else 0 end) end");
                    wrapper.addSelect(select.toString(), SqlStatement.Type.DOUBLE, "sum" + (cnt++));
                }
                wrapper.addFrom(configured, "sumQuery", true);
                return wrapper;
            }
            return configured;
        }
    }

    static class NativeCountRequestConsolidator
        implements NativeRequestConsolidator, ConsolidationHandler.ConsolidationHandlerFactory {

        private List<RolapNativeSet.NativeCountRequest> requests
            = new ArrayList<RolapNativeSet.NativeCountRequest>();

        private List<RolapNativeRequest> consolidatedRequests = new ArrayList<RolapNativeRequest>();

        public boolean addRequest(RolapNativeRequest request) {
            if (request instanceof RolapNativeSet.NativeCountRequest) {
                RolapNativeSet.NativeCountRequest counter
                    = (RolapNativeSet.NativeCountRequest) request;
                requests.add(counter);
                return true;
            }
            return false;
        }

        @Override
        public List<RolapNativeRequest> getConsolidatedRequests() {
            buildRequests();
            return consolidatedRequests;
        }

        /**
         * This calls
         * {@link mondrian.rolap.ConsolidationUtils(mondrian.rolap.ConsolidationHandler.ConsolidationHandlerFactory,
         * List, int, int) groupPairs} initially to get the list
         * of lists for different requests.
         * For the grouped requests that have been matched with a LevelMatch, this calls
         * getLevelMembersSql on the evaluator of the of the request whose constraint has been
         * augmented with the LevelMember objects. This process causes those level members to be
         * included in the generated SQL and also casues the LevelMember column expression to be
         * populated. This expression can then be used to create the wrapper select which uses a
         * COUNT + CASE pattern based on the value of the members matching the column expression.
         * The target column is also added to the group by if it is not already present. It may be
         * necessary to check for the nature of the group by clause(s) to check this won't
         * screw anything up.
         * If the level members cannot be found, or some other internal inconsistency occurs, then
         * the level members are cleared from the constraint and the grouped list is added to the
         * simple consolidation lists, where they are simply wrapped in a count(*) and unioned with
         * each other.
         */
        private void buildRequests() {
            if (requests.size() == 0) {
                ConsolidationUtils.CONSOLIDATION_LOGGER.debug(
                    "ConsolidatedNativeCountRequest execute no requests");
                return;
            }
            int maxSingle = MAX_UNION_QUERIES;
            int maxMulti = MAX_CONSOLIDATED_QUERIES;
            Dialect dialect;
            Evaluator eval = requests.get(0).getSetEvaluator().getConstraint().getEvaluator();
            if (eval instanceof RolapEvaluator) {
                dialect = ((RolapEvaluator) eval).getDialect();
            } else {
                dialect = DialectManager.createDialect(requests.get(0).getSetEvaluator()
                                                               .getSchemaReader()
                                                               .getDataSource(), null);
            }
            if (!dialect.supportsUnlimitedValueList()) {
                maxMulti = Math.min(MondrianProperties.instance().MaxConstraints.get(), maxMulti);
            }
            Pair<List<List<Pair<RolapNativeSet.NativeCountRequest,
                ConsolidationUtils.LevelMatch>>>,
                List<List<Pair<RolapNativeSet.NativeCountRequest, ConsolidationUtils.LevelMatch>>>>
                orderedRequestsPair = ConsolidationUtils.groupPairs(this, requests, maxSingle,
                maxMulti);
            List<Pair<RolapNativeSet.NativeCountRequest, ConsolidationUtils.LevelMatch>> currSingles
                = new ArrayList<Pair<RolapNativeSet.NativeCountRequest,
                ConsolidationUtils.LevelMatch>>();

            List<List<Pair<RolapNativeSet.NativeCountRequest,
                ConsolidationUtils.LevelMatch>>> singleRequests
                = orderedRequestsPair.left;
            List<List<Pair<RolapNativeSet.NativeCountRequest,
                ConsolidationUtils.LevelMatch>>> orderedRequests
                = orderedRequestsPair.right;


            for (List<Pair<RolapNativeSet.NativeCountRequest,
                ConsolidationUtils.LevelMatch>> orderedRequest : orderedRequests) {

                int singles = 0;
                int multiples = 0;
                int unsatisfiedMultiples = 0;

                multiples++;
                Pair<RolapNativeSet.NativeCountRequest,
                    ConsolidationUtils.LevelMatch> pair = orderedRequest.get(0);
                RolapNativeSet.NativeCountRequest ncr = pair.left;
                RolapNativeSet.SetConstraint constraint
                    = (RolapNativeSet.SetConstraint) ncr.getSetEvaluator().getConstraint();
                // this needs to execute before we grab the column expression
                // because this is determined in the context of a cube and agg star
                SqlQuery root = ncr.getSetEvaluator().getLevelMembersSql(ncr.getTupleReader(),
                    true).left;

                ConsolidationHandler handler = constraint.getConsolidationHandler();
                if (handler == null) {
                    ConsolidationUtils.CONSOLIDATION_LOGGER.debug(
                        "No consolidation handler found:");
                    for (int i = 0; i < orderedRequest.size(); i++) {
                        currSingles.add(orderedRequest.get(i));
                        if (currSingles.size() >= maxSingle) {
                            singleRequests.add(currSingles);
                            currSingles
                                = new ArrayList<Pair<RolapNativeSet.NativeCountRequest,
                                ConsolidationUtils.LevelMatch>>();
                        }
                    }
                    unsatisfiedMultiples++;
                } else {
                    ConsolidationMembers mm = handler.getLevelMembers();
                    if (mm == null
                        || mm.getColumnExpression() == null
                        || mm.getStarColumn() == null) {
                        ConsolidationUtils.CONSOLIDATION_LOGGER.debug(
                            "LevelMambers are null or no column Expression:" + mm);
                        // clear constaint even though we can't actually find the members
                        handler.clearMembers();
                        for (int i = 0; i < orderedRequest.size(); i++) {
                            currSingles.add(orderedRequest.get(i));
                            if (currSingles.size() >= maxSingle) {
                                singleRequests.add(currSingles);
                                currSingles
                                    = new ArrayList<Pair<RolapNativeSet.NativeCountRequest,
                                    ConsolidationUtils.LevelMatch>>();
                            }
                        }
                        unsatisfiedMultiples++;
                    } else {
                        List<RolapMember> allMembers = mm.getAllMembers();
                        if (allMembers.size() != orderedRequest.size()) {
                            ConsolidationUtils.CONSOLIDATION_LOGGER.debug(
                                "LevelMambers size does not match request list size" + mm);
                            // something has gone pear shaped - remove the level members
                            // and fall back to simple consolidation
                            handler.clearMembers();
                            for (int i = 0; i < orderedRequest.size(); i++) {
                                currSingles.add(orderedRequest.get(i));
                                if (currSingles.size() >= maxSingle) {
                                    singleRequests.add(currSingles);
                                    currSingles
                                        = new ArrayList<Pair<RolapNativeSet.NativeCountRequest,
                                        ConsolidationUtils.LevelMatch>>();
                                }
                            }
                            unsatisfiedMultiples++;
                        } else {
                            List<CacheKey> keys = new ArrayList<CacheKey>();
                            for (Pair<RolapNativeSet.NativeCountRequest,
                                ConsolidationUtils.LevelMatch> matchPair : orderedRequest) {
                                keys.add(matchPair.left.getCacheKey());
                            }
                            SqlQuery wrapper = handler.wrap(root,
                                ncr.getSetEvaluator().getSchemaReader().getDataSource(), dialect);
                            ConsolidationUtils.CONSOLIDATION_LOGGER.debug(
                                "SENDING CONSOLIDATED SQL:" + wrapper);
                            consolidatedRequests.add(
                                new CountRequest(ncr, wrapper.toString(), keys, true));
                        }
                    }
                }
                ConsolidationUtils.CONSOLIDATION_LOGGER.debug("Requests contain " +
                    singles + " single requests and " + multiples + " multi requests. " +
                    unsatisfiedMultiples + " had to get combined in a simple way.");

            }
            if (currSingles.size() > 0) {
                singleRequests.add(currSingles);
            }
            if (singleRequests.size() > 0) {
                for (List<Pair<RolapNativeSet.NativeCountRequest,
                    ConsolidationUtils.LevelMatch>> singleRequest : singleRequests) {
                    executeSimpleGroup(singleRequest, dialect);
                }
            }
        }

        /**
         * Group the requests together by simply wrapping the whole query in a count and unioning
         * with the other queries.
         *
         * @param singleRequests
         */
        private void executeSimpleGroup(List<Pair<RolapNativeSet.NativeCountRequest,
            ConsolidationUtils.LevelMatch>> singleRequests,
                                        Dialect dialect) {
            // mostly so that tests will pass unchanged.
            ConsolidationUtils.CONSOLIDATION_LOGGER.debug(
                "ConsolidatedNativeCountRequest executing simple group of size "
                    + singleRequests.size());
            if (singleRequests.size() == 0) {
                return;
            }
            if (singleRequests.size() == 1) {
                RolapNativeSet.NativeCountRequest request = singleRequests.get(0).left;
                consolidatedRequests.add(new CountRequest(request, null, null, false));
            } else {
                boolean pretty = MondrianProperties.instance().GenerateFormattedSql.get();
                String spc = pretty ? Util.nl : " ";
                String indent = pretty ? "    " : "";
                StringBuilder sql = new StringBuilder();
                List<CacheKey> keys = new ArrayList<CacheKey>();
                RolapNativeSet.NativeCountRequest request = null;
                int cnt = 0;
                for (Pair<RolapNativeSet.NativeCountRequest,
                    ConsolidationUtils.LevelMatch> pair : singleRequests) {
                    RolapNativeSet.NativeCountRequest ncr = pair.left;
                    if (request == null) {
                        request = ncr;
                    }
                    sql.append("select").append(spc).append(indent).append("COUNT(*)")
                       .append(spc).append("from").append(spc).append(indent).append("(");
                    sql.append(ncr.getSetEvaluator().getLevelMembersSql(ncr.getTupleReader(),
                        true).left.toString());
                    String alias = dialect.quoteIdentifier("count" + (cnt++));
                    sql.append(" ) as ").append(alias);
                    if (cnt < singleRequests.size()) {
                        sql.append(spc).append("union all").append(spc);
                    }
                    keys.add(ncr.getCacheKey());
                }
                consolidatedRequests.add(new CountRequest(request, sql.toString(), keys, true));
            }
        }

        @Override
        public ConsolidationHandler newConsolidationHandler() {
            return new CountConsolidationHandler();
        }

        private static class CountRequest implements RolapNativeRequest {

            private RolapNativeSet.NativeCountRequest request;
            private String sql;
            private List<CacheKey> keys;
            private boolean isMulti;

            public CountRequest(RolapNativeSet.NativeCountRequest request, String sql,
                                List<CacheKey> keys,
                                boolean isMulti) {
                this.request = request;
                this.sql = sql;
                this.keys = keys;
                this.isMulti = isMulti;
            }

            @Override
            public void execute() {
                if (isMulti) {
                    request.getSetEvaluator()
                           .populateMultiCountCache(request.getTupleReader(), sql, keys);
                } else {
                    request.getSetEvaluator().populateCountCache(request.getTupleReader(),
                        request.getCacheKey());
                }
            }
        }
    }

    static class CountConsolidationHandler extends ConsolidationHandler {

        @Override
        public SqlQuery wrap(SqlQuery configured, DataSource dataSource, Dialect dialect) {
            ConsolidationMembers members = getLevelMembers();
            if (members != null && members.getMembers().size() > 0) {
                String columnExpr = members.getColumnExpression();
                String colAlias = configured.getAlias(columnExpr);
                if (colAlias == null) {
                    // if there is no alias, then create one and add a select and group by
                    colAlias = "m1";
                    configured.addSelect(columnExpr, null, colAlias);
                    configured.addGroupBy(columnExpr, colAlias);
                }
                int cnt = 0;
                SqlQuery wrapper = SqlQuery.newQuery(dataSource, "foobar");
                Dialect.Datatype dt = null;
                List<RolapMember> allMembers = members.getAllMembers();
                for (int i = 0; i < allMembers.size(); i++) {
                    RolapMember allMember = allMembers.get(i);
                    if (dt == null) {
                        dt = allMember.getLevel().getDatatype();
                    }
                    StringBuilder sb = new StringBuilder();
                    dt.quoteValue(sb, dialect, allMember.getKey().toString());
                    String v1 = sb.toString();
                    sb.setLength(0);

                    StringBuilder select = new StringBuilder("count(case when ");
                    select.append(dialect.quoteIdentifier(colAlias))
                          .append(" = ")
                          .append(v1)
                          .append(" then 1 else null end)");
                    wrapper.addSelect(select.toString(), SqlStatement.Type.LONG, "count" + (cnt++));
                
                }
                wrapper.addFrom(configured, "countQuery", true);
                return wrapper;
            }
            return configured;
        }
    }

}
