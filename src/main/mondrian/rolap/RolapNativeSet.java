/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.ResultStyle;
import mondrian.calc.TupleList;
import mondrian.calc.impl.*;
import mondrian.mdx.NamedSetExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.rolap.RolapNativeCount.CountConstraint;
import mondrian.rolap.RolapNativeSum.SumConstraint;
import mondrian.rolap.TupleReader.MemberBuilder;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.cache.*;
import mondrian.rolap.sql.*;

import mondrian.util.Pair;
import org.apache.commons.collections.*;
import org.apache.log4j.Logger;

import java.util.*;

import javax.sql.DataSource;

import static org.apache.commons.collections.CollectionUtils.*;

/**
 * Analyses set expressions and executes them in SQL if possible.
 * Supports crossjoin, member.children, level.members and member.descendants -
 * all in non empty mode, i.e. there is a join to the fact table.<p/>
 *
 * <p>TODO: the order of the result is different from the order of the
 * enumeration. Should sort.
 *
 * @author av
 * @since Nov 12, 2005
  */
public abstract class RolapNativeSet extends RolapNative {
    protected static final Logger LOGGER =
        Logger.getLogger(RolapNativeSet.class);

    private SmartCache<Object, TupleList> cache =
        new SoftSmartCache<Object, TupleList>();

    private SmartCache<Object, Integer> countCache =
        new SoftSmartCache<Object, Integer>();

    private SmartCache<Object, Double> sumCache =
        new SoftSmartCache<Object, Double>();
    /**
     * Returns whether certain member types (e.g. calculated members) should
     * disable native SQL evaluation for expressions containing them.
     *
     * <p>If true, expressions containing calculated members will be evaluated
     * by the interpreter, instead of using SQL.
     *
     * <p>If false, calc members will be ignored and the computation will be
     * done in SQL, returning more members than requested.  This is ok, if
     * the superflous members are filtered out in java code afterwards.
     *
     * @return whether certain member types should disable native SQL evaluation
     */
    protected abstract boolean restrictMemberTypes();

    protected CrossJoinArgFactory crossJoinArgFactory() {
        return new CrossJoinArgFactory(restrictMemberTypes());
    }

    /**
     * This function is used to allow nesting of native functions, for instance
     * TopCount(Filter(NonEmpty())) as an example.
     *
     * @param exp The expression to nest
     * @param evaluator the current evaluation context
     * @return a set evaluator if available for this function
     */
    protected SetEvaluator getNestedEvaluator(Exp exp, RolapEvaluator evaluator) {
        SetEvaluator eval = null;
        if (exp instanceof NamedSetExpr) {
            NamedSet namedSet = ((NamedSetExpr)exp).getNamedSet();
            return getNestedEvaluator(namedSet.getExp(), evaluator);
        }
        if (exp instanceof ResolvedFunCall) {
            ResolvedFunCall call = (ResolvedFunCall)exp;
            if (call.getFunDef().getName().equals("Cache")) {
                if (call.getArg( 0 ) instanceof ResolvedFunCall) {
                    call = (ResolvedFunCall)call.getArg(0);
                } else {
                    return null;
                }
            }
            if (supportsNesting(call)) {
                eval = (SetEvaluator)evaluator.getSchemaReader().getSchema().getNativeRegistry().createEvaluator(
                    evaluator, call.getFunDef(), call.getArgs());
            }
        }
        return eval;
    }

    /**
     * This function checks to make sure that native evaluation is possible for this
     * usecase.  Note that at the moment Existing() is not compatible with nested evaluation.
     *
     * @param fun The function to verify
     * @return true if this function is available for nesting
     */
    protected boolean supportsNesting(ResolvedFunCall fun) {
        if (fun.getFunName().equalsIgnoreCase("existing")) {
            return false;
        } else {
            boolean containsExisting = false;
            for (Exp exp : fun.getArgs()) {
                if (exp instanceof ResolvedFunCall) {
                    if (!supportsNesting((ResolvedFunCall)exp)) {
                        containsExisting = true;
                    }
                }
            }
            return !containsExisting;
        }
    }

    /**
     * Constraint for non empty {crossjoin, member.children,
     * member.descendants, level.members}
     */
    protected static abstract class SetConstraint extends SqlContextConstraint {
        CrossJoinArg[] args;

        ConsolidationHandler consolidationHandler;

        SetConstraint(
            CrossJoinArg[] args,
            RolapEvaluator evaluator,
            boolean strict)
        {
            super(evaluator, strict);
            this.args = args;
        }

        /**
         * The consolidation handler is used by the constraint to inject the SQL query
         * with any members that the handler has been configured with. This happens
         * during the calls to addConstraint and the two variants of addMemberConstaint.
         * @param consolidationHandler the consolidation handler to set
         */
        public void setConsolidationHandler(ConsolidationHandler consolidationHandler) {
            this.consolidationHandler = consolidationHandler;
        }

        /**
         * Get the consolidation handler.
         * @return the consolidation handler or null if it has not been set.
         */
        public ConsolidationHandler getConsolidationHandler() {
            return consolidationHandler;
        }

        /**
         * {@inheritDoc}
         *
         * <p>If there is a crossjoin, we need to join the fact table - even if
         * the evaluator context is empty.
         */
        protected boolean isJoinRequired() {
            return args.length > 1 || super.isJoinRequired();
        }

        /**
         * If no join is required, there is no need to resolve an agg table.
         * specifing an aggstar downstream leads to unnecessary joins during
         * member resolution.
         */
        public boolean skipAggTable() {
            return !isJoinRequired();
        }

        /**
         * Add a SQL constraint. This passes the consolidation handler to SqlConstraintUtils, which
         * will call its configureQuery() method while setting the context contraint.
         * This constraint object will also ask the handler to augment any cross join args with
         * configured members, before calling the args addConstraint() method.
         * @param sqlQuery
         * @param baseCube
         * @param aggStar
         */
        public void addConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar)
        {

            SqlConstraintUtils.addContextConstraint(sqlQuery, aggStar, getEvaluator(), baseCube,
                isStrict(), getConsolidationHandler());
            for (CrossJoinArg arg : args) {
                // if the cross join argument has calculated members in its
                // enumerated set, ignore the constraint since we won't
                // produce that set through the native sql and instead
                // will simply enumerate through the members in the set
                if (!(arg instanceof MemberListCrossJoinArg)
                    || !((MemberListCrossJoinArg) arg).hasCalcMembers())
                {
                    RolapLevel level = arg.getLevel();
                    if (level != null && arg instanceof DescendantsCrossJoinArg) {
                        Level olapLevel = level.getParentLevel();
                        if (olapLevel instanceof RolapLevel) {
                            level = (RolapLevel) olapLevel;
                        }
                    }
                    if (level != null) {
                        RolapStar.Column c = ((RolapCubeLevel) level).getBaseStarKeyColumn(baseCube);
                        RolapEvaluator eval = (RolapEvaluator) getEvaluator();
                        if(getConsolidationHandler() != null) {
                            arg = getConsolidationHandler().augmentCrossJoinArg(sqlQuery, level, c,
                                eval, aggStar, arg);
                        }
                    }
                    if (level == null || levelIsOnBaseCube(baseCube, level)) {
                        arg.addConstraint(sqlQuery, baseCube, aggStar);
                    }
                }
            }
        }

        /**
         * Called from MemberChildren: adds <code>parent</code> to the current
         * context and restricts the SQL resultset to that new context.
         * This passes the consolidation handler to SqlConstaintUtils.addContextConstaint.
         */
        public void addMemberConstraint(SqlQuery sqlQuery, RolapCube baseCube, AggStar aggStar,
            RolapMember parent) {
            if (parent.isCalculated()) {
                throw Util.newInternal("cannot restrict SQL to calculated member");
            }
            final int savepoint = getEvaluator().savepoint();
            try {
                getEvaluator().setContext(parent);
                SqlConstraintUtils.addContextConstraint(sqlQuery, aggStar, getEvaluator(), baseCube,
                    isStrict(), getConsolidationHandler());
            } finally {
                getEvaluator().restore(savepoint);
            }

            // comment out addMemberConstraint here since constraint
            // is already added by addContextConstraint
            // SqlConstraintUtils.addMemberConstraint(
            //        sqlQuery, baseCube, aggStar, parent, true);
        }

        /**
         * Adds <code>parents</code> to the current
         * context and restricts the SQL resultset to that new context.
         */
        public void addMemberConstraint(SqlQuery sqlQuery, RolapCube baseCube, AggStar aggStar,
            List<RolapMember> parents) {
            ConsolidationHandler handler = getConsolidationHandler();
            SqlConstraintUtils.addContextConstraint(sqlQuery, aggStar, getEvaluator(), baseCube,
                isStrict(), handler);
            if (parents.size() > 0) {
                RolapStar.Column column = null;
                RolapLevel memberLevel = parents.get(0).getLevel();
                if (memberLevel instanceof RolapCubeLevel) {
                    column = ((RolapCubeLevel) memberLevel).getBaseStarKeyColumn(baseCube);
                    if (column != null && handler != null) {
                        handler.augmentParentMembers(column, sqlQuery, aggStar, parents);
                    }
                }
            }
            SqlConstraintUtils.addMemberConstraint(sqlQuery, baseCube, aggStar, parents, true, false, false);
        }

        protected boolean levelIsOnBaseCube(
            final RolapCube baseCube, final RolapLevel level)
        {
            return baseCube.findBaseCubeHierarchy(level.getHierarchy()) != null;
        }

        /**
         * Returns null to prevent the member/childern from being cached. There
         * exists no valid MemberChildrenConstraint that would fetch those
         * children that were extracted as a side effect from evaluating a non
         * empty crossjoin
         */
        public MemberChildrenConstraint getMemberChildrenConstraint(
            RolapMember parent)
        {
            return null;
        }

        /**
         * Used by SqlTupleReader for specific native functions to determine
         * if an Aggregate table can be utilized for SQL generation.
         *
         * @param baseCube the main cube related to the active measure
         * @param levelBitKey will be populated with various extra level bits
         */
        public void constrainExtraLevels(
            RolapCube baseCube,
            BitKey levelBitKey)
        {
            // noop
        }

        /**
         * This returns a CacheKey object
         */
        public Object getCacheKey() {
            CacheKey key = new CacheKey((CacheKey) super.getCacheKey());
            // only add args that will be retrieved through native sql;
            // args that are sets with calculated members aren't executed
            // natively
            List<CrossJoinArg> crossJoinArgs = new ArrayList<CrossJoinArg>();
            for (CrossJoinArg arg : args) {
                if (!(arg instanceof MemberListCrossJoinArg)
                    || !((MemberListCrossJoinArg) arg).hasCalcMembers())
                {
                    crossJoinArgs.add(arg);
                }
            }
            key.setCrossJoinArgs(crossJoinArgs);
            return key;
        }

        /**
         * Returns args
         */
        public CrossJoinArg[] getArgs() {
            return args;
        }
    }

    /**
     * This is an optionally delegating set constraint used for nested native evaluation.
     */
    protected static abstract class DelegatingSetConstraint extends SetConstraint {
        protected SetConstraint parentConstraint;

        DelegatingSetConstraint(
            CrossJoinArg[] args,
            RolapEvaluator evaluator,
            boolean strict,
            SetConstraint parentConstraint)
        {
            super(args, evaluator, strict);
            this.args = args;
            this.parentConstraint = parentConstraint;
        }

        protected boolean isJoinRequired() {
            if (parentConstraint != null) {
                return parentConstraint.isJoinRequired();
            } else {
                return super.isJoinRequired();
            }
        }

        public void addConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar)
        {
            if (parentConstraint != null) {
                parentConstraint.addConstraint(sqlQuery, baseCube, aggStar);
            } else {
                super.addConstraint(sqlQuery, baseCube, aggStar);
            }
        }

        public Object getCacheKey() {
            if (parentConstraint != null) {
                return parentConstraint.getCacheKey();
            } else {
                return super.getCacheKey();
            }
        }



        public MemberChildrenConstraint getMemberChildrenConstraint(
            RolapMember parent)
        {
            if (parentConstraint != null) {
                return parentConstraint.getMemberChildrenConstraint(parent);
            } else {
                return super.getMemberChildrenConstraint(parent);
            }
        }

        public void constrainExtraLevels(
            RolapCube baseCube,
            BitKey levelBitKey)
        {
            if (parentConstraint != null) {
                parentConstraint.constrainExtraLevels(baseCube, levelBitKey);
            } else {
                super.constrainExtraLevels(baseCube, levelBitKey);
            }
        }

        /**
         * Returns args
         */
        public CrossJoinArg[] getArgs() {
            return parentConstraint != null ? parentConstraint.getArgs() : super.getArgs();
        }
    }

    private boolean multiThreaded = MondrianProperties.instance().SegmentCacheManagerNumberNativeThreads.get() > 0;

    protected class SetEvaluator implements NativeEvaluator {

        public static final String KEY_SET_EVALUATOR_CROSSJOIN_ARGS = "mondrian.rolap.RolapNativeSet.SetEvaluator.crossjoin.args";
        public static final String KEY_SET_EVALUATOR_MAX_ROWS = "mondrian.rolap.RolapNativeSet.SetEvaluator.max.rows";
        private final CrossJoinArg[] args;
        private final SchemaReaderWithMemberReaderAvailable schemaReader;
        private TupleConstraint constraint;
        private int maxRows = 0;
        private Member measure = null;

        public SetEvaluator(
            CrossJoinArg[] args,
            SchemaReader schemaReader,
            TupleConstraint constraint,
            Member measure
            )
        {
          this(args, schemaReader, constraint);
          this.measure = measure;
        }

        public SetEvaluator(
            CrossJoinArg[] args,
            SchemaReader schemaReader,
            TupleConstraint constraint)
        {
            this.args = args;
            if (schemaReader instanceof SchemaReaderWithMemberReaderAvailable) {
                this.schemaReader =
                    (SchemaReaderWithMemberReaderAvailable) schemaReader;
            } else {
                this.schemaReader =
                    new SchemaReaderWithMemberReaderCache(schemaReader);
            }
            this.constraint = constraint;
        }

        /**
         * Used by native evaluation verifying that a nested native set exists in the same cube as others.
         * See RolapNativeSubset.
         *
         * @return The measure related to this set evaluation.
         */
        public Member getMeasure() {
            return measure;
        }

        public TupleConstraint getConstraint() {
            return constraint;
        }

        public void setConstraint(TupleConstraint constraint) {
            this.constraint = constraint;
        }

        public CrossJoinArg[] getArgs() {
            return args;
        }

        public SchemaReader getSchemaReader() {
            return schemaReader;
        }

        public Object execute(ResultStyle desiredResultStyle) {
            switch (desiredResultStyle) {
            case ITERABLE:
                for (CrossJoinArg arg : this.args) {
                    if (arg.getLevel().getDimension().isHighCardinality()) {
                        // If any of the dimensions is a HCD,
                        // use the proper tuple reader.
                        return executeList(
                            new HighCardSqlTupleReader(constraint));
                    }
                    // Use the regular tuple reader.
                    return executeList(
                        new SqlTupleReader(constraint));
                }
            case MUTABLE_LIST:
            case LIST:
                return executeList(new SqlTupleReader(constraint));
            case VALUE:
                if (constraint instanceof SumConstraint) {
                    return executeSum(new SqlTupleReader(constraint));
                } else {
                    return executeCount(new SqlTupleReader(constraint));
                }
            default:
                throw ResultStyleException.generate(
                    ResultStyle.ITERABLE_MUTABLELIST_LIST,
                    Collections.singletonList(desiredResultStyle));
            }
        }

        protected TupleList executeList(final SqlTupleReader tr) {
            tr.setMaxRows(maxRows);
            for (CrossJoinArg arg : args) {
                addLevel(tr, arg);
            }

            CacheKey key = createCacheKey(tr);

            TupleList result = cache.get(key);
            boolean hasEnumTargets = (tr.getEnumTargetCount() > 0);
            if (result != null && !hasEnumTargets) {
                if (listener != null) {
                    TupleEvent e = new TupleEvent(this, tr);
                    listener.foundInCache(e);
                }
                return new DelegatingTupleList(
                    args.length, Util.<List<Member>>cast(result));
            }

            // instead of executing this now,
            // store this request in the cell reader,
            // to be executed in a separate thread.  return a dummy empty list for now.
            // TODO: figure out partial result case

            if (multiThreaded && !hasEnumTargets && !MondrianProperties.instance().DisableCaching.get()) {
                // register this for separate thread execution
                tr.constraint.getEvaluator().addNativeRequest(
                    new NativeRequest(this, key, tr));
                TupleList dummy = new ListTupleList(args.length, new ArrayList<Member>());
                return dummy;
            }
            // execute sql and store the result
            if (result == null && listener != null) {
                TupleEvent e = new TupleEvent(this, tr);
                listener.executingSql(e);
            }

            // if we don't have a cached result in the case where we have
            // enumerated targets, then retrieve and cache that partial result
            TupleList partialResult = result;
            List<List<RolapMember>> newPartialResult = null;
            if (hasEnumTargets && partialResult == null) {
                newPartialResult = new ArrayList<List<RolapMember>>();
            }
            DataSource dataSource = schemaReader.getDataSource();
            if (args.length == 1) {
                result =
                    tr.readMembers(
                        dataSource, partialResult, newPartialResult);
            } else {
                result =
                    tr.readTuples(
                        dataSource, partialResult, newPartialResult);
            }

            if (!MondrianProperties.instance().DisableCaching.get()) {
                if (hasEnumTargets) {
                    if (newPartialResult != null) {
                        cache.put(
                            key,
                            new DelegatingTupleList(
                                args.length,
                                Util.<List<Member>>cast(newPartialResult)));
                    }
                } else {
                    cache.put(key, result);
                }
            }
            return filterInaccessibleTuples(result);
        }

        /**
         * Checks access rights and hidden status on the members
         * in each tuple in tupleList.
         */
        private TupleList filterInaccessibleTuples(TupleList tupleList) {
            if (needsFiltering(tupleList)) {
                final Predicate memberInaccessible =
                    memberInaccessiblePredicate();
                filter(
                    tupleList, tupleAccessiblePredicate(memberInaccessible));
            }
            return tupleList;
        }

        private boolean needsFiltering(TupleList tupleList) {
            return tupleList.size() > 0
                   && exists(tupleList.get(0), needsFilterPredicate());
        }

        private Predicate needsFilterPredicate() {
            return new Predicate() {
                public boolean evaluate(Object o) {
                    Member member = (Member) o;
                    return isRaggedLevel(member.getLevel())
                           || isCustomAccess(member.getHierarchy());
                }
            };
        }

        private boolean isRaggedLevel(Level level) {
            if (level instanceof RolapLevel) {
                return ((RolapLevel) level).getHideMemberCondition()
                       != RolapLevel.HideMemberCondition.Never;
            }
            // don't know if it's ragged, so assume it is.
            // should not reach here
            return true;
        }

        private boolean isCustomAccess(Hierarchy hierarchy) {
            if (constraint.getEvaluator() == null) {
                return false;
            }
            Access access =
                constraint
                    .getEvaluator()
                    .getSchemaReader()
                    .getRole()
                    .getAccess(hierarchy);
            return access == Access.CUSTOM;
        }

        private Predicate memberInaccessiblePredicate() {
            if (constraint.getEvaluator() != null) {
                return new Predicate() {
                    public boolean evaluate(Object o) {
                        Role role =
                            constraint
                                .getEvaluator().getSchemaReader().getRole();
                        Member member = (Member) o;
                        return member.isHidden() || !role.canAccess(member);
                    }
                };
            }
            return new Predicate() {
                public boolean evaluate(Object o) {
                    return ((Member) o).isHidden();
                }
            };
        }

        private Predicate tupleAccessiblePredicate(
            final Predicate memberInaccessible)
        {
            return new Predicate() {
                @SuppressWarnings("unchecked")
                public boolean evaluate(Object o) {
                    return !exists((List<Member>) o, memberInaccessible);
                }};
        }

        public void populateListCache(final SqlTupleReader tr, CacheKey key) {
            if (listener != null) {
                TupleEvent e = new TupleEvent(this, tr);
                listener.executingSql(e);
            }

            // if we don't have a cached result in the case where we have
            // enumerated targets, then retrieve and cache that partial result
            TupleList result;
            List<List<RolapMember>> newPartialResult = null;
            DataSource dataSource = schemaReader.getDataSource();
            if (args.length == 1) {
                result =
                    tr.readMembers(
                        dataSource, null, null);
            } else {
                result =
                    tr.readTuples(
                        dataSource, null, null);
            }
            cache.put(key, result);
            filterInaccessibleTuples(result);
        }

        public void populateSumCache(final SqlTupleReader tr, CacheKey key) {
            DataSource dataSource = schemaReader.getDataSource();
            double sum = ((SqlTupleReader)tr).sumTuples(dataSource);
            if (!MondrianProperties.instance().DisableCaching.get()) {
                sumCache.put(key, new Double(sum));
            }
        }

        protected double executeSum(final SqlTupleReader tr) {
            tr.setMaxRows(maxRows);
            for (CrossJoinArg arg : args) {
                addLevel(tr, arg);
            }
            CacheKey key = createCacheKey(tr);
            Double result = sumCache.get(key);
            if (result != null) {
              // TODO: Add Listener Interface?  Is this for testing only?
  //              if (listener != null) {
  //                  TupleEvent e = new TupleEvent(this, tr);
  //                  listener.foundInCache(e);
  //              }
                return result;
            }

            if (multiThreaded && !MondrianProperties.instance().DisableCaching.get()) {
                tr.constraint.getEvaluator().addNativeRequest(
                    new NativeSumRequest(this, key, tr));
                return 0.0;
            }

            // execute sql and store the result
  //          if (result == null && listener != null) {
  //              TupleEvent e = new TupleEvent(this, tr);
  //              listener.executingSql(e);
  //          }

            DataSource dataSource = schemaReader.getDataSource();
            double sum = ((SqlTupleReader)tr).sumTuples(dataSource);
            if (!MondrianProperties.instance().DisableCaching.get()) {
                sumCache.put(key, new Double(sum));
            }

            // TODO: Note, this method won't work against secured tuples, we
            // should have already detected this earlier in the process.
            // filterInaccessibleTuples(result)
            return  sum;
        }

        public void populateCountCache(final SqlTupleReader tr, CacheKey key) {
            DataSource dataSource = schemaReader.getDataSource();
            int count = ((SqlTupleReader)tr).countTuples(dataSource);

            if (tr.constraint instanceof CountConstraint) {
                // adds calc and all members to the overall count.
                count += ((CountConstraint)tr.constraint).addlCount;
            }

            if (!MondrianProperties.instance().DisableCaching.get()) {
                countCache.put(key, new Integer(count));
            }
        }


        /**
         * This executes a SQL query that will return multiple count results in one call. For each
         * returned result, the result is cached using the list of keys at the index of the
         * returned
         * result.
         *
         * @param tr    the SqlTupleReader
         * @param query the SQL query
         * @param keys  the list of cache keys
         */
        public void populateMultiCountCache(final SqlTupleReader tr, String query,
                                            List<CacheKey> keys) {
            DataSource dataSource = schemaReader.getDataSource();
            List<Integer> counts = tr.multiCountTuples(dataSource, query);

            assert counts.size() == keys.size() :
                "multi count did not return the same number counts as there are keys. Counts:"
                    + counts.size()
                    + " Keys:"
                    + keys.size();

            if (!MondrianProperties.instance().DisableCaching.get()) {
                for (int i = 0; i < keys.size(); i++) {
                    int count = counts.get(i);
                    if (tr.constraint instanceof CountConstraint) {
                        // adds calc and all members to the overall count.
                        count += ((CountConstraint) tr.constraint).addlCount;
                    }
                    CacheKey key = keys.get(i);
                    countCache.put(key, count);
                }
            }
        }

        /**
         * This executes a SQL query that will return multiple sum results in one call. For each
         * returned result, the result is cached using the list of keys at the index of the
         * returned
         * result.
         *
         * @param tr    the SqlTupleReader
         * @param query the SQL query
         * @param keys  the list of cache keys
         */
        public void populateMultiSumCache(final SqlTupleReader tr, String query,
                                          List<CacheKey> keys) {
            DataSource dataSource = schemaReader.getDataSource();
            List<Double> sums = tr.multiSumTuples(dataSource, query);

            assert sums.size() == keys.size() :
                "multi sum did not return the same number counts as there are keys. Counts:"
                    + sums.size()
                    + " Keys:"
                    + keys.size();

            if (!MondrianProperties.instance().DisableCaching.get()) {
                for (int i = 0; i < keys.size(); i++) {

                    CacheKey key = keys.get(i);
                    sumCache.put(key, sums.get(i));
                }
            }
        }

        public SqlQuery getSumSql(SqlTupleReader tr) {
            return tr.getSumSql(schemaReader.getDataSource());
        }

        public SqlQuery getUnwrappedSumSql(SqlTupleReader tr) {
            return tr.getUnwrappedSumSql(schemaReader.getDataSource());
        }


        public Pair<SqlQuery, List<SqlStatement.Type>> getLevelMembersSql(SqlTupleReader tr,
                                                                          boolean keyOnly) {
            return tr.getLevelMembersSql(schemaReader.getDataSource(), keyOnly);
        }

        protected int executeCount(final SqlTupleReader tr) {
            tr.setMaxRows(maxRows);
            for (CrossJoinArg arg : args) {
                addLevel(tr, arg);
            }
            CacheKey key = createCacheKey(tr);

            Integer result = countCache.get(key);
            if (result != null) {
              // TODO: Add Listener Interface?  Is this for testing only?
  //              if (listener != null) {
  //                  TupleEvent e = new TupleEvent(this, tr);
  //                  listener.foundInCache(e);
  //              }
                return result;
            }

            if (multiThreaded && !MondrianProperties.instance().DisableCaching.get()) {
              tr.constraint.getEvaluator().addNativeRequest(new NativeCountRequest(
                  this, key, tr));
              return 0;
            }

            // execute sql and store the result
  //          if (result == null && listener != null) {
  //              TupleEvent e = new TupleEvent(this, tr);
  //              listener.executingSql(e);
  //          }

            DataSource dataSource = schemaReader.getDataSource();
            int count = ((SqlTupleReader)tr).countTuples(dataSource);

            if (tr.constraint instanceof CountConstraint) {
                // adds calc and all members to the overall count.
                count += ((CountConstraint)tr.constraint).addlCount;
            }

            if (!MondrianProperties.instance().DisableCaching.get()) {
                countCache.put(key, new Integer(count));
            }

            // TODO: Note, this method won't work against secured tuples, we 
            // should have already detected this eariler in the process.
            // filterInaccessibleTuples(result)
            return  count;
        }

        protected CacheKey createCacheKey(SqlTupleReader tr) {
            // Look up the result in cache; we can't return the cached
            // result if the tuple reader contains a target with calculated
            // members because the cached result does not include those
            // members; so we still need to cross join the cached result
            // with those enumerated members.
            //
            // The key needs to include the arguments (projection) as well as
            // the constraint, because it's possible (see bug MONDRIAN-902)
            // that independent axes have identical constraints but different
            // args (i.e. projections). REVIEW: In this case, should we use the
            // same cached result and project different columns?
            CacheKey key = tr.getCacheKey();

            key.setValue(KEY_SET_EVALUATOR_CROSSJOIN_ARGS, Arrays.asList(args));
            key.setValue(KEY_SET_EVALUATOR_MAX_ROWS, maxRows);
            return key;
        }

        private void addLevel(TupleReader tr, CrossJoinArg arg) {
            RolapLevel level = arg.getLevel();
            if (level == null) {
                // Level can be null if the CrossJoinArg represent
                // an empty set.
                // This is used to push down the "1 = 0" predicate
                // into the emerging CJ so that the entire CJ can
                // be natively evaluated.
                tr.incrementEmptySets();
                return;
            }

            RolapHierarchy hierarchy = level.getHierarchy();
            MemberReader mr = schemaReader.getMemberReader(hierarchy);
            MemberBuilder mb = mr.getMemberBuilder();
            Util.assertTrue(mb != null, "MemberBuilder not found");

            if (arg instanceof MemberListCrossJoinArg
                && ((MemberListCrossJoinArg) arg).hasCalcMembers())
            {
                // only need to keep track of the members in the case
                // where there are calculated members since in that case,
                // we produce the values by enumerating through the list
                // rather than generating the values through native sql
                tr.addLevelMembers(level, mb, arg.getMembers());
            } else {
                tr.addLevelMembers(level, mb, null);
            }
        }

        int getMaxRows() {
            return maxRows;
        }

        void setMaxRows(int maxRows) {
            this.maxRows = maxRows;
        }
    }

    /**
     * Tests whether non-native evaluation is preferred for the
     * given arguments.
     *
     * @param joinArg true if evaluating a cross-join; false if
     * evaluating a single-input expression such as filter
     *
     * @return true if <em>all</em> args prefer the interpreter
     */
    protected boolean isPreferInterpreter(
        CrossJoinArg[] args,
        boolean joinArg)
    {
        for (CrossJoinArg arg : args) {
            if (!arg.isPreferInterpreter(joinArg)) {
                return false;
            }
        }
        return true;
    }

    /** disable garbage collection for test */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    void useHardCache(boolean hard) {
        if (hard) {
            cache = new HardSmartCache();
            countCache = new HardSmartCache();
            sumCache = new HardSmartCache();
        } else {
            cache = new SoftSmartCache();
            countCache = new SoftSmartCache();
            sumCache = new SoftSmartCache();
        }
    }

    /**
     * Overrides current members in position by default members in
     * hierarchies which are involved in this filter/topcount.
     * Stores the RolapStoredMeasure into the context because that is needed to
     * generate a cell request to constraint the sql.
     *
     * <p>The current context may contain a calculated measure, this measure
     * was translated into an sql condition (filter/topcount). The measure
     * is not used to constrain the result but only to access the star.
     *
     * @param evaluator Evaluation context to modify
     * @param cargs Cross join arguments
     * @param storedMeasure Stored measure
     *
     * @see RolapAggregationManager#makeRequest(RolapEvaluator)
     */
    protected void overrideContext(
        RolapEvaluator evaluator,
        CrossJoinArg[] cargs,
        RolapStoredMeasure storedMeasure)
    {
        SchemaReader schemaReader = evaluator.getSchemaReader();
        for (CrossJoinArg carg : cargs) {
            RolapLevel level = carg.getLevel();
            if (level != null) {
                RolapHierarchy hierarchy = level.getHierarchy();

                final Member contextMember;
                if (hierarchy.hasAll()
                    || schemaReader.getRole()
                    .getAccess(hierarchy) == Access.ALL)
                {
                    // The hierarchy may have access restrictions.
                    // If it does, calling .substitute() will retrieve an
                    // appropriate LimitedRollupMember.
                    contextMember =
                        schemaReader.substitute(hierarchy.getAllMember());
                } else {
                    // If there is no All member on a role restricted hierarchy,
                    // use a restricted member based on the set of accessible
                    // root members.
                    contextMember = new RestrictedMemberReader
                        .MultiCardinalityDefaultMember(
                            hierarchy.getMemberReader()
                                .getRootMembers().get(0));
                }
                evaluator.setContext(contextMember);
            }
        }
        if (storedMeasure != null) {
            evaluator.setContext(storedMeasure);

            // if a specific measure is in play, we need to force
            // the native evaluation to only focus on it's referenced
            // cube.
            if (evaluator.getCube().isVirtual()) {
                List<RolapCube> baseCubes = new ArrayList<RolapCube>();
                baseCubes.add(storedMeasure.getCube());
                evaluator.setBaseCubes(baseCubes);
            }
        }
    }

    /**
     * checkCrossJoinArg returns a list of CrossJoinArg arrays.  The first
     * array is the CrossJoin dimensions.  The second array, if any,
     * contains additional constraints on the dimensions. If either the list
     * or the first array is null, then native cross join is not feasible.
     * @param args list of CrossJoinArg arrays
     * @return whether arguments are valid
     */
    protected static boolean failedCjArg(List<CrossJoinArg[]> args) {
        return args == null || args.isEmpty() || args.get(0) == null;
    }

    protected static void alertNonNative(
        RolapEvaluator evaluator,
        FunDef fun,
        Exp offendingArg)
    {
        if (!evaluator.getQuery().shouldAlertForNonNative(fun)) {
            return;
        }
        RolapUtil.alertNonNative(
            fun.getName(),
            "set argument " + offendingArg.toString());
    }

    public interface SchemaReaderWithMemberReaderAvailable
        extends SchemaReader
    {
        MemberReader getMemberReader(Hierarchy hierarchy);
    }

    private static class SchemaReaderWithMemberReaderCache
        extends DelegatingSchemaReader
        implements SchemaReaderWithMemberReaderAvailable
    {
        private final Map<Hierarchy, MemberReader> hierarchyReaders =
            new HashMap<Hierarchy, MemberReader>();

        SchemaReaderWithMemberReaderCache(SchemaReader schemaReader) {
            super(schemaReader);
        }

        public synchronized MemberReader getMemberReader(Hierarchy hierarchy) {
            MemberReader memberReader = hierarchyReaders.get(hierarchy);
            if (memberReader == null) {
                memberReader =
                    ((RolapHierarchy) hierarchy).createMemberReader(
                        schemaReader.getRole());
                hierarchyReaders.put(hierarchy, memberReader);
            }
            return memberReader;
        }
    }

    /**
     * This is the native request for multi-threaded processing in FastBatchingCellReader
     */
    public static class NativeRequest implements RolapNativeRequest {
        RolapNativeSet.SetEvaluator eval;
        CacheKey key;
        SqlTupleReader tr;

        public NativeRequest(RolapNativeSet.SetEvaluator eval, CacheKey key, SqlTupleReader tr) {
            this.eval = eval;
            this.key = key;
            this.tr = tr;
        }

        public void execute() {
            eval.populateListCache(tr, key);
        }

        public SetEvaluator getSetEvaluator() {
            return eval;
        }

        public CacheKey getCacheKey() {
            return key;
        }

        public SqlTupleReader getTupleReader() {
            return tr;
        }

        /**
         * two requests are considered identical if their keys match.
         * This prevents duplicate requests to be executed in parallel.
         */
        public boolean equals(Object other) {
            if (other instanceof NativeRequest) {
                return ((NativeRequest)other).key.equals(key);
            }
            return false;
        }
    }

    /**
     * This is the native sum request for multi-threaded processing in FastBatchingCellReader
     */
    public static class NativeSumRequest extends NativeRequest {
        public NativeSumRequest(RolapNativeSet.SetEvaluator eval, CacheKey key, SqlTupleReader tr) {
            super(eval, key, tr);
        }

        public void execute() {
            eval.populateSumCache(tr, key);
        }
    }

    /**
     * This is the native count request for multi-threaded processing in FastBatchingCellReader
     */
    public static class NativeCountRequest extends NativeRequest {
        public NativeCountRequest(RolapNativeSet.SetEvaluator eval, CacheKey key, SqlTupleReader tr) {
            super(eval, key, tr);
        }

        public void execute() {
            eval.populateCountCache(tr, key);
        }
    }

}

// End RolapNativeSet.java

