/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2016 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.sql;

import mondrian.olap.*;
import mondrian.rolap.*;
import mondrian.spi.Dialect;
import mondrian.spi.DialectManager;
import mondrian.util.Pair;

import java.util.*;
import java.util.Map.Entry;

import javax.sql.DataSource;

/**
 * <code>SqlQuery</code> allows us to build a <code>select</code>
 * statement and generate it in database-specific SQL syntax.
 *
 * <p> Notable differences in database syntax are:<dl>
 *
 * <dt> Identifier quoting </dt>
 * <dd> Oracle (and all JDBC-compliant drivers) uses double-quotes,
 * for example <code>select * from "emp"</code>. Access prefers brackets,
 * for example <code>select * from [emp]</code>. mySQL allows single- and
 * double-quotes for string literals, and therefore does not allow
 * identifiers to be quoted, for example <code>select 'foo', "bar" from
 * emp</code>. </dd>
 *
 * <dt> AS in from clause </dt>
 * <dd> Oracle doesn't like AS in the from * clause, for example
 * <code>select from emp as e</code> vs. <code>select * from emp
 * e</code>. </dd>
 *
 * <dt> Column aliases </dt>
 * <dd> Some databases require that every column in the select list
 * has a valid alias. If the expression is an expression containing
 * non-alphanumeric characters, an explicit alias is needed. For example,
 * Oracle will barfs at <code>select empno + 1 from emp</code>. </dd>
 *
 * <dt> Parentheses around table names </dt>
 * <dd> Oracle doesn't like <code>select * from (emp)</code> </dd>
 *
 * <dt> Queries in FROM clause </dt>
 * <dd> PostgreSQL and hsqldb don't allow, for example, <code>select * from
 * (select * from emp) as e</code>.</dd>
 *
 * <dt> Uniqueness of index names </dt>
 * <dd> In PostgreSQL and Oracle, index names must be unique within the
 * database; in Access and hsqldb, they must merely be unique within their
 * table </dd>
 *
 * <dt> Datatypes </dt>
 * <dd> In Oracle, BIT is CHAR(1), TIMESTAMP is DATE.
 *      In PostgreSQL, DOUBLE is DOUBLE PRECISION, BIT is BOOL. </dd>
 * </ul>
 *
 * <p>
 * NOTE: Instances of this class are NOT thread safe so the user must make
 * sure this is accessed by only one thread at a time.
 *
 * @author jhyde
 */
public class SqlQuery {
    /** Controls the formatting of the sql string. */
    private final boolean generateFormattedSql;

    private boolean distinct;

    private final ClauseList select;
    private final FromClauseList from;
    public final ClauseList where;
    private final ClauseList groupBy;
    private final ClauseList having;
    private final ClauseList orderBy;
    private final ClauseList with;
    private final List<ClauseList> groupingSets;
    private final ClauseList groupingFunctions;
    private Integer offset;
    private Integer limit;
    private boolean enableDistinctSubquery;

    private final List<SqlStatement.Type> types =
        new ArrayList<SqlStatement.Type>();

    /** Controls whether table optimization hints are used */
    private boolean allowHints;

    /** Is query supported by database vendor. Default is true*/
    private boolean isSupported = true;

    /**
     * This list is used to keep track of what aliases have been  used in the
     * FROM clause. One might think that a java.util.Set would be a more
     * appropriate Collection type, but if you only have a couple of "from
     * aliases", then iterating over a list is faster than doing a hash lookup
     * (as is used in java.util.HashSet).
     */
    private final List<String> fromAliases;

    /** The SQL dialect this query is to be generated in. */
    private final Dialect dialect;

    /** Scratch buffer. Clear it before use. */
    private final StringBuilder buf;

    private final Set<MondrianDef.Relation> relations =
        new HashSet<MondrianDef.Relation>();

    private final Map<MondrianDef.Relation, MondrianDef.RelationOrJoin>
        mapRelationToRoot =
        new HashMap<MondrianDef.Relation, MondrianDef.RelationOrJoin>();

    private final Map<MondrianDef.RelationOrJoin, List<RelInfo>>
        mapRootToRelations =
        new HashMap<MondrianDef.RelationOrJoin, List<RelInfo>>();

    private final Map<String, String> columnAliases =
        new HashMap<String, String>();

    private static final String INDENT = "    ";

    /**
     * Base constructor used by all other constructors to create an empty
     * instance.
     *
     * @param dialect Dialect
     * @param formatted Whether to generate SQL formatted on multiple lines
     */
    public SqlQuery(Dialect dialect, boolean formatted) {
        assert dialect != null;
        this.generateFormattedSql = formatted;

        // both select and from allow duplications
        this.select = new ClauseList(true);
        this.with = new ClauseList(true);
        this.from = new FromClauseList(true);

        this.groupingFunctions = new ClauseList(false);
        this.where = new ClauseList(false);
        this.groupBy = new ClauseList(false);
        this.having = new ClauseList(false);
        this.orderBy = new ClauseList(false);
        this.fromAliases = new ArrayList<String>();
        this.buf = new StringBuilder(128);
        this.groupingSets = new ArrayList<ClauseList>();
        this.dialect = dialect;

        // REVIEW emcdermid 10-Jul-2009: It might be okay to allow
        // hints in all cases, but for initial implementation this
        // allows us to them on selectively in specific situations.
        // Usage will likely expand with experimentation.
        this.allowHints = false;
    }

    /**
     * Creates a SqlQuery using a given dialect and inheriting the formatting
     * preferences from {@link MondrianProperties#GenerateFormattedSql}
     * property.
     *
     * @param dialect Dialect
     */
    public SqlQuery(Dialect dialect) {
        this(
            dialect,
            MondrianProperties.instance().GenerateFormattedSql.get());
    }

    /**
     * Creates an empty <code>SqlQuery</code> with the same environment as this
     * one. (As per the Gang of Four 'prototype' pattern.)
     */
    public SqlQuery cloneEmpty()
    {
        return new SqlQuery(dialect);
    }

    public void setDistinct(final boolean distinct) {
        this.distinct = distinct;
    }

    /**
     * Informs processes that call various methods how to interact
     * with the SQL Query related to many to many dimensions.
     * @return
     */
    public boolean getEnableDistinctSubquery() {
      return enableDistinctSubquery;
    }

    public void setEnableDistinctSubquery(boolean enableDistinctSubquery) {
      this.enableDistinctSubquery = enableDistinctSubquery;
    }

    /**
     * Chooses whether table optimization hints may be used
     * (assuming the dialect supports it).
     *
     * @param t True to allow hints to be used, false otherwise
     */
    public void setAllowHints(boolean t) {
        this.allowHints = t;
    }

    /**
     * If supported by the dialect, set an offset for the query.
     */
    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    /**
     * Return the offset value if any
     *
     * @return offset
     */
    public Integer getOffset() {
        return offset;
    }

    /**
     * If supported by the dialect, set a limit for the query.
     */
    public void setLimit(Integer limit) {
      this.limit = limit;
    }

    /**
     * Return the limit value if any
     *
     * @return limit
     */
    public Integer getLimit() {
      return limit;
    }

    /**
     * Adds a subquery to the FROM clause of this Query with a given alias.
     * If the query already exists it either, depending on
     * <code>failIfExists</code>, throws an exception or does not add the query
     * and returns false.
     *
     * @param query Subquery
     * @param alias (if not null, must not be zero length).
     * @param failIfExists if true, throws exception if alias already exists
     * @return true if query *was* added
     *
     * @pre alias != null
     */
    public boolean addFromQuery(
        final String query,
        final String alias,
        final boolean failIfExists)
    {
        assert alias != null;
        assert alias.length() > 0;

        if (containsFromAlias(alias)) {
            if (failIfExists) {
                throw Util.newInternal(
                    "query already contains alias '" + alias + "'");
            } else {
                return false;
            }
        }

        String q = query;
        if (!dialect.supportsOrderInSubqueries()) {
            int start = query.lastIndexOf(" order by ");
            if (start > -1) {
                int end = dialect.supportsLimitAndOffset()
                    ? query.contains(" limit ")
                        ? query.lastIndexOf(" limit ")
                        : query.contains(" offset ")
                            ? query.lastIndexOf(" offset ")
                            : -1
                    : -1;
                q = query.substring(0, start);
                if (end > -1) {
                    q += query.substring(end);
                }
            }
        }

        buf.setLength(0);

        buf.append('(');
        buf.append(q);
        buf.append(')');
        if (dialect.allowsAs()) {
            buf.append(" as ");
        } else {
            buf.append(' ');
        }
        dialect.quoteIdentifier(alias, buf);
        fromAliases.add(alias);

        from.add(buf.toString());
        return true;
    }

    /**
     * Adds <code>[schema.]table AS alias</code> to the FROM clause.
     *
     * @param schema schema name; may be null
     * @param name table name
     * @param alias table alias, may not be null
     *              (if not null, must not be zero length).
     * @param filter Extra filter condition, or null
     * @param hints table optimization hints, if any
     * @param failIfExists Whether to throw a RuntimeException if from clause
     *   already contains this alias
     *
     * @pre alias != null
     * @return true if table was added
     */
    boolean addFromTable(
        final String schema,
        final String name,
        final String alias,
        final String filter,
        final Map hints,
        final boolean failIfExists)
    {
        if (containsFromAlias(alias)) {
            if (failIfExists) {
                throw Util.newInternal(
                    "query already contains alias '" + alias + "'");
            } else {
                return false;
            }
        }

        buf.setLength(0);
        dialect.quoteIdentifier(buf, schema, name);
        if (alias != null) {
            Util.assertTrue(alias.length() > 0);

            if (dialect.allowsAs()) {
                buf.append(" as ");
            } else {
                buf.append(' ');
            }
            dialect.quoteIdentifier(alias, buf);
            fromAliases.add(alias);
        }

        if (this.allowHints) {
            dialect.appendHintsAfterFromClause(buf, hints);
        }

        from.add(buf.toString());

        if (filter != null) {
            // append filter condition to where clause
            addWhere("(", filter, ")");
        }
        return true;
    }

    public void addFrom(
        final SqlQuery sqlQuery,
        final String alias,
        final boolean failIfExists)
    {
        ClauseList prevOrder = null;
        if (!sqlQuery.getDialect().supportsOrderInSubqueries()
            && !sqlQuery.orderBy.isEmpty())
        {
            prevOrder = new ClauseList(false);
            prevOrder.addAll(sqlQuery.orderBy);
            sqlQuery.orderBy.clear();
        }
        addFromQuery(sqlQuery.toString(), alias, failIfExists);
        if (prevOrder != null) {
            sqlQuery.orderBy.addAll(prevOrder);
            prevOrder.clear();
        }
    }

    // TODO: Clean up access to these methods, now used in
    // SqlConstraintUtils to do some compound tuple many to many processing.
    public Map<String, SqlQuery> subqueries = new HashMap<String, SqlQuery>();
    // Separate the subqueries from the correlated subqueries so they both can exist in a single
    // query.
    public Map<String, SqlQuery> correlatedSubqueries = new HashMap<String, SqlQuery>();
    public Map<String, List<String>> subwhereExpr = new HashMap<String, List<String>>();
    public List<String> subwhereExprKeys = new ArrayList<String>();
    public boolean correlatedSubquery = false;

    public boolean addFrom(
        final MondrianDef.RelationOrJoin relation,
        final String alias,
        final boolean failIfExists,
        final String subquery)
    {
        if (subquery != null) {
            SqlQuery subq = getSubQuery(subquery);
            if (subq == null) {
                subq = new SqlQuery(dialect, generateFormattedSql);
                subq.setDistinct(true);
                if (correlatedSubquery) {
                    correlatedSubqueries.put(subquery, subq);
                } else {
                    subqueries.put(subquery, subq);
                    fromAliases.add(subquery);
                }
            }
            return subq.addFrom(relation, alias, failIfExists, null);
        } else {
            return addFrom(relation, alias, failIfExists);
        }
    }
    
    /**
     * Adds a relation to a query, adding appropriate join conditions, unless
     * it is already present.
     *
     * <p>Returns whether the relation was added to the query.
     *
     * @param relation Relation to add
     * @param alias Alias of relation. If null, uses relation's alias.
     * @param failIfExists Whether to fail if relation is already present
     * @return true, if relation *was* added to query
     */
    public boolean addFrom(
        final MondrianDef.RelationOrJoin relation,
        final String alias,
        final boolean failIfExists)
    {
        registerRootRelation(relation);

        if (relation instanceof MondrianDef.Relation) {
            MondrianDef.Relation relation1 = (MondrianDef.Relation) relation;
            if (relations.add(relation1)
                && !MondrianProperties.instance()
                .FilterChildlessSnowflakeMembers.get())
            {
                // This relation is new to this query. Add a join to any other
                // relation in the same dimension.
                //
                // (If FilterChildlessSnowflakeMembers were false,
                // this would be unnecessary. Adding a relation automatically
                // adds all relations between it and the fact table.)
                MondrianDef.RelationOrJoin root =
                    mapRelationToRoot.get(relation1);
                List<MondrianDef.Relation> relationsCopy =
                    new ArrayList<MondrianDef.Relation>(relations);
                for (MondrianDef.Relation relation2 : relationsCopy) {
                    if (relation2 != relation1
                        && mapRelationToRoot.get(relation2) == root)
                    {
                        addJoinBetween(root, relation1, relation2);
                    }
                }
            }
        }

        if (relation instanceof MondrianDef.View) {
            final MondrianDef.View view = (MondrianDef.View) relation;
            final String viewAlias =
                (alias == null)
                ? view.getAlias()
                : alias;
            final String sqlString = view.getCodeSet().chooseQuery(dialect);
            return addFromQuery(sqlString, viewAlias, false);

        } else if (relation instanceof MondrianDef.InlineTable) {
            final MondrianDef.Relation relation1 =
                RolapUtil.convertInlineTableToRelation(
                    (MondrianDef.InlineTable) relation, dialect);
            return addFrom(relation1, alias, failIfExists, null);

        } else if (relation instanceof MondrianDef.Table) {
            final MondrianDef.Table table = (MondrianDef.Table) relation;
            final String tableAlias =
                (alias == null)
                ? table.getAlias()
                : alias;
            return addFromTable(
                table.schema,
                table.name,
                tableAlias,
                table.getFilter(),
                table.getHintMap(),
                failIfExists);

        } else if (relation instanceof MondrianDef.Join) {
            final MondrianDef.Join join = (MondrianDef.Join) relation;
            return addJoin(
                join.left,
                join.getLeftAlias(),
                join.leftKey,
                join.right,
                join.getRightAlias(),
                join.rightKey,
                failIfExists);
        } else {
            throw Util.newInternal("bad relation type " + relation);
        }
    }

    /**
     * Checks to see if the alias exists within the query or subqueries.
     */
    private boolean containsFromAlias(String alias) {
        if (fromAliases.contains(alias)) {
            return true;
        }
        for (SqlQuery subquery : subqueries.values()) {
          if (subquery.containsFromAlias( alias )) {
              return true;
          }
        }
        return false;
    }

    /**
     * Checks to see if a relation exists in the query.
     *
     * @param relation Relation to check
     * @param alias Alias of relation. If null, uses relation's alias.
     * @return true, if relation exists already
     */
    public boolean hasFrom(
        final MondrianDef.RelationOrJoin relation,
        final String alias)
    {

        if (relation instanceof MondrianDef.View) {
            final MondrianDef.View view = (MondrianDef.View) relation;
            final String viewAlias =
                (alias == null)
                ? view.getAlias()
                : alias;
            return containsFromAlias(viewAlias);
        } else if (relation instanceof MondrianDef.InlineTable) {
            final MondrianDef.Relation relation1 =
                RolapUtil.convertInlineTableToRelation(
                    (MondrianDef.InlineTable) relation, dialect);
            return hasFrom(relation1, alias);

        } else if (relation instanceof MondrianDef.Table) {
            final MondrianDef.Table table = (MondrianDef.Table) relation;
            final String tableAlias =
                (alias == null)
                ? table.getAlias()
                : alias;
            return containsFromAlias(tableAlias);
        } else if (relation instanceof MondrianDef.Join) {
            final MondrianDef.Join join = (MondrianDef.Join) relation;
            return hasJoin(
                join.left,
                join.getLeftAlias(),
                join.leftKey,
                join.right,
                join.getRightAlias(),
                join.rightKey);
        } else {
            throw Util.newInternal("bad relation type " + relation);
        }
    }

    private boolean addJoin(
        MondrianDef.RelationOrJoin left,
        String leftAlias,
        String leftKey,
        MondrianDef.RelationOrJoin right,
        String rightAlias,
        String rightKey,
        boolean failIfExists)
    {
        boolean addLeft = addFrom(left, leftAlias, failIfExists, null);
        boolean addRight = addFrom(right, rightAlias, failIfExists, null);

        boolean added = addLeft || addRight;
        if (added) {
            buf.setLength(0);

            dialect.quoteIdentifier(buf, leftAlias, leftKey);
            buf.append(" = ");
            dialect.quoteIdentifier(buf, rightAlias, rightKey);
            final String condition = buf.toString();
            if (dialect.allowsJoinOn()) {
                from.addOn(
                    leftAlias, leftKey, rightAlias, rightKey,
                    condition);
            } else {
                addWhere(condition);
            }
        }
        return added;
    }

    private boolean hasJoin(
        MondrianDef.RelationOrJoin left,
        String leftAlias,
        String leftKey,
        MondrianDef.RelationOrJoin right,
        String rightAlias,
        String rightKey)
    {
        boolean hasLeft = hasFrom(left, leftAlias);
        boolean hasRight = hasFrom(right, rightAlias);
        return hasLeft || hasRight;
    }

    private void addJoinBetween(
        MondrianDef.RelationOrJoin root,
        MondrianDef.Relation relation1,
        MondrianDef.Relation relation2)
    {
        List<RelInfo> relations = mapRootToRelations.get(root);
        int index1 = find(relations, relation1);
        int index2 = find(relations, relation2);
        assert index1 != -1;
        assert index2 != -1;
        int min = Math.min(index1, index2);
        int max = Math.max(index1, index2);
        for (int i = max - 1; i >= min; i--) {
            RelInfo relInfo = relations.get(i);
                addJoin(
                    relInfo.relation,
                    relInfo.leftAlias != null
                        ? relInfo.leftAlias
                        : relInfo.relation.getAlias(),
                    relInfo.leftKey,
                    relations.get(i + 1).relation,
                    relInfo.rightAlias != null
                        ? relInfo.rightAlias
                        : relations.get(i + 1).relation.getAlias(),
                    relInfo.rightKey,
                    false);
        }
    }

    private int find(List<RelInfo> relations, MondrianDef.Relation relation) {
        for (int i = 0, n = relations.size(); i < n; i++) {
            RelInfo relInfo = relations.get(i);
            if (relInfo.relation.equals(relation)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Adds an expression to the select clause, and returns
     * a string of the rendered expression along with it's alias.
     * The rendered expression may be use later for grouping and ordering.
     *
     * @param expression the expression to add to the select clause
     * @param type the type of the expression
     * @return a string array containing two elements.  The 0th element is the
     *         alias, the 1st element is the rendered expression
     */
    public String[] addSelect(
        final MondrianDef.Expression expression,
        final SqlStatement.Type type)
    {
        // return the expression generated along with the alias
        // if expression.getTableAlias() is part of a subquery, we need to
        // first add this expression to the subquery, then
        // access it via the main query.
        if (expression.getTableAlias() != null && subqueries.size() > 0) {
            for (Entry<String, SqlQuery> subquery : subqueries.entrySet()) {
                if (subquery.getValue()
                    .containsFromAlias(expression.getTableAlias()))
                {
                  String internalAlias =
                      subquery.getValue().addSelect(expression, type)[0];
                  String alias =
                      addSelect(
                          getDialect().quoteIdentifier(
                              subquery.getKey(),
                              internalAlias),
                          type);
                  return new String[] {alias, internalAlias};
                }
            }
        }
        String exprStr = expression.getExpression(this);
        String alias = addSelect(exprStr, type);
        return new String[] {alias, exprStr};
    }

    public String getSubqueryColumnAlias(MondrianDef.Expression expression) {
        if (expression.getTableAlias() != null && subqueries.size() > 0) {
            for (SqlQuery subquery : subqueries.values()) {
                if (subquery.containsFromAlias(expression.getTableAlias()))
                {
                    return subquery.getAlias(expression.getExpression(subquery));
                }
            }
        }
        return null;
    }

    /**
     * Adds an expression to the select clause, automatically creating a
     * column alias.
     */
    public String addSelect(final String expression, SqlStatement.Type type) {
        // Some DB2 versions (AS/400) throw an error if a column alias is
        //  *not* used in a subsequent order by (Group by).
        // Derby fails on 'SELECT... HAVING' if column has alias.
        switch (dialect.getDatabaseProduct()) {
        case DB2_AS400:
        case DERBY:
            return addSelect(expression, type, null);
        default:
            return addSelect(expression, type, nextColumnAlias());
        }
    }

    /**
     * Adds an expression to the SELECT and GROUP BY clauses. Uses the alias in
     * the GROUP BY clause, if the dialect requires it.
     *
     * @param expression Expression
     * @return Alias of expression
     */
    public String addSelectGroupBy(
        final String expression,
        SqlStatement.Type type)
    {
        final String alias = addSelect(expression, type);
        addGroupBy(expression, alias);
        return alias;
    }

    public int getCurrentSelectListSize()
    {
        return select.size();
    }

    public String nextColumnAlias() {
        return "c" + select.size();
    }

    public int getCurrentWithListSize() {
      return with.size();
    }

    public void updateSelectListForWithClause(int id) {
        String queryAlias = "wq" + id;
        for (int i = 0; i < select.size(); i++) {
            String col = select.get(i);
            String ncol = col + " as " + dialect.quoteIdentifier(queryAlias + "_c" + i);
            select.set(i, ncol);
        }
    }

    public void clearSelectListForWithClause() {
        for (int i = 0; i < select.size(); i++) {
            String col = select.get(i);
            String ncol = col.substring(0, col.indexOf(" as "));
            select.set(i, ncol);
        }
    }

    public SqlQuery getSubQuery(String subQueryAlias) {
      if (correlatedSubquery) {
          return correlatedSubqueries.get(subQueryAlias);
      } else {
          return subqueries.get(subQueryAlias);
      }
    }

    /**
     * Adds an expression to the select clause, with a specified type and
     * column alias.
     *
     * @param expression Expression
     * @param type Java type to be used to hold cursor column
     * @param alias Column alias (or null for no alias)
     * @return Column alias
     */
    public String addSelect(
        final String expression,
        final SqlStatement.Type type,
        String alias)
    {
        buf.setLength(0);

        buf.append(expression);
        if (alias != null) {
            buf.append(" as ");
            dialect.quoteIdentifier(alias, buf);
        }

        select.add(buf.toString());
        addType(type);
        columnAliases.put(expression, alias);
        return alias;
    }

    public String getAlias(String expression) {
        return columnAliases.get(expression);
    }

    public void addWhere(
        final String exprLeft,
        final String exprMid,
        final String exprRight)
    {
        addWhere(exprLeft, exprMid,exprRight, null);
    }

    public void addWhere(
        final String exprLeft,
        final String exprMid,
        final String exprRight,
        final String subquery)
    {
        int len = exprLeft.length() + exprMid.length() + exprRight.length();
        StringBuilder buf = new StringBuilder(len);

        buf.append(exprLeft);
        buf.append(exprMid);
        buf.append(exprRight);

        addWhere(buf.toString(), subquery);
    }

    public void addWhere(RolapStar.Condition joinCondition) {
        String left = joinCondition.getLeft().getTableAlias();
        String right = joinCondition.getRight().getTableAlias();
        if (containsFromAlias(left) && containsFromAlias(right)) {
            addWhere(
                joinCondition.getLeft(this),
                " = ",
                joinCondition.getRight(this));
        }
    }

    public void addWhere(final String expression, String subquery) {
        if (subquery != null) {
            if (correlatedSubqueries.containsKey(subquery)) {
                correlatedSubqueries.get(subquery).addWhere(expression);
            }
            if (subqueries.containsKey(subquery)) {
                subqueries.get(subquery).addWhere(expression);
            }
        } else {
            addWhere(expression);
        }
    }

    public void addSubWhere(
        String left, String right,
        String expression,
        final String subquery)
    {
        String subSelectExpr = right;
        if (correlatedSubquery) {
            List<String> subwhereList = subwhereExpr.get(subquery);
            if (subwhereList == null) {
                subwhereList = new ArrayList<String>();
                subwhereExpr.put(subquery, subwhereList);
                subwhereExprKeys.add(subquery);
            }
            String condition = left;
            // only add the condition once
            if (!subwhereList.contains(condition)) {
                subwhereList.add(left);
                getSubQuery(subquery).addSelect( subSelectExpr,  null, null );
            }
        } else {
            assert expression != null && !expression.equals("");
            boolean added = where.add(expression);
            if (added) {
                getSubQuery(subquery).addSelect( subSelectExpr,  null, null );
            }
        }
    }

    public String addWith(final String expression) {
        assert expression != null && !expression.equals("");
        with.add(expression);
        return "wq" + with.size();
    }

    public void addWhere(final String expression)
    {
        assert expression != null && !expression.equals("");
        where.add(expression);
    }

    public void addGroupBy(final String expression)
    {
        assert expression != null && !expression.equals("");
        groupBy.add(expression);
    }

    public void addGroupBy(final String expression, final String alias) {
        if (dialect.requiresGroupByAlias()) {
            addGroupBy(dialect.quoteIdentifier(alias));
        } else {
            addGroupBy(expression);
        }
    }

    public void addHaving(final String expression)
    {
        assert expression != null && !expression.equals("");
        having.add(expression);
    }

    /**
     * Adds an item to the ORDER BY clause.
     *
     * @param expr the expr to order by
     * @param ascending sort direction
     * @param prepend whether to prepend to the current list of items
     * @param nullable whether the expression might be null
     */
    public void addOrderBy(
        String expr,
        boolean ascending,
        boolean prepend,
        boolean nullable)
    {
        this.addOrderBy(expr, expr, ascending, prepend, nullable, true);
    }

    /**
     * Adds an item to the ORDER BY clause.
     *
     * @param expr the expr to order by
     * @param alias the alias of the column, as returned by addSelect
     * @param ascending sort direction
     * @param prepend whether to prepend to the current list of items
     * @param nullable whether the expression might be null
     * @param collateNullsLast whether null values should appear first or last.
     */
    public void addOrderBy(
        String expr,
        String alias,
        boolean ascending,
        boolean prepend,
        boolean nullable,
        boolean collateNullsLast)
    {
        String orderExpr =
            dialect.generateOrderItem(
                dialect.requiresOrderByAlias() && alias != null
                    ? dialect.quoteIdentifier(alias)
                    : expr,
                nullable,
                ascending,
                collateNullsLast);
        if (prepend) {
            orderBy.add(0, orderExpr);
        } else {
            orderBy.add(orderExpr);
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(128);
        toBuffer(sb, "");
        return sb.toString();
    }

    /**
     * Writes this SqlQuery to a StringBuilder with each clause on a separate
     * line, and with the specified indentation prefix.
     *
     * @param buf String builder
     * @param prefix Prefix for each line
     */
    public void toBuffer(StringBuilder buf, String prefix) {
        // generate with clause
        if (dialect.supportsWithClause()) {
            int withCount = 0;
            for (int i = 0; i < with.size(); i++) {
                if (withCount == 0) {
                    buf.append( "with " );
                } else {
                    buf.append(", ");
                }
                buf.append("wq" + (++withCount));
                buf.append(" as (");
                buf.append(with.get(i));
                buf.append(")");
            }

            if (correlatedSubqueries.size() > 0 && subwhereExpr.size() > 0) {
                for (String subquery : subwhereExprKeys) {
                    if (withCount == 0) {
                        buf.append( "with " );
                    } else {
                        buf.append(", ");
                    }
                    buf.append("wq" + (++withCount));
                    buf.append(" as (");
                    SqlQuery sqlsub = correlatedSubqueries.get(subquery);
                    sqlsub.updateSelectListForWithClause(withCount);
                    buf.append(sqlsub.toString());
                    sqlsub.clearSelectListForWithClause();
                    buf.append(")");
                }
            }
            if (withCount > 0) {
                buf.append(" ");
            }
        }

        // generate select clause
        final String first = distinct ? "select distinct " : "select ";
        select.toBuffer(buf, generateFormattedSql, prefix, first, ", ", "", "");
        groupingFunctionsToBuffer(buf, prefix);

        if (subqueries.size() > 0) {
            FromClauseList newfrom = from.clone();
            for (String alias : subqueries.keySet()) {
                SqlQuery subquery = subqueries.get( alias );
                StringBuilder subbuf = new StringBuilder();
                subbuf.setLength(0);
                subbuf.append('(');
                subbuf.append(subquery.toString());
                subbuf.append(')');
                if (dialect.allowsAs()) {
                    subbuf.append(" as ");
                } else {
                    subbuf.append(' ');
                }
                dialect.quoteIdentifier(alias, subbuf);
                newfrom.add(subbuf.toString());
            }
            newfrom.toBuffer(
                buf, generateFormattedSql, prefix, " from ", ", ", "", "");
        } else {
          from.toBuffer(
              buf, generateFormattedSql, prefix, " from ", ", ", "", "");
        }

        // generate correlated sub queries if necessary for
        // many to many dimensions
        if (correlatedSubqueries.size() > 0 && subwhereExpr.size() > 0) {
            ClauseList newWhere = (ClauseList)where.clone();
            int withCount = with.size();
            for (String subquery : subwhereExprKeys) {
                SqlQuery sqlsub = correlatedSubqueries.get(subquery);
                StringBuilder subbuf = new StringBuilder();
                subbuf.append("(");
                List<String> keys = subwhereExpr.get(subquery);
                for (int i = 0; i < keys.size(); i++) {
                    if (i != 0) {
                        subbuf.append(",");
                    }
                    subbuf.append(keys.get(i));
                }
                subbuf.append(") IN (");
                if (dialect.supportsWithClause()) {
                    String name = "wq" + (++withCount);
                    subbuf.append("select ");
                    for (int i = 0; i < sqlsub.getCurrentSelectListSize(); i++) {
                        if (i > 0) {
                            subbuf.append(", ");
                        }
                        subbuf.append(name + "_c" + i);
                    }
                    subbuf.append(" from " + name);
                } else {
                    subbuf.append(sqlsub.toString());
                }
                subbuf.append(")");
                newWhere.add(subbuf.toString());
            }
            newWhere.toBuffer(
                buf, generateFormattedSql, prefix, " where ", " and ", "", "");
        } else {
            where.toBuffer(
                buf, generateFormattedSql, prefix, " where ", " and ", "", "");
        }
        if (groupingSets.isEmpty()) {
            groupBy.toBuffer(
                buf, generateFormattedSql, prefix, " group by ", ", ", "", "");
        } else {
            ClauseList.listToBuffer(
                buf,
                groupingSets,
                generateFormattedSql,
                prefix,
                " group by grouping sets (",
                ", ",
                ")");
        }
        having.toBuffer(
            buf, generateFormattedSql, prefix, " having ", " and ", "", "");
        orderBy.toBuffer(
            buf, generateFormattedSql, prefix, " order by ", ", ", "", "");
        if (offset != null || limit != null) {
            buf.append(dialect.generateLimitOffsetClause(limit, offset));
        }
    }

    private void groupingFunctionsToBuffer(StringBuilder buf, String prefix) {
        if (groupingSets.isEmpty()) {
            return;
        }
        int n = 0;
        for (String groupingFunction : groupingFunctions) {
            if (generateFormattedSql) {
                buf.append(",")
                    .append(Util.nl)
                    .append(INDENT)
                    .append(prefix);
            } else {
                buf.append(", ");
            }
            buf.append("grouping(")
                .append(groupingFunction)
                .append(") as ");
            dialect.quoteIdentifier("g" + n++, buf);
        }
    }

    public Dialect getDialect() {
        return dialect;
    }

    public static SqlQuery newQuery(DataSource dataSource, String err) {
        final Dialect dialect =
            DialectManager.createDialect(dataSource, null);
        return new SqlQuery(dialect);
    }

    public void addGroupingSet(List<String> groupingColumnsExpr) {
        ClauseList groupingList = new ClauseList(false);
        for (String columnExp : groupingColumnsExpr) {
            groupingList.add(columnExp);
        }
        groupingSets.add(groupingList);
    }

    public void addGroupingFunction(String columnExpr) {
        groupingFunctions.add(columnExpr);

        // A grouping function will end up in the select clause implicitly. It
        // needs a corresponding type.
        types.add(null);
    }

    /**
     * Accessor to the types. This saves turning the query into a String/Types pair
     * when not needed.
     * @return the list of types. The list is not wrapped or made unmodifiable.
     */
    public List<SqlStatement.Type> getTypes() {
        return types;
    }

    private void addType(SqlStatement.Type type) {
        types.add(type);
    }

    public Pair<String, List<SqlStatement.Type>> toSqlAndTypes() {
        assert types.size() == select.size() + groupingFunctions.size()
            : types.size() + " types, "
              + (select.size() + groupingFunctions.size())
              + " select items in query " + this;
        return Pair.of(toString(), types);
    }

    public void registerRootRelation(MondrianDef.RelationOrJoin root) {
        // REVIEW: In this method, we are building data structures about the
        // structure of a star schema. These should be built into the schema,
        // not constructed afresh for each SqlQuery. In mondrian-4.0,
        // these methods and the data structures 'mapRootToRelations',
        // 'relations', 'mapRelationToRoot' will disappear.
        if (mapRelationToRoot.containsKey(root)) {
            return;
        }
        if (mapRootToRelations.containsKey(root)) {
            return;
        }
        List<RelInfo> relations = new ArrayList<RelInfo>();
        flatten(relations, root, null, null, null, null);
        for (RelInfo relation : relations) {
            mapRelationToRoot.put(relation.relation, root);
        }
        mapRootToRelations.put(root, relations);
    }

    private void flatten(
        List<RelInfo> relations,
        MondrianDef.RelationOrJoin root,
        String leftKey,
        String leftAlias,
        String rightKey,
        String rightAlias)
    {
        if (root instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) root;
            flatten(
                relations, join.left, join.leftKey, join.leftAlias,
                join.rightKey, join.rightAlias);
            flatten(
                relations, join.right, leftKey, leftAlias, rightKey,
                rightAlias);
        } else {
            relations.add(
                new RelInfo(
                    (MondrianDef.Relation) root,
                    leftKey,
                    leftAlias,
                    rightKey,
                    rightAlias));
        }
    }

    public boolean isSupported() {
        return isSupported;
    }

    public void setSupported(boolean supported) {
        isSupported = supported;
    }

    private static class JoinOnClause {
        private final String condition;
        private final String left;
        private final String right;

        JoinOnClause(String condition, String left, String right) {
            this.condition = condition;
            this.left = left;
            this.right = right;
        }
    }

    static class FromClauseList extends ClauseList {
        private final List<JoinOnClause> joinOnClauses =
            new ArrayList<JoinOnClause>();

        public FromClauseList clone() {
          FromClauseList list = new FromClauseList(this.allowDups);
          list.joinOnClauses.addAll(this.joinOnClauses);
          list.addAll( this );
          return list;
        }

        FromClauseList(boolean allowsDups) {
            super(allowsDups);
        }

        public void addOn(
            String leftAlias,
            String leftKey,
            String rightAlias,
            String rightKey,
            String condition)
        {
            if (leftAlias == null && rightAlias == null) {
                // do nothing
            } else if (leftAlias == null) {
                // left is the form of 'Table'.'column'
                leftAlias = rightAlias;
            } else if (rightAlias == null) {
                // Only left contains table name, Table.Column = abc
                // store the same name for right tables
                rightAlias = leftAlias;
            }
            joinOnClauses.add(
                new JoinOnClause(condition, leftAlias, rightAlias));
        }

        public void toBuffer(StringBuilder buf, List<String> fromAliases) {
            int n = 0;
            for (int i = 0; i < size(); i++) {
                final String s = get(i);
                final String alias = fromAliases.get(i);
                if (n++ == 0) {
                    buf.append(" from ");
                    buf.append(s);
                } else {
                    // Add "JOIN t ON (a = b ,...)" to the FROM clause. If there
                    // is no JOIN clause matching this alias (or no JOIN clauses
                    // at all), append just ", t".
                    appendJoin(fromAliases.subList(0, i), s, alias, buf);
                }
            }
        }

        void appendJoin(
            final List<String> addedTables,
            final String from,
            final String alias,
            final StringBuilder buf)
        {
            int n = 0;
            // first check when the current table is on the left side
            for (JoinOnClause joinOnClause : joinOnClauses) {
                // the first table was added before join, it has to be handled
                // specially: Table.column = expression
                if ((addedTables.size() == 1
                     && addedTables.get(0).equals(joinOnClause.left)
                     && joinOnClause.left.equals(joinOnClause.right))
                    || (alias.equals(joinOnClause.left)
                        && addedTables.contains(joinOnClause.right))
                    || (alias.equals(joinOnClause.right)
                        && addedTables.contains(joinOnClause.left)))
                {
                    if (n++ == 0) {
                        buf.append(" join ").append(from).append(" on ");
                    } else {
                        buf.append(" and ");
                    }
                    buf.append(joinOnClause.condition);
                }
            }
            if (n == 0) {
                // No "JOIN ... ON" clause matching this alias (or maybe no
                // JOIN ... ON clauses at all, if this is a database that
                // doesn't support ANSI-join syntax). Append an old-style FROM
                // item separated by a comma.
                buf.append(joinOnClauses.isEmpty() ? ", " : " cross join ")
                    .append(from);
            }
        }
    }

    static class ClauseList extends ArrayList<String> {
        protected final boolean allowDups;

        ClauseList(final boolean allowDups) {
            this.allowDups = allowDups;
        }

        /**
         * Adds an element to this ClauseList if either duplicates are allowed
         * or if it has not already been added.
         *
         * @param element Element to add
         * @return whether element was added, per
         * {@link java.util.Collection#add(Object)}
         */
        public boolean add(final String element) {
            if (allowDups || !contains(element)) {
                return super.add(element);
            }
            return false;
        }

        final void toBuffer(
            StringBuilder buf,
            boolean generateFormattedSql,
            String prefix,
            String first,
            String sep,
            String last,
            String empty)
        {
            if (isEmpty()) {
                buf.append(empty);
                return;
            }
            first = foo(generateFormattedSql, prefix, first);
            sep = foo(generateFormattedSql, prefix, sep);
            toBuffer(buf, first, sep, last);
        }

        static String foo(
            boolean generateFormattedSql,
            String prefix,
            String s)
        {
            if (generateFormattedSql) {
                if (s.startsWith(" ")) {
                    // E.g. " and "
                    s = Util.nl + prefix + s.substring(1);
                }
                if (s.endsWith(" ")) {
                    // E.g. ", "
                    s =
                        s.substring(0, s.length() - 1) + Util.nl + prefix
                        +  INDENT;
                } else if (s.endsWith("(")) {
                    // E.g. "("
                    s = s + Util.nl + prefix + INDENT;
                }
            }
            return s;
        }

        final void toBuffer(
            final StringBuilder buf,
            final String first,
            final String sep,
            final String last)
        {
            int n = 0;
            buf.append(first);
            for (String s : this) {
                if (n++ > 0) {
                    buf.append(sep);
                }
                buf.append(s);
            }
            buf.append(last);
        }

        static void listToBuffer(
            StringBuilder buf,
            List<ClauseList> clauseListList,
            boolean generateFormattedSql,
            String prefix,
            String first,
            String sep,
            String last)
        {
            first = foo(generateFormattedSql, prefix, first);
            sep = foo(generateFormattedSql, prefix, sep);
            buf.append(first);
            int n = 0;
            for (ClauseList clauseList : clauseListList) {
                if (n++ > 0) {
                    buf.append(sep);
                }
                clauseList.toBuffer(
                    buf, false, prefix, "(", ", ", ")", "()");
            }
            buf.append(last);
        }
    }

    /**
     * Collection of alternative code for alternative dialects.
     */
    public static class CodeSet {
        private final Map<String, String> dialectCodes =
            new HashMap<String, String>();

        public String put(String dialect, String code) {
            return dialectCodes.put(dialect, code);
        }

        /**
         * Chooses the code variant which best matches the given Dialect.
         */
        public String chooseQuery(Dialect dialect) {
            String best = getBestName(dialect);
            String bestCode = dialectCodes.get(best);
            if (bestCode != null) {
                return bestCode;
            }
            String genericCode = dialectCodes.get("generic");
            if (genericCode == null) {
                throw Util.newError("View has no 'generic' variant");
            }
            return genericCode;
        }

        private static String getBestName(Dialect dialect) {
          final String dialectName = dialect.getDatabaseProduct().getFamily()
              .name().toLowerCase();
          if (dialectName.equals("postgresql")) {
              // Luc Boudreau's comment
              // Special case for the discrepancy between the value used
              // in schemas and the actual name of the dialect. The former is
              // 'postgresql' while the later is 'postgres'.
              // We can't change the schemas or we will break compatibility,
              // so we have to switch it here.
              // This needs to be fixed post 4.0.
              return "postgres";
           } else {
             return dialectName;
           }
        }
    }

    private static class RelInfo {
        final MondrianDef.Relation relation;
        final String leftKey;
        final String leftAlias;
        final String rightKey;
        final String rightAlias;

        public RelInfo(
            MondrianDef.Relation relation,
            String leftKey,
            String leftAlias,
            String rightKey,
            String rightAlias)
        {
            this.relation = relation;
            this.leftKey = leftKey;
            this.leftAlias = leftAlias;
            this.rightKey = rightKey;
            this.rightAlias = rightAlias;
        }
    }
}

// End SqlQuery.java
