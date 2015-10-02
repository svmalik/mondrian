/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2015 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.MemberCalc;
import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.NullType;
import mondrian.olap.type.SetType;
import mondrian.olap.type.StringType;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.SqlQuery;
import mondrian.spi.Dialect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * Creates SQL from parse tree nodes. Currently it creates the SQL that
 * accesses a measure for the ORDER BY that is generated for a TopCount.<p/>
 *
 * @author av
 * @since Nov 17, 2005
  */
public class RolapNativeSql {

    protected static final Logger LOGGER =
        Logger.getLogger(RolapNativeSql.class);

    private SqlQuery sqlQuery;
    private Dialect dialect;

    CompositeSqlCompiler numericCompiler;
    CompositeSqlCompiler booleanCompiler;

    RolapStoredMeasure storedMeasure;
    final AggStar aggStar;
    final Evaluator evaluator;
    final RolapLevel rolapLevel;

    int storedMeasureCount = 0;
    final Set<Member> addlContext = new HashSet<Member>();
    final Map<String, String> preEvalExprs;
    final Map<String, MemberExpr> preEvalMembers = new HashMap<String, MemberExpr>();

    /**
     * We remember one of the measures so we can generate
     * the constraints from RolapAggregationManager. Also
     * make sure all measures live in the same star.
     *
     * @return false if one or more saved measures are not
     * from the same star (or aggStar if defined), true otherwise.
     *
     * @see RolapAggregationManager#makeRequest(RolapEvaluator)
     */
    private boolean saveStoredMeasure(RolapStoredMeasure m) {
        if (aggStar != null && !storedMeasureIsPresentOnAggStar(m)) {
            return false;
        }
        if (storedMeasure != null) {
            RolapStar star1 = getStar(storedMeasure);
            RolapStar star2 = getStar(m);
            if (star1 != star2) {
                return false;
            }
        }
        storedMeasureCount++;
        this.storedMeasure = m;
        return true;
    }

    private boolean storedMeasureIsPresentOnAggStar(RolapStoredMeasure m) {
        RolapStar.Column column =
            (RolapStar.Column) m.getStarMeasure();
        int bitPos = column.getBitPosition();
        return  aggStar.lookupColumn(bitPos) != null;
    }

    private RolapStar getStar(RolapStoredMeasure m) {
        return ((RolapStar.Measure) m.getStarMeasure()).getStar();
    }

    /**
     * Translates an expression into SQL
     */
    interface SqlCompiler {
        /**
         * Returns SQL. If <code>exp</code> can not be compiled into SQL,
         * returns null.
         *
         * @param exp Expression
         * @return SQL, or null if cannot be converted into SQL
         */
        String compile(Exp exp);
    }

    /**
     * Implementation of {@link SqlCompiler} that uses chain of responsibility
     * to find a matching sql compiler.
     */
    static class CompositeSqlCompiler implements SqlCompiler {
        List<SqlCompiler> compilers = new ArrayList<SqlCompiler>();

        public void add(SqlCompiler compiler) {
            compilers.add(compiler);
        }

        public String compile(Exp exp) {
            for (SqlCompiler compiler : compilers) {
                String s = compiler.compile(exp);
                if (s != null) {
                    return s;
                }
            }
            return null;
        }

        public String toString() {
            return compilers.toString();
        }
    }

    /**
     * Compiles a numeric literal to SQL.
     */
    class NumberSqlCompiler implements SqlCompiler {
        public String compile(Exp exp) {
            if (!(exp instanceof Literal)) {
                return null;
            }
            if ((exp.getCategory() & Category.Numeric) == 0) {
                return null;
            }
            Literal literal = (Literal) exp;
            String expr = String.valueOf(literal.getValue());
            if (dialect.getDatabaseProduct().getFamily()
                == Dialect.DatabaseProduct.DB2)
            {
                expr = "FLOAT(" + expr + ")";
            }
            return expr;
        }

        public String toString() {
            return "NumberSqlCompiler";
        }
    }

    /**
     * Base class to remove MemberScalarExp.
     */
    abstract class MemberSqlCompiler implements SqlCompiler {
        protected Exp unwind(Exp exp) {
            return exp;
        }
    }

    /**
     * Compiles a measure into SQL, the measure will be aggregated
     * like <code>sum(measure)</code>.
     */
    class StoredMeasureSqlCompiler extends MemberSqlCompiler {

        protected RolapStoredMeasure getMeasure(Exp exp) {
            exp = unwind(exp);
            if (!(exp instanceof MemberExpr)) {
                return null;
            }
            final Member member = ((MemberExpr) exp).getMember();
            if (!(member instanceof RolapStoredMeasure)) {
                return null;
            }
            RolapStoredMeasure measure = (RolapStoredMeasure) member;
            if (measure.isCalculated()) {
                return null; // ??
            }
            return measure;
        }

        public String compile(Exp exp) {
            RolapStoredMeasure measure = getMeasure(exp);
            if (measure == null || !saveStoredMeasure(measure)) {
                return null;
            }

            RolapAggregator aggregator = measure.getAggregator();
            String exprInner;
            // Use aggregate table to create condition if available
            if (aggStar != null
                && measure.getStarMeasure() instanceof RolapStar.Column)
            {
                RolapStar.Column column =
                    (RolapStar.Column) measure.getStarMeasure();
                int bitPos = column.getBitPosition();
                AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
                exprInner = aggColumn.generateExprString(sqlQuery);
                if (aggColumn instanceof AggStar.FactTable.Measure) {
                    RolapAggregator aggTableAggregator =
                        ((AggStar.FactTable.Measure) aggColumn)
                            .getAggregator();
                    // aggregating data that has already been aggregated
                    // should be done with another aggregators
                    // e.g., counting facts should be proceeded via computing
                    // sum, as a row can aggregate several facts
                    aggregator = (RolapAggregator) aggTableAggregator
                        .getRollup();
                }
            } else {
                MondrianDef.Expression defExp =
                    measure.getMondrianDefExpression();
                exprInner = (defExp == null)
                    ? "*" : defExp.getExpression(sqlQuery);
            }

            String expr = aggregator.getExpression(exprInner);
            if (dialect.getDatabaseProduct().getFamily()
                == Dialect.DatabaseProduct.DB2)
            {
                expr = "FLOAT(" + expr + ")";
            }
            return expr;
        }

        public String toString() {
            return "StoredMeasureSqlCompiler";
        }
    }

    /**
     * Compiles a MATCHES MDX operator into SQL regular
     * expression match.
     */
    class MatchingSqlCompiler extends FunCallSqlCompilerBase {

        protected MatchingSqlCompiler()
        {
            super(Category.Logical, "MATCHES", 2);
        }

        public String compile(Exp exp) {
            if (!match(exp)) {
                return null;
            }
            if (!dialect.allowsRegularExpressionInWhereClause()
                || !(exp instanceof ResolvedFunCall)
                || evaluator == null)
            {
                return null;
            }

            final Exp arg0 = ((ResolvedFunCall)exp).getArg(0);
            final Exp arg1 = ((ResolvedFunCall)exp).getArg(1);

            // Must finish by ".Caption" or ".Name"
            if (!(arg0 instanceof ResolvedFunCall)
                || ((ResolvedFunCall)arg0).getArgCount() != 1
                || !(arg0.getType() instanceof StringType)
                || (!((ResolvedFunCall)arg0).getFunName().equals("Name")
                    && !((ResolvedFunCall)arg0)
                            .getFunName().equals("Caption")))
            {
                return null;
            }

            final boolean useCaption;
            if (((ResolvedFunCall)arg0).getFunName().equals("Name")) {
                useCaption = false;
            } else {
                useCaption = true;
            }

            // Must be ".CurrentMember"
            final Exp currMemberExpr = ((ResolvedFunCall)arg0).getArg(0);
            if (!(currMemberExpr instanceof ResolvedFunCall)
                || ((ResolvedFunCall)currMemberExpr).getArgCount() != 1
                || !(currMemberExpr.getType() instanceof MemberType)
                || !((ResolvedFunCall)currMemberExpr)
                        .getFunName().equals("CurrentMember"))
            {
                return null;
            }

            // Must be a dimension, a hierarchy or a level.
            final RolapCubeDimension dimension;
            final Exp dimExpr = ((ResolvedFunCall)currMemberExpr).getArg(0);
            if (dimExpr instanceof DimensionExpr) {
                dimension =
                    (RolapCubeDimension) evaluator.getCachedResult(
                        new ExpCacheDescriptor(dimExpr, evaluator));
            } else if (dimExpr instanceof HierarchyExpr) {
                final RolapCubeHierarchy hierarchy =
                    (RolapCubeHierarchy) evaluator.getCachedResult(
                        new ExpCacheDescriptor(dimExpr, evaluator));
                dimension = (RolapCubeDimension) hierarchy.getDimension();
            } else if (dimExpr instanceof LevelExpr) {
                final RolapCubeLevel level =
                    (RolapCubeLevel) evaluator.getCachedResult(
                        new ExpCacheDescriptor(dimExpr, evaluator));
                dimension = (RolapCubeDimension) level.getDimension();
            } else {
                return null;
            }

            if (rolapLevel != null
                && dimension.equals(rolapLevel.getDimension()))
            {
                // We can't use the evaluator because the filter is filtering
                // a set which is uses same dimension as the predicate.
                // We must use, in order of priority,
                //  - caption requested: caption->name->key
                //  - name requested: name->key
                MondrianDef.Expression expression = useCaption
                ? rolapLevel.captionExp == null
                        ? rolapLevel.nameExp == null
                            ? rolapLevel.keyExp
                            : rolapLevel.nameExp
                        : rolapLevel.captionExp
                    : rolapLevel.nameExp == null
                        ? rolapLevel.keyExp
                        : rolapLevel.nameExp;
                 // If an aggregation table is used, it might be more efficient
                 // to use only the aggregate table and not the hierarchy table.
                 // Try to lookup the column bit key. If that fails, we will
                 // link the aggregate table to the hierarchy table. If no
                 // aggregate table is used, we can use the column expression
                 // directly.
                String sourceExp;
                if (aggStar != null
                    && rolapLevel instanceof RolapCubeLevel
                    && expression == rolapLevel.keyExp)
                {
                    int bitPos =
                        ((RolapCubeLevel)rolapLevel).getStarKeyColumn()
                            .getBitPosition();
                    mondrian.rolap.aggmatcher.AggStar.Table.Column col =
                        aggStar.lookupColumn(bitPos);
                    if (col != null) {
                        sourceExp = col.generateExprString(sqlQuery);
                    } else {
                        // Make sure the level table is part of the query.
                        rolapLevel.getHierarchy().addToFrom(
                            sqlQuery,
                            expression);
                        sourceExp = expression.getExpression(sqlQuery);
                    }
                } else if (aggStar != null) {
                    // Make sure the level table is part of the query.
                    rolapLevel.getHierarchy().addToFrom(sqlQuery, expression);
                    sourceExp = expression.getExpression(sqlQuery);
                } else {
                    sourceExp = expression.getExpression(sqlQuery);
                }

                // The dialect might require the use of the alias rather
                // then the column exp.
                if (dialect.requiresHavingAlias()) {
                    sourceExp = sqlQuery.getAlias(sourceExp);
                }
                return
                    dialect.generateRegularExpression(
                        sourceExp,
                        String.valueOf(
                            evaluator.getCachedResult(
                                new ExpCacheDescriptor(arg1, evaluator))));
            } else {
                return null;
            }
        }
        public String toString() {
            return "MatchingSqlCompiler";
        }
    }


    /**
     * Compiles CurrentMember IN {} into SQL.
     */
    class CurrentMemberInSqlCompiler extends FunCallSqlCompilerBase {
        SqlCompiler compiler;

        protected CurrentMemberInSqlCompiler(SqlCompiler argumentCompiler) {
            super(Category.Logical, "IN", 2);
            compiler = argumentCompiler;
        }

        public String compile(Exp exp) {
            if (!match(exp)) {
                return null;
            }
            if (!dialect.supportsMultiValueInExpr()
                || !(exp instanceof ResolvedFunCall)
                || evaluator == null)
            {
                return null;
            }

            final Exp arg0 = ((ResolvedFunCall)exp).getArg(0);
            final Exp arg1 = ((ResolvedFunCall)exp).getArg(1);

            if (!(arg0 instanceof ResolvedFunCall)
                || !(arg1 instanceof ResolvedFunCall)) {
                return null;
            }

            // Must be ".CurrentMember"
            final ResolvedFunCall currMemberExpr = (ResolvedFunCall)arg0;
            if (currMemberExpr.getArgCount() != 1
                || !(currMemberExpr.getType() instanceof MemberType)
                || !currMemberExpr.getFunName().equals("CurrentMember"))
            {
                return null;
            }

            // Must be a dimension, a hierarchy or a level.
            final RolapCubeDimension dimension;
            final Exp dimExpr = currMemberExpr.getArg(0);
            if (dimExpr instanceof DimensionExpr) {
                dimension =
                    (RolapCubeDimension) evaluator.getCachedResult(
                        new ExpCacheDescriptor(dimExpr, evaluator));
            } else if (dimExpr instanceof HierarchyExpr) {
                final RolapCubeHierarchy hierarchy =
                    (RolapCubeHierarchy) evaluator.getCachedResult(
                        new ExpCacheDescriptor(dimExpr, evaluator));
                dimension = (RolapCubeDimension) hierarchy.getDimension();
            } else if (dimExpr instanceof LevelExpr) {
                final RolapCubeLevel level =
                    (RolapCubeLevel) evaluator.getCachedResult(
                        new ExpCacheDescriptor(dimExpr, evaluator));
                dimension = level.getDimension();
            } else {
                return null;
            }

            if (rolapLevel != null
                && dimension.equals(rolapLevel.getDimension()))
            {
                // We can't use the evaluator because the filter is filtering
                // a set which is uses same dimension as the predicate.
                // We must use, in order of priority,
                //  - caption requested: caption->name->key
                //  - name requested: name->key
                MondrianDef.Expression expression = rolapLevel.keyExp;
                // If an aggregation table is used, it might be more efficient
                // to use only the aggregate table and not the hierarchy table.
                // Try to lookup the column bit key. If that fails, we will
                // link the aggregate table to the hierarchy table. If no
                // aggregate table is used, we can use the column expression
                // directly.
                String sourceExp;
                if (aggStar != null && rolapLevel instanceof RolapCubeLevel)
                {
                    int bitPos =
                        ((RolapCubeLevel)rolapLevel).getStarKeyColumn()
                            .getBitPosition();
                    mondrian.rolap.aggmatcher.AggStar.Table.Column col =
                        aggStar.lookupColumn(bitPos);
                    if (col != null) {
                        sourceExp = col.generateExprString(sqlQuery);
                    } else {
                        // Make sure the level table is part of the query.
                        rolapLevel.getHierarchy().addToFrom(
                            sqlQuery,
                            expression);
                        sourceExp = expression.getExpression(sqlQuery);
                    }
                } else if (aggStar != null) {
                    // Make sure the level table is part of the query.
                    rolapLevel.getHierarchy().addToFrom(sqlQuery, expression);
                    sourceExp = expression.getExpression(sqlQuery);
                } else {
                    sourceExp = expression.getExpression(sqlQuery);
                }

                // The dialect might require the use of the alias rather
                // then the column exp.
                if (dialect.requiresHavingAlias()) {
                    sourceExp = sqlQuery.getAlias(sourceExp);
                }

                final ResolvedFunCall set = (ResolvedFunCall)arg1;
                if (sourceExp != null
                    && set.getType() instanceof SetType
                    && "{}".equals(set.getFunName()))
                {
                    /*String[] args = compileArgs(set, compiler);
                    if (args == null) {
                        return null;
                    }*/
                    StringBuilder buf = new StringBuilder(sourceExp);
                    buf.append(" IN (");
                    int i = 0;
                    for (Exp setArg : set.getArgs()) {
                        if (setArg instanceof MemberExpr
                            && ((MemberExpr)setArg).getMember() instanceof RolapCubeMember)
                        {
                            if (i++ > 0) {
                                buf.append(", ");
                            }
                            RolapCubeMember cubeMember = (RolapCubeMember) ((MemberExpr) setArg).getMember();
                            if (cubeMember.isCalculated() || cubeMember.getKey() == null) {
                                return null;
                            }
                            dialect.quote(buf, cubeMember.getKey(), rolapLevel.getDatatype());
                        }
                        else {
                            return null;
                        }
                    }
                    buf.append(") ");
                    return buf.toString();
                }

                return null;
                        /*dialect.generateRegularExpression(
                                sourceExp,
                                String.valueOf(
                                        evaluator.getCachedResult(
                                                new ExpCacheDescriptor(arg1, evaluator))));*/
            } else {
                return null;
            }
        }
        public String toString() {
            return "CurrentMemberInSqlCompiler";
        }
    }

    /**
     * Compiles the underlying expression of a calculated member.
     */
    class CalculatedMemberSqlCompiler extends MemberSqlCompiler {
        SqlCompiler compiler;

        CalculatedMemberSqlCompiler(SqlCompiler argumentCompiler) {
            this.compiler = argumentCompiler;
        }

        public String compile(Exp exp) {
            exp = unwind(exp);
            if (!(exp instanceof MemberExpr)) {
                return null;
            }
            final Member member = ((MemberExpr) exp).getMember();
            if (!(member instanceof RolapCalculatedMember)) {
                return null;
            }
            exp = member.getExpression();
            if (exp == null) {
                return null;
            }
            return compiler.compile(exp);
        }

        public String toString() {
            return "CalculatedMemberSqlCompiler";
        }
    }

    /**
     * Contains utility methods to compile FunCall expressions into SQL.
     */
    abstract class FunCallSqlCompilerBase implements SqlCompiler {
        int category;
        String mdx;
        int argCount;

        FunCallSqlCompilerBase(int category, String mdx, int argCount) {
            this.category = category;
            this.mdx = mdx;
            this.argCount = argCount;
        }

        /**
         * @return true if exp is a matching FunCall
         */
        protected boolean match(Exp exp) {
            if ((exp.getCategory() & category) == 0) {
                return false;
            }
            if (!(exp instanceof FunCall)) {
                return false;
            }
            FunCall fc = (FunCall) exp;
            if (!mdx.equalsIgnoreCase(fc.getFunName())) {
                return false;
            }
            Exp[] args = fc.getArgs();
            if (args.length != argCount) {
                return false;
            }
            return true;
        }

        /**
         * compiles the arguments of a FunCall
         *
         * @return array of expressions or null if either exp does not match or
         * any argument could not be compiled.
         */
        protected String[] compileArgs(Exp exp, SqlCompiler compiler) {
            if (!match(exp)) {
                return null;
            }
            Exp[] args = ((FunCall) exp).getArgs();
            String[] sqls = new String[args.length];
            for (int i = 0; i < args.length; i++) {
                sqls[i] = compiler.compile(args[i]);
                if (sqls[i] == null) {
                    return null;
                }
            }
            return sqls;
        }
    }

    /**
     * Compiles a funcall, e.g. foo(a, b, c).
     */
    class FunCallSqlCompiler extends FunCallSqlCompilerBase {
        SqlCompiler compiler;
        String sql;

        protected FunCallSqlCompiler(
            int category, String mdx, String sql,
            int argCount, SqlCompiler argumentCompiler)
        {
            super(category, mdx, argCount);
            this.sql = sql;
            this.compiler = argumentCompiler;
        }

        public String compile(Exp exp) {
            String[] args = compileArgs(exp, compiler);
            if (args == null) {
                return null;
            }
            StringBuilder buf = new StringBuilder();
            buf.append(sql);
            buf.append("(");
            for (int i = 0; i < args.length; i++) {
                if (i > 0) {
                    buf.append(", ");
                }
                buf.append(args[i]);
            }
            buf.append(") ");
            return buf.toString();
        }

        public String toString() {
            return "FunCallSqlCompiler[" + mdx + "]";
        }
    }

    /**
     * Shortcut for an unary operator like NOT(a).
     */
    class UnaryOpSqlCompiler extends FunCallSqlCompiler {
        protected UnaryOpSqlCompiler(
            int category,
            String mdx,
            String sql,
            SqlCompiler argumentCompiler)
        {
            super(category, mdx, sql, 1, argumentCompiler);
        }
    }

    /**
     * Shortcut for ().
     */
    class ParenthesisSqlCompiler extends FunCallSqlCompiler {
        protected ParenthesisSqlCompiler(
            int category,
            SqlCompiler argumentCompiler)
        {
            super(category, "()", "", 1, argumentCompiler);
        }

        public String toString() {
            return "ParenthesisSqlCompiler";
        }
    }

    /**
     * Compiles an infix operator like addition into SQL like <code>(a
     * + b)</code>.
     */
    class InfixOpSqlCompiler extends FunCallSqlCompilerBase {
        private final String sql;
        private final SqlCompiler compiler;

        protected InfixOpSqlCompiler(
            int category,
            String mdx,
            String sql,
            SqlCompiler argumentCompiler)
        {
            super(category, mdx, 2);
            this.sql = sql;
            this.compiler = argumentCompiler;
        }

        public String compile(Exp exp) {
            String[] args = compileArgs(exp, compiler);
            if (args == null) {
                return null;
            }
            return "(" + args[0] + " " + sql + " " + args[1] + ")";
        }

        public String toString() {
            return "InfixSqlCompiler[" + mdx + "]";
        }
    }

    /**
     * Compiles an <code>IsEmpty(measure)</code>
     * expression into SQL <code>measure is null</code>.
     */
    class IsEmptySqlCompiler extends FunCallSqlCompilerBase {
        private final SqlCompiler compiler;

        protected IsEmptySqlCompiler(
            int category, String mdx,
            SqlCompiler argumentCompiler)
        {
            super(category, mdx, 1);
            this.compiler = argumentCompiler;
        }

        public String compile(Exp exp) {
            String[] args = compileArgs(exp, compiler);
            if (args == null) {
                return null;
            }
            return "(" + args[0] + " is null" + ")";
        }

        public String toString() {
            return "IsEmptySqlCompiler[" + mdx + "]";
        }
    }

    class UpperLowerCaseSqlCompiler extends FunCallSqlCompilerBase {
        private final SqlCompiler compiler;
        private final boolean isUpper;

        protected UpperLowerCaseSqlCompiler(
            int category, String mdx,
            SqlCompiler argumentCompiler)
        {
            super(category, mdx, 1);
            this.isUpper = "UCase".equalsIgnoreCase(mdx);
            this.compiler = argumentCompiler;
        }

        public String compile(Exp exp) {
            String sourceExp = null;
            /*String[] args = compileArgs(exp, compiler);
            if (args != null) {
                sourceExp = args[0];
            } else */if (match(exp) && exp instanceof ResolvedFunCall && evaluator != null) {
                final Exp arg0 = ((ResolvedFunCall) exp).getArg(0);

                // Must finish by ".Caption" or ".Name"
                if (!(arg0 instanceof ResolvedFunCall)
                    || ((ResolvedFunCall) arg0).getArgCount() != 1
                    || !(arg0.getType() instanceof StringType)
                    || (!((ResolvedFunCall)arg0).getFunName().equals("Name")
                    && !((ResolvedFunCall)arg0).getFunName().equals("Caption")))
                {
                    return null;
                }

                final boolean useCaption;
                if (((ResolvedFunCall) arg0).getFunName().equals("Name")) {
                    useCaption = false;
                } else {
                    useCaption = true;
                }

                // Must be ".CurrentMember"
                final Exp currMemberExpr = ((ResolvedFunCall) arg0).getArg(0);
                if (!(currMemberExpr instanceof ResolvedFunCall)
                    || ((ResolvedFunCall) currMemberExpr).getArgCount() != 1
                    || !(currMemberExpr.getType() instanceof MemberType)
                    || !((ResolvedFunCall) currMemberExpr).getFunName().equals("CurrentMember"))
                {
                    return null;
                }

                // Must be a dimension, a hierarchy or a level.
                final RolapCubeDimension dimension;
                final Exp dimExpr = ((ResolvedFunCall) currMemberExpr).getArg(0);
                if (dimExpr instanceof DimensionExpr) {
                    dimension =
                        (RolapCubeDimension) evaluator.getCachedResult(
                            new ExpCacheDescriptor(dimExpr, evaluator));
                } else if (dimExpr instanceof HierarchyExpr) {
                    final RolapCubeHierarchy hierarchy =
                        (RolapCubeHierarchy) evaluator.getCachedResult(
                            new ExpCacheDescriptor(dimExpr, evaluator));
                    dimension = (RolapCubeDimension) hierarchy.getDimension();
                } else if (dimExpr instanceof LevelExpr) {
                    final RolapCubeLevel level =
                        (RolapCubeLevel) evaluator.getCachedResult(
                            new ExpCacheDescriptor(dimExpr, evaluator));
                    dimension = level.getDimension();
                } else {
                    return null;
                }

                if (rolapLevel != null
                    && dimension.equals(rolapLevel.getDimension()))
                {
                    // We can't use the evaluator because the filter is filtering
                    // a set which is uses same dimension as the predicate.
                    // We must use, in order of priority,
                    //  - caption requested: caption->name->key
                    //  - name requested: name->key
                    MondrianDef.Expression expression = useCaption
                            ? rolapLevel.captionExp == null
                            ? rolapLevel.nameExp == null
                            ? rolapLevel.keyExp
                            : rolapLevel.nameExp
                            : rolapLevel.captionExp
                            : rolapLevel.nameExp == null
                            ? rolapLevel.keyExp
                            : rolapLevel.nameExp;
                    // If an aggregation table is used, it might be more efficient
                    // to use only the aggregate table and not the hierarchy table.
                    // Try to lookup the column bit key. If that fails, we will
                    // link the aggregate table to the hierarchy table. If no
                    // aggregate table is used, we can use the column expression
                    // directly.
                    if (aggStar != null
                        && rolapLevel instanceof RolapCubeLevel
                        && expression == rolapLevel.keyExp)
                    {
                        int bitPos =
                            ((RolapCubeLevel) rolapLevel).getStarKeyColumn()
                                .getBitPosition();
                        mondrian.rolap.aggmatcher.AggStar.Table.Column col =
                            aggStar.lookupColumn(bitPos);
                        if (col != null) {
                            sourceExp = col.generateExprString(sqlQuery);
                        } else {
                            // Make sure the level table is part of the query.
                            rolapLevel.getHierarchy().addToFrom(
                                sqlQuery,
                                expression);
                            sourceExp = expression.getExpression(sqlQuery);
                        }
                    } else if (aggStar != null) {
                        // Make sure the level table is part of the query.
                        rolapLevel.getHierarchy().addToFrom(sqlQuery, expression);
                        sourceExp = expression.getExpression(sqlQuery);
                    } else {
                        sourceExp = expression.getExpression(sqlQuery);
                    }
                }
            } else {
                String[] args = compileArgs(exp, compiler);
                if (args != null) {
                    sourceExp = args[0];
                }
            }

            return sourceExp != null ? (this.isUpper ? "UPPER(" : "LOWER(") + sourceExp + ")" : null;
        }

        public String toString() {
            return "UpperLowerCaseSqlCompiler[" + mdx + "]";
        }
    }

    class InStrSqlCompiler extends FunCallSqlCompilerBase {
        private final SqlCompiler compiler;

        protected InStrSqlCompiler(
            int category, SqlCompiler argumentCompiler)
        {
            super(category, "InStr", 2);
            this.compiler = argumentCompiler;
        }

        public String compile(Exp exp) {
            String[] args = compileArgs(exp, compiler);
            if (args == null) {
                return null;
            }
            StringBuilder sb = new StringBuilder("POSITION(");
            dialect.quoteStringLiteral(sb, args[1]);
            sb.append(" IN ");
            sb.append(args[0]);
            sb.append(")");
            return sb.toString();
        }

        public String toString() {
            return "InStrSqlCompiler[" + mdx + "]";
        }
    }

    /**
     * This use case allows non empty filtering on count == 0 when EXCLUDEEMPTY is 
     * specified for a specific set of tuples.
     * 
     * Note that at this time the following circumstances must be met:
     *  - <Filter Level>.CurrentMember must be specified
     *  - All other dimensions must include their all member in a crossjoin with the current member
     *  - a measure must be specified in the cross join
     *  
     *  So the syntax would look something like this:
     *  
     *  Filter([Store].Children, 
     *  Count(CrossJoin([Store].CurrentMember, CrossJoin([Customer].[All], [Measure].[Sales])), EXCLUDEEMPTY))
     *  
     *  This basically is a NON EMPTY check with Filters.
     *  
     */
    private class BooleanCountSqlCompiler extends FunCallSqlCompilerBase {
      
        BooleanCountSqlCompiler(int category, SqlCompiler valueCompiler) {
          super(category, "count", 2);
        }
        
        /**
         * Struct used to maintain state during recursion
         */
        private class RecursiveState {
            boolean unknownFound = false;
            boolean measureFound = false;
            boolean multipleFactsFound = false;
            boolean currentMemberFound = false;
            boolean nonAllHierarchyFound = false;
            List<Hierarchy> allHierarchies = new ArrayList<Hierarchy>();
            List<Hierarchy> nonAllHierarchies = new ArrayList<Hierarchy>();
        }

        /**
         * recursive method traversing everything within the count() function
         */
        private void traverseExp(Exp exp, RecursiveState rs) {
            if (exp instanceof ResolvedFunCall) {
                ResolvedFunCall call = (ResolvedFunCall)exp;
                if (call.getFunName().equals("Cache")) {
                    traverseExp(call.getArg(0), rs);
                } else if (call.getFunName().equals("Crossjoin")) {
                    for (int i = 0; i < call.getArgCount(); i++) {
                        traverseExp(call.getArg(i), rs);
                    }
                } else if (call.getFunName().equals("{}")) {
                    for (int i = 0; i < call.getArgCount(); i++) {
                        traverseExp(call.getArg(i), rs);
                    }
                } else if (call.getFunName().equals("CurrentMember")) {
                    // Must be a dimension, a hierarchy or a level.
                    RolapCubeDimension dimension = null;
                    final Exp dimExpr = call.getArg(0);
                    if (dimExpr instanceof DimensionExpr) {
                        dimension =
                            (RolapCubeDimension) evaluator.getCachedResult(
                                new ExpCacheDescriptor(dimExpr, evaluator));
                    } else if (dimExpr instanceof HierarchyExpr) {
                        final RolapCubeHierarchy hierarchy =
                            (RolapCubeHierarchy) evaluator.getCachedResult(
                                new ExpCacheDescriptor(dimExpr, evaluator));
                        dimension = (RolapCubeDimension) hierarchy.getDimension();
                    } else if (dimExpr instanceof LevelExpr) {
                        final RolapCubeLevel level =
                            (RolapCubeLevel) evaluator.getCachedResult(
                                new ExpCacheDescriptor(dimExpr, evaluator));
                        dimension = (RolapCubeDimension) level.getDimension();
                    }
                    if (rolapLevel != null && dimension != null && dimension.equals(rolapLevel.getDimension())) {
                        rs.currentMemberFound = true;
                    }          
                } else if (call.getFunName().equals("AddCalculatedMembers")) {
                    // TODO: Verify there are no calculated members for set referenced, which must only be one-dimensional.
                    // We can ignore this for now because we only support joining with related dims that include
                    // the all member reference.
                    traverseExp(call.getArg(0), rs);
                } else if (call.getFunName().equals("Children")) {
                    // for now, will fail if all member isn't in the crossjoin, so no need for specific
                    // Children behavior.
                    if (call.getArg(0) instanceof MemberExpr) {
                        Member m = ((MemberExpr)call.getArg(0)).getMember();
                        rs.nonAllHierarchies.add(m.getHierarchy());
                    } else {
                        LOGGER.debug("BooleanCountSqlCompiler: Unknown Param for Children: " + call.getArg(0));
                        rs.unknownFound = true;
                    }
                } else if (call.getFunName().equals("Descendants")) {
                    // this usecase only behaves if the all member is referenced
                    boolean allInDecendants = false;
                    if (call.getArgCount() == 3 && call.getArg(0) instanceof MemberExpr) {
                        Member m = ((MemberExpr)call.getArg(0)).getMember();
                        if (m.isAll()) {
                            if (call.getArg(2) instanceof Literal) {
                                if(((Literal)call.getArg(2)).getValue().toString().toLowerCase().indexOf("self") >= 0) {
                                    allInDecendants = true;
                                }
                            }
                        }
                    }
                    if (!allInDecendants) {
                      rs.unknownFound = true;
                    }
                    // TODO: Add info to allHierarchies / nonAllHierarchies for later processing
                } else {
                    LOGGER.debug("BooleanCountSqlCompiler: Unknown Function Name: " + call.getFunName());
                    rs.unknownFound = true; 
                }
            } else if (exp instanceof MemberExpr) {
                // Note that we cannot assume that calculated measures are directly tied to a member.  There could be logic in the 
                // calculated member that sometimes evaluates to empty and other times where it will not.
                Member m = ((MemberExpr) exp).getMember();
                if (m.isMeasure()) {
                    if (!m.isCalculated()
                        || SqlConstraintUtils.isSupportedCalculatedMember(m)) 
                    {
                          rs.measureFound = true;
                          if (m instanceof RolapStoredMeasure) {
                              if (!saveStoredMeasure((RolapStoredMeasure)m)) {
                                  // Virtual Cube with multiple Measures from
                                  // different cubes, returning false
                                  rs.multipleFactsFound = true;
                              }
                          }
                    }
                } else {
                    if (m.isAll()) {
                        rs.allHierarchies.add(m.getHierarchy());
                    } else {
                        rs.nonAllHierarchies.add(m.getHierarchy());
                    }
                }
            } else if (exp instanceof NamedSetExpr) {
                NamedSet set = ((NamedSetExpr)exp).getNamedSet();
                // verify all the elements in the set are measures, which imply that we are joining to the fact table.  At this time we don't support
                // other scenarios
                Exp nsExp = set.getExp();
                traverseExp(nsExp, rs);
            } else {
                LOGGER.debug("BooleanCountSqlCompiler: Unknown Expression Type: " + exp);
                rs.unknownFound = true;
            }
        }

        private List<Hierarchy> getNonEmptyHierarchies(Exp exp) {
            // return null if currentMember is not present at all
            // return null if a measure is not found
            // return a list of related hierarchies, if their all member is present somewhere in the stack
            // if there is a hierarchy present without an all member, return null for now.

            RecursiveState rs = new RecursiveState();
            traverseExp(exp, rs);
  
            // Check all non hierarchies, making sure they are associated with an all member selection for now.
            // long term, all expressions from non-all hierarchies should get added as predicates to the filter.
              for (Hierarchy h : rs.nonAllHierarchies) {
                boolean found = false;
                for (Hierarchy h1 : rs.allHierarchies) {
                    if (h.equals(h1)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    rs.nonAllHierarchyFound = true;
                }
            }
            if (!rs.measureFound || rs.multipleFactsFound
                || !rs.currentMemberFound || rs.nonAllHierarchyFound
                || rs.unknownFound) 
            {
                return null;
            }
            return rs.allHierarchies;
        }

        public String compile(Exp exp) {
            if (!match(exp)) {
                return null;
            }
            Exp[] args = ((FunCall) exp).getArgs();
            
            // if (args[1]) is EXCLUDEEMPTY then we have to check for non-emptyness. 
            // otherwise this is always true and the filter is meaningless.
            if (!(args[1] instanceof Literal) || !((Literal)args[1]).getValue().equals("EXCLUDEEMPTY")) {
                return null;
            }
  
            List<Hierarchy> hierarchies = getNonEmptyHierarchies(args[0]);
            if (hierarchies == null) {
                return null;
            }
            
            // args[0]
            // This is a set, the set should consist of the filtered currentMember and other dimensions,
            // potentially also specifying the measure in which to join the fact table with.  this will be important in the context
            // of virtual cubes.
    
            
            // TODO: if the other dimensions in play do not include their all
            // member, we need to filter the result of this query down to those
            // members.  For instance, if [Another Dim].[Member1] is specified, we need a 
            // filter on that member to get the right count.  Very similar to 
            // non empty crossjoin
            // but without selecting the value from the results.
            
            // Returning a no-op, because we want the filter expression to be 
            // successful, but don't have anything really to add to the SQL 
            // other than the measure we reference for joining on the dimension.
            return "<NOOP>";
        }
    }

    /**
     * Compiles an <code>IIF(cond, val1, val2)</code> expression into SQL
     * <code>CASE WHEN cond THEN val1 ELSE val2 END</code>.
     */
    class IifSqlCompiler extends FunCallSqlCompilerBase {

        SqlCompiler valueCompiler;

        IifSqlCompiler(int category, SqlCompiler valueCompiler) {
            super(category, "iif", 3);
            this.valueCompiler = valueCompiler;
        }

        public String compile(Exp exp) {
            if (!match(exp)) {
                return null;
            }
            Exp[] args = ((FunCall) exp).getArgs();
            String cond = booleanCompiler.compile(args[0]);
            String val1 = valueCompiler.compile(args[1]);
            String val2 = valueCompiler.compile(args[2]);
            if (cond == null || (val1 == null && !isNullLiteral(args[1])) || (val2 == null && !isNullLiteral(args[2]))) {
                return null;
            }
            return sqlQuery.getDialect().caseWhenElse(cond, val1, val2);
        }

        private boolean isNullLiteral(Exp exp) {
            return exp instanceof Literal && exp.getType() instanceof NullType;
        }
    }

    /**
     * This compiler should capture a measure and apply the context specified by the tuple.
     * Note that tests like BasicQueryTest.testDependsOn() demonstrate that only a single
     * tuple can be used in this context, more than one and there's an issue.
     */
    class TupleSqlCompiler extends StoredMeasureSqlCompiler {

        // The measue at play may be stored or calculated, bot are supported
        // so we inherit from storedmeasure and delegate to the calculated member compiler
        CalculatedMemberSqlCompiler calcCompiler;

        // pre eval compiler to help with handling function call expressions
        // in the tuple, specifically StrToMember
        PreEvalSqlCompiler preEvalSqlCompiler;

        public TupleSqlCompiler(CalculatedMemberSqlCompiler calcCompiler,
                                PreEvalSqlCompiler preEvalSqlCompiler) {
            this.calcCompiler = calcCompiler;
            this.preEvalSqlCompiler = preEvalSqlCompiler;
        }

        public String compile(Exp exp) {
          // first determine if we are in a tuple function

          if (!(exp instanceof FunCall)) {
              return null;
          }
          FunCall fc = (FunCall) exp;
          if (!"()".equalsIgnoreCase(fc.getFunName())) {
              return null;
          }
          Exp[] args = fc.getArgs();
          // for now, support regular members and one calculation
          Exp measureExp = null;
          boolean calculated = false;
          for (Exp argExp : args) {
              if (getMeasure( argExp) != null) {
                  if (measureExp != null) {
                      // already found measure, can't support two
                      return null;
                  }
                  measureExp = argExp;
              } else {
                  // if there is a current member reference, we skip over it
                  // because we'd just be using the current context.
                  boolean currentMember = false;
                  if (argExp instanceof ResolvedFunCall && ((ResolvedFunCall) argExp).getFunName().equalsIgnoreCase("CurrentMember")) {
                      currentMember = true;
                  }
                  if (!currentMember) {
                      if (!(argExp instanceof MemberExpr)) {
                          if ((argExp instanceof FunCall)
                              && ((FunCall) argExp).getFunName()
                                     .equalsIgnoreCase("strtomember")
                              && preEvalSqlCompiler.supportsExp(argExp)) {
                                  argExp = preEvalSqlCompiler.getMemberExpr(argExp);
                          } else {
                              return null;
                          }
                      }
                      Member member = ((MemberExpr)argExp).getMember();
                      if (member.isCalculated()) {
                          // first check to see if this is of the measures dim
                          if (measureExp != null) {
                              return null;
                          }
                          calculated = true;
                          measureExp = argExp;
                      } else {
                          addlContext.add(member);
                      }
                  }
              }
          }
          if (calculated) {
              return calcCompiler.compile(measureExp);
          } else {
              return super.compile(measureExp);
          }
        }
    }

    /**
     * This compiler attempts to pre-evaluate static values before pushing down to SQL
     */
    class PreEvalSqlCompiler implements SqlCompiler {
        
        private SqlCompiler parentCompiler;

        public PreEvalSqlCompiler(SqlCompiler compiler) {
            this.parentCompiler = compiler;
        }

        public boolean supportsExp( Exp exp ) {
            if (exp instanceof FunCall) {
                FunCall funcall = (FunCall) exp;
                if (funcall.getFunName().equalsIgnoreCase("val") && funcall.getArgCount() == 1) {
                    return supportsExp(funcall.getArg(0));
                } else if (funcall.getFunName().equalsIgnoreCase("ccur") && funcall.getArgCount() == 1) {
                    return supportsExp(funcall.getArg(0));
                } else if (funcall.getFunName().equalsIgnoreCase("strtomember")) {
                    if(funcall.getArgCount() == 2 && !(funcall.getArg(1) instanceof Literal)) {
                        return false;
                    }
                    return supportsExp(funcall.getArg(0)) || funcall.getArg(0) instanceof Literal;
                } else if (funcall.getFunName().equalsIgnoreCase("CurrentMember")) {
                    // we need to verify that the current member isn't in the filter section
                    final RolapCubeDimension dimension;
                    final Exp dimExpr = ((ResolvedFunCall)funcall).getArg(0);
                    if (dimExpr instanceof DimensionExpr) {
                        dimension =
                            (RolapCubeDimension) evaluator.getCachedResult(
                                new ExpCacheDescriptor(dimExpr, evaluator));
                    } else if (dimExpr instanceof HierarchyExpr) {
                        final RolapCubeHierarchy hierarchy =
                            (RolapCubeHierarchy) evaluator.getCachedResult(
                                new ExpCacheDescriptor(dimExpr, evaluator));
                        dimension = (RolapCubeDimension) hierarchy.getDimension();
                    } else if (dimExpr instanceof LevelExpr) {
                        final RolapCubeLevel level =
                            (RolapCubeLevel) evaluator.getCachedResult(
                                new ExpCacheDescriptor(dimExpr, evaluator));
                        dimension = (RolapCubeDimension) level.getDimension();
                    } else {
                        return false;
                    }

                    // If this dimension is being filtered, we can't pre evaluate it.
                    if (rolapLevel == null
                        || dimension.equals(rolapLevel.getDimension()))
                    {
                      return false;
                    }

                    return true;
                } else if (funcall.getFunName().equalsIgnoreCase("Properties") && funcall.getArgCount() == 2) {
                    if (!(funcall.getArg(1) instanceof Literal)) {
                        return false;
                    }
                    boolean validMember = false;
                    if(funcall.getArg(0) instanceof MemberExpr) {
                        Member member = ((MemberExpr) funcall.getArg(0)).getMember();
                        validMember = !member.isCalculated() && !member.isMeasure();
                    }
                    return validMember || supportsExp(funcall.getArg(0));
                } else if ((funcall.getFunName().equalsIgnoreCase("Name")
                            || funcall.getFunName().equalsIgnoreCase("Caption"))
                    && funcall.getArgCount() == 1) {
                    boolean validMember = false;
                    if (funcall.getArg(0) instanceof MemberExpr) {
                        Member member = ((MemberExpr) funcall.getArg(0)).getMember();
                        validMember = !member.isCalculated() && !member.isMeasure();
                    }
                    return validMember || supportsExp(funcall.getArg(0));
                }
            } else if (exp instanceof ParameterExpr) {
                return true;
            } 
            return false;
        }
        
        public MemberExpr getMemberExpr(Exp exp) {
            MemberExpr memberExpr = preEvalMembers.get(exp.toString());
            if(memberExpr != null) {
                return memberExpr;
            }
            ExpCompiler compiler = evaluator.getQuery().createCompiler();
            MemberCalc calc = compiler.compileMember(exp);
            Member member = calc.evaluateMember(evaluator);
            memberExpr = new MemberExpr(member);
            preEvalMembers.put(exp.toString(), memberExpr);
            return memberExpr;
        }

        public String compile(Exp exp) {
            if (!supportsExp(exp)) {
                return null;
            }
            // Evaluate this expression and turn it into an appropriate Value.
            if (preEvalExprs.containsKey(exp.toString())) {
                return preEvalExprs.get(exp.toString());
            }
            Object results;
            if ((exp instanceof FunCall) && ((FunCall) exp).getFunName()
                                                           .equalsIgnoreCase("strtomember")) {
                MemberExpr expr = getMemberExpr(exp);
                results = parentCompiler.compile(expr);
            } else {
                ExpCompiler compiler = evaluator.getQuery().createCompiler();
                Calc calc = compiler.compileScalar(exp, true);
                results = calc.evaluate(evaluator);
                // convert a null string to a SQL string
                if (results == null) {
                    results = "NULL";
                }
            }
            preEvalExprs.put(exp.toString(), results.toString());
            return results.toString();
        }

        public String toString() {
            return "PreEvalSqlCompiler";
        }
    }

    /**
     * Creates a RolapNativeSql.
     *
     * @param sqlQuery the query which is needed for different SQL dialects -
     * it is not modified
     */
    public RolapNativeSql(
        SqlQuery sqlQuery,
        AggStar aggStar,
        Evaluator evaluator,
        RolapLevel rolapLevel,
        Map<String, String> preEvalExprs)
    {
        this.sqlQuery = sqlQuery;
        this.rolapLevel = rolapLevel;
        this.evaluator = evaluator;
        this.dialect = sqlQuery.getDialect();
        this.aggStar = aggStar;
        this.preEvalExprs = preEvalExprs;

        numericCompiler = new CompositeSqlCompiler();
        booleanCompiler = new CompositeSqlCompiler();
        PreEvalSqlCompiler preEvalSqlCompiler = new PreEvalSqlCompiler(numericCompiler);
        numericCompiler.add(preEvalSqlCompiler );
        numericCompiler.add(new NumberSqlCompiler());
        numericCompiler.add(new StoredMeasureSqlCompiler());
        CalculatedMemberSqlCompiler calcCompiler = new CalculatedMemberSqlCompiler(numericCompiler);
        numericCompiler.add(calcCompiler);
        numericCompiler.add(new TupleSqlCompiler(calcCompiler, preEvalSqlCompiler));
        numericCompiler.add(
            new ParenthesisSqlCompiler(Category.Numeric, numericCompiler));
        numericCompiler.add(
            new InfixOpSqlCompiler(
                Category.Numeric, "+", "+", numericCompiler));
        numericCompiler.add(
            new InfixOpSqlCompiler(
                Category.Numeric, "-", "-", numericCompiler));
        numericCompiler.add(
            new InfixOpSqlCompiler(
                Category.Numeric, "/", "/", numericCompiler));
        numericCompiler.add(
            new InfixOpSqlCompiler(
                Category.Numeric, "*", "*", numericCompiler));
        numericCompiler.add(
            new IifSqlCompiler(Category.Numeric, numericCompiler));
        numericCompiler.add(
            new UpperLowerCaseSqlCompiler(
                Category.String, "UCase", numericCompiler));
        numericCompiler.add(
            new UpperLowerCaseSqlCompiler(
                Category.String, "LCase", numericCompiler));
        numericCompiler.add(
            new InStrSqlCompiler(
                Category.String, numericCompiler));

        booleanCompiler.add(
            new InfixOpSqlCompiler(
                Category.Logical, "<", "<", numericCompiler));
        booleanCompiler.add(
            new InfixOpSqlCompiler(
                Category.Logical, "<=", "<=", numericCompiler));
        booleanCompiler.add(
            new InfixOpSqlCompiler(
                Category.Logical, ">", ">", numericCompiler));
        booleanCompiler.add(
            new InfixOpSqlCompiler(
                Category.Logical, ">=", ">=", numericCompiler));
        booleanCompiler.add(
            new InfixOpSqlCompiler(
                Category.Logical, "=", "=", numericCompiler));
        booleanCompiler.add(
            new InfixOpSqlCompiler(
                Category.Logical, "<>", "<>", numericCompiler));
        booleanCompiler.add(
            new IsEmptySqlCompiler(
                Category.Logical, "IsEmpty", numericCompiler));
        booleanCompiler.add(
            new BooleanCountSqlCompiler(
                Category.Logical,numericCompiler));
        booleanCompiler.add(
            new InfixOpSqlCompiler(
                Category.Logical, "and", "AND", booleanCompiler));
        booleanCompiler.add(
            new InfixOpSqlCompiler(
                Category.Logical, "or", "OR", booleanCompiler));
        booleanCompiler.add(
            new UnaryOpSqlCompiler(
                Category.Logical, "not", "NOT", booleanCompiler));
        booleanCompiler.add(
            new MatchingSqlCompiler());
        booleanCompiler.add(
            new ParenthesisSqlCompiler(Category.Logical, booleanCompiler));
        booleanCompiler.add(
            new IifSqlCompiler(Category.Logical, booleanCompiler));
        //booleanCompiler.add(new CurrentMemberInSqlCompiler(numericCompiler));
    }

    /**
     * Generates an aggregate of a measure, e.g. "sum(Store_Sales)" for
     * TopCount. The returned expr will be added to the select list and to the
     * order by clause.
     */
    public String generateTopCountOrderBy(Exp exp) {
        return numericCompiler.compile(exp);
    }

    public String generateFilterCondition(Exp exp) {
        return booleanCompiler.compile(exp);
    }

    public RolapStoredMeasure getStoredMeasure() {
        return storedMeasure;
    }

}

// End RolapNativeSql.java
