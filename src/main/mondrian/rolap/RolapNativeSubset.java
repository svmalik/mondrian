/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2014 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import java.util.List;

import javax.sql.DataSource;

import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Literal;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;

/**
 * Computes a Subset in SQL.
 *
 * @author Will Gorman <wgorman@pentaho.com>
 * @since Mar 6, 2014
  */
public class RolapNativeSubset extends RolapNativeSet {

    public RolapNativeSubset() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeSubset.get());
    }

    /**
     * This constraint can act in two ways.  Either as a self contained
     * constraint, or as a wrapper around an existing native constraint.
     */
    static class SubsetConstraint extends DelegatingSetConstraint {
        Integer start;
        Integer count;

        public SubsetConstraint(
            Integer start,
            Integer count,
            CrossJoinArg[] args, 
            RolapEvaluator evaluator,
            SetConstraint parentConstraint
            )
        {
            super(args, evaluator, true, parentConstraint);
            this.start = start;
            this.count = count;
        }

        public void addConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar)
        {
            if (start != null) {
                sqlQuery.setOffset(start);
            }
            if (count != null) {
              sqlQuery.setLimit(count);
            }

            if (isJoinRequired() || parentConstraint != null) {
                super.addConstraint(sqlQuery, baseCube, aggStar);
            } else if (args.length == 1) {
                args[0].addConstraint(sqlQuery, baseCube, null, false);
            }
        }

        protected boolean isJoinRequired() {
            if (parentConstraint != null) {
                return parentConstraint.isJoinRequired();
            } else {
                return args.length > 1;
            }
        }

        /**
         * This returns a CacheKey object
         * @return
         */
        public Object getCacheKey() {
            CacheKey key = new CacheKey((CacheKey) super.getCacheKey());
            if (this.getEvaluator() instanceof RolapEvaluator) {
                key.setSlicerMembers(((RolapEvaluator) this.getEvaluator()).getSlicerMembers());
            }
            key.setValue(getClass().getName() + ".start", start);
            key.setValue(getClass().getName()+ ".count", count);

            return key;
        }
    }

    protected boolean restrictMemberTypes() {
        return true;
    }

    NativeEvaluator createEvaluator(
        RolapEvaluator evaluator,
        FunDef fun,
        Exp[] args)
    {
        if (!isEnabled()) {
            return null;
        }
        if (!SubsetConstraint.isValidContext(
                evaluator, false, new Level[]{}, restrictMemberTypes(), false))
        {
            return null;
        }

        // is this "Subset(<set>, <start>, [<count>])"
        String funName = fun.getName();
        if (!"Subset".equalsIgnoreCase(funName)) {
            return null;
        }
        if (args.length < 2 || args.length > 3) {
            return null;
        }

        // extract start
        if (!(args[1] instanceof Literal)) {
            return null;
        }
        int start = ((Literal) args[1]).getIntValue();

        Integer count = null;
        if (args.length == 3) {
            if (!(args[2] instanceof Literal)) {
                return null;
            }
            count = ((Literal) args[2]).getIntValue();
        }

        SchemaReader schemaReader = evaluator.getSchemaReader();
        DataSource ds = schemaReader.getDataSource();
        SqlQuery sqlQuery = SqlQuery.newQuery(ds, "NativeSubset");

        if (!sqlQuery.getDialect().supportsLimitAndOffset()) {
          return null;
        }

        CrossJoinArg[] cjArgs = new CrossJoinArg[0];

        // first see if subset wraps another native evaluation (other than count and sum)
        SetEvaluator eval = getNestedEvaluator(args[0], evaluator);

        if (eval == null) {
            // extract the set expression
            List<CrossJoinArg[]> allArgs =
                crossJoinArgFactory().checkCrossJoinArg(evaluator, args[0]);

            // checkCrossJoinArg returns a list of CrossJoinArg arrays.  The first
            // array is the CrossJoin dimensions.  The second array, if any,
            // contains additional constraints on the dimensions. If either the list
            // or the first array is null, then native cross join is not feasible.
            if (allArgs == null || allArgs.isEmpty() || allArgs.get(0) == null) {
                return null;
            }

            cjArgs = allArgs.get(0);
            if (isPreferInterpreter(cjArgs, false)) {
                return null;
            }

            final int savepoint = evaluator.savepoint();
            try {
                overrideContext(evaluator, cjArgs, null);
                CrossJoinArg[] predicateArgs = null;
                if (allArgs.size() == 2) {
                    predicateArgs = allArgs.get(1);
                }
                CrossJoinArg[] combinedArgs;
                if (predicateArgs != null) {
                    // Combined the CJ and the additional predicate args
                    // to form the TupleConstraint.
                    combinedArgs =
                            Util.appendArrays(cjArgs, predicateArgs);
                } else {
                    combinedArgs = cjArgs;
                }

                if (cjArgs.length == 1) {
                    // reset all other hierarchies to their default member
                    Hierarchy h = cjArgs[0].getLevel() != null ? cjArgs[0].getLevel().getHierarchy() : null;
                    for (Member m : evaluator.getMembers()) {
                        if (!m.isMeasure() && !(m instanceof RolapAllCubeMember)
                            && (h == null || !h.equals(m.getLevel().getHierarchy())))
                        {
                            evaluator.setContext(m.getLevel().getHierarchy().getAllMember());
                        }
                    }
                }

                SetConstraint constraint =
                    new SubsetConstraint(
                        start, count, combinedArgs, evaluator, null);
                if (isInvalidConstraint(constraint, evaluator)) {
                    return null;
                }
                SetEvaluator sev =
                    new SetEvaluator(cjArgs, schemaReader, constraint);
                if (count != null) {
                    sev.setMaxRows(count);
                }
                LOGGER.debug("using native subset");
                return sev;
            } finally {
                evaluator.restore(savepoint);
            }
        } else {
            // if subset wraps another native function, add start and count to the constraint.
            TupleConstraint constraint =
                new SubsetConstraint(
                    start, count, cjArgs, evaluator, (SetConstraint)eval.getConstraint());
            eval.setConstraint(constraint);
            if (count != null) {
                eval.setMaxRows(count);
            }
            LOGGER.debug("using native subset");
            return eval;
        }
    }
}

// End RolapNativeSubset.java
