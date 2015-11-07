package mondrian.rolap;

import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Level;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

/**
 * Computes an Except([set], [set]) in SQL.
 *
 * @author smalik
 * @since 8/17/2015
 */
public class RolapNativeExcept extends RolapNativeSet {
    public RolapNativeExcept() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeExcept.get());
    }

    protected boolean restrictMemberTypes() {
        // can't really handle calculated measures
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

        String funName = fun.getName();
        if (!"Except".equalsIgnoreCase(funName)) {
            return null;
        }

        if (args.length != 2) {
            return null;
        }

        if (!ExceptFunctionConstraint.isValidContext(
                evaluator, false, new Level[]{}, restrictMemberTypes(), false))
        {
            return null;
        }

        SchemaReader schemaReader = evaluator.getSchemaReader();
        DataSource ds = schemaReader.getDataSource();
        SqlQuery sqlQuery = SqlQuery.newQuery(ds, "NativeExcept");
        if (!sqlQuery.getDialect().supportsMultiValueInExpr()) {
            return null;
        }

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

        CrossJoinArg[] cjArgs = allArgs.get(0);
        if (isPreferInterpreter(cjArgs, false)) {
            return null;
        }

        List<CrossJoinArg[]> exceptArgs =
            crossJoinArgFactory().checkCrossJoinArg(evaluator, args[1]);
        if (exceptArgs == null || exceptArgs.isEmpty() || exceptArgs.get(0) == null) {
            return null;
        }

        List<List<RolapMember>> exceptMembers = new ArrayList<List<RolapMember>>();
        for (CrossJoinArg crossJoinArg : exceptArgs.get(0)) {
            exceptMembers.add(crossJoinArg.getMembers());
        }

        if (exceptMembers.isEmpty()) {
            return null;
        }

        SetEvaluator eval = getNestedEvaluator(args[0], evaluator);
        if (eval == null) {
/*
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

            CrossJoinArg[] cjArgs = allArgs.get(0);
            if (isPreferInterpreter(cjArgs, false)) {
                return null;
            }
*/
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

                TupleConstraint constraint =
                    new ExceptFunctionConstraint(
                        combinedArgs, exceptMembers, evaluator, null);
                SetEvaluator sev =
                    new SetEvaluator(cjArgs, schemaReader, constraint);
                LOGGER.debug("using native except");
                return sev;
            } finally {
                evaluator.restore(savepoint);
            }
        } else {
            // if subset wraps another native function, add start and count to the constraint.
            TupleConstraint constraint =
                new ExceptFunctionConstraint(
                    cjArgs, exceptMembers, evaluator, (SetConstraint)eval.getConstraint());
            eval.setConstraint(constraint);
            LOGGER.debug("using native except");
            return eval;
        }
    }

    static class ExceptFunctionConstraint extends DelegatingSetConstraint {
        List<List<RolapMember>> exceptMembers;

        ExceptFunctionConstraint(
            CrossJoinArg[] args,
            List<List<RolapMember>> exceptMembers,
            RolapEvaluator evaluator,
            SetConstraint parentConstraint)
        {
            super(args, evaluator, true, parentConstraint);
            this.exceptMembers = exceptMembers;
        }

        @Override
        public void addConstraint(SqlQuery sqlQuery, RolapCube baseCube, AggStar aggStar) {
            super.addConstraint(sqlQuery, baseCube, aggStar);
            for (List<RolapMember> members : exceptMembers) {
                SqlConstraintUtils.addMemberConstraint(sqlQuery, baseCube, aggStar, members, true, false, true);
            }
        }

        @Override
        public Object getCacheKey() {
            CacheKey key = new CacheKey((CacheKey) super.getCacheKey());
            if (this.getEvaluator() instanceof RolapEvaluator) {
                key.setSlicerMembers(((RolapEvaluator) this.getEvaluator()).getSlicerMembers());
            }
            key.setValue(getClass().getName() + ".exceptMembers", exceptMembers);
            return key;
        }
    }
}
