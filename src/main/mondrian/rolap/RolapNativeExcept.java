package mondrian.rolap;

import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.olap.fun.FunUtil;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.DescendantsCrossJoinArg;
import mondrian.rolap.sql.MemberListCrossJoinArg;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
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
        if (failedCjArg(allArgs)) {
            return null;
        }

        CrossJoinArg[] cjArgs = allArgs.get(0);
        if (isPreferInterpreter(cjArgs, false)) {
            return null;
        }

        List<CrossJoinArg[]> exceptArgs = getExceptArgs(evaluator, cjArgs, args[1]);
        if (exceptArgs == null) {
            exceptArgs =
                crossJoinArgFactory().checkCrossJoinArg(evaluator, args[1]);
        }

        if (failedCjArg(exceptArgs)) {
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

                ExceptFunctionConstraint constraint =
                    new ExceptFunctionConstraint(
                        combinedArgs, exceptMembers, evaluator, null);
                if (isInvalidConstraint(constraint, evaluator)) {
                    return null;
                }
                SetEvaluator sev =
                    new SetEvaluator(cjArgs, schemaReader, constraint);
                LOGGER.debug("using native except");
                return sev;
            } finally {
                evaluator.restore(savepoint);
            }
        } else {
            SetConstraint parentConstraint = (SetConstraint) eval.getConstraint();
            RolapEvaluator parentEvaluator = (RolapEvaluator) parentConstraint.getEvaluator();
            TupleConstraint constraint =
                new ExceptFunctionConstraint(
                    cjArgs, exceptMembers, parentEvaluator, parentConstraint);
            eval.setConstraint(constraint);
            LOGGER.debug("using native except");
            return eval;
        }
    }

    private List<CrossJoinArg[]> getExceptArgs(
        RolapEvaluator evaluator, CrossJoinArg[] cjArgs, Exp arg1)
    {
        if (cjArgs.length == 1 && cjArgs[0] instanceof DescendantsCrossJoinArg) {
            // try to remove members from other levels and NULL to not fail
            Level level = cjArgs[0].getLevel();
            ResolvedFunCall arg1Call = FunUtil.extractResolvedFunCall(arg1);
            if (level != null && arg1Call != null && "{}".equals(arg1Call.getFunName())) {
                HashSet<Level> levels = new HashSet<>();
                while (level != null) {
                    levels.add(level);
                    level = level.getChildLevel();
                }
                List<RolapMember> exceptMembers = new ArrayList<>(arg1Call.getArgCount());
                for (Exp exp : arg1Call.getArgs()) {
                    if (exp instanceof MemberExpr) {
                        Member member = ((MemberExpr) exp).getMember();
                        if (!levels.contains(member.getLevel()) || member.isNull()) {
                            continue;
                        }
                        if (member.isCalculated() && !member.isParentChildLeaf()) {
                            exceptMembers.clear();
                            break;
                        }
                        exceptMembers.add((RolapMember) member);
                    } else {
                        exceptMembers.clear();
                        break;
                    }
                }

                if (!exceptMembers.isEmpty()) {
                    CrossJoinArg exceptArg =
                        MemberListCrossJoinArg.create(
                            evaluator, exceptMembers, restrictMemberTypes(), false);
                    if (exceptArg != null) {
                        return Collections.singletonList(new CrossJoinArg[] { exceptArg });
                    }
                }
            }
        }
        return null;
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
            if (isJoinRequired()) {
                super.addConstraint(sqlQuery, baseCube, aggStar);
            } else if (args.length == 1) {
                args[0].addConstraint(sqlQuery, baseCube, null, false);
            }

            for (List<RolapMember> members : exceptMembers) {
                SqlConstraintUtils.addMemberConstraint(sqlQuery, baseCube, aggStar, members, true, false, true, false);
            }
        }

        protected boolean isJoinRequired() {
            if (parentConstraint != null) {
                return parentConstraint.isJoinRequired();
            } else {
                return args.length > 1;
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
