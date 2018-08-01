package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.DoubleCalc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.impl.AbstractDoubleCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;

public class DivideFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Divide",
            "Divide(<numerator>, <denominator> [,<alternateResult>])",
            "Performs division and returns alternate result or NULL on division by 0.",
            new String[] {"fnnn", "fnnnn"},
            DivideFunDef.class);

    public DivideFunDef(FunDef funDef) {
        super(funDef);
    }

    @Override
    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Exp[] args = call.getArgs();
        final DoubleCalc calc0 = compiler.compileDouble(call.getArg(0));
        final DoubleCalc calc1 = compiler.compileDouble(call.getArg(1));
        final DoubleCalc altCalc = args.length > 2
            ? compiler.compileDouble(call.getArg(2))
            : null;

        return new AbstractDoubleCalc(call, new Calc[] {calc0, calc1, altCalc}) {
            @Override
            public double evaluateDouble(Evaluator evaluator) {
                final double numerator = calc0.evaluateDouble(evaluator);
                final double denominator = calc1.evaluateDouble(evaluator);

                if (numerator == DoubleNull || denominator == DoubleNull || denominator == 0.0) {
                    return altCalc == null ? FunUtil.DoubleNull : altCalc.evaluateDouble(evaluator);
                } else {
                    return numerator / denominator;
                }
            }
        };
    }
}
