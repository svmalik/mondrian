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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mondrian.calc.Calc;
import mondrian.mdx.LevelExpr;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.mdx.UnresolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.Formula;
import mondrian.olap.FunTable;
import mondrian.olap.MondrianDef;
import mondrian.olap.Parameter;
import mondrian.olap.Property;
import mondrian.olap.Query;
import mondrian.olap.QueryAxis;
import mondrian.olap.QueryPart;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.olap.ValidatorImpl;
import mondrian.olap.fun.DescendantsFunDef;
import mondrian.olap.fun.DescendantsFunDef.Flag;
import mondrian.olap.fun.AggregateFunDef;
import mondrian.olap.fun.FunUtil;
import mondrian.olap.fun.LevelMembersFunDef;
import mondrian.resource.MondrianResource;
import mondrian.server.Statement;

/**
 * While under development, this class holds the majority of the logic for calculated cells, to reduce merge impacts across the various classes in Mondrian.
 * 
 * @author Will Gorman <wgorman@pentaho.com>
 *
 */
public class CalculatedCellUtil {

    /**
     * Note, this function needs to be highly performant.  Cell Checks
     * are pre-calculated for efficent execution.
     *
     * Additional Ideas for increasing performance include:
     *  - look at prioritizing the cube expressions in an order based on cardinality / usage / etc
     *  - review role based security processing for performance ideas
     *  
     * @param calc
     * @return
     */
    public static boolean insideSubcube(RolapEvaluator eval, CellCalc calc) {
        for (int i = 0; i < calc.cubeExps.length; i++) {
            if (!calc.inside[i].inside(eval)) {
              return false;
            }
        }
        return true;
    }
    
    static public class CellCalcReturn {
      public List<CellCalc> currentCellCalcs = new ArrayList<CellCalc>();
      public List<RolapCellCalculation> newCellCalcs = new ArrayList<RolapCellCalculation>();
    }

    /**
     * This method is called by RolapEvaluator to determine which cell calculations apply to the current context. 
     * 
     * @param The evaluator
     * 
     * @return a list of cell calculations to apply.
     */
    public static CellCalcReturn applyCellCalculations(RolapEvaluator eval) {
        CellCalcReturn ret = null;
        if (eval.getCube().cellCalcs != null) {
            for (CellCalc calc : eval.getCube().cellCalcs) {
              if (!eval.root.activeCellCalcs.contains(calc) && insideSubcube(eval, calc)) {
                  // track that we are inside this subcube, otherwise
                  // this calculation would get executed repeatedly.
                  eval.root.activeCellCalcs.add(calc);
      
                  // lazy init to avoid extra memory use and object creation
                  if (ret == null) {
                    ret = new CellCalcReturn();
                  }
                  ret.currentCellCalcs.add(calc);
                  final Calc calcVal = eval.root.getCompiled(calc.cellExp, true, null);
                  RolapCellCalculation calcObj = new RolapCellCalculation(calcVal, calc.solve_order, calc.cellExp);
                  // adds the cell calculation to the evaluator.
                  eval.addCalculation(calcObj, true);
                  ret.newCellCalcs.add(calcObj);
                } 
            }
        }
        return ret;
    }

    /**
     * This class encapsulates a cell calculation for the evaluator.
     */
    static class RolapCellCalculation implements RolapCalculation {
        int calcSolveOrder;
        Calc calcVal;
        Boolean containsAggregateFunction = null;
        Exp cellExp;
        
        public RolapCellCalculation(Calc calcVal, int calcSolveOrder, Exp cellExp) {
            this.calcVal = calcVal;
            this.calcSolveOrder = calcSolveOrder;
            this.cellExp = cellExp;
        }
        
        public void setContextIn( RolapEvaluator evaluator ) {
            evaluator.removeCalculation(this, true);
        }
    
        public int getSolveOrder() {
            return calcSolveOrder;
        }
    
        public int getHierarchyOrdinal() {
            // This calc is not associated with a hierarchy, it should be calculated first if
            // there is a solve order tie.
            return -1;
        }
    
        public boolean isCalculatedInQuery() {
            return false;
        }
    
        public Calc getCompiledExpression( RolapEvaluatorRoot root ) {
            return calcVal;
        }
    
        public boolean containsAggregateFunction() {
          // searching for agg functions is expensive, so cache result
          if (containsAggregateFunction == null) {
              containsAggregateFunction =
                  foundAggregateFunction(cellExp);
          }
          return containsAggregateFunction;
      }

      /**
       * Returns whether an expression contains a call to an aggregate
       * function such as "Aggregate" or "Sum".
       *
       * @param exp Expression
       * @return Whether expression contains a call to an aggregate function.
       */
      private static boolean foundAggregateFunction(Exp exp) {
          if (exp instanceof ResolvedFunCall) {
              ResolvedFunCall resolvedFunCall = (ResolvedFunCall) exp;
              if (resolvedFunCall.getFunDef() instanceof AggregateFunDef) {
                  return true;
              } else {
                  for (Exp argExp : resolvedFunCall.getArgs()) {
                      if (foundAggregateFunction(argExp)) {
                          return true;
                      }
                  }
              }
          }
          return false;
      }

    };
    
    /**
     * This function is called by cubes and virtual cubes during initialization
     *
     * @param The cube initializing
     * @param The calculated cells defined in the schema for this cube
     * @param a list to populate with processed cell calculations
     */
    public static void processCalculatedCells(RolapCube cube, MondrianDef.CalculatedCell[] calculatedCells, List<CellCalc> cellCalcs) {
        if (calculatedCells == null) return;
        for (int i = 0; i < calculatedCells.length; i++) {
            final String subcubeString = calculatedCells[i].subcube.cdata;
            final String cellString = calculatedCells[i].formula.cdata;
            final CellCalc cellCalc = new CellCalc();
            try {
                cellCalc.cubeExp = cube.getSchema().getInternalConnection().parseExpression(subcubeString);
                cellCalc.cellExp = cube.getSchema().getInternalConnection().parseExpression(cellString);
                CellValidator valid = new CellValidator(cube.getSchema().getFunTable(), cube.getSchemaReader(), cube);
                for (MondrianDef.CalculatedCellProperty prop : calculatedCells[i].cellProperties) {
                    if (prop.name.equals(Property.SOLVE_ORDER.name)) {
                        try {
                            cellCalc.solve_order = Integer.parseInt(prop.value);
                        } catch (NumberFormatException e) {
                            // not a number, ignore
                        }
                    } else {
                        if (prop.value == null && prop.expression == null) {
                            // TODO: create clean error message
                            throw new UnsupportedOperationException();
                        }
                        String propExpr = null;
                        if (prop.value != null) {
                            // quote literal
                            propExpr = Util.quoteForMdx(prop.value);
                        } else {
                            propExpr = prop.expression;
                        }
                        Exp propExp = cube.getSchema().getInternalConnection().parseExpression(propExpr);
                        Exp resolvedExp = valid.validate(propExp, false);
                        cellCalc.cellProperties.put(prop.name, resolvedExp);
                    }
                }

                for (String prop : Property.FORMAT_PROPERTIES) {
                    Exp formatExp = (Exp)cellCalc.cellProperties.get(prop);
                        if (formatExp != null) {
                            cellCalc.cellProperties.put(
                                Property.FORMAT_EXP_PARSED.name, formatExp);
                            cellCalc.cellProperties.put(
                                Property.FORMAT_EXP.name, Util.unparse(formatExp));
                        break;
                    }
                }

                if (((UnresolvedFunCall)cellCalc.cubeExp).getFunName().equals("()")) {
                    cellCalc.cubeExps = new Exp[((UnresolvedFunCall)cellCalc.cubeExp).getArgCount()];
                    cellCalc.inside = new InsideCellCalc[((UnresolvedFunCall)cellCalc.cubeExp).getArgCount()];
                    for (int j = 0; j < ((UnresolvedFunCall)cellCalc.cubeExp).getArgCount(); j++) {
                        cellCalc.cubeExps[j] = valid.validate( ((UnresolvedFunCall)cellCalc.cubeExp).getArg(j), false );
                        cellCalc.inside[j] = genInside(cellCalc.cubeExps[j]);
                    }
                } else {
                    throw new UnsupportedOperationException();
                }
                cellCalc.cellExp = valid.validate( cellCalc.cellExp, false);
                cellCalcs.add( cellCalc );
            } catch (Exception e) {
                throw MondrianResource.instance().NamedSetHasBadFormula.ex(
                    subcubeString, e);
            }
        }
    }

    private static InsideCellCalc genInside(Exp exp) {
        if (exp instanceof MemberExpr) {
            MemberInside mi = new MemberInside();
            mi.member = (RolapMember)((MemberExpr)exp).getMember();
            mi.ordinal = mi.member.getHierarchy().getOrdinalInCube();
            return mi;
        } else if (exp instanceof ResolvedFunCall && ((ResolvedFunCall)exp).getFunDef() instanceof LevelMembersFunDef) {
            LevelInside li = new LevelInside();
            li.lvl = (RolapCubeLevel)((LevelExpr)((ResolvedFunCall)exp).getArg(0)).getLevel();
            li.ordinal = li.lvl.getHierarchy().getOrdinalInCube();
            return li;
        } else if (exp instanceof ResolvedFunCall && ((ResolvedFunCall)exp).getFunDef() instanceof DescendantsFunDef) {
            if (((ResolvedFunCall)exp).getArgCount() == 1 && ((ResolvedFunCall)exp).getArg(0) instanceof MemberExpr) {
                DescendantsMemberInside dmi = new DescendantsMemberInside();
                dmi.member = (RolapMember)((MemberExpr)((ResolvedFunCall)exp).getArg(0)).getMember();
                dmi.ordinal = dmi.member.getHierarchy().getOrdinalInCube();
                return dmi;
            } else if (((ResolvedFunCall)exp).getArg(0) instanceof MemberExpr) {
                DescendantsMemberLevelInside dmli = new DescendantsMemberLevelInside();
                // first grab the member object
                dmli.member = (RolapMember)((MemberExpr)((ResolvedFunCall)exp).getArg( 0 )).getMember();
                dmli.ordinal = dmli.member.getHierarchy().getOrdinalInCube();
                if (((ResolvedFunCall)exp).getArg(1) instanceof LevelExpr) {
                    dmli.level = (RolapCubeLevel)((LevelExpr)((ResolvedFunCall)exp).getArg( 1 )).getLevel();
                } else {
                    // TODO: Support Numeric Expressions (Level Depth)
                    throw new UnsupportedOperationException();
                }
                dmli.flag = Flag.SELF;
                if (((ResolvedFunCall)exp).getArgCount() > 2) {
                    dmli.flag = FunUtil.getLiteralArg((ResolvedFunCall)exp, 2, Flag.SELF, Flag.class);
                }
                return dmli;
            } else {
                // TODO: Throw better exception when sets are in play - check during schema time vs. runtime.
                throw new UnsupportedOperationException();
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * This class is used to process the cell calculation subcube and formula definitions.
     */
    private static class CellValidator extends ValidatorImpl {
        SchemaReader reader;
        Query query;
        public CellValidator(FunTable funTable, SchemaReader reader, RolapCube cube) {
            super(funTable, new HashMap<QueryPart, QueryPart>());
            this.reader = reader;
            final Statement statement =
                reader.getSchema().getInternalConnection().getInternalStatement();
            this.query =
                new Query(
                    statement,
                    cube,
                    new Formula[] {},
                    new QueryAxis[0],
                    null,
                    new QueryPart[0],
                    Collections.<Parameter>emptyList(),
                    false);
        }
        public Query getQuery() {
            return query;
        }
        public SchemaReader getSchemaReader() {
            return reader;
        }
        protected void defineParameter(Parameter param) {
        }
    }

    /**
     * This interface defines a calculation to determine if we are within a
     * cell calculation during regular evaluation.
     *
     * These calculations are prepared during schema load, increasing the
     * performance of cell calculations by reducing interpreting during execution.
     */
    static interface InsideCellCalc {
        boolean inside(RolapEvaluator eval);
    }

    /**
     * A Member Calculation
     */
    static class MemberInside implements InsideCellCalc {
        RolapMember member;
        int ordinal;
        public boolean inside(RolapEvaluator eval) {
            final RolapMember curr = (RolapMember)eval.getMembers()[ordinal];
            if (!curr.equals(member)) {
                return false;
            }
            return true;
        }
    }

    /**
     * A Level Calculation
     */
    static class LevelInside implements InsideCellCalc {
        RolapCubeLevel lvl;
        int ordinal;
        public boolean inside(RolapEvaluator eval) {
            final RolapMember curr = (RolapMember)eval.getMembers()[ordinal];
            if (!curr.getLevel().equals(lvl)) {
                return false;
            }
            return true;
        }
    }

    /**
     * A Descendants Calculation
     */
    static class DescendantsMemberInside implements InsideCellCalc {
        RolapMember member;
        int ordinal;
        public boolean inside(RolapEvaluator eval) {
            final RolapMember curr = (RolapMember)eval.getMembers()[ordinal];
            if (!curr.isChildOrEqualTo(member)) {
                return false;
            }
            return true;
        }
    }

    /**
     * A Descendants with Level Calculation
     */
    static class DescendantsMemberLevelInside implements InsideCellCalc {
        RolapMember member;
        int ordinal;
        RolapCubeLevel level;
        Flag flag;
        public boolean inside(RolapEvaluator eval) {
            // first grab the member object
            final RolapMember curr = (RolapMember)eval.getMembers()[ordinal];
            // all of the checks below need to make sure the member is at least a descendant.
            if (!curr.isChildOrEqualTo(member)) {
                return false;
            }
            if (flag.leaves) {
                if (curr.getLevel().getHierarchy().getLevels()[curr.getLevel().getHierarchy().getLevels().length - 1]
                        != curr.getLevel()) {
                    return false;
                }
            } else {
                if (flag.self) {
                    if (curr.getLevel().getDepth() == level.getDepth()) {
                        return true;
                    }
                }
                if (flag.before) {
                    if (curr.getLevel().getDepth() < level.getDepth()) {
                        return true;
                    }
                }
                if (flag.after) {
                    if (curr.getLevel().getDepth() > level.getDepth()) {
                        return true;
                    }
                }
                return false;
            }
            return true;
        }
    }

    /**
     * This struct contains the necessary info for processing cell calculations.
     */
    public static class CellCalc {
        Exp cubeExp;
        Exp cubeExps[];
        InsideCellCalc inside[];
        Exp cellExp;
        int solve_order;
        Map<String, Object> cellProperties = new HashMap<String, Object>();

        public Object getPropertyValue(String name) {
            return cellProperties.get(name);
        }
    }
}
