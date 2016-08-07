package com.bloom.runtime.compiler.select;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import com.bloom.runtime.Pair;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.CompilerUtils;
import com.bloom.runtime.compiler.exprs.Expr;
import com.bloom.runtime.compiler.visitors.ExprGenerationVisitor;
import com.bloom.runtime.compiler.visitors.ExprGenerationVisitor.WndAggCtx;
import com.bloom.runtime.utils.Factory;

import javassist.CannotCompileException;
import javassist.CtClass;
import javassist.CtField;

public abstract class ExprGenerator
{
  private static class GeneratedExpr
  {
    public final Expr expr;
    public final String code;
    
    public GeneratedExpr(Var var, Expr expr, String code)
    {
      this.expr = expr;
      this.code = code;
    }
  }
  
  private final Map<Integer, Var> varTable = Factory.makeLinkedMap();
  private final LinkedList<GeneratedExpr> exprStack = new LinkedList();
  private final List<Var> variables = new ArrayList();
  private List<Var> staticVars = new ArrayList();
  private List<String> staticInitExprs = new ArrayList();
  private int nextVarID = 0;
  private int nextGVarID = 0;
  private Compiler compiler;
  private final boolean isPatternMatch;
  private int loopId = -1;
  
  public ExprGenerator(Compiler compiler, boolean isPatternMatch)
  {
    this.compiler = compiler;
    this.isPatternMatch = isPatternMatch;
  }
  
  public void removeAllTmpVarsAndFrames()
  {
    popFrame(0);
  }
  
  private String getBitsFieldPrefix(boolean isStatic)
  {
    return isStatic ? "__gbits" : "__bits";
  }
  
  private String getBitsField(int fieldIndex, boolean isStatic)
  {
    return getBitsFieldPrefix(isStatic) + fieldIndex;
  }
  
  private String getBitsField(Var v)
  {
    return getBitsField(v.getID() >> 6, v.isStatic());
  }
  
  public String setNullBit(Var v, boolean isNull)
  {
    int bit = v.getID();
    long mask = 1L << (bit & 0x3F);
    String fld = getBitsField(v);
    String op = " &= " + (mask ^ 0xFFFFFFFFFFFFFFFF);
    return fld + op + "L;";
  }
  
  public String getNullBit(Var v)
  {
    int bit = v.getID();
    long mask = 1L << (bit & 0x3F);
    String fld = getBitsField(v);
    return "(" + fld + " & " + mask + "L)";
  }
  
  public String checkNullBits(List<Var> args)
  {
    BitSet lbs = new BitSet();
    BitSet gbs = new BitSet();
    for (Var v : args)
    {
      int bit = v.getID();
      if (v.isStatic()) {
        gbs.set(bit);
      } else {
        lbs.set(bit);
      }
    }
    StringBuilder sb = new StringBuilder("(");
    long[] lbits = lbs.toLongArray();
    long[] gbits = gbs.toLongArray();
    String del = "";
    for (int i = 0; i < lbits.length; i++)
    {
      long mask = lbits[i];
      if (mask != 0L)
      {
        String fld = getBitsField(i, false);
        sb.append(del).append("(" + fld + " & " + mask + "L)");
        del = " | ";
      }
    }
    for (int i = 0; i < gbits.length; i++)
    {
      long mask = gbits[i];
      if (mask != 0L)
      {
        String fld = getBitsField(i, true);
        sb.append(del).append("(" + fld + " & " + mask + "L)");
        del = " | ";
      }
    }
    sb.append(")");
    return sb.toString();
  }
  
  private Var makeNewStaticVar(Class<?> type)
  {
    assert (type != null);
    Var v = new Var("gvar", this.nextGVarID++, type, true);
    return v;
  }
  
  private Var makeNewTmpVar(Class<?> type)
  {
    assert (type != null);
    if (CompilerUtils.isParam(type)) {
      throw new RuntimeException("Invalid variable type");
    }
    Var v = new Var("var", this.nextVarID++, type, false);
    this.variables.add(v);
    return v;
  }
  
  public Var getVar(Expr e)
  {
    Var var = (Var)this.varTable.get(Integer.valueOf(e.eid));
    if (var == null)
    {
      Class<?> etype = e.getType();
      if (e.isConst())
      {
        Var newVar = makeNewStaticVar(etype);
        ExprGenerationVisitor ee = new ExprGenerationVisitor(this, e, newVar);
        String code = ee.exprCode();
        this.varTable.put(Integer.valueOf(e.eid), newVar);
        addStaticVar(newVar, code);
        return newVar;
      }
      Var newVar = makeNewTmpVar(etype);
      ExprGenerationVisitor ee = new ExprGenerationVisitor(this, e, newVar);
      String code = ee.exprCode();
      this.varTable.put(Integer.valueOf(e.eid), newVar);
      this.exprStack.push(new GeneratedExpr(newVar, e, code));
      return newVar;
    }
    return var;
  }
  
  public int makeFrame()
  {
    return this.exprStack.size();
  }
  
  public List<GeneratedExpr> popFrame(int label)
  {
    assert (label >= 0);
    LinkedList<GeneratedExpr> res = new LinkedList();
    while (this.exprStack.size() > label)
    {
      GeneratedExpr ge = (GeneratedExpr)this.exprStack.pop();
      this.varTable.remove(Integer.valueOf(ge.expr.eid));
      res.push(ge);
    }
    return res;
  }
  
  public String popFrameAndGetCode(int label)
  {
    StringBuilder buf = new StringBuilder();
    for (GeneratedExpr ge : popFrame(label)) {
      buf.append(ge.code);
    }
    return buf.toString();
  }
  
  private List<GeneratedExpr> getFrame(int label)
  {
    int offset = this.exprStack.size() - label;
    ListIterator<GeneratedExpr> it = this.exprStack.listIterator(offset);
    List<GeneratedExpr> res = new ArrayList(offset);
    while (it.hasPrevious())
    {
      GeneratedExpr ge = (GeneratedExpr)it.previous();
      res.add(ge);
    }
    return res;
  }
  
  public String getFrameCode(int label)
  {
    StringBuilder buf = new StringBuilder();
    for (GeneratedExpr ge : getFrame(label)) {
      buf.append(ge.code);
    }
    return buf.toString();
  }
  
  public Pair<Var, String> genExpr(Expr expr)
  {
    StringBuilder code = new StringBuilder();
    int varFrame = makeFrame();
    Var var = getVar(expr);
    for (GeneratedExpr ge : getFrame(varFrame)) {
      code.append(ge.code);
    }
    return Pair.make(var, code.toString());
  }
  
  public Pair<List<Var>, String> genExprs(List<? extends Expr> exprs)
  {
    StringBuilder code = new StringBuilder();
    List<Var> vars = new ArrayList(exprs.size());
    for (Expr e : exprs)
    {
      Pair<Var, String> p = genExpr(e);
      vars.add(p.first);
      code.append((String)p.second);
    }
    return Pair.make(vars, code.toString());
  }
  
  public String genAggExprs(Expr expr)
  {
    StringBuilder sb = new StringBuilder();
    int varFrame = makeFrame();
    ExprGenerationVisitor ee = new ExprGenerationVisitor(this, expr, null, ExprGenerationVisitor.WndAggCtx.UPDATE);
    
    String code = ee.exprCode();
    this.exprStack.push(new GeneratedExpr(null, expr, code));
    for (GeneratedExpr ge : getFrame(varFrame)) {
      sb.append(ge.code);
    }
    return sb.toString();
  }
  
  private void addStaticVar(Var var, String initExpr)
  {
    this.staticVars.add(var);
    this.staticInitExprs.add(initExpr);
  }
  
  public Var addStaticExpr(Class<?> exprType, String expr)
  {
    Var gvar = makeNewStaticVar(exprType);
    String initexpr = gvar + " = " + expr + ";\n";
    addStaticVar(gvar, initexpr);
    return gvar;
  }
  
  private List<Var> createBitFields(boolean isStatic)
  {
    List<Var> ret = new ArrayList();
    int n = ((isStatic ? this.nextGVarID : this.nextVarID) + 63) / 64;
    for (int i = 0; i < n; i++) {
      ret.add(new Var(getBitsFieldPrefix(isStatic), i, Long.TYPE, isStatic));
    }
    return ret;
  }
  
  private void genVars(CtClass cc, StringBuilder src, Collection<Var> vars)
    throws CannotCompileException
  {
    for (Var var : vars)
    {
      String typeName = var.getType().getCanonicalName();
      String varName = var.getName();
      String statMod = var.isStatic() ? " static" : "";
      String decl = String.format("private%s %-24s %s;\n", new Object[] { statMod, typeName, varName });
      
      CtField fld = CtField.make(decl, cc);
      cc.addField(fld);
      src.append(decl);
    }
  }
  
  private <T> List<T> join(List<T> a, List<T> b)
  {
    List<T> ret = new ArrayList(a);
    ret.addAll(b);
    return ret;
  }
  
  public void genVarsDeclaration(CtClass cc, StringBuilder src)
    throws CannotCompileException
  {
    genVars(cc, src, join(createBitFields(false), this.variables));
  }
  
  public void genStaticVarsDeclaration(CtClass cc, StringBuilder src)
    throws CannotCompileException
  {
    genVars(cc, src, join(createBitFields(true), this.staticVars));
  }
  
  public List<String> getStaticInitExprs()
  {
    return this.staticInitExprs;
  }
  
  public String getExprText(Expr e)
  {
    return this.compiler.getExprText(e);
  }
  
  public boolean isPatternMatch()
  {
    return this.isPatternMatch;
  }
  
  public void setLoopId(int loopId)
  {
    this.loopId = loopId;
  }
  
  public int getLoopId()
  {
    return this.loopId;
  }
  
  public abstract void setPrevEventIndex(int paramInt);
}
