package com.bloom.runtime.compiler.visitors;

import com.bloom.runtime.BuiltInFunc;
import com.bloom.runtime.compiler.CompilerUtils;
import com.bloom.runtime.compiler.custom.AggHandlerDesc;
import com.bloom.runtime.compiler.custom.CustomFunctionTranslator;
import com.bloom.runtime.compiler.exprs.ArrayInitializer;
import com.bloom.runtime.compiler.exprs.ArrayTypeRef;
import com.bloom.runtime.compiler.exprs.BoxCastOperation;
import com.bloom.runtime.compiler.exprs.Case;
import com.bloom.runtime.compiler.exprs.CaseExpr;
import com.bloom.runtime.compiler.exprs.CastExpr;
import com.bloom.runtime.compiler.exprs.CastNull;
import com.bloom.runtime.compiler.exprs.CastOperation;
import com.bloom.runtime.compiler.exprs.CastParam;
import com.bloom.runtime.compiler.exprs.CastToStringOperation;
import com.bloom.runtime.compiler.exprs.ClassRef;
import com.bloom.runtime.compiler.exprs.ComparePredicate;
import com.bloom.runtime.compiler.exprs.Constant;
import com.bloom.runtime.compiler.exprs.ConstructorCall;
import com.bloom.runtime.compiler.exprs.DataSetRef;
import com.bloom.runtime.compiler.exprs.Expr;
import com.bloom.runtime.compiler.exprs.ExprCmd;
import com.bloom.runtime.compiler.exprs.FieldRef;
import com.bloom.runtime.compiler.exprs.FuncCall;
import com.bloom.runtime.compiler.exprs.IndexExpr;
import com.bloom.runtime.compiler.exprs.InstanceOfPredicate;
import com.bloom.runtime.compiler.exprs.LogicalPredicate;
import com.bloom.runtime.compiler.exprs.NumericOperation;
import com.bloom.runtime.compiler.exprs.ObjectRef;
import com.bloom.runtime.compiler.exprs.ParamRef;
import com.bloom.runtime.compiler.exprs.PatternVarRef;
import com.bloom.runtime.compiler.exprs.Predicate;
import com.bloom.runtime.compiler.exprs.RowSetRef;
import com.bloom.runtime.compiler.exprs.StaticFieldRef;
import com.bloom.runtime.compiler.exprs.StringConcatenation;
import com.bloom.runtime.compiler.exprs.TypeRef;
import com.bloom.runtime.compiler.exprs.UnboxBoxCastOperation;
import com.bloom.runtime.compiler.exprs.UnboxCastOperation;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.exprs.WildCardExpr;
import com.bloom.runtime.compiler.select.DataSet;
import com.bloom.runtime.compiler.select.ExprGenerator;
import com.bloom.runtime.compiler.select.RowSet;
import com.bloom.runtime.compiler.select.Var;
import com.bloom.runtime.exceptions.EndOfAggLoop;
import com.bloom.runtime.utils.NamePolicy;
import com.bloom.runtime.utils.StringUtils;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;

public class ExprGenerationVisitor
  implements ExpressionVisitor<Object>
{
  public static enum WndAggCtx
  {
    UPDATE,  GET;
    
    private WndAggCtx() {}
  }
  
  private final StringBuilder buf = new StringBuilder();
  private final ExprGenerator ctx;
  private final Var exprVar;
  private final WndAggCtx aggCtx;
  
  public ExprGenerationVisitor(ExprGenerator ctx, Expr e, Var resVar)
  {
    this(ctx, e, resVar, WndAggCtx.GET);
  }
  
  public ExprGenerationVisitor(ExprGenerator ctx, Expr e, Var resVar, WndAggCtx aggCtx)
  {
    this.ctx = ctx;
    this.exprVar = resVar;
    this.aggCtx = aggCtx;
    e.visit(this, null);
  }
  
  private Var getVar(Expr e)
  {
    return this.ctx.getVar(e);
  }
  
  private int makeFrame()
  {
    return this.ctx.makeFrame();
  }
  
  private void popFrame(int label)
  {
    this.ctx.popFrame(label);
  }
  
  private String getExprText(Expr e)
  {
    return this.ctx.getExprText(e);
  }
  
  private List<Var> getArgs(List<? extends Expr> args)
  {
    List<Var> res = new ArrayList();
    for (Expr arg : args)
    {
      Var var = getVar(arg);
      res.add(var);
    }
    return res;
  }
  
  private String join(List<Var> list)
  {
    return StringUtils.join(list);
  }
  
  private void popAndAppendFrame(int label)
  {
    String code = this.ctx.popFrameAndGetCode(label);
    this.buf.append(code);
  }
  
  private void getAndAppendFrame(int label)
  {
    String code = this.ctx.getFrameCode(label);
    this.buf.append(code);
  }
  
  public String exprCode()
  {
    return this.buf.toString();
  }
  
  private String getOp(ExprCmd op)
  {
    switch (op)
    {
    case UMINUS: 
      return "-";
    case UPLUS: 
      return "";
    case PLUS: 
      return "+";
    case MINUS: 
      return "-";
    case DIV: 
      return "/";
    case MUL: 
      return "*";
    case MOD: 
      return "%";
    case BITAND: 
      return "&";
    case BITOR: 
      return "|";
    case BITXOR: 
      return "^";
    case INVERT: 
      return "~";
    case LSHIFT: 
      return "<<";
    case RSHIFT: 
      return ">>";
    case URSHIFT: 
      return ">>>";
    }
  
    return null;
  }
  
  private String getCmpOp(ExprCmd op)
  {
    switch (op)
    {
    case EQ: 
      return "==";
    case NOTEQ: 
      return "!=";
    case GT: 
      return ">";
    case GTEQ: 
      return ">=";
    case LT: 
      return "<";
    case LTEQ: 
      return "<=";
    }
   
    return null;
  }
  
  private String addCheckNotNull(Var arg1, Var arg2, String cmpExpr)
  {
    return "(" + arg1 + " != null && " + arg2 + " != null && " + cmpExpr + ")";
  }
  
  private String addCheckNull(Var arg1, Var arg2, String cmpExpr)
  {
    return "(" + arg1 + " == null || " + arg2 + " == null || " + cmpExpr + ")";
  }
  
  private String genCompare(ExprCmd relop, Var arg1, Var arg2, Expr expr1, Expr expr2)
  {
    Class<?> t1 = expr1.getType();
    Class<?> t2 = expr2.getType();
    
    assert (t1.equals(t2));
    if (t1.isPrimitive()) {
      return arg1 + getCmpOp(relop) + arg2;
    }
    if (relop == ExprCmd.EQ) {
      return addCheckNotNull(arg1, arg2, arg1 + ".equals(" + arg2 + ")");
    }
    if (relop == ExprCmd.NOTEQ) {
      return addCheckNull(arg1, arg2, "(!" + arg1 + ".equals(" + arg2 + "))");
    }
    assert (CompilerUtils.isComparable(t1));
    return addCheckNotNull(arg1, arg2, arg1 + ".compareTo(" + arg2 + ") " + getCmpOp(relop) + " 0");
  }
  
  private String setNullBit(Var res, List<Var> args)
  {
    return "if(" + this.ctx.checkNullBits(args) + " != 0) { " + setNullBit(res) + " }";
  }
  
  public Expr visitNumericOperation(NumericOperation operation, Object params)
  {
    List<Var> args = getArgs(operation.args);
    String op = getOp(operation.op);
    if (args.size() > 1)
    {
      StringBuilder sb = new StringBuilder();
      String sep = "";
      String newsep = " " + op + " ";
      for (Var arg : args)
      {
        sb.append(sep).append(arg);
        sep = newsep;
      }
      this.buf.append(this.exprVar + " = " + sb + ";\n");
    }
    else
    {
      assert (args.size() == 1);
      this.buf.append(this.exprVar + " = " + op + " " + args.get(0) + ";\n");
    }
    this.buf.append(setNullBit(this.exprVar, args) + "\n");
    return null;
  }
  
  private static String convertStringToJavaLiteral(String s)
  {
    return "\"" + s.replace("\n", "\\n").replace("\"", "\\\"") + "\"";
  }
  
  public Expr visitConstant(Constant constant, Object params)
  {
    String value;
    if (constant.value == null)
    {
      assert (constant.op == ExprCmd.NULL);
      value = "null";
    }
    else
    {
      if ((constant.value instanceof String))
      {
        value = convertStringToJavaLiteral((String)constant.value);
      }
      else
      {
        if ((constant.value instanceof DateTime))
        {
          DateTime d = (DateTime)constant.value;
          value = "new " + DateTime.class.getName() + "(" + d.getMillis() + "L)";
        }
        else
        {
          if ((constant.value instanceof Timestamp))
          {
            Timestamp t = (Timestamp)constant.value;
            value = BuiltInFunc.class.getName() + ".toTimestamp(" + t.getTime() + "L," + t.getNanos() + ")";
          }
          else
          {
            if ((constant.value instanceof Class)) {
              value = ((Class)constant.value).getCanonicalName() + ".class";
            } else {
              value = constant.value.toString();
            }
          }
        }
      }
    }
    this.buf.append(this.exprVar + " = " + value + ";\n");
    return null;
  }
  
  public Expr visitCase(CaseExpr caseExpr, Object params)
  {
    this.buf.append("while(true) {\n");
    Var selector = caseExpr.selector == null ? null : getVar(caseExpr.selector);
    int i = 0;
    int caseFrame = makeFrame();
    for (Case c : caseExpr.cases)
    {
      int condFrame = makeFrame();
      Var cond = getVar(c.cond);
      if (i == 0) {
        caseFrame = makeFrame();
      } else {
        getAndAppendFrame(condFrame);
      }
      String condExpr;
      if (caseExpr.selector == null) {
        condExpr = cond.getName();
      } else {
        condExpr = genCompare(ExprCmd.EQ, cond, selector, c.cond, caseExpr.selector);
      }
      this.buf.append("if(" + condExpr + ") {\n");
      int exprFrame = makeFrame();
      Var expr = getVar(c.expr);
      popAndAppendFrame(exprFrame);
      this.buf.append(this.exprVar + " = " + expr + ";\n");
      this.buf.append("break;\n");
      this.buf.append("}\n");
      i++;
    }
    if (caseExpr.else_expr != null)
    {
      int exprFrame = makeFrame();
      Var expr = getVar(caseExpr.else_expr);
      getAndAppendFrame(exprFrame);
      this.buf.append(this.exprVar + " = " + expr + ";\n");
      this.buf.append("break;\n");
    }
    else
    {
      this.buf.append("caseNotFound();");
      this.buf.append("break;\n");
    }
    this.buf.append("}\n");
    popFrame(caseFrame);
    return null;
  }
  
  private void visitAggFunc(FuncCall funcCall, AggHandlerDesc desc)
  {
    int aggExprIndex = funcCall.getIndex();
    assert (aggExprIndex >= 0);
    String obj = "((" + funcCall.getAggrClass().getCanonicalName() + ")getAggVec(" + aggExprIndex + "))";
    if (this.aggCtx == WndAggCtx.GET)
    {
      String methodName = desc.getMethod();
      this.buf.append(this.exprVar + " = " + obj + "." + methodName + "();\n");
    }
    else
    {
      assert (this.aggCtx == WndAggCtx.UPDATE);
      String args = join(getArgs(funcCall.getFuncArgs()));
      String incMethodName = desc.incMethod();
      String decMethodName = desc.decMethod();
      this.buf.append("if(isAdd())\n");
      this.buf.append("\t" + obj + "." + incMethodName + "(" + args + ");\n");
      this.buf.append("else\n");
      this.buf.append("\t" + obj + "." + decMethodName + "(" + args + ");\n");
    }
  }
  
  private void visitAggFuncInMatch(FuncCall funcCall, AggHandlerDesc desc)
  {
    int aggExprIndex = funcCall.getIndex();
    assert (aggExprIndex >= 0);
    String aggt = funcCall.getAggrClass().getCanonicalName();
    String obj = "agg" + aggExprIndex;
    String iter = "i" + aggExprIndex;
    this.buf.append(aggt + " " + obj + " = new " + aggt + "();\n");
    this.buf.append("int " + iter + " = 0;\n");
    this.buf.append("try {\n");
    this.buf.append("while(true) {\n");
    this.buf.append("\tboolean allNulls = true;\n");
    this.ctx.setLoopId(aggExprIndex);
    int frame = makeFrame();
    String args = join(getArgs(funcCall.getFuncArgs()));
    popAndAppendFrame(frame);
    this.ctx.setLoopId(-1);
    this.buf.append("if(allNulls) throw new " + EndOfAggLoop.class.getCanonicalName() + "();\n");
    
    String incMethodName = desc.incMethod();
    this.buf.append("\t" + obj + "." + incMethodName + "(" + args + ");\n");
    this.buf.append(iter + "++;\n");
    this.buf.append("}/*while(true)*/\n");
    this.buf.append("} catch (" + EndOfAggLoop.class.getCanonicalName() + " e) {}\n");
    String getName = desc.getMethod();
    this.buf.append(this.exprVar + " = " + obj + "." + getName + "();\n");
  }
  
  public Expr visitFuncCall(FuncCall funcCall, Object params)
  {
    Method me = funcCall.getMethd();
    assert (me != null);
    assert (NamePolicy.isEqual(me.getName(), funcCall.getName()));
    
    AggHandlerDesc desc = funcCall.getAggrDesc();
    if (desc != null)
    {
      if (this.ctx.isPatternMatch()) {
        visitAggFuncInMatch(funcCall, desc);
      } else {
        visitAggFunc(funcCall, desc);
      }
    }
    else
    {
      CustomFunctionTranslator ct = funcCall.isCustomFunc();
      String callExpr;
      if (ct != null)
      {
        callExpr = ct.generate(this.ctx, funcCall);
      }
      else
      {
        String args = join(getArgs(funcCall.getFuncArgs()));
        String methodName = me.getName();
        
        Expr fthis = funcCall.getThis();
        String obj;
        if (fthis == null)
        {
          obj = me.getDeclaringClass().getName();
        }
        else
        {
          if (fthis.isTypeRef()) {
            obj = fthis.getType().getName();
          } else {
            obj = getVar(fthis).getName();
          }
        }
        callExpr = obj + "." + methodName + "(" + args + ")";
      }
      this.buf.append(this.exprVar + " = " + callExpr + ";\n");
    }
    return null;
  }
  
  public Expr visitConstructorCall(ConstructorCall constructorCall, Object params)
  {
    String args = join(getArgs(constructorCall.args));
    this.buf.append(this.exprVar + " = new " + constructorCall.objtype + "(" + args + ");\n");
    return null;
  }
  
  public Expr visitCastExpr(CastExpr castExpr, Object params)
  {
   
    return null;
  }
  
  private void appendCheckNull(String obj, String name)
  {
    this.buf.append("if(" + obj + " == null) { ");
    this.buf.append("throw new RuntimeException(\"accessed field " + name + " from null object\"); }\n");
  }
  
  public Expr visitFieldRef(FieldRef fieldRef, Object params)
  {
    Var obj = getVar(fieldRef.getExpr());
    String fldexpr = fieldRef.genFieldAccess(obj.getName());
    Class<?> ft = fieldRef.getType();
    this.buf.append("if(" + obj + " == null) {\n");
    if (ft.isPrimitive()) {
      this.buf.append("throw new NullPointerException(\"cannot unreference field of primitive type <" + fieldRef.getName() + ">with null value\");\n");
    } else {
      this.buf.append(this.exprVar + " = null;\n");
    }
    this.buf.append("} else {\n");
    this.buf.append(this.exprVar + " = " + fldexpr + ";\n");
    this.buf.append("}\n");
    return null;
  }
  
  public Expr visitIndexExpr(IndexExpr indexExpr, Object params)
  {
    Var index = getVar(indexExpr.getIndex());
    Var array = getVar(indexExpr.getExpr());
    String expr = getExprText(indexExpr);
    appendCheckNull(array.getName(), expr);
    if (indexExpr.baseIsJsonNode())
    {
      this.buf.append(this.exprVar + " = " + array + ".get(" + index + ");\n");
    }
    else
    {
      this.buf.append("try { " + this.exprVar + " = " + array + "[" + index + "]; } ");
      this.buf.append("catch(java.lang.ArrayIndexOutOfBoundsException e) { ");
      this.buf.append("throw new RuntimeException(\"in expression <" + expr + "> trying to access \" + " + index + "+\"-th element of array, while it has only \" +" + array + ".length + \" elements\", e);");
      
      this.buf.append("}\n");
    }
    return null;
  }
  
  public Expr visitLogicalPredicate(LogicalPredicate logicalPredicate, Object params)
  {
    ExprCmd op = logicalPredicate.op;
    if (op == ExprCmd.NOT)
    {
      assert (logicalPredicate.args.size() == 1);
      Var arg = getVar((Expr)logicalPredicate.args.get(0));
      this.buf.append(this.exprVar + " = !" + arg + ";\n");
    }
    else
    {
      assert ((op == ExprCmd.AND) || (op == ExprCmd.OR));
      
      this.buf.append(this.exprVar + " = " + (op == ExprCmd.AND ? "false" : "true") + ";\n");
      this.buf.append("while(true) {\n");
      int i = 0;
      int exprFrame = makeFrame();
      for (Predicate p : logicalPredicate.args)
      {
        int condFrame = makeFrame();
        Var cond = getVar(p);
        if (i == 0) {
          exprFrame = makeFrame();
        } else {
          getAndAppendFrame(condFrame);
        }
        if (op == ExprCmd.AND) {
          this.buf.append("if(!(" + cond + ")) {\n");
        } else {
          this.buf.append("if(" + cond + ") {\n");
        }
        this.buf.append("break;\n");
        this.buf.append("}\n");
        i++;
      }
      this.buf.append(this.exprVar + " = " + (op == ExprCmd.AND ? "true" : "false") + ";\n");
      this.buf.append("break;\n");
      this.buf.append("}\n");
      popFrame(exprFrame);
    }
    return null;
  }
  
  private void visitInListPredicate(ComparePredicate comparePredicate)
  {
    assert (comparePredicate.op == ExprCmd.INLIST);
    List<ValueExpr> list = comparePredicate.args;
    Expr firstExpr = (Expr)list.get(0);
    Var arg = getVar(firstExpr);
    this.buf.append(this.exprVar + " = true;\n");
    this.buf.append("while(true) {\n");
    int i = 0;
    int listFrame = makeFrame();
    for (ValueExpr e : list.subList(1, list.size()))
    {
      int condFrame = makeFrame();
      Var cond = getVar(e);
      if (i == 0) {
        listFrame = makeFrame();
      } else {
        getAndAppendFrame(condFrame);
      }
      String condExpr = genCompare(ExprCmd.EQ, cond, arg, firstExpr, e);
      this.buf.append("if(" + condExpr + ") {\n");
      this.buf.append("break;\n");
      this.buf.append("}\n");
      i++;
    }
    this.buf.append(this.exprVar + " = false;\n");
    this.buf.append("break;\n");
    this.buf.append("}\n");
    popFrame(listFrame);
  }
  
  public Expr visitComparePredicate(ComparePredicate comparePredicate, Object params)
  {
    switch (comparePredicate.op)
    {
    case BOOLEXPR: 
      Var arg = getVar((Expr)comparePredicate.args.get(0));
      this.buf.append(this.exprVar + " = " + arg + ";\n");
      break;
    case CHECKRECTYPE: 
       arg = getVar((Expr)comparePredicate.args.get(0));
      this.buf.append(this.exprVar + " = checkDynamicRecType(" + arg + ");\n");
      break;
    case ISNULL: 
      ValueExpr e = (ValueExpr)comparePredicate.args.get(0);
      if (e.getType().isPrimitive())
      {
        this.buf.append(this.exprVar + " = false;\n");
      }
      else
      {
         arg = getVar(e);
        this.buf.append(this.exprVar + " = " + arg + " == null;\n");
      }
      break;
    case LIKE: 
      String args = join(getArgs(comparePredicate.args));
      this.buf.append(this.exprVar + " = doLike(" + args + ");\n");
      break;
    case INLIST: 
      visitInListPredicate(comparePredicate);
      break;
    case BETWEEN: 
      Expr argExpr = (Expr)comparePredicate.args.get(0);
      Expr lbExpr = (Expr)comparePredicate.args.get(1);
      Expr ubExpr = (Expr)comparePredicate.args.get(2);
       arg = getVar(argExpr);
      Var lowerbound = getVar(lbExpr);
      String condExpr1 = genCompare(ExprCmd.LT, arg, lowerbound, argExpr, lbExpr);
      this.buf.append("if(" + condExpr1 + ") {\n");
      this.buf.append(this.exprVar + " = false;\n");
      this.buf.append("} else {\n");
      int frame = makeFrame();
      Var upperbound = getVar(ubExpr);
      popAndAppendFrame(frame);
      String condExpr2 = genCompare(ExprCmd.GT, arg, upperbound, argExpr, ubExpr);
      this.buf.append("if(" + condExpr2 + ") {\n");
      this.buf.append(this.exprVar + " = false;\n");
      this.buf.append("} else {\n");
      this.buf.append(this.exprVar + " = true;\n");
      this.buf.append("}\n");
      this.buf.append("}\n");
      break;
    default: 
      Expr arg1 = (Expr)comparePredicate.args.get(0);
      Expr arg2 = (Expr)comparePredicate.args.get(1);
      List<Var> args = getArgs(comparePredicate.args);
      String condExpr = genCompare(comparePredicate.op, (Var)args.get(0), (Var)args.get(1), arg1, arg2);
      
      this.buf.append(this.exprVar + " = " + condExpr + ";\n");
    }
    return null;
  }
  
  public Expr visitInstanceOfPredicate(InstanceOfPredicate instanceOfPredicate, Object Params)
  {
    Var arg = getVar(instanceOfPredicate.getExpr());
    this.buf.append(this.exprVar + " = " + arg + " instanceof " + instanceOfPredicate.checktype + ";\n");
    return null;
  }
  
  public Expr visitExpr(Expr e, Object params)
  {
    return e.visit(this, params);
  }
  
  public Expr visitExprDefault(Expr e, Object params)
  {
    if (!$assertionsDisabled) {
      throw new AssertionError();
    }
    return null;
  }
  
  public Expr visitClassRef(ClassRef classRef, Object params)
  {
    this.buf.append(this.exprVar + " = " + classRef.typeName + ".class;\n");
    return null;
  }
  
  public Expr visitArrayTypeRef(ArrayTypeRef arrayTypeRef, Object params)
  {
    
    return null;
  }
  
  public Expr visitObjectRef(ObjectRef objectRef, Object params)
  {
    
    return null;
  }
  
  public Expr visitTypeRef(TypeRef typeRef, Object params)
  {
    
    return null;
  }
  
  public Expr visitStaticFieldRef(StaticFieldRef fieldRef, Object params)
  {
    Field f = fieldRef.fieldRef;
    Class<?> c = f.getDeclaringClass();
    this.buf.append(this.exprVar + " = " + c.getName() + "." + f.getName() + ";\n");
    return null;
  }
  
  private static String convertToStringLiteral(String s)
  {
    return s == null ? "" : s.replace("\"", "\\\"");
  }
  
  private static String defaultTypeValue(Class<?> type)
  {
    return type == Boolean.TYPE ? "false" : "0";
  }
  
  private String setNullBit(Var var)
  {
    return this.ctx.setNullBit(var, true);
  }
  
  private String clearNullBit(Var var)
  {
    return this.ctx.setNullBit(var, false);
  }
  
  private String getNullBit(Var v)
  {
    return this.ctx.getNullBit(v);
  }
  
  public Expr visitCastOperation(CastOperation castOp, Object params)
  {
    String type = castOp.getType().getCanonicalName();
    if ((castOp instanceof CastNull))
    {
      this.buf.append(this.exprVar + " = (" + type + ")null;\n");
    }
    else if ((castOp instanceof CastParam))
    {
      ParamRef p = (ParamRef)castOp.getExpr();
      this.buf.append(this.exprVar + " = (" + type + ")getParamVal(" + p.getIndex() + ",\"" + p.getName() + "\", " + type + ".class);\n");
    }
    else
    {
      ValueExpr expr = castOp.getExpr();
      Var arg = getVar(expr);
      if ((castOp instanceof CastToStringOperation))
      {
        this.buf.append(this.exprVar + " = String.valueOf(" + arg + ");\n");
      }
      else if ((castOp instanceof UnboxCastOperation))
      {
        String val = defaultTypeValue(castOp.getType());
        this.buf.append("if(" + arg + " == null) { ");
        this.buf.append(this.exprVar + " = " + val + ";");
        this.buf.append(setNullBit(this.exprVar) + " }\n");
        this.buf.append("else {");
        this.buf.append(this.exprVar + " = " + arg + "." + type + "Value();");
        this.buf.append(clearNullBit(this.exprVar) + "}\n");
      }
      else if ((castOp instanceof BoxCastOperation))
      {
        this.buf.append("if(" + getNullBit(arg) + " != 0) { " + this.exprVar + " = null; }\n");
        this.buf.append("else {" + this.exprVar + " = " + type + ".valueOf(" + arg + "); }\n");
      }
      else if ((castOp instanceof UnboxBoxCastOperation))
      {
        String unboxedSrcType = CompilerUtils.getUnboxingType(expr.getType()).getCanonicalName();
        
        String unboxedTgtType = CompilerUtils.getUnboxingType(castOp.getType()).getCanonicalName();
        
        this.buf.append("if(" + arg + " == null) { " + this.exprVar + " = null; }\n");
        this.buf.append("else { " + this.exprVar + " = " + type + ".valueOf((" + unboxedTgtType + ")(" + arg + ")." + unboxedSrcType + "Value()); }\n");
      }
      else
      {
        Class<?> targetType = castOp.getType();
        Class<?> sourceType = expr.getType();
        if (((sourceType.isPrimitive()) && (targetType.isPrimitive())) || (targetType.isAssignableFrom(sourceType)))
        {
          this.buf.append(this.exprVar + " = ((" + type + ")" + arg + ");\n");
        }
        else
        {
          String exprText = convertToStringLiteral(getExprText(expr));
          this.buf.append("try { " + this.exprVar + " = ((" + type + ")" + arg + "); } ");
          this.buf.append("catch(java.lang.ClassCastException e) { throw new ClassCastException(\"cannot cast expression <" + exprText + "> of type [\" + " + arg + ".getClass().getCanonicalName() + \"] to type [" + type + "]\"); }\n");
        }
      }
    }
    return null;
  }
  
  public Expr visitWildCardExpr(WildCardExpr e, Object params)
  {
   
    return null;
  }
  
  public Expr visitParamRef(ParamRef e, Object params)
  {
    
    return null;
  }
  
  public Expr visitPatternVarRef(PatternVarRef e, Object params)
  {
    int loopid = this.ctx.getLoopId();
    String vartype = e.getType().getCanonicalName();
    String idx = loopid >= 0 ? "i" + loopid : String.valueOf(e.getIndex());
    
    this.buf.append(this.exprVar + " = (" + vartype + ")ctx.getVar(" + e.getVarId() + ", " + idx + ");\n");
    if (loopid >= 0) {
      this.buf.append("if(" + this.exprVar + " != null) allNulls = false;\n");
    }
    return null;
  }
  
  private void emitRowSetRef(RowSet rs, String obj)
  {
    String row = rs.genRowAccessor(obj);
    this.buf.append(this.exprVar + " = (" + rs.getTypeName() + ")" + row + ";\n");
  }
  
  public Expr visitDataSetRef(DataSetRef ref, Object params)
  {
    DataSet ds = ref.getDataSet();
    String obj = "getRowData(" + ds.getID() + ")";
    
    this.buf.append("// get row from " + ds.getFullName() + "\n");
    emitRowSetRef(ds, obj);
    return null;
  }
  
  public Expr visitRowSetRef(RowSetRef ref, Object params)
  {
    RowSet rs = ref.getRowSet();
    Var obj = getVar(ref.getExpr());
    emitRowSetRef(rs, obj.getName());
    return null;
  }
  
  public Expr visitArrayInitializer(ArrayInitializer arrayInit, Object params)
  {
    String type = arrayInit.getType().getComponentType().getCanonicalName();
    
    String initialValues = join(getArgs(arrayInit.args));
    this.buf.append(this.exprVar + " = new " + type + "[]{" + initialValues + "};\n");
    return null;
  }
  
  public Expr visitStringConcatenation(StringConcatenation expr, Object params)
  {
    List<Var> args = getArgs(expr.args);
    StringBuilder sb = new StringBuilder();
    sb.append("\"\"");
    for (Var arg : args) {
      sb.append(" + " + arg);
    }
    this.buf.append(this.exprVar + " = " + sb + ";\n");
    return null;
  }
}
