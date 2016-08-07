package com.bloom.runtime.compiler.visitors;

import com.bloom.runtime.BuiltInFunc;
import com.bloom.runtime.compiler.CompilerUtils;
import com.bloom.runtime.compiler.TypeName;
import com.bloom.runtime.compiler.custom.CustomFunctionTranslator;
import com.bloom.runtime.compiler.exprs.ArrayInitializer;
import com.bloom.runtime.compiler.exprs.ArrayTypeRef;
import com.bloom.runtime.compiler.exprs.Case;
import com.bloom.runtime.compiler.exprs.CaseExpr;
import com.bloom.runtime.compiler.exprs.CastExpr;
import com.bloom.runtime.compiler.exprs.CastOperation;
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
import com.bloom.runtime.compiler.exprs.IntegerOperation;
import com.bloom.runtime.compiler.exprs.JsonFieldRef;
import com.bloom.runtime.compiler.exprs.LogicalPredicate;
import com.bloom.runtime.compiler.exprs.NumericOperation;
import com.bloom.runtime.compiler.exprs.ObjectRef;
import com.bloom.runtime.compiler.exprs.Operation;
import com.bloom.runtime.compiler.exprs.ParamRef;
import com.bloom.runtime.compiler.exprs.PatternVarRef;
import com.bloom.runtime.compiler.exprs.Predicate;
import com.bloom.runtime.compiler.exprs.RowSetRef;
import com.bloom.runtime.compiler.exprs.StaticFieldRef;
import com.bloom.runtime.compiler.exprs.StringConcatenation;
import com.bloom.runtime.compiler.exprs.TypeRef;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.exprs.WildCardExpr;
import com.bloom.runtime.compiler.select.DataSet;
import com.bloom.runtime.compiler.select.ExprValidator;
import com.bloom.runtime.exceptions.AmbiguousSignature;
import com.bloom.runtime.exceptions.SignatureNotFound;
import com.fasterxml.jackson.databind.JsonNode;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class ExprValidationVisitor
  implements ExpressionVisitor<AFVparams>
{
  private final ExprValidator ctx;
  private Expr parent;
  private static final boolean rewriteBuiltInOp = false;
  
  static class AFVparams
  {
    public final List<Boolean> seenList = new ArrayList();
    public boolean wasSeenHere = false;
    
    public boolean wasSeen()
    {
      for (Iterator i$ = this.seenList.iterator(); i$.hasNext();)
      {
        boolean seen = ((Boolean)i$.next()).booleanValue();
        if (seen) {
          return true;
        }
      }
      return false;
    }
  }
  
  public ExprValidationVisitor(ExprValidator ctx)
  {
    this.ctx = ctx;
  }
  
  public Expr validate(Expr e)
  {
    this.parent = null;
    Expr result = visitExpr(e, new AFVparams());
    return result.resolve();
  }
  
  private void error(String msg, Object info)
  {
    this.ctx.error(msg, info);
  }
  
  private Class<?> getTypeInfo(TypeName t)
  {
    return this.ctx.getTypeInfo(t);
  }
  
  private ValueExpr cast(ValueExpr expr, Class<?> targetType)
  {
    return this.ctx.cast(expr, targetType);
  }
  
  public Expr visitExpr(Expr e, AFVparams params)
  {
    AFVparams p = new AFVparams();
    Expr save_parent = this.parent;
    this.parent = e;
    e.visitArgs(this, p);
    this.parent = save_parent;
    Expr newe = e.visit(this, p);
    if (newe != null) {
      e = newe;
    }
    params.seenList.add(Boolean.valueOf((p.wasSeen()) || (p.wasSeenHere)));
    return e;
  }
  
  public Expr visitExprDefault(Expr e, AFVparams params)
  {
    if (!$assertionsDisabled) {
      throw new AssertionError();
    }
    return null;
  }
  
  private static boolean hasAnyStringArg(List<ValueExpr> args)
  {
    for (ValueExpr arg : args) {
      if (arg.getType().equals(String.class)) {
        return true;
      }
    }
    return false;
  }
  
  private static WildCardExpr findWildCardExpr(List<ValueExpr> args)
  {
    for (ValueExpr arg : args) {
      if ((arg instanceof WildCardExpr)) {
        return (WildCardExpr)arg;
      }
    }
    return null;
  }
  
  private void castArgsTo(List<ValueExpr> args, Class<?> resulttype)
  {
    ListIterator<ValueExpr> it = args.listIterator();
    while (it.hasNext())
    {
      ValueExpr e = (ValueExpr)it.next();
      ValueExpr enew = cast(e, resulttype);
      it.set(enew);
    }
  }
  
  public Expr visitNumericOperation(NumericOperation operation, AFVparams params)
  {
    boolean isInteger = operation instanceof IntegerOperation;
    if ((operation.op == ExprCmd.PLUS) && (hasAnyStringArg(operation.args)))
    {
      WildCardExpr wc = findWildCardExpr(operation.args);
      if (wc != null) {
        error("syntax error", wc);
      }
      Operation newop = new StringConcatenation(operation.args);
      return newop.setOriginalExpr(operation);
    }
    Class<?> widestType = isInteger ? Long.TYPE : Double.TYPE;
    Class<?> resulttype = Integer.TYPE;
    for (ValueExpr arg : operation.args)
    {
      Class<?> argType = arg.getType();
      if (!CompilerUtils.isConvertibleWithBoxing(widestType, argType)) {
        error("invalid type of operand", arg);
      }
      resulttype = CompilerUtils.getCommonSuperType(resulttype, arg.getType());
      if (resulttype == null) {
        error("incompatible expression type", arg);
      }
    }
    resulttype = CompilerUtils.getDefaultTypeIfCannotInfer(resulttype, Integer.TYPE);
    
    operation.setType(resulttype);
    castArgsTo(operation.args, resulttype);
    return null;
  }
  
  public Expr visitConstant(Constant constant, AFVparams params)
  {
    return null;
  }
  
  public Expr visitCase(CaseExpr caseExpr, AFVparams params)
  {
    int i = 0;
    Class<?> resulttype = null;
    Class<?> seltype = caseExpr.selector == null ? null : caseExpr.selector.getType();
    for (Case c : caseExpr.cases)
    {
      if (i == 0)
      {
        resulttype = c.expr.getType();
      }
      else
      {
        resulttype = CompilerUtils.getCommonSuperType(resulttype, c.expr.getType());
        if (resulttype == null) {
          error("incompatible expression type", c.expr);
        }
      }
      if (seltype != null)
      {
        seltype = CompilerUtils.getCommonSuperType(seltype, c.cond.getType());
        if (seltype == null) {
          error("incompatible types of selector and case", c.cond);
        }
      }
      i++;
    }
    ValueExpr els = caseExpr.else_expr;
    if (els != null)
    {
      resulttype = CompilerUtils.getCommonSuperType(resulttype, els.getType());
      if (resulttype == null) {
        error("incompatible expression type", els);
      }
    }
    resulttype = CompilerUtils.getDefaultTypeIfCannotInfer(resulttype, String.class);
    if (seltype != null) {
      seltype = CompilerUtils.getDefaultTypeIfCannotInfer(seltype, String.class);
    }
    caseExpr.setType(resulttype);
    for (Case c : caseExpr.cases)
    {
      c.expr = cast(c.expr, resulttype);
      if (seltype != null) {
        c.cond = cast((ValueExpr)c.cond, seltype);
      }
    }
    if (els != null) {
      caseExpr.else_expr = cast(els, resulttype);
    }
    return null;
  }
  
  private Method findMethod(List<Class<?>> classes, String funcName, Class<?>[] sig)
  {
    boolean foundWithAnotherSignature = false;
    Method ret = null;
    for (Class<?> klass : classes) {
      try
      {
        Method me = CompilerUtils.findMethod(klass, funcName, sig);
        if (ret != null) {
          error("ambiguous function call: same function exists in more than one package", funcName);
        } else {
          ret = me;
        }
      }
      catch (NoSuchMethodException e) {}catch (AmbiguousSignature e)
      {
        error("ambiguous function call", funcName);
      }
      catch (SignatureNotFound e)
      {
        foundWithAnotherSignature = true;
      }
    }
    if (ret == null) {
      if (foundWithAnotherSignature)
      {
        ArrayList<String> args = new ArrayList();
        for (Class<?> k : sig) {
          args.add(k.getSimpleName());
        }
        error("no function for such arguments " + args, funcName);
      }
      else
      {
        error("no such function", funcName);
      }
    }
    return ret;
  }
  
  public Expr visitFuncCall(FuncCall funcCall, AFVparams params)
  {
    if (funcCall.isValidated()) {
      return null;
    }
    String funcName = funcCall.getName();
    Class<?>[] sig = Expr.getSignature(funcCall.getFuncArgs());
    ValueExpr fthis = funcCall.getThis();
    List<Class<?>> klasses = null;
    boolean checkStatic = false;
    if (fthis != null)
    {
      checkStatic = fthis.isTypeRef();
      klasses = Collections.singletonList(fthis.getType());
    }
    else
    {
      klasses = this.ctx.getListOfStaticMethods(funcName);
    }
    Method me = findMethod(klasses, funcName, sig);
    funcCall.setMethodRef(me);
    if ((checkStatic) && (!Modifier.isStatic(me.getModifiers()))) {
      error("non-static method invocation", funcName);
    }
    CustomFunctionTranslator ct = funcCall.isCustomFunc();
    WildCardExpr wc = findWildCardExpr(funcCall.getFuncArgs());
    
    Class<?>[] parameterTypes = me.getParameterTypes();
    List<ValueExpr> argList = funcCall.getFuncArgs();
    int numParameters = parameterTypes.length;int numArguments = argList.size();
    boolean needsVarArgsTreatment = (me.isVarArgs()) && ((numParameters != numArguments) || (!parameterTypes[(numParameters - 1)].isAssignableFrom(((ValueExpr)argList.get(numArguments - 1)).getType())));
    
    ListIterator<ValueExpr> it = argList.listIterator();
    int itCount = numParameters - (needsVarArgsTreatment ? 1 : 0);
    for (int pInd = 0; pInd < itCount; pInd++)
    {
      ValueExpr expr = (ValueExpr)it.next();
      Class<?> paramType = parameterTypes[pInd];
      if (((wc == null) && (ct == null)) || (paramType != Object.class))
      {
        ValueExpr exprCasted = cast(expr, paramType);
        it.set(exprCasted);
      }
    }
    if (needsVarArgsTreatment)
    {
      Class<?> variableArgumentType = parameterTypes[itCount].getComponentType();
      List<ValueExpr> exprList = new ArrayList();
      while (it.hasNext())
      {
        ValueExpr expr = (ValueExpr)it.next();
        ValueExpr exprCasted = cast(expr, variableArgumentType);
        exprList.add(exprCasted);
        it.remove();
      }
      argList.add(new ArrayInitializer(variableArgumentType, exprList));
    }
    if (ct != null)
    {
      ValueExpr ret = ct.validate(this.ctx, funcCall);
      if (ret != null) {
        return ret;
      }
    }
    if (funcCall.getAggrDesc() != null)
    {
      if (!this.ctx.acceptAgg()) {
        error("invalid use of aggregation function", funcName);
      } else if (params.wasSeen()) {
        error("nested aggregation function", funcName);
      } else {
        params.wasSeenHere = true;
      }
      this.ctx.setHaveAggFuncs();
    }
    if ((funcCall.isDistinct()) && (funcCall.getAggrClass() == null)) {
      error("this function cannot count DISTINCT values", funcName);
    }
    if (wc != null)
    {
      if ((this.ctx.acceptWildcard()) && (this.parent == null) && (funcCall.acceptWildcard()))
      {
        List<ValueExpr> listOfFuncCall = new ArrayList();
        for (ValueExpr arg : wc.getExprs())
        {
          List<ValueExpr> fcargs = new ArrayList();
          fcargs.add(cast(arg, Object.class));
          FuncCall fc = new FuncCall(funcCall.getName(), fcargs, 0);
          fc.setMethodRef(funcCall.getMethd());
          listOfFuncCall.add(fc);
        }
        wc.updateExprs(listOfFuncCall);
        return wc;
      }
      error("syntax error", wc);
    }
    return null;
  }
  
  public Expr visitConstructorCall(ConstructorCall constructorCall, AFVparams params)
  {
    Class<?> type = getTypeInfo(constructorCall.objtype);
    Class<?>[] sig = Expr.getSignature(constructorCall.args);
    try
    {
      Constructor<?> con = CompilerUtils.findConstructor(type, sig);
      constructorCall.setConstructor(con);
      
      Class<?>[] parameterTypes = con.getParameterTypes();
      List<ValueExpr> argList = constructorCall.args;
      int numParameters = parameterTypes.length;int numArguments = argList.size();
      boolean needsVarArgsTreatment = (con.isVarArgs()) && ((numParameters != numArguments) || (!parameterTypes[(numParameters - 1)].isAssignableFrom(((ValueExpr)argList.get(numArguments - 1)).getType())));
      
      ListIterator<ValueExpr> it = argList.listIterator();
      int itCount = numParameters - (needsVarArgsTreatment ? 1 : 0);
      for (int pInd = 0; pInd < itCount; pInd++)
      {
        ValueExpr expr = (ValueExpr)it.next();
        ValueExpr exprCasted = cast(expr, parameterTypes[pInd]);
        it.set(exprCasted);
      }
      if (needsVarArgsTreatment)
      {
        Class<?> variableArgumentType = parameterTypes[itCount].getComponentType();
        List<ValueExpr> exprList = new ArrayList();
        while (it.hasNext())
        {
          ValueExpr expr = (ValueExpr)it.next();
          ValueExpr exprCasted = cast(expr, variableArgumentType);
          exprList.add(exprCasted);
          it.remove();
        }
        argList.add(new ArrayInitializer(variableArgumentType, exprList));
      }
    }
    catch (AmbiguousSignature e)
    {
      error("ambiguous constructor call", constructorCall.objtype.name);
    }
    catch (SignatureNotFound e)
    {
      error("no such constructor", constructorCall.objtype.name);
    }
    return null;
  }
  
  public Expr visitCastExpr(CastExpr castExpr, AFVparams params)
  {
    Class<?> resultType = getTypeInfo(castExpr.casttype);
    ValueExpr expr = castExpr.getExpr();
    ValueExpr ret = cast(expr, resultType);
    return ret.setOriginalExpr(castExpr);
  }
  
  public Expr visitFieldRef(FieldRef fieldRef, AFVparams params)
  {
    String name = fieldRef.getName();
    ValueExpr expr = fieldRef.getExpr();
    if ((expr instanceof ObjectRef))
    {
      ObjectRef o = (ObjectRef)expr; ObjectRef 
      
        tmp35_33 = o;tmp35_33.name = (tmp35_33.name + "." + name);
      return o.setOriginalExpr(fieldRef);
    }
    try
    {
      FieldRef f = expr.getField(name);
      if ((expr.isTypeRef()) && (!f.isStatic())) {
        error("cannot access non-static field of class " + expr.getType().getName(), name);
      }
      return f.setOriginalExpr(fieldRef);
    }
    catch (NoSuchFieldException|SecurityException e)
    {
      if (JsonNode.class.isAssignableFrom(expr.getType())) {
        return new JsonFieldRef(expr, name);
      }
      error("cannot resolve field name", name);
    }
    return null;
  }
  
  private int getIndex(ValueExpr index)
  {
    if (!(index instanceof Constant)) {
      error("index must be an integer constant", index);
    }
    Constant c = (Constant)index;
    Number n = (Number)c.value;
    return n.intValue();
  }
  
  public Expr visitIndexExpr(IndexExpr indexExpr, AFVparams params)
  {
    ValueExpr index = indexExpr.getIndex();
    Class<?> itype = index.getType();
    if (!CompilerUtils.isConvertibleWithBoxing(Integer.TYPE, itype)) {
      error("invalid type of array index", index);
    }
    ValueExpr e = indexExpr.getExpr();
    if ((e instanceof PatternVarRef))
    {
      PatternVarRef pv = (PatternVarRef)e;
      pv.setIndex(getIndex(index));
      return pv.setOriginalExpr(indexExpr);
    }
    if ((e instanceof DataSetRef))
    {
      DataSetRef dsref = (DataSetRef)e;
      dsref.setIndex(getIndex(index));
      error("indexed access to a dataset is not implemented yet", indexExpr);
      return dsref.setOriginalExpr(indexExpr);
    }
    if (!indexExpr.baseIsArray()) {
      error("cannot apply index to expression", e);
    }
    index = cast(index, Integer.TYPE);
    indexExpr.setIndex(index);
    return null;
  }
  
  public Expr visitLogicalPredicate(LogicalPredicate logicalPredicate, AFVparams params)
  {
    for (Predicate arg : logicalPredicate.args) {
      if (arg.getType() != Boolean.TYPE) {
        error("invalid boolean expression", arg);
      }
    }
    return null;
  }
  
  public Expr visitComparePredicate(ComparePredicate comparePredicate, AFVparams params)
  {
    switch (comparePredicate.op)
    {
    case CHECKRECTYPE: 
      break;
    case ISNULL: 
      Class<?> t = ((ValueExpr)comparePredicate.args.get(0)).getType();
      t = CompilerUtils.getDefaultTypeIfCannotInfer(t, Object.class);
      castArgsTo(comparePredicate.args, t);
      break;
    case LIKE: 
      castArgsTo(comparePredicate.args, String.class);
      ValueExpr pat = compilePattern((ValueExpr)comparePredicate.args.get(1));
      comparePredicate.args.set(1, pat);
      break;
    case BOOLEXPR: 
      castArgsTo(comparePredicate.args, Boolean.TYPE);
      break;
    default: 
      int i = 0;
      Class<?> commontype = null;
      for (ValueExpr arg : comparePredicate.args)
      {
        Class<?> type = arg.getType();
        if (!CompilerUtils.isComparable(type)) {
          error("expression of uncomparable type", arg);
        }
        if (i == 0)
        {
          commontype = type;
        }
        else
        {
          commontype = CompilerUtils.getCommonSuperType(commontype, type);
          if (commontype == null) {
            error("expression of uncomparable type", arg);
          }
        }
        i++;
      }
      assert (commontype != null);
      commontype = CompilerUtils.getDefaultTypeIfCannotInfer(commontype, String.class);
      
      castArgsTo(comparePredicate.args, commontype);
      break;
    }
    return null;
  }
  
  public Expr visitInstanceOfPredicate(InstanceOfPredicate instanceOfPredicate, AFVparams params)
  {
    Class<?> t = ((ValueExpr)instanceOfPredicate.args.get(0)).getType();
    t = CompilerUtils.getDefaultTypeIfCannotInfer(t, Object.class);
    castArgsTo(instanceOfPredicate.args, t);
    getTypeInfo(instanceOfPredicate.checktype);
    return null;
  }
  
  public Expr visitClassRef(ClassRef classRef, AFVparams params)
  {
    getTypeInfo(classRef.typeName);
    return null;
  }
  
  public Expr visitArrayTypeRef(ArrayTypeRef arrayTypeRef, AFVparams params)
  {
    error("syntax error", arrayTypeRef);
    return null;
  }
  
  public Expr visitObjectRef(ObjectRef objectRef, AFVparams params)
  {
    String name = objectRef.name.toString();
    
    DataSet ds = this.ctx.getDataSet(name);
    if (ds != null)
    {
      DataSetRef dsref = ds.makeRef();
      return dsref.setOriginalExpr(objectRef);
    }
    FieldRef f = this.ctx.findField(name);
    if (f != null) {
      return f.setOriginalExpr(objectRef);
    }
    Expr e = this.ctx.resolveAlias(name);
    if (e != null) {
      return e;
    }
    objectRef.setContext(this.ctx);
    return null;
  }
  
  public Expr visitDataSetRef(DataSetRef dataSetRef, AFVparams params)
  {
    return null;
  }
  
  public Expr visitTypeRef(TypeRef typeRef, AFVparams params)
  {
    return null;
  }
  
  public Expr visitStaticFieldRef(StaticFieldRef fieldRef, AFVparams params)
  {
    if (!$assertionsDisabled) {
      throw new AssertionError();
    }
    return null;
  }
  
  public Expr visitCastOperation(CastOperation castOp, AFVparams params)
  {
    return null;
  }
  
  public static ValueExpr compilePattern(ValueExpr str)
  {
    List<ValueExpr> fargs = new ArrayList();
    fargs.add(str);
    FuncCall fc = new FuncCall("compile__like__pattern", fargs, 0);
    try
    {
      Method m = BuiltInFunc.class.getMethod("compile__like__pattern", new Class[] { String.class });
      
      fc.setMethodRef(m);
    }
    catch (NoSuchMethodException|SecurityException e)
    {
      e.printStackTrace();
    }
    return fc;
  }
  
  public Expr visitWildCardExpr(WildCardExpr e, AFVparams params)
  {
    if ((this.ctx.acceptWildcard()) && ((this.parent == null) || ((this.parent instanceof FuncCall))))
    {
      DataSet ds = this.ctx.getDataSet(e.getDataSetName());
      if (ds == null) {
        error("no such data source", e);
      }
      e.setExprs(ds.makeListOfAllFields());
    }
    else
    {
      error("syntax error", e);
    }
    return null;
  }
  
  public Expr visitParamRef(ParamRef e, AFVparams params)
  {
    return this.ctx.addParameter(e);
  }
  
  public Expr visitPatternVarRef(PatternVarRef e, AFVparams params)
  {
    return null;
  }
  
  public Expr visitRowSetRef(RowSetRef rowSetRef, AFVparams params)
  {
    return null;
  }
  
  public Expr visitArrayInitializer(ArrayInitializer arrayInit, AFVparams params)
  {
    Class<?> compType = arrayInit.getType().getComponentType();
    for (ValueExpr e : arrayInit.args)
    {
      Class<?> type = e.getType();
      if (!compType.isAssignableFrom(type)) {
        error("cannot convert expression to a array component type", e);
      }
    }
    return null;
  }
  
  public Expr visitStringConcatenation(StringConcatenation stringConcatenation, AFVparams params)
  {
    return null;
  }
}
