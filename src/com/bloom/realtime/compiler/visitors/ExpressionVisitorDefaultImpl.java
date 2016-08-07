package com.bloom.runtime.compiler.visitors;

import com.bloom.runtime.compiler.exprs.ArrayInitializer;
import com.bloom.runtime.compiler.exprs.ArrayTypeRef;
import com.bloom.runtime.compiler.exprs.CaseExpr;
import com.bloom.runtime.compiler.exprs.CastExpr;
import com.bloom.runtime.compiler.exprs.CastOperation;
import com.bloom.runtime.compiler.exprs.ClassRef;
import com.bloom.runtime.compiler.exprs.ComparePredicate;
import com.bloom.runtime.compiler.exprs.Constant;
import com.bloom.runtime.compiler.exprs.ConstructorCall;
import com.bloom.runtime.compiler.exprs.DataSetRef;
import com.bloom.runtime.compiler.exprs.Expr;
import com.bloom.runtime.compiler.exprs.FieldRef;
import com.bloom.runtime.compiler.exprs.FuncCall;
import com.bloom.runtime.compiler.exprs.IndexExpr;
import com.bloom.runtime.compiler.exprs.InstanceOfPredicate;
import com.bloom.runtime.compiler.exprs.LogicalPredicate;
import com.bloom.runtime.compiler.exprs.NumericOperation;
import com.bloom.runtime.compiler.exprs.ObjectRef;
import com.bloom.runtime.compiler.exprs.ParamRef;
import com.bloom.runtime.compiler.exprs.PatternVarRef;
import com.bloom.runtime.compiler.exprs.RowSetRef;
import com.bloom.runtime.compiler.exprs.StaticFieldRef;
import com.bloom.runtime.compiler.exprs.StringConcatenation;
import com.bloom.runtime.compiler.exprs.TypeRef;
import com.bloom.runtime.compiler.exprs.WildCardExpr;

public class ExpressionVisitorDefaultImpl<T>
  implements ExpressionVisitor<T>
{
  public Expr visitExpr(Expr e, T params)
  {
    return e.visit(this, params);
  }
  
  public Expr visitExprDefault(Expr e, T params)
  {
    return null;
  }
  
  public Expr visitNumericOperation(NumericOperation operation, T params)
  {
    return visitExprDefault(operation, params);
  }
  
  public Expr visitConstant(Constant constant, T params)
  {
    return visitExprDefault(constant, params);
  }
  
  public Expr visitCase(CaseExpr caseExpr, T params)
  {
    return visitExprDefault(caseExpr, params);
  }
  
  public Expr visitFuncCall(FuncCall funcCall, T params)
  {
    return visitExprDefault(funcCall, params);
  }
  
  public Expr visitConstructorCall(ConstructorCall constructorCall, T params)
  {
    return visitExprDefault(constructorCall, params);
  }
  
  public Expr visitCastExpr(CastExpr castExpr, T params)
  {
    return visitExprDefault(castExpr, params);
  }
  
  public Expr visitFieldRef(FieldRef fieldRef, T params)
  {
    return visitExprDefault(fieldRef, params);
  }
  
  public Expr visitIndexExpr(IndexExpr index, T params)
  {
    return visitExprDefault(index, params);
  }
  
  public Expr visitLogicalPredicate(LogicalPredicate logicalPredicate, T params)
  {
    return visitExprDefault(logicalPredicate, params);
  }
  
  public Expr visitComparePredicate(ComparePredicate comparePredicate, T params)
  {
    return visitExprDefault(comparePredicate, params);
  }
  
  public Expr visitInstanceOfPredicate(InstanceOfPredicate instanceOfPredicate, T params)
  {
    return visitExprDefault(instanceOfPredicate, params);
  }
  
  public Expr visitClassRef(ClassRef classRef, T params)
  {
    return visitExprDefault(classRef, params);
  }
  
  public Expr visitArrayTypeRef(ArrayTypeRef arrayTypeRef, T params)
  {
    return visitExprDefault(arrayTypeRef, params);
  }
  
  public Expr visitObjectRef(ObjectRef objectRef, T params)
  {
    return visitExprDefault(objectRef, params);
  }
  
  public Expr visitDataSetRef(DataSetRef dataSetRef, T params)
  {
    return visitExprDefault(dataSetRef, params);
  }
  
  public Expr visitTypeRef(TypeRef typeRef, T params)
  {
    return visitExprDefault(typeRef, params);
  }
  
  public Expr visitStaticFieldRef(StaticFieldRef fieldRef, T params)
  {
    return visitExprDefault(fieldRef, params);
  }
  
  public Expr visitCastOperation(CastOperation castOp, T params)
  {
    return visitExprDefault(castOp, params);
  }
  
  public Expr visitWildCardExpr(WildCardExpr e, T params)
  {
    return visitExprDefault(e, params);
  }
  
  public Expr visitParamRef(ParamRef e, T params)
  {
    return visitExprDefault(e, params);
  }
  
  public Expr visitPatternVarRef(PatternVarRef e, T params)
  {
    return visitExprDefault(e, params);
  }
  
  public Expr visitRowSetRef(RowSetRef e, T params)
  {
    return visitExprDefault(e, params);
  }
  
  public Expr visitArrayInitializer(ArrayInitializer e, T params)
  {
    return visitExprDefault(e, params);
  }
  
  public Expr visitStringConcatenation(StringConcatenation e, T params)
  {
    return visitExprDefault(e, params);
  }
}
