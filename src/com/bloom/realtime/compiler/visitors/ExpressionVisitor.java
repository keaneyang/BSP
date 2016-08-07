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

public abstract interface ExpressionVisitor<T>
{
  public abstract Expr visitExpr(Expr paramExpr, T paramT);
  
  public abstract Expr visitExprDefault(Expr paramExpr, T paramT);
  
  public abstract Expr visitNumericOperation(NumericOperation paramNumericOperation, T paramT);
  
  public abstract Expr visitConstant(Constant paramConstant, T paramT);
  
  public abstract Expr visitCase(CaseExpr paramCaseExpr, T paramT);
  
  public abstract Expr visitFuncCall(FuncCall paramFuncCall, T paramT);
  
  public abstract Expr visitConstructorCall(ConstructorCall paramConstructorCall, T paramT);
  
  public abstract Expr visitFieldRef(FieldRef paramFieldRef, T paramT);
  
  public abstract Expr visitIndexExpr(IndexExpr paramIndexExpr, T paramT);
  
  public abstract Expr visitLogicalPredicate(LogicalPredicate paramLogicalPredicate, T paramT);
  
  public abstract Expr visitComparePredicate(ComparePredicate paramComparePredicate, T paramT);
  
  public abstract Expr visitInstanceOfPredicate(InstanceOfPredicate paramInstanceOfPredicate, T paramT);
  
  public abstract Expr visitClassRef(ClassRef paramClassRef, T paramT);
  
  public abstract Expr visitArrayTypeRef(ArrayTypeRef paramArrayTypeRef, T paramT);
  
  public abstract Expr visitObjectRef(ObjectRef paramObjectRef, T paramT);
  
  public abstract Expr visitDataSetRef(DataSetRef paramDataSetRef, T paramT);
  
  public abstract Expr visitTypeRef(TypeRef paramTypeRef, T paramT);
  
  public abstract Expr visitStaticFieldRef(StaticFieldRef paramStaticFieldRef, T paramT);
  
  public abstract Expr visitCastExpr(CastExpr paramCastExpr, T paramT);
  
  public abstract Expr visitCastOperation(CastOperation paramCastOperation, T paramT);
  
  public abstract Expr visitWildCardExpr(WildCardExpr paramWildCardExpr, T paramT);
  
  public abstract Expr visitParamRef(ParamRef paramParamRef, T paramT);
  
  public abstract Expr visitPatternVarRef(PatternVarRef paramPatternVarRef, T paramT);
  
  public abstract Expr visitRowSetRef(RowSetRef paramRowSetRef, T paramT);
  
  public abstract Expr visitArrayInitializer(ArrayInitializer paramArrayInitializer, T paramT);
  
  public abstract Expr visitStringConcatenation(StringConcatenation paramStringConcatenation, T paramT);
}

