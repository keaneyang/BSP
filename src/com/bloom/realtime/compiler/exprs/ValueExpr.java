package com.bloom.runtime.compiler.exprs;

import com.bloom.runtime.compiler.visitors.ExpressionVisitor;
import com.bloom.runtime.utils.NamePolicy;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME)
@JsonSubTypes({@com.fasterxml.jackson.annotation.JsonSubTypes.Type(name="Constant", value=Constant.class), @com.fasterxml.jackson.annotation.JsonSubTypes.Type(name="Case", value=Case.class), @com.fasterxml.jackson.annotation.JsonSubTypes.Type(name="CaseExpr", value=CaseExpr.class), @com.fasterxml.jackson.annotation.JsonSubTypes.Type(name="DataSetRef", value=DataSetRef.class), @com.fasterxml.jackson.annotation.JsonSubTypes.Type(name="ObjectRef", value=ObjectRef.class), @com.fasterxml.jackson.annotation.JsonSubTypes.Type(name="TypeRef", value=TypeRef.class), @com.fasterxml.jackson.annotation.JsonSubTypes.Type(name="ArrayTypeRef", value=ArrayTypeRef.class), @com.fasterxml.jackson.annotation.JsonSubTypes.Type(name="ClassRef", value=ClassRef.class), @com.fasterxml.jackson.annotation.JsonSubTypes.Type(name="StaticFieldRef", value=StaticFieldRef.class)})
public abstract class ValueExpr
  extends Expr
{
  public ValueExpr(ExprCmd op)
  {
    super(op);
  }
  
  public List<? extends Expr> getArgs()
  {
    return Collections.emptyList();
  }
  
  public <T> void visitArgs(ExpressionVisitor<T> v, T params) {}
  
  public FieldRef getField(String fieldName)
    throws NoSuchFieldException, SecurityException
  {
    Class<?> type = getType();
    Field field = NamePolicy.getField(type, fieldName);
    return new ObjFieldRef(this, fieldName, field);
  }
}
