package com.bloom.runtime.compiler.exprs;

import java.lang.reflect.Constructor;
import java.util.List;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.bloom.runtime.compiler.TypeName;
import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class ConstructorCall
  extends Operation
{
  public final TypeName objtype;
  private Constructor<?> constructor;
  
  public ConstructorCall(TypeName type, List<ValueExpr> args)
  {
    super(ExprCmd.CONSTRUCTOR, args);
    this.objtype = type;
    this.constructor = null;
  }
  
  public String toString()
  {
    return exprToString() + this.objtype + "(" + argsToString() + ")";
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitConstructorCall(this, params);
  }
  
  public void setConstructor(Constructor<?> con)
  {
    this.constructor = con;
  }
  
  public Constructor<?> getConstructor()
  {
    return this.constructor;
  }
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ConstructorCall)) {
      return false;
    }
    ConstructorCall o = (ConstructorCall)other;
    return (super.equals(o)) && (this.objtype.equals(o.objtype)) && (this.constructor.equals(o.constructor));
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(toString()).toHashCode();
  }
  
  public Class<?> getType()
  {
    return this.constructor.getDeclaringClass();
  }
}
