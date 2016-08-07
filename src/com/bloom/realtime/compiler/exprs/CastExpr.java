package com.bloom.runtime.compiler.exprs;

import java.util.List;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.bloom.runtime.compiler.AST;
import com.bloom.runtime.compiler.TypeName;
import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class CastExpr
  extends Operation
{
  public final TypeName casttype;
  
  public CastExpr(TypeName type, ValueExpr arg)
  {
    super(ExprCmd.CAST, AST.NewList(arg));
    this.casttype = type;
  }
  
  public String toString()
  {
    return exprToString() + "CAST(" + getExpr() + " AS " + this.casttype + ")";
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitCastExpr(this, params);
  }
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof CastExpr)) {
      return false;
    }
    CastExpr o = (CastExpr)other;
    return (super.equals(o)) && (this.casttype.equals(o.casttype));
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(super.toString()).append(this.casttype).toHashCode();
  }
  
  public Class<?> getType()
  {
    return UndefinedType.class;
  }
  
  public ValueExpr getExpr()
  {
    return (ValueExpr)this.args.get(0);
  }
  
  public static class UndefinedType {}
}
