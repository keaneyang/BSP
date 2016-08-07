package com.bloom.runtime.compiler.exprs;

import java.util.ArrayList;
import java.util.List;

import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class WildCardExpr
  extends ValueExpr
{
  private final String dsname;
  private final List<ValueExpr> exprs;
  private final List<String> aliases;
  private boolean visited = false;
  
  public WildCardExpr(String dsname)
  {
    super(ExprCmd.WILDCARD);
    this.dsname = dsname;
    this.exprs = new ArrayList();
    this.aliases = new ArrayList();
  }
  
  public Class<?> getType()
  {
    return WildCardExprType.class;
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitWildCardExpr(this, params);
  }
  
  public String getDataSetName()
  {
    return this.dsname;
  }
  
  public List<ValueExpr> getExprs()
  {
    return this.exprs;
  }
  
  public List<String> getAliases()
  {
    return this.aliases;
  }
  
  public void setExprs(List<FieldRef> fields)
  {
    if (!this.visited)
    {
      this.exprs.addAll(fields);
      for (FieldRef f : fields) {
        this.aliases.add(f.getName());
      }
      this.visited = true;
    }
  }
  
  public void updateExprs(List<ValueExpr> exprs)
  {
    this.exprs.clear();
    this.exprs.addAll(exprs);
  }
  
  public static class WildCardExprType {}
}
