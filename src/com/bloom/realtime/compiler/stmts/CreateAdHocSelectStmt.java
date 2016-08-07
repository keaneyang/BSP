package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

public class CreateAdHocSelectStmt
  extends CreateStmt
{
  public final Select select;
  public final String select_text;
  
  public CreateAdHocSelectStmt(Select sel, String select_text, String queryName)
  {
    super(EntityType.CQ, queryName, false);
    this.select = sel;
    this.select_text = select_text;
  }
  
  public String toString()
  {
    return stmtToString() + this.select;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileCreateAdHocSelect(this);
  }
}
