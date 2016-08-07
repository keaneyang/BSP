package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

public class UseStmt
  extends Stmt
{
  public final EntityType what;
  public final String schemaName;
  public final boolean recompile;
  
  public UseStmt(EntityType what, String schemaName, boolean recompile)
  {
    this.what = what;
    this.schemaName = schemaName;
    this.recompile = recompile;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileUseStmt(this);
  }
  
  public String toString()
  {
    if ((this.sourceText != null) && (!this.sourceText.isEmpty())) {
      return this.sourceText;
    }
    return "USE " + this.schemaName;
  }
}
