package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.ActionType;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

public class ActionStmt
  extends Stmt
{
  public final ActionType what;
  public final String name;
  public final EntityType type;
  public final RecoveryDescription recov;
  
  public ActionStmt(ActionType what, String name, EntityType type)
  {
    this.what = what;
    this.name = name;
    this.type = type;
    this.recov = null;
  }
  
  public ActionStmt(ActionType what, String name, EntityType type, RecoveryDescription recov)
  {
    this.what = what;
    this.name = name;
    this.type = type;
    this.recov = recov;
  }
  
  public String toString()
  {
    return this.what + " " + this.type + " " + this.name;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileActionStmt(this);
  }
}
