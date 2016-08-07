package com.bloom.runtime.compiler.stmts;

import com.bloom.drop.DropMetaObject;
import com.bloom.drop.DropMetaObject.DropRule;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

public class DropStmt
  extends Stmt
{
  public final EntityType objectType;
  public final String objectName;
  public final DropMetaObject.DropRule dropRule;
  
  public DropStmt(EntityType objectType, String objectName, DropMetaObject.DropRule dropRule)
  {
    this.objectType = objectType;
    this.objectName = objectName;
    this.dropRule = dropRule;
  }
  
  public String toString()
  {
    return "DROP " + this.objectType + " " + this.objectName;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileDropStmt(this);
  }
}
