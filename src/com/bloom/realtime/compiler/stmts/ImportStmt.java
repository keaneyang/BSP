package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;

public class ImportStmt
  extends Stmt
{
  public final boolean hasStar;
  public final boolean hasStaticKeyword;
  public final String name;
  
  public ImportStmt(String name, boolean hasStar, boolean hasStaticKeyword)
  {
    this.name = name;
    this.hasStar = hasStar;
    this.hasStaticKeyword = hasStaticKeyword;
  }
  
  public String toString()
  {
    return "IMPORT " + (this.hasStaticKeyword ? " STATIC " : "") + this.name + (this.hasStar ? ".*" : "");
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileImportStmt(this);
  }
}
