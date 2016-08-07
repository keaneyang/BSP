package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.tungsten.Tungsten;

import org.apache.log4j.Logger;

public class PrintMetaDataStmt
  extends Stmt
{
  String mode = "";
  String type = null;
  String command = "";
  
  public PrintMetaDataStmt(String string, String type, String string2)
  {
    this.mode = string.toLowerCase();
    this.type = type;
    this.command = string2;
  }
  
  private static Logger logger = Logger.getLogger(PrintMetaDataStmt.class);
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    switch (this.mode)
    {
    case "list": 
      Tungsten.list(this.mode + " " + this.type + "");
      break;
    case "describe": 
      Tungsten.describe(this.mode + " " + (this.type == null ? "" : new StringBuilder().append(this.type).append(" ").toString()) + this.command);
    }
    return null;
  }
}
