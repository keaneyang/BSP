package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.tungsten.Tungsten;

import java.util.concurrent.atomic.AtomicBoolean;

public class CreateShowStreamStmt
  extends Stmt
{
  public final String stream_name;
  public final int line_count;
  public final boolean isTungsten;
  
  public CreateShowStreamStmt(String stream_name)
  {
    this.stream_name = stream_name;
    this.line_count = 10;
    Tungsten.isAdhocRunning.set(true);
    this.isTungsten = true;
  }
  
  public CreateShowStreamStmt(String stream_name, int line_count)
  {
    this.stream_name = stream_name;
    this.line_count = line_count;
    Tungsten.isAdhocRunning.set(true);
    this.isTungsten = true;
  }
  
  public CreateShowStreamStmt(String stream_name, int line_count, boolean isTungsten)
  {
    this.stream_name = stream_name;
    this.line_count = line_count;
    this.isTungsten = isTungsten;
  }
  
  public String toString()
  {
    return "SHOW " + this.stream_name + " line_count " + this.line_count;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileShowStreamStmt(this);
  }
}
