package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.Property;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

import java.util.List;

public class CreateCacheStmt
  extends CreateStmt
{
  public final AdapterDescription src;
  public final AdapterDescription parser;
  public final List<Property> query_props;
  public final String typename;
  
  public CreateCacheStmt(EntityType what, String name, Boolean doReplace, AdapterDescription src, AdapterDescription parser, List<Property> query_props, String typename)
  {
    super(what, name, doReplace.booleanValue());
    
    this.src = src;
    this.parser = parser;
    this.query_props = query_props;
    this.typename = typename;
  }
  
  public String toString()
  {
    String stmtString = null;
    stmtString = stmtToString();
    stmtString = stmtString + " " + this.src.getAdapterTypeName() + " (" + this.src.getProps() + ") ";
    if (this.parser.getAdapterTypeName() != null) {
      stmtString = stmtString + "PARSE USING " + this.parser.getAdapterTypeName() + " (" + this.src.getProps() + ") ";
    }
    stmtString = stmtString + "QUERY " + this.query_props + " TYPE " + this.typename;
    return stmtString;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileCreateCacheStmt(this);
  }
}
