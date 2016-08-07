package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.Property;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

import java.util.List;

public class CreatePropertySetStmt
  extends CreateStmt
{
  public final List<Property> props;
  
  public CreatePropertySetStmt(String name, Boolean doReplace, List<Property> props)
  {
    super(EntityType.PROPERTYSET, name, doReplace.booleanValue());
    this.props = props;
  }
  
  public String toString()
  {
    return stmtToString() + " (" + this.props + ")";
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileCreatePropertySetStmt(this);
  }
}
