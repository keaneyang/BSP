package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.Property;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

import java.util.ArrayList;
import java.util.List;

public class CreatePropertyVariableStmt
  extends CreateStmt
{
  public final List<Property> props;
  
  public CreatePropertyVariableStmt(String paramname, Object paramvalue, Boolean doReplace)
  {
    super(EntityType.PROPERTYVARIABLE, paramname, doReplace.booleanValue());
    List<Property> props = new ArrayList();
    Property prop1 = new Property(paramname, paramvalue);
    props.add(prop1);
    Object obj = Boolean.valueOf(false);
    Property prop2 = new Property(paramname + "_encrypted", obj);
    props.add(prop2);
    this.props = props;
  }
  
  public String toString()
  {
    return stmtToString() + " (" + this.props + ")";
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileCreatePropertyVariableStmt(this);
  }
}
