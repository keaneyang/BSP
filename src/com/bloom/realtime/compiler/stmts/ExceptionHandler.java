package com.bloom.runtime.compiler.stmts;

import java.util.ArrayList;
import java.util.List;

import com.bloom.runtime.Property;

public class ExceptionHandler
{
  public final List<Property> props;
  
  public ExceptionHandler(List<Property> props)
  {
    List<Property> transformedProps = new ArrayList();
    if ((props != null) && (!props.isEmpty())) {
      for (Property prop : props) {
        transformedProps.add(new Property(prop.name, prop.value != null ? ((String)prop.value).toUpperCase() : null));
      }
    }
    this.props = transformedProps;
  }
}
