package com.bloom.runtime.compiler.stmts;

import java.util.List;

import com.bloom.runtime.Property;

public class AdapterDescription
{
  private final String adapterTypeName;
  private final List<Property> props;
  
  public AdapterDescription(String adapterTypeName, List<Property> props)
  {
    this.adapterTypeName = adapterTypeName;
    this.props = props;
  }
  
  public String getAdapterTypeName()
  {
    return this.adapterTypeName;
  }
  
  public List<Property> getProps()
  {
    return this.props;
  }
}
