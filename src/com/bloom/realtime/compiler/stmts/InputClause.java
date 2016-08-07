package com.bloom.runtime.compiler.stmts;

import java.util.List;

import com.bloom.runtime.compiler.TypeField;

public class InputClause
  implements InputOutputSink
{
  String stream;
  MappedStream mp;
  List<String> part;
  Select select;
  
  public InputClause(String stream, MappedStream mp, List<String> part, Select select)
  {
    this.stream = stream;
    this.mp = mp;
    this.part = part;
    this.select = select;
  }
  
  public String getStreamName()
  {
    return this.stream;
  }
  
  public MappedStream getGeneratedStream()
  {
    return this.mp;
  }
  
  public List<String> getPartitionFields()
  {
    return this.part;
  }
  
  public boolean isFiltered()
  {
    return this.select != null;
  }
  
  public Select getFilter()
  {
    return this.select;
  }
  
  public List<TypeField> getTypeDefinition()
  {
    return null;
  }
  
  public String getFilterText()
  {
    return null;
  }
}
