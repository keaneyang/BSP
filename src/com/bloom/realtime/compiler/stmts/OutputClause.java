package com.bloom.runtime.compiler.stmts;

import com.bloom.runtime.compiler.TypeField;
import com.bloom.utility.Utility;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

public class OutputClause
  implements InputOutputSink
{
  public String stream;
  public MappedStream mp;
  public List<String> part;
  public List<TypeField> fields;
  @JsonIgnore
  private transient Select select;
  public String selectTQL;
  
  public OutputClause() {}
  
  public OutputClause(String stream, MappedStream mp, List<String> part, List<TypeField> fields, Select select, String selectTQL)
  {
    this.stream = stream;
    this.mp = mp;
    this.part = part;
    this.fields = fields;
    this.select = select;
    this.selectTQL = selectTQL;
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
    return this.fields;
  }
  
  public String getFilterText()
  {
    return this.selectTQL;
  }
  
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    if (this.stream != null) {
      sb.append(this.stream + " ");
    }
    if (this.mp != null) {
      sb.append(this.mp + " ");
    }
    if (this.selectTQL != null) {
      sb.append(Utility.cleanUpSelectStatement(this.selectTQL));
    }
    String str = sb.toString().replaceAll("[\000-\037]", "");
    
    return str;
  }
}
