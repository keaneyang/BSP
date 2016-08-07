package com.bloom.runtime.compiler.stmts;

import com.bloom.runtime.compiler.TypeField;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.Serializable;
import java.util.List;

public abstract interface InputOutputSink
  extends Serializable
{
  @JsonIgnore
  public abstract String getStreamName();
  
  @JsonIgnore
  public abstract MappedStream getGeneratedStream();
  
  @JsonIgnore
  public abstract List<String> getPartitionFields();
  
  @JsonIgnore
  public abstract boolean isFiltered();
  
  @JsonIgnore
  public abstract Select getFilter();
  
  @JsonIgnore
  public abstract List<TypeField> getTypeDefinition();
  
  @JsonIgnore
  public abstract String getFilterText();
}

