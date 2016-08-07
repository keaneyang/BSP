package com.bloom.runtime.compiler.select;

import java.util.List;
import java.util.Set;

public abstract class IndexInfo
{
  public abstract List<Integer> getListOfIndexesForFields(Set<String> paramSet);
  
  public abstract List<String> indexFieldList(int paramInt);
  
  public abstract String toString();
}
