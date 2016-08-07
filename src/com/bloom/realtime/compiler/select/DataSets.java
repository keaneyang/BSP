package com.bloom.runtime.compiler.select;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.bloom.runtime.Pair;
import com.bloom.runtime.compiler.exprs.DataSetRef;
import com.bloom.runtime.compiler.exprs.FieldRef;
import com.bloom.runtime.exceptions.AmbiguousFieldNameException;
import com.bloom.runtime.utils.Factory;

public class DataSets
  implements Iterable<DataSet>
{
  private final Map<String, DataSet> dataSets = Factory.makeNameLinkedMap();
  private final Map<Integer, DataSet> dataSetsById = Factory.makeMap();
  private final Map<String, List<DataSet>> fieldIndex = Factory.makeNameMap();
  
  public int size()
  {
    return this.dataSets.size();
  }
  
  public boolean add(DataSet ds)
  {
    String dsname = ds.getName();
    if (this.dataSets.containsKey(dsname)) {
      return false;
    }
    this.dataSets.put(dsname, ds);
    DataSet xds = (DataSet)this.dataSetsById.put(Integer.valueOf(ds.getID()), ds);
    assert (xds == null) : "dataset id duplication";
    for (Pair<String, Class<?>> p : ds.getAllTypeFields())
    {
      String fieldName = (String)p.first;
      if (this.fieldIndex.containsKey(fieldName))
      {
        ((List)this.fieldIndex.get(fieldName)).add(ds);
      }
      else
      {
        List<DataSet> value = new LinkedList();
        value.add(ds);
        this.fieldIndex.put(fieldName, value);
      }
    }
    return true;
  }
  
  public DataSet get(int id)
  {
    DataSet ds = (DataSet)this.dataSetsById.get(Integer.valueOf(id));
    assert (ds != null) : "invalid dataset index";
    return ds;
  }
  
  public DataSet get(String name)
  {
    return (DataSet)this.dataSets.get(name);
  }
  
  public FieldRef findField(String name)
    throws AmbiguousFieldNameException
  {
    List<DataSet> dssets = (List)this.fieldIndex.get(name);
    if (dssets != null)
    {
      if (dssets.size() != 1) {
        throw new AmbiguousFieldNameException();
      }
      try
      {
        DataSet ds = (DataSet)dssets.get(0);
        DataSetRef dsref = ds.makeRef();
        return dsref.getField(name);
      }
      catch (NoSuchFieldException|SecurityException e) {}
    }
    return null;
  }
  
  public Iterator<DataSet> iterator()
  {
    return this.dataSets.values().iterator();
  }
}
