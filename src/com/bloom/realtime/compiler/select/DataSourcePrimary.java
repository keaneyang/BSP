package com.bloom.runtime.compiler.select;

import java.util.List;
import java.util.Map;

import com.bloom.metaRepository.MetaDataRepositoryException;

public abstract class DataSourcePrimary
  implements DataSource
{
  private DataSet dataset;
  
  public void resolveDataSource(DataSource.Resolver r, Map<String, DataSet> resolved, List<DataSourcePrimary> unresolved, boolean onlyPrimary)
    throws MetaDataRepositoryException
  {
    this.dataset = resolveDataSource(r, resolved, onlyPrimary);
    if (this.dataset != null)
    {
      String dsname = this.dataset.getName();
      if (resolved.containsKey(dsname)) {
        r.error("data source name duplication", dsname);
      }
      resolved.put(dsname, this.dataset);
    }
    else
    {
      unresolved.add(this);
    }
  }
  
  public DataSet resolveDataSource(DataSource.Resolver r, Map<String, DataSet> resolved, boolean onlyPrimary)
    throws MetaDataRepositoryException
  {
    return resolveDataSourceImpl(r);
  }
  
  public abstract DataSet resolveDataSourceImpl(DataSource.Resolver paramResolver)
    throws MetaDataRepositoryException;
  
  public Join.Node addDataSource(DataSource.Resolver r)
  {
    assert (this.dataset != null);
    return r.addDataSet(this.dataset);
  }
}
