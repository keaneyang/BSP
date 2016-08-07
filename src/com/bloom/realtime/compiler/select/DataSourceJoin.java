package com.bloom.runtime.compiler.select;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.exprs.Predicate;

import java.util.List;
import java.util.Map;

public class DataSourceJoin
  implements DataSource
{
  public final DataSource left;
  public final DataSource right;
  public final Join.Kind kindOfJoin;
  public final Predicate joinCondition;
  
  public DataSourceJoin(DataSource left, DataSource right, Join.Kind kindOfJoin, Predicate join_cond)
  {
    this.left = left;
    this.right = right;
    this.kindOfJoin = kindOfJoin;
    this.joinCondition = join_cond;
  }
  
  public String toString()
  {
    return this.left + " " + this.kindOfJoin + " JOIN " + this.right + " ON " + this.joinCondition;
  }
  
  public void resolveDataSource(DataSource.Resolver r, Map<String, DataSet> resolved, List<DataSourcePrimary> unresolved, boolean onlyPrimary)
    throws MetaDataRepositoryException
  {
    this.left.resolveDataSource(r, resolved, unresolved, onlyPrimary);
    this.right.resolveDataSource(r, resolved, unresolved, onlyPrimary);
  }
  
  public Join.Node addDataSource(DataSource.Resolver r)
  {
    return r.addDataSourceJoin(this);
  }
}
