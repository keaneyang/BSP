package com.bloom.runtime.compiler.select;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.TraceOptions;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.TypeName;
import com.bloom.runtime.compiler.exprs.Predicate;
import com.bloom.uuid.UUID;

import java.util.List;
import java.util.Map;

public abstract interface DataSource
{
  public abstract String toString();
  
  public abstract void resolveDataSource(Resolver paramResolver, Map<String, DataSet> paramMap, List<DataSourcePrimary> paramList, boolean paramBoolean)
    throws MetaDataRepositoryException;
  
  public abstract Join.Node addDataSource(Resolver paramResolver);
  
  public static abstract interface Resolver
  {
    public abstract Compiler getCompiler();
    
    public abstract int getNextDataSetID();
    
    public abstract Class<?> getTypeInfo(TypeName paramTypeName);
    
    public abstract TraceOptions getTraceOptions();
    
    public abstract boolean isAdhoc();
    
    public abstract void addTypeID(UUID paramUUID);
    
    public abstract void addPredicate(Predicate paramPredicate);
    
    public abstract Join.Node addDataSourceJoin(DataSourceJoin paramDataSourceJoin);
    
    public abstract Join.Node addDataSet(DataSet paramDataSet);
    
    public abstract void error(String paramString, Object paramObject);
  }
}

