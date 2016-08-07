package com.bloom.runtime.meta;

import com.bloom.runtime.TraceOptions;
import com.bloom.runtime.compiler.select.ParamDesc;
import com.bloom.runtime.compiler.select.RSFieldDesc;
import com.bloom.runtime.components.AggAlgo;
import com.bloom.uuid.UUID;
import com.fasterxml.jackson.annotation.JsonIgnore;

import flexjson.JSON;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CQExecutionPlan
  implements Serializable, Cloneable
{
  public static final int DUMP_INPUT = 1;
  public static final int DUMP_OUTPUT = 2;
  public static final int DUMP_CODE = 4;
  public static final int DUMP_PLAN = 8;
  public static final int DUMP_PROC = 16;
  public static final int DUMP_MULTILINE = 32;
  public static final int DEBUG_INFO = 64;
  public static final int IS_STATEFUL = 1;
  public static final int IS_MATCH = 2;
  private static final long serialVersionUID = 3505193508744220237L;
  public final List<RSFieldDesc> resultSetDesc;
  public final List<ParamDesc> paramsDesc;
  public final int kindOfOutputStream;
  public List<DataSource> dataSources;
  public final int indexCount;
  public final List<Class<?>> aggObjFactories;
  public final List<UUID> usedTypes;
  public final AggAlgo howToAggregate;
  public final TraceOptions traceOptions;
  public final int dataSetCount;
  public final int flags;
  @JSON(include=false)
  @JsonIgnore
  public final List<byte[]> code;
  @JSON(include=false)
  @JsonIgnore
  public final List<String> sourcecode;
  
  public static class DataSource
    implements Serializable, Cloneable
  {
    private static final long serialVersionUID = 181781829866062244L;
    public final String name;
    private UUID dataSourceID;
    public final UUID typeID;
    
    public DataSource()
    {
      this.name = null;
      this.dataSourceID = null;
      this.typeID = null;
    }
    
    public DataSource(String name, UUID dataSourceID, UUID typeID)
    {
      this.name = name;
      this.dataSourceID = dataSourceID;
      this.typeID = typeID;
    }
    
    public String toString()
    {
      return this.name + ":" + this.dataSourceID;
    }
    
    @JsonIgnore
    public UUID getDataSourceID()
    {
      return this.dataSourceID;
    }
    
    @JsonIgnore
    public void replaceDataSourceID(UUID newid)
    {
      this.dataSourceID = newid;
    }
    
    public DataSource clone()
      throws CloneNotSupportedException
    {
      return (DataSource)super.clone();
    }
  }
  
  public CQExecutionPlan()
  {
    this.resultSetDesc = null;
    this.paramsDesc = null;
    this.kindOfOutputStream = 0;
    this.dataSources = null;
    this.code = null;
    this.indexCount = 0;
    this.aggObjFactories = null;
    this.usedTypes = null;
    this.howToAggregate = null;
    this.traceOptions = null;
    this.dataSetCount = 0;
    this.flags = 0;
    this.sourcecode = null;
  }
  
  private CQExecutionPlan(CQExecutionPlan cq)
    throws CloneNotSupportedException
  {
    this.resultSetDesc = cq.resultSetDesc;
    this.paramsDesc = cq.paramsDesc;
    this.kindOfOutputStream = cq.kindOfOutputStream;
    this.dataSources = new ArrayList();
    for (DataSource ds : cq.dataSources) {
      this.dataSources.add(ds.clone());
    }
    this.code = cq.code;
    this.indexCount = cq.indexCount;
    this.aggObjFactories = cq.aggObjFactories;
    this.usedTypes = cq.usedTypes;
    this.howToAggregate = cq.howToAggregate;
    this.traceOptions = cq.traceOptions;
    this.dataSetCount = cq.dataSetCount;
    this.flags = cq.flags;
    this.sourcecode = cq.sourcecode;
  }
  
  public CQExecutionPlan(List<RSFieldDesc> rsdesc, List<ParamDesc> paramsDesc, int kindOfOutputStream, List<DataSource> dataSources, List<byte[]> code, int indexCount, List<Class<?>> aggObjFactories, List<UUID> usedTypes, boolean hasGroupBy, boolean hasAggFunc, TraceOptions traceOptions, int dataSetCount, boolean isStateful, boolean isMatch, List<String> sourcecodeList)
  {
    assert (!code.isEmpty());
    this.resultSetDesc = rsdesc;
    this.paramsDesc = paramsDesc;
    this.kindOfOutputStream = kindOfOutputStream;
    this.dataSources = dataSources;
    this.code = code;
    this.indexCount = indexCount;
    this.aggObjFactories = aggObjFactories;
    this.usedTypes = usedTypes;
    this.howToAggregate = getAggAlgorithm(hasAggFunc, hasGroupBy);
    this.traceOptions = traceOptions;
    this.dataSetCount = dataSetCount;
    this.sourcecode = sourcecodeList;
    this.flags = ((isStateful ? 1 : 0) | (isMatch ? 2 : 0));
  }
  
  public String toString()
  {
    return "ExecutionPlan(resultSetDesc:" + this.resultSetDesc + "paramsDesc:" + this.paramsDesc + "\noutputStreamKind:" + this.kindOfOutputStream + "\ndataSources:" + this.dataSources + "\naggObjFactories:" + this.aggObjFactories + "\nusedTypes:" + this.usedTypes + "\nhowToAggregate:" + this.howToAggregate + "\ntraceOptions:" + this.traceOptions + "\ndataset count:" + this.dataSetCount + "\nstateful:" + isStateful() + "\nisMatch:" + isMatch() + ")";
  }
  
  @JsonIgnore
  private static AggAlgo getAggAlgorithm(boolean hasAggFunc, boolean hasGroupBy)
  {
    int val = (hasAggFunc ? 2 : 0) | (hasGroupBy ? 1 : 0);
    switch (val)
    {
    case 0: 
      return AggAlgo.SIMPLE;
    case 1: 
      return AggAlgo.GROUPBY_ONLY;
    case 2: 
      return AggAlgo.AGG_ONLY;
    case 3: 
      return AggAlgo.AGG_AND_GROUP_BY;
    }
    if (!$assertionsDisabled) {
      throw new AssertionError();
    }
    return AggAlgo.SIMPLE;
  }
  
  @JsonIgnore
  public List<UUID> getDataSources()
  {
    ArrayList<UUID> ret = new ArrayList();
    DataSource ds;
    for (Iterator i$ = this.dataSources.iterator(); i$.hasNext(); ret.add(ds.dataSourceID)) {
      ds = (DataSource)i$.next();
    }
    return ret;
  }
  
  public List<DataSource> getDataSourceList()
  {
    return this.dataSources;
  }
  
  public void setDataSourceList(List<DataSource> dataSources)
  {
    this.dataSources = dataSources;
  }
  
  @JsonIgnore
  public DataSource findDataSource(UUID id)
  {
    for (DataSource ds : this.dataSources) {
      if (ds.dataSourceID.equals(id)) {
        return ds;
      }
    }
    return null;
  }
  
  @JsonIgnore
  public Map<String, Long> makeFieldsMap()
  {
    long i = 1L;
    Map<String, Long> map = new HashMap();
    for (RSFieldDesc f : this.resultSetDesc) {
      map.put(f.name, Long.valueOf(i++));
    }
    return map;
  }
  
  public CQExecutionPlan clone()
    throws CloneNotSupportedException
  {
    return new CQExecutionPlan(this);
  }
  
  @JsonIgnore
  public boolean isStateful()
  {
    return (this.flags & 0x1) != 0;
  }
  
  @JsonIgnore
  public boolean isMatch()
  {
    return (this.flags & 0x2) != 0;
  }
  
  public static abstract interface CQExecutionPlanFactory
  {
    public abstract CQExecutionPlan createPlan()
      throws Exception;
  }
}
