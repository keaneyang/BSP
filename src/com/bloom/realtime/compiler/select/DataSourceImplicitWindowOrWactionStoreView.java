package com.bloom.runtime.compiler.select;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.metaRepository.PermissionUtility;
import com.bloom.runtime.Context;
import com.bloom.runtime.Pair;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.IntervalPolicy;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.meta.MetaInfo.Window;
import com.bloom.runtime.utils.RuntimeUtils;
import com.bloom.security.ObjectPermission.Action;

import java.util.List;

public class DataSourceImplicitWindowOrWactionStoreView
  extends DataSourcePrimary
{
  private final String streamOrWactionStoreName;
  private final String alias;
  private final Pair<IntervalPolicy, IntervalPolicy> window_len;
  private final boolean isJumping;
  private final List<String> partitionFields;
  
  public DataSourceImplicitWindowOrWactionStoreView(String streamOrWactionStoreName, String alias, Pair<IntervalPolicy, IntervalPolicy> window_len, boolean isJumping, List<String> part)
  {
    this.streamOrWactionStoreName = streamOrWactionStoreName;
    this.alias = alias;
    this.window_len = window_len;
    this.isJumping = isJumping;
    this.partitionFields = part;
  }
  
  public String toString()
  {
    return this.streamOrWactionStoreName + "[" + (this.isJumping ? "JUMPING " : "") + this.window_len + " " + this.partitionFields + "]" + this.alias;
  }
  
  public DataSet resolveDataSourceImpl(DataSource.Resolver r)
    throws MetaDataRepositoryException
  {
    Compiler co = r.getCompiler();
    Context ctx = co.getContext();
    MetaInfo.MetaObject dataSource = ctx.getDataSourceInCurSchema(this.streamOrWactionStoreName);
    if (dataSource == null) {
      r.error("no such stream", this.streamOrWactionStoreName);
    }
    if (!PermissionUtility.checkPermission(dataSource, ObjectPermission.Action.select, ctx.getSessionID(), false)) {
      r.error("No permission to select from " + dataSource.type.toString().toLowerCase() + " " + dataSource.nsName + "." + dataSource.name, dataSource.name);
    }
    if ((dataSource instanceof MetaInfo.Stream))
    {
      MetaInfo.Stream stream = (MetaInfo.Stream)dataSource;
      co.checkPartitionFields(this.partitionFields, stream.dataType, this.streamOrWactionStoreName);
      
      MetaInfo.Window w = ctx.putWindow(false, ctx.makeObjectName(RuntimeUtils.genRandomName("window")), stream.uuid, this.partitionFields, this.window_len, this.isJumping, false, true, null);
      
      w.getMetaInfoStatus().setAnonymous(true);
      MetadataRepository.getINSTANCE().updateMetaObject(w, ctx.getSessionID());
      r.addTypeID(stream.dataType);
      Class<?> type = co.getTypeClass(stream.dataType);
      DataSet ds = DataSetFactory.createWindowDS(r.getNextDataSetID(), this.streamOrWactionStoreName, this.alias, type, w);
      
      return ds;
    }
    r.error("implicit window can be applied only to stream", this.streamOrWactionStoreName);
    
    return null;
  }
}
