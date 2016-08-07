package com.bloom.runtime.compiler.select;

import com.bloom.classloading.WALoader;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.PermissionUtility;
import com.bloom.runtime.Context;
import com.bloom.runtime.Interval;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.IntervalPolicy;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.meta.MetaInfo.WAStoreView;
import com.bloom.runtime.meta.MetaInfo.WActionStore;
import com.bloom.runtime.utils.RuntimeUtils;
import com.bloom.security.ObjectPermission.Action;

public class DataSourceWactionStoreView
  extends DataSourcePrimary
{
  private final String wactionStoreName;
  private final String alias;
  private final Interval window_len;
  private final Boolean isJumping;
  private final boolean subscribeToUpdates;
  
  public DataSourceWactionStoreView(String wactionStoreName, String alias, Boolean isJumping, Interval window_len, boolean subscribeToUpdates)
  {
    this.wactionStoreName = wactionStoreName;
    this.alias = alias;
    this.window_len = window_len;
    this.subscribeToUpdates = subscribeToUpdates;
    this.isJumping = isJumping;
  }
  
  public String toString()
  {
    return this.wactionStoreName + "[" + this.window_len + (this.subscribeToUpdates ? " STREAM" : "") + "]" + this.alias;
  }
  
  public DataSet resolveDataSourceImpl(DataSource.Resolver r)
    throws MetaDataRepositoryException
  {
    Compiler co = r.getCompiler();
    Context ctx = co.getContext();
    MetaInfo.MetaObject swc = ctx.getDataSourceInCurSchema(this.wactionStoreName);
    if (!(swc instanceof MetaInfo.WActionStore)) {
      r.error("no such waction store", this.wactionStoreName);
    }
    if (!PermissionUtility.checkPermission(swc, ObjectPermission.Action.select, ctx.getSessionID(), false)) {
      r.error("No permission to select from " + swc.type.toString().toLowerCase() + " " + swc.nsName + "." + swc.name, swc.name);
    }
    if ((this.isJumping.booleanValue()) && (this.subscribeToUpdates)) {
      r.error("Jumping and push cannot be used together.", this.wactionStoreName);
    }
    MetaInfo.WActionStore store = (MetaInfo.WActionStore)swc;
    Class<?> type = null;
    try
    {
      MetaInfo.Type typeMetaObject = (MetaInfo.Type)ctx.getObject(store.contextType);
      type = WALoader.get().loadClass(typeMetaObject.className);
    }
    catch (ClassNotFoundException e)
    {
      e.printStackTrace();
    }
    MetaInfo.WAStoreView w = ctx.putWAStoreView(RuntimeUtils.genRandomName("wastoreview"), ctx.getCurNamespaceName(), store.uuid, this.window_len == null ? null : IntervalPolicy.createTimePolicy(this.window_len), this.isJumping, this.subscribeToUpdates, r.isAdhoc());
    
    DataSet ds = DataSetFactory.createWAStoreDS(r.getNextDataSetID(), this.wactionStoreName, this.alias, type, w, store);
    
    return ds;
  }
}
