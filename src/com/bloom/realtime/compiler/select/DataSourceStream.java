package com.bloom.runtime.compiler.select;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.PermissionUtility;
import com.bloom.proc.events.DynamicEvent;
import com.bloom.runtime.Context;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.exprs.ComparePredicate;
import com.bloom.runtime.compiler.exprs.DataSetRef;
import com.bloom.runtime.compiler.exprs.ExprCmd;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo.Cache;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.meta.MetaInfo.WAStoreView;
import com.bloom.runtime.meta.MetaInfo.WActionStore;
import com.bloom.runtime.meta.MetaInfo.Window;
import com.bloom.runtime.utils.RuntimeUtils;
import com.bloom.security.ObjectPermission.Action;
import com.bloom.uuid.UUID;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class DataSourceStream
  extends DataSourcePrimary
  implements Serializable
{
  private final String streamName;
  private final String alias;
  private final String opt_typename;
  
  public DataSourceStream(String stream_name, String opt_typename, String alias)
  {
    this.streamName = stream_name;
    this.opt_typename = opt_typename;
    this.alias = alias;
  }
  
  public String toString()
  {
    return this.streamName + (this.opt_typename == null ? "" : new StringBuilder().append("<").append(this.opt_typename).append("> ").toString()) + (this.alias == null ? "" : new StringBuilder().append(" AS ").append(this.alias).toString());
  }
  
  public DataSet resolveDataSourceImpl(DataSource.Resolver r)
    throws MetaDataRepositoryException
  {
    Compiler co = r.getCompiler();
    Context ctx = co.getContext();
    MetaInfo.MetaObject swc = ctx.getDataSourceInCurSchema(this.streamName);
    if (swc == null) {
      r.error("no such stream", this.streamName);
    }
    if (!PermissionUtility.checkPermission(swc, ObjectPermission.Action.select, ctx.getSessionID(), false)) {
      r.error("No permission to select from " + swc.type.toString().toLowerCase() + " " + swc.nsName + "." + swc.name, swc.name);
    }
    UUID dataTypeID = null;
    DataSet ds;
    if (((swc instanceof MetaInfo.Stream)) && (this.opt_typename != null))
    {
      MetaInfo.Stream stream = (MetaInfo.Stream)swc;
      dataTypeID = stream.dataType;
      Class<?> type = co.getTypeClass(dataTypeID);
      if (!DynamicEvent.class.isAssignableFrom(type)) {
        r.error("type of events in data source should be derived from " + DynamicEvent.class.getName(), this.streamName);
      }
      MetaInfo.Type t = ctx.getTypeInCurSchema(this.opt_typename);
      if (t == null) {
        r.error("no such type", this.opt_typename);
      }
      List<RSFieldDesc> fields = new ArrayList();
      for (Map.Entry<String, String> fieldDesc : t.fields.entrySet()) {
        try
        {
          String fieldName = (String)fieldDesc.getKey();
          Class<?> fieldType = ctx.getClass((String)fieldDesc.getValue());
          RSFieldDesc f = new RSFieldDesc(fieldName, fieldType);
          fields.add(f);
        }
        catch (ClassNotFoundException e)
        {
          e.printStackTrace();
        }
      }
      DataSet ds = DataSetFactory.createTranslatedDS(r.getNextDataSetID(), this.streamName, this.alias, fields, t.uuid, swc);
      
      List<ValueExpr> args = new ArrayList();
      args.add(DataSetRef.makeDataSetRef(ds));
      r.addPredicate(new ComparePredicate(ExprCmd.CHECKRECTYPE, args));
    }
    else
    {
      DataSet ds;
      if ((swc instanceof MetaInfo.WActionStore))
      {
        MetaInfo.WActionStore store = (MetaInfo.WActionStore)swc;
        dataTypeID = store.contextType;
        Class<?> type = co.getTypeClass(dataTypeID);
        MetaInfo.WAStoreView w = ctx.putWAStoreView(RuntimeUtils.genRandomName("wastoreview"), ctx.getCurNamespaceName(), store.uuid, null, null, false, r.isAdhoc());
        
        ds = DataSetFactory.createWAStoreDS(r.getNextDataSetID(), this.streamName, this.alias, type, w, store);
      }
      else
      {
        DataSet ds;
        if ((swc instanceof MetaInfo.Window))
        {
          MetaInfo.Window window = (MetaInfo.Window)swc;
          MetaInfo.Stream stream = (MetaInfo.Stream)ctx.getObject(window.stream);
          Class<?> type = co.getTypeClass(stream.dataType);
          ds = DataSetFactory.createWindowDS(r.getNextDataSetID(), this.streamName, this.alias, type, window);
        }
        else
        {
          DataSet ds;
          if ((swc instanceof MetaInfo.Cache))
          {
            MetaInfo.Cache cache = (MetaInfo.Cache)swc;
            Class<?> type = co.getTypeClass(cache.typename);
            ds = DataSetFactory.createCacheDS(r.getNextDataSetID(), this.streamName, this.alias, type, cache);
          }
          else
          {
            DataSet ds;
            if ((swc instanceof MetaInfo.Stream))
            {
              MetaInfo.Stream stream = (MetaInfo.Stream)swc;
              Class<?> type = co.getTypeClass(stream.dataType);
              ds = DataSetFactory.createStreamDS(r.getNextDataSetID(), this.streamName, this.alias, type, stream);
            }
            else
            {
              r.error(swc.type + " data source is not implemented yet", this.streamName);
              
              ds = null;
            }
          }
        }
      }
    }
    r.addTypeID(dataTypeID);
    return ds;
  }
}
