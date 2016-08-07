package com.bloom.runtime.compiler.select;

import com.bloom.runtime.Pair;
import com.bloom.runtime.compiler.CompilerUtils;
import com.bloom.runtime.compiler.exprs.FieldRef;
import com.bloom.runtime.compiler.exprs.ObjFieldRef;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.exprs.VirtFieldRef;
import com.bloom.runtime.meta.MetaInfo.Cache;
import com.bloom.runtime.meta.MetaInfo.WAStoreView;
import com.bloom.runtime.meta.MetaInfo.WActionStore;
import com.bloom.runtime.meta.MetaInfo.Window;
import com.bloom.runtime.utils.NamePolicy;
import com.bloom.uuid.UUID;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.joda.time.DateTime;

public class DataSetBuilder
{
  private static final int ROW_AS_FIELD = -2;
  private final int id;
  private final DataSet.Kind kind;
  private final Class<?> dsType;
  private final String dsName;
  private String fullName;
  private Boolean isStateful;
  private UUID uuid;
  private boolean nested;
  private FieldAccessor fieldAccessor;
  private RowAccessor rowAccessor;
  private IndexInfo indexInfo;
  private IteratorInfo iterInfo;
  private JsonWAStoreInfo jsonWAStoreInfo;
  private TraslatedSchemaInfo transSchemaInfo;
  private FieldFactory fieldFactory;
  private List<Pair<String, Class<?>>> virtFields;
  
  static List<Field> getFields(Class<?> type)
  {
    List<Field> nsfields = CompilerUtils.getNonStaticFields(type);
    List<Field> flds = CompilerUtils.getNotSpecial(nsfields);
    return flds;
  }
  
  static List<RSFieldDesc> makeRSDesc(Class<?> type)
  {
    List<Field> flds = getFields(type);
    List<RSFieldDesc> ret = new ArrayList();
    for (Field f : flds) {
      ret.add(new RSFieldDesc(f.getName(), f.getType()));
    }
    return ret;
  }
  
  public static boolean isRowAsField(VirtFieldRef fld)
  {
    return fld.getIndex() == -2;
  }
  
  public DataSetBuilder(int id, DataSet.Kind kind, String dsName, Class<?> dsType)
  {
    this.id = id;
    this.kind = kind;
    this.dsName = dsName;
    this.dsType = dsType;
  }
  
  DataSetBuilder addRSDescFields(final List<RSFieldDesc> fields)
  {
    this.fieldFactory = new FieldFactory()
    {
      public List<Pair<String, Class<?>>> getAllTypeFieldsImpl()
      {
        List<Pair<String, Class<?>>> ret = new ArrayList();
        for (RSFieldDesc field : fields) {
          ret.add(Pair.make(field.name, field.type));
        }
        return ret;
      }
      
      public List<FieldRef> makeListOfAllFields(ValueExpr obj, FieldAccessor fa)
      {
        List<FieldRef> ret = new ArrayList();
        int i = 0;
        for (RSFieldDesc field : fields)
        {
          VirtFieldRef fld = new VirtFieldRef(obj, field.name, field.type, i, fa);
          ret.add(fld);
          i++;
        }
        return ret;
      }
      
      public FieldRef makeFieldRefImpl(ValueExpr obj, FieldAccessor fa, String fieldName)
        throws NoSuchFieldException, SecurityException
      {
        int i = 0;
        for (RSFieldDesc field : fields)
        {
          if (NamePolicy.isEqual(field.name, fieldName))
          {
            VirtFieldRef fld = new VirtFieldRef(obj, field.name, field.type, i, fa);
            return fld;
          }
          i++;
        }
        throw new NoSuchFieldException(fieldName);
      }
      
      public String toString()
      {
        return "ResultSet" + fields + " field factory";
      }
    };
    return this;
  }
  
  DataSetBuilder addUnstructredFields(final Class<?> dsType, final Class<?> dynFldType)
  {
    this.fieldFactory = new FieldFactory()
    {
      public List<Pair<String, Class<?>>> getAllTypeFieldsImpl()
      {
        return Collections.emptyList();
      }
      
      public List<FieldRef> makeListOfAllFields(ValueExpr obj, FieldAccessor fa)
      {
        FieldRef ref = new VirtFieldRef(obj, DataSetBuilder.this.dsName, dsType, -2, fa);
        return Collections.singletonList(ref);
      }
      
      public FieldRef makeFieldRefImpl(ValueExpr obj, FieldAccessor fa, String fieldName)
        throws NoSuchFieldException, SecurityException
      {
        return new VirtFieldRef(obj, fieldName, dynFldType, 0, fa);
      }
      
      public String toString()
      {
        return "dynamic " + dsType.getCanonicalName() + " field factory";
      }
    };
    return this;
  }
  
  DataSetBuilder addJavatypeFields(final Class<?> dsType)
  {
    this.fieldFactory = new FieldFactory()
    {
      public List<Pair<String, Class<?>>> getAllTypeFieldsImpl()
      {
        List<Pair<String, Class<?>>> ret = new ArrayList();
        for (Field field : dsType.getFields()) {
          ret.add(Pair.make(field.getName(), field.getType()));
        }
        return ret;
      }
      
      public List<FieldRef> makeListOfAllFields(ValueExpr obj, FieldAccessor fa)
      {
        List<FieldRef> fields = new ArrayList();
        for (Field f : DataSetBuilder.getFields(dsType))
        {
          FieldRef r = new ObjFieldRef(obj, f.getName(), f);
          fields.add(r);
        }
        return fields;
      }
      
      public FieldRef makeFieldRefImpl(ValueExpr obj, FieldAccessor fa, String fieldName)
        throws NoSuchFieldException, SecurityException
      {
        Field field = NamePolicy.getField(dsType, fieldName);
        return new ObjFieldRef(obj, fieldName, field);
      }
      
      public String toString()
      {
        return "java type " + dsType.getCanonicalName() + " field factory";
      }
    };
    this.fieldAccessor = new FieldAccessor()
    {
      public String genFieldAccessor(String obj, VirtFieldRef f)
      {
        throw new UnsupportedOperationException();
      }
      
      public String toString()
      {
        return "java object field accessor";
      }
    };
    return this;
  }
  
  DataSetBuilder addWindowIndexInfo(final MetaInfo.Window wi)
  {
    this.indexInfo = new IndexInfo()
    {
      public List<Integer> getListOfIndexesForFields(Set<String> set)
      {
        if (!wi.partitioningFields.isEmpty())
        {
          boolean windowPartitionKeyInCond = true;
          for (String fieldName : wi.partitioningFields) {
            if (!set.contains(fieldName))
            {
              windowPartitionKeyInCond = false;
              break;
            }
          }
          if (windowPartitionKeyInCond) {
            return Collections.singletonList(Integer.valueOf(0));
          }
        }
        return Collections.emptyList();
      }
      
      public List<String> indexFieldList(int idxID)
      {
        if (idxID == 0)
        {
          assert (!wi.partitioningFields.isEmpty());
          return wi.partitioningFields;
        }
        return Collections.emptyList();
      }
      
      public String toString()
      {
        return "window " + wi.name + " index info";
      }
    };
    return this;
  }
  
  DataSetBuilder addCacheIndexInfo(final MetaInfo.Cache ci)
  {
    this.indexInfo = new IndexInfo()
    {
      public List<Integer> getListOfIndexesForFields(Set<String> set)
      {
        Object o = ci.query_properties.get("keytomap");
        if ((o instanceof String))
        {
          String keyfield = (String)o;
          if (set.contains(keyfield)) {
            return Collections.singletonList(Integer.valueOf(0));
          }
        }
        return Collections.emptyList();
      }
      
      public List<String> indexFieldList(int idxID)
      {
        if (idxID == 0)
        {
          Object o = ci.query_properties.get("keytomap");
          if ((o instanceof String))
          {
            String keyfield = (String)o;
            return Collections.singletonList(keyfield);
          }
        }
        return Collections.emptyList();
      }
      
      public String toString()
      {
        return "cache " + ci.name + " index info";
      }
    };
    return this;
  }
  
  DataSetBuilder addSchemaTranslation(final UUID expectedTypeID)
  {
    this.transSchemaInfo = new TraslatedSchemaInfo()
    {
      public UUID expectedTypeID()
      {
        return expectedTypeID;
      }
      
      public String toString()
      {
        return "expected type uuid " + expectedTypeID;
      }
    };
    this.rowAccessor = new RowAccessor()
    {
      public String genRowAccessor(String obj)
      {
        return "getRowDataAndTranslate(" + obj + ", " + DataSetBuilder.this.id + ")";
      }
      
      public String toString()
      {
        return "getRowDataAndTranslate(" + DataSetBuilder.this.id + ") row accessor";
      }
    };
    this.fieldAccessor = new FieldAccessor()
    {
      public String genFieldAccessor(String obj, VirtFieldRef f)
      {
        return "getDynamicField(" + obj + "," + f.getIndex() + "," + DataSetBuilder.this.id + ")";
      }
      
      public String toString()
      {
        return "getDynamicField(" + DataSetBuilder.this.id + ") field accessor";
      }
    };
    return this;
  }
  
  DataSetBuilder addIteratorInfo(final DataSet parent, final FieldRef parentField)
  {
    this.fullName = (parent.getName() + "." + parentField.getName());
    this.isStateful = Boolean.valueOf(true);
    this.uuid = null;
    this.nested = true;
    this.iterInfo = new IteratorInfo()
    {
      public DataSet getParent()
      {
        return parent;
      }
      
      public FieldRef getIterableField()
      {
        return parentField;
      }
      
      public String toString()
      {
        return "Iterator(" + parent.getFullName() + ", " + parentField.getName() + ")";
      }
    };
    return this;
  }
  
  DataSetBuilder addJsonWAStoreInfo(final MetaInfo.WAStoreView view, final MetaInfo.WActionStore store)
  {
    this.jsonWAStoreInfo = new JsonWAStoreInfo()
    {
      public MetaInfo.WAStoreView getStoreView()
      {
        return view;
      }
      
      public MetaInfo.WActionStore getStore()
      {
        return store;
      }
      
      public String toString()
      {
        return "json wastore " + store.name + " info";
      }
    };
    this.fieldAccessor = new FieldAccessor()
    {
      public String genFieldAccessor(String obj, VirtFieldRef f)
      {
        return "dynamicCast(" + obj + ".getData(), \"" + f.getName() + "\", " + f.getTypeName() + ".class)";
      }
      
      public String toString()
      {
        return "JsonNodeEvent.getData() json dynamicCast(" + DataSetBuilder.this.id + ") field accessor";
      }
    };
    addVirtualField("$timestamp", DateTime.class);
    
    addVirtualField("$id", UUID.class);
    
    return this;
  }
  
  DataSetBuilder addVirtualField(String name, Class<?> type)
  {
    if (this.virtFields == null) {
      this.virtFields = new ArrayList();
    }
    this.virtFields.add(Pair.make(name, type));
    return this;
  }
  
  DataSetBuilder addStateful_UUID_FullName(boolean isStateful, UUID uuid, String fullName)
  {
    this.isStateful = Boolean.valueOf(isStateful);
    this.uuid = uuid;
    this.fullName = fullName;
    return this;
  }
  
  DataSetBuilder addPayloadFieldAccessor()
  {
    this.fieldAccessor = new FieldAccessor()
    {
      public String genFieldAccessor(String obj, VirtFieldRef f)
      {
        return obj + ".payload[" + f.getIndex() + "]";
      }
      
      public String toString()
      {
        return "payload[] field accessor";
      }
    };
    return this;
  }
  
  DataSetBuilder addJavaReflectFieldAccessor()
  {
    this.fieldAccessor = new FieldAccessor()
    {
      public String genFieldAccessor(String obj, VirtFieldRef f)
      {
        if (DataSetBuilder.isRowAsField(f)) {
          return obj;
        }
        return "getJavaObjectField(" + obj + ", \"" + f.getName() + "\")";
      }
      
      public String toString()
      {
        return "java getJavaObjectField() field accessor";
      }
    };
    return this;
  }
  
  DataSetBuilder addJSONFieldAccessor()
  {
    this.fieldAccessor = new FieldAccessor()
    {
      public String genFieldAccessor(String obj, VirtFieldRef f)
      {
        if (DataSetBuilder.isRowAsField(f)) {
          return obj;
        }
        return "getJsonObjectField(" + obj + ", \"" + f.getName() + "\", " + f.getTypeName() + ".class)";
      }
      
      public String toString()
      {
        return "json getJsonObjectField() field accessor";
      }
    };
    return this;
  }
  
  DataSetBuilder addWAContext2RowAccessor()
  {
    this.rowAccessor = new RowAccessor()
    {
      public String genRowAccessor(String obj)
      {
        return "getWARowContext(" + obj + ", " + DataSetBuilder.this.dsType.getCanonicalName() + ".class)";
      }
      
      public String toString()
      {
        return "getWARowContext(" + DataSetBuilder.this.id + ") row accessor";
      }
    };
    return this;
  }
  
  DataSet build()
  {
    if (this.rowAccessor == null) {
      this.rowAccessor = new RowAccessor()
      {
        public String genRowAccessor(String obj)
        {
          return obj;
        }
        
        public String toString()
        {
          return "getRowData(" + DataSetBuilder.this.id + ") row accessor";
        }
      };
    }
    if ((this.fullName == null) || (this.fieldAccessor == null) || (this.isStateful == null) || (this.fieldFactory == null) || ((!this.nested) && (this.uuid == null))) {
      throw new RuntimeException("Not all parameters set to build DataSet");
    }
    return new DataSet(this.id, this.dsName, this.dsType, this.kind, this.fullName, this.isStateful.booleanValue(), this.uuid, this.fieldAccessor, this.rowAccessor, this.fieldFactory, this.virtFields, this.indexInfo, this.iterInfo, this.transSchemaInfo, this.jsonWAStoreInfo);
  }
}
