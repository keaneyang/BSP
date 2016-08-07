package com.bloom.runtime.compiler.select;

import com.bloom.runtime.Pair;
import com.bloom.runtime.compiler.exprs.DataSetRef;
import com.bloom.runtime.compiler.exprs.FieldRef;
import com.bloom.uuid.UUID;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class DataSet
  extends RowSet
{
  private final int id;
  private final String name;
  private final Kind kind;
  private final String fullName;
  private final boolean isStateful;
  private final UUID uuid;
  private final IndexInfo indexInfo;
  private final IteratorInfo iterInfo;
  private final TraslatedSchemaInfo transSchemaInfo;
  private final JsonWAStoreInfo jsonWAStoreInfo;
  
  public static enum Kind
  {
    WINDOW_JAVA_OBJECTS,  CACHE_JAVA_OBJECTS,  STREAM_JAVA_OBJECTS,  STREAM_GENERATOR,  SELECT_VIEW,  TRANSLATED_DYNAMIC,  WASTORE_JSON_TYPED,  WASTORE_JAVA_OBJECTS,  NESTED_JAVA_UNSTRUCT,  NESTED_JSON_UNSTRUCT,  NESTED_JAVA_OBJECTS,  NESTED_JSON_TYPED;
    
    private Kind() {}
  }
  
  protected DataSet(int id, String name, Class<?> type, Kind kind, String fullName, boolean isStateful, UUID uuid, FieldAccessor fieldAccessor, RowAccessor rowAccessor, FieldFactory fieldFactory, List<Pair<String, Class<?>>> virtFields, IndexInfo indexInfo, IteratorInfo iterInfo, TraslatedSchemaInfo transSchemaInfo, JsonWAStoreInfo jsonWAStoreInfo)
  {
    super(type, fieldAccessor, rowAccessor, fieldFactory, virtFields);
    this.id = id;
    this.name = name;
    this.kind = kind;
    this.fullName = fullName;
    this.isStateful = isStateful;
    this.uuid = uuid;
    this.indexInfo = indexInfo;
    this.iterInfo = iterInfo;
    this.transSchemaInfo = transSchemaInfo;
    this.jsonWAStoreInfo = jsonWAStoreInfo;
  }
  
  public final int getID()
  {
    return this.id;
  }
  
  public final String getName()
  {
    return this.name;
  }
  
  public final Kind getKind()
  {
    return this.kind;
  }
  
  public final BitSet id2bitset()
  {
    BitSet bs = new BitSet();
    bs.set(this.id);
    return bs;
  }
  
  public final DataSetRef makeRef()
  {
    return DataSetRef.makeDataSetRef(this);
  }
  
  public String toString()
  {
    return "DataSet<" + this.kind + ">(" + this.id + ", " + getFullName() + ", alias " + this.name + ", " + rsToString() + ")";
  }
  
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DataSet)) {
      return false;
    }
    DataSet other = (DataSet)o;
    return this.id == other.id;
  }
  
  public int hashCode()
  {
    return this.id;
  }
  
  public boolean isStateful()
  {
    return this.isStateful;
  }
  
  public UUID getUUID()
  {
    if (this.uuid == null) {
      throw new UnsupportedOperationException();
    }
    return this.uuid;
  }
  
  public String getFullName()
  {
    return this.fullName;
  }
  
  public TraslatedSchemaInfo isTranslated()
  {
    return this.transSchemaInfo;
  }
  
  public IteratorInfo isIterator()
  {
    return this.iterInfo;
  }
  
  public JsonWAStoreInfo isJsonWAStore()
  {
    return this.jsonWAStoreInfo;
  }
  
  public List<FieldRef> makeListOfAllFields()
  {
    return makeListOfAllFields(makeRef());
  }
  
  public List<Integer> getListOfIndexesForFields(Set<String> set)
  {
    return this.indexInfo == null ? Collections.emptyList() : this.indexInfo.getListOfIndexesForFields(set);
  }
  
  public List<String> indexFieldList(int idxID)
  {
    return this.indexInfo == null ? Collections.emptyList() : this.indexInfo.indexFieldList(idxID);
  }
}
