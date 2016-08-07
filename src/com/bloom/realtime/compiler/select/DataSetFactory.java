package com.bloom.runtime.compiler.select;

import com.bloom.proc.events.JsonNodeEvent;
import com.bloom.runtime.compiler.exprs.FieldRef;
import com.bloom.runtime.containers.DynamicEventWrapper;
import com.bloom.runtime.meta.MetaInfo.Cache;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.meta.MetaInfo.StreamGenerator;
import com.bloom.runtime.meta.MetaInfo.WAStoreView;
import com.bloom.runtime.meta.MetaInfo.WActionStore;
import com.bloom.runtime.meta.MetaInfo.Window;
import com.bloom.uuid.UUID;
import com.fasterxml.jackson.databind.JsonNode;
import com.bloom.event.SimpleEvent;

import java.util.List;

public class DataSetFactory
{
  private static String makeName(String name, String alias)
  {
    return alias == null ? name : alias;
  }
  
  private static DataSetBuilder createJavaObjectDS(int id, DataSet.Kind kind, String name, String alias, Class<?> type, MetaInfo.MetaObject si, boolean isStateful)
  {
    return new DataSetBuilder(id, kind, makeName(name, alias), type).addJavatypeFields(type).addStateful_UUID_FullName(isStateful, si.uuid, si.getFullName());
  }
  
  public static DataSet createWindowDS(int id, String name, String alias, Class<?> type, MetaInfo.Window wi)
  {
    return createJavaObjectDS(id, DataSet.Kind.WINDOW_JAVA_OBJECTS, name, alias, type, wi, true).addWindowIndexInfo(wi).build();
  }
  
  public static DataSet createCacheDS(int id, String name, String alias, Class<?> type, MetaInfo.Cache ci)
  {
    return createJavaObjectDS(id, DataSet.Kind.CACHE_JAVA_OBJECTS, name, alias, type, ci, true).addCacheIndexInfo(ci).build();
  }
  
  public static DataSet createStreamDS(int id, String name, String alias, Class<?> type, MetaInfo.Stream si)
  {
    return createJavaObjectDS(id, DataSet.Kind.STREAM_JAVA_OBJECTS, name, alias, type, si, false).build();
  }
  
  public static DataSet createStreamFunctionDS(int id, String name, Class<?> type, MetaInfo.StreamGenerator gi)
  {
    return createJavaObjectDS(id, DataSet.Kind.STREAM_GENERATOR, name, name, type, gi, false).build();
  }
  
  public static DataSet createSelectViewDS(int id, String alias, UUID selectId, List<RSFieldDesc> resultSetDesc, boolean isStateful)
  {
    return new DataSetBuilder(id, DataSet.Kind.SELECT_VIEW, alias, SimpleEvent.class).addRSDescFields(resultSetDesc).addStateful_UUID_FullName(isStateful, selectId, alias).addPayloadFieldAccessor().build();
  }
  
  public static DataSet createTranslatedDS(int id, String name, String alias, List<RSFieldDesc> resultSetDesc, UUID expectedTypeID, MetaInfo.MetaObject si)
  {
    return new DataSetBuilder(id, DataSet.Kind.TRANSLATED_DYNAMIC, makeName(name, alias), DynamicEventWrapper.class).addRSDescFields(resultSetDesc).addStateful_UUID_FullName(false, si.uuid, si.getFullName()).addSchemaTranslation(expectedTypeID).build();
  }
  
  public static DataSet createJSONWAStoreDS(int id, String name, String alias, List<RSFieldDesc> fields, MetaInfo.WAStoreView view, MetaInfo.WActionStore store)
  {
    return new DataSetBuilder(id, DataSet.Kind.WASTORE_JSON_TYPED, makeName(name, alias), JsonNodeEvent.class).addRSDescFields(fields).addStateful_UUID_FullName(true, view.uuid, store.getFullName()).addJsonWAStoreInfo(view, store).build();
  }
  
  public static DataSet createWAStoreDS(int id, String name, String alias, Class<?> type, MetaInfo.WAStoreView view, MetaInfo.WActionStore store)
  {
    if (store.usesOldWActionStore()) {
      return createJavaObjectDS(id, DataSet.Kind.WASTORE_JAVA_OBJECTS, name, alias, type, view, true).addWAContext2RowAccessor().build();
    }
    return createJSONWAStoreDS(id, name, alias, DataSetBuilder.makeRSDesc(type), view, store);
  }
  
  public static DataSet createNestedJavaReflect(int id, DataSet parent, FieldRef parentField, String alias)
  {
    return new DataSetBuilder(id, DataSet.Kind.NESTED_JAVA_UNSTRUCT, alias, Object.class).addIteratorInfo(parent, parentField).addUnstructredFields(Object.class, Object.class).addJavaReflectFieldAccessor().build();
  }
  
  public static DataSet createNestedJsonUnstruct(int id, DataSet parent, FieldRef parentField, String alias)
  {
    return new DataSetBuilder(id, DataSet.Kind.NESTED_JSON_UNSTRUCT, alias, JsonNode.class).addIteratorInfo(parent, parentField).addUnstructredFields(JsonNode.class, JsonNode.class).addJSONFieldAccessor().build();
  }
  
  public static DataSet createNestedJavaObjects(int id, DataSet parent, FieldRef parentField, String alias, Class<?> itemType)
  {
    return new DataSetBuilder(id, DataSet.Kind.NESTED_JAVA_OBJECTS, alias, itemType).addIteratorInfo(parent, parentField).addJavatypeFields(itemType).build();
  }
  
  public static DataSet createNestedJsonTyped(int id, DataSet parent, FieldRef parentField, String alias, List<RSFieldDesc> fields)
  {
    return new DataSetBuilder(id, DataSet.Kind.NESTED_JSON_TYPED, alias, JsonNode.class).addIteratorInfo(parent, parentField).addRSDescFields(fields).addJSONFieldAccessor().build();
  }
}
