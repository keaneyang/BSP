package com.bloom.wactionstore.elasticsearch;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.persistence.WactionStore;
import com.bloom.wactionstore.Utility;
import com.bloom.wactionstore.base.WActionStoreBase;
import com.bloom.wactionstore.constants.NameType;
import com.bloom.wactionstore.exceptions.WActionStoreException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.TypeMissingException;

@PropertyTemplate(name="elasticsearch", type=AdapterType.wactionstore, properties={@com.bloom.anno.PropertyTemplateProperty(name="storageProvider", type=String.class, required=true, defaultValue="elasticsearch"), @com.bloom.anno.PropertyTemplateProperty(name="elasticsearch.cluster_name", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="elasticsearch.data_path", type=String.class, required=false, defaultValue="./data"), @com.bloom.anno.PropertyTemplateProperty(name="elasticsearch.replicas", type=Integer.class, required=false, defaultValue="1"), @com.bloom.anno.PropertyTemplateProperty(name="elasticsearch.shards", type=Integer.class, required=false, defaultValue="5"), @com.bloom.anno.PropertyTemplateProperty(name="elasticsearch.storage_type", type=String.class, required=false, defaultValue="any"), @com.bloom.anno.PropertyTemplateProperty(name="elasticsearch.time_to_live", type=String.class, required=false, defaultValue="")})
public class WActionStore
  extends WActionStoreBase<WActionStoreManager>
{
  private static final Class<WActionStore> thisClass = WActionStore.class;
  private static final Logger logger = Logger.getLogger(thisClass);
  private static final String USE_WACTIONSTORE_PROPERTY = "PROPERTY:";
  private String dataTypeName;
  private JsonNode dataTypeSchema;
  private static final String[][] ES_METADATA_FIELDS = { { "id", "_id", "index", "not_analyzed", "path", "$id" }, { "timestamp", "_timestamp", "store", "true", "enabled", "true", "path", "$timestamp" }, { "checkpoint", "$checkpoint" }, { "any", "_all" }, { "ttl", "_ttl", "enabled", "true", "default", "PROPERTY:elasticsearch.time_to_live" } };
  private static final String[][] ES_METADATA_FIELDS_DISABLED = { { "any", "_all", "enabled", "false" } };
  private final String actualName;
  private String indexName;
  
  WActionStore(WActionStoreManager manager, String wActionStoreName, Map<String, Object> properties, String indexName)
  {
    super(manager, wActionStoreName, properties);
    this.actualName = manager.translateName(NameType.WACTIONSTORE, wActionStoreName);
    this.indexName = indexName;
  }
  
  public void setIndexName(String indexName)
  {
    this.indexName = indexName;
  }
  
  private static Map<String, Object> getSourceMapFromSchema(String wActionStoreName, JsonNode wActionStoreSchema, Map<String, Object> properties)
  {
    Map<String, Object> mapping = new HashMap(1);
    
    JsonNode context = wActionStoreSchema.get("context");
    Map<String, Object> contextMapping = getContextMapping(context);
    
    JsonNode eventTypes = wActionStoreSchema.get("events");
    if (eventTypes != null) {
      addEventTypeMapping(contextMapping, eventTypes);
    }
    JsonNode metadata = wActionStoreSchema.get("metadata");
    addCheckpointMapping(metadata, contextMapping);
    mapping.put("properties", contextMapping);
    if (metadata != null) {
      addMetadataMapping(mapping, metadata, properties);
    }
    addDisabledMetadataMapping(mapping, metadata);
    
    Map<String, Object> sourceMap = new HashMap(1);
    sourceMap.put(wActionStoreName, mapping);
    return sourceMap;
  }
  
  private static void addCheckpointMapping(JsonNode metadata, Map<String, Object> properties)
  {
    if ((metadata != null) && (metadata.isArray())) {
      for (JsonNode item : metadata) {
        if ("checkpoint".equals(item.asText()))
        {
          Map<String, Object> columnProperties = new HashMap(1);
          String columnType = "long";
          addColumnProperties(columnType, columnProperties, null);
          String columnName = "$checkpoint";
          properties.put(columnName, columnProperties);
          return;
        }
      }
    }
  }
  
  private static void addEventTypeMapping(Map<String, Object> properties, JsonNode eventTypes)
  {
    for (JsonNode eventType : eventTypes)
    {
      String eventName = eventType.get("name").asText();
      JsonNode eventAttributes = eventType.get("type");
      Map<String, Object> eventProperties = getContextMapping(eventAttributes);
      
      Map<String, Object> eventMapping = new HashMap(1);
      eventMapping.put("type", "nested");
      eventMapping.put("properties", eventProperties);
      
      properties.put(eventName, eventMapping);
    }
  }
  
  private static Map<String, Object> getContextMapping(JsonNode context)
  {
    Map<String, Object> properties = new HashMap(context.size());
    for (JsonNode contextItem : context)
    {
      String columnName = contextItem.get("name").asText();
      String columnType = contextItem.get("type").asText();
      
      JsonNode indexNode = contextItem.get("index");
      
      Map<String, Object> columnProperties = new HashMap(1);
      addColumnProperties(columnType, columnProperties, indexNode);
      properties.put(columnName, columnProperties);
    }
    return properties;
  }
  
  private static void addMetadataMapping(Map<String, Object> mapping, JsonNode metadataItems, Map<String, Object> properties)
  {
    for (JsonNode metadataItem : metadataItems)
    {
      String metadataItemName = metadataItem.asText();
      String[] metadataFields = getMetadataFields(metadataItemName);
      if (metadataFields.length > 2)
      {
        Map<String, Object> metadataProperties = new HashMap((metadataFields.length - 2) / 2);
        for (int index = 2; index < metadataFields.length; index += 2)
        {
          String metadataName = metadataFields[index];
          String metadataValue = metadataFields[(index + 1)];
          addMetadataValue(metadataProperties, metadataName, metadataValue, properties);
        }
        mapping.put(metadataFields[1], metadataProperties);
      }
    }
  }
  
  private static void addMetadataValue(Map<String, Object> metadataProperties, String metadataName, String metadataValue, Map<String, Object> properties)
  {
    if (metadataValue.startsWith("PROPERTY:"))
    {
      String propertyName = metadataValue.substring("PROPERTY:".length());
      String propertyValue = Utility.getPropertyString(properties, propertyName);
      if (propertyValue != null) {
        metadataProperties.put(metadataName, propertyValue);
      }
    }
    else
    {
      metadataProperties.put(metadataName, metadataValue);
    }
  }
  
  private static void addDisabledMetadataMapping(Map<String, Object> mapping, JsonNode metadataItems)
  {
    Collection<String> metadataList = new ArrayList();
    if (metadataItems != null) {
      for (JsonNode metadataItem : metadataItems)
      {
        String metadataItemName = metadataItem.asText();
        metadataList.add(metadataItemName);
      }
    }
    for (String[] metadataFields : ES_METADATA_FIELDS_DISABLED) {
      if (!metadataList.contains(metadataFields[0]))
      {
        Map<String, Object> metadataProperties = new HashMap((metadataFields.length - 2) / 2);
        for (int index = 2; index < metadataFields.length; index += 2) {
          metadataProperties.put(metadataFields[index], metadataFields[(index + 1)]);
        }
        mapping.put(metadataFields[1], metadataProperties);
      }
    }
  }
  
  private static String[] getMetadataFields(String metadataItemName)
  {
    for (String[] metadataFields : ES_METADATA_FIELDS) {
      if (metadataFields[0].equals(metadataItemName)) {
        return metadataFields;
      }
    }
    throw new WActionStoreException(String.format("Invalid metadata item name, '%s'", new Object[] { metadataItemName }));
  }
  
  private static void addColumnProperties(String columnType, Map<String, Object> columnProperties, JsonNode indexNode)
  {
    switch (columnType)
    {
    case "string": 
      columnProperties.put("type", "string");
      columnProperties.put("index", "not_analyzed");
      break;
    case "text": 
      columnProperties.put("type", "string");
      break;
    case "binary64": 
      columnProperties.put("type", "binary");
      break;
    case "boolean": 
      columnProperties.put("type", "boolean");
      break;
    case "integer": 
      columnProperties.put("type", "integer");
      break;
    case "long": 
      columnProperties.put("type", "long");
      break;
    case "double": 
      columnProperties.put("type", "double");
      break;
    case "date": 
      columnProperties.put("type", "date");
      columnProperties.put("format", "basic_date");
      break;
    case "time": 
      columnProperties.put("type", "date");
      columnProperties.put("format", "basic_time");
      break;
    case "datetime": 
      columnProperties.put("type", "date");
      columnProperties.put("format", "basic_date_time");
      break;
    default: 
      throw new WActionStoreException(String.format("Unable to map WAction data type '%s'", new Object[] { columnType }));
    }
    if (indexNode != null) {
      columnProperties.put("index", indexNode.asText());
    }
  }
  
  private static JsonNode getSchemaFromMapping(MappingMetaData mapping)
  {
    ObjectNode result = null;
    Map<String, Object> sourceMap = getSourceMapFromMapping(mapping);
    Object properties = sourceMap != null ? sourceMap.get("properties") : null;
    if ((properties instanceof Map))
    {
      Map<String, Object> propertyMap = (Map)properties;
      ArrayNode context = Utility.nodeFactory.arrayNode();
      ArrayNode metadata = Utility.nodeFactory.arrayNode();
      for (Map.Entry<String, Object> property : propertyMap.entrySet())
      {
        String name = (String)property.getKey();
        Object value = property.getValue();
        String type = getTypeName(value);
        
        String metadataName = getMetadataName(name);
        if (metadataName == null)
        {
          ObjectNode contextItem = Utility.nodeFactory.objectNode();
          contextItem.put("name", name);
          contextItem.put("type", type);
          context.add(contextItem);
        }
        else
        {
          metadata.add(metadataName);
        }
      }
      for (String[] metadataFields : ES_METADATA_FIELDS) {
        if ((metadataFields.length > 2) && (sourceMap.get(metadataFields[1]) != null)) {
          metadata.add(metadataFields[0]);
        }
      }
      if (hasMetadataAll(sourceMap)) {
        metadata.add("any");
      }
      result = Utility.nodeFactory.objectNode();
      result.set("context", context);
      if (metadata.size() > 0) {
        result.set("metadata", metadata);
      }
    }
    return result;
  }
  
  private static String getMetadataName(String propertyName)
  {
    String result = null;
    for (String[] metadataFields : ES_METADATA_FIELDS) {
      if (metadataFields[1].equals(propertyName))
      {
        result = metadataFields[0];
        break;
      }
    }
    return result;
  }
  
  private static boolean hasMetadataAll(Map<String, Object> sourceMap)
  {
    Object all = sourceMap.get("_all");
    if ((all instanceof Map))
    {
      Object allEnabled = ((Map)all).get("enabled");
      if ((allEnabled instanceof Boolean)) {
        return ((Boolean)allEnabled).booleanValue();
      }
    }
    return true;
  }
  
  private static String getTypeName(Object value)
  {
    if (!(value instanceof Map)) {
      throw new WActionStoreException(String.format("Unexpected object type: '%s'", new Object[] { value.getClass().getSimpleName() }));
    }
    Map<String, Object> typeMap = (Map)value;
    String nativeType = (String)typeMap.get("type");
    String result;
    switch (nativeType)
    {
    case "string": 
      String index = (String)typeMap.get("index");
      result = "not_analyzed".equals(index) ? "string" : "text";
      break;
    case "binary": 
      result = "binary64";
      break;
    case "boolean": 
      result = "boolean";
      break;
    case "integer": 
      result = "integer";
      break;
    case "long": 
      result = "long";
      break;
    case "double": 
      result = "double";
      break;
    case "date": 
      result = getDateTypeName(typeMap);
      break;
    default: 
      throw new WActionStoreException(String.format("Cannot map data type '%s'", new Object[] { nativeType }));
    }
    return result;
  }
  
  private static String getDateTypeName(Map<String, Object> typeMap)
  {
    String format = (String)typeMap.get("format");
    String result;
    switch (format)
    {
    case "basic_date": 
      result = "date";
      break;
    case "basic_time": 
      result = "time";
      break;
    case "basic_date_time": 
      result = "datetime";
      break;
    default: 
      throw new WActionStoreException(String.format("Cannot map date format '%s'", new Object[] { format }));
    }
    return result;
  }
  
  private static Map<String, Object> getSourceMapFromMapping(MappingMetaData mapping)
  {
    Map<String, Object> result = null;
    if (mapping != null) {
      try
      {
        result = mapping.sourceAsMap();
      }
      catch (IOException exception)
      {
        logger.error("Unable to create a source map", exception);
      }
    }
    return result;
  }
  
  public Iterator<String> getNames()
  {
    ImmutableOpenMap<String, MappingMetaData> mappings = getMappings(null);
    if (mappings != null) {
      return mappings.keysIt();
    }
    return Collections.emptyIterator();
  }
  
  public DataType get(String dataTypeName, Map<String, Object> properties)
  {
    DataType result = null;
    MappingMetaData mapping = getMapping(dataTypeName);
    JsonNode wActionStoreSchema = getSchemaFromMapping(mapping);
    if (wActionStoreSchema != null)
    {
      DataType wActionStore = new DataType(this, dataTypeName, wActionStoreSchema, null);
      if (wActionStore.isValid()) {
        result = wActionStore;
      }
    }
    return result;
  }
  
  public boolean remove(String dataTypeName)
  {
    boolean result = false;
    Client client = getClient();
    try
    {
      if (client != null)
      {
        DeleteMappingRequestBuilder request = client.admin().indices().prepareDeleteMapping(new String[] { this.indexName }).setType(new String[] { dataTypeName });
        
        DeleteMappingResponse response = (DeleteMappingResponse)request.execute().actionGet();
        result = response.isAcknowledged();
        fsync();
      }
    }
    catch (TypeMissingException ignored)
    {
      logger.warn(String.format("Context type '%s' does not exist in WActionStore '%s'", new Object[] { dataTypeName, getName() }));
    }
    return result;
  }
  
  public DataType createDataType(String dataTypeName, JsonNode dataTypeSchema)
  {
    DataType result = null;
    if (getMapping(dataTypeName) == null) {
      result = setDataType(dataTypeName, dataTypeSchema, null);
    } else {
      logger.warn(String.format("Context type '%s' already exists in WActionStore '%s'", new Object[] { dataTypeName, getName() }));
    }
    return result;
  }
  
  public DataType setDataType(String dataTypeName, JsonNode dataTypeSchema, WactionStore ws)
  {
    DataType result = null;
    this.dataTypeName = dataTypeName;
    this.dataTypeSchema = dataTypeSchema;
    DataType wActionStore = new DataType(this, dataTypeName, dataTypeSchema, ws);
    Map<String, Object> sourceMap = wActionStore.isValid() ? getSourceMapFromSchema(dataTypeName, dataTypeSchema, getProperties()) : null;
    Client client = getClient();
    if ((sourceMap != null) && (client != null))
    {
      logger.debug(String.format("Context type '%s' has mapping '%s'", new Object[] { dataTypeName, sourceMap }));
      PutMappingRequestBuilder request = client.admin().indices().preparePutMapping(new String[0]).setIndices(new String[] { this.indexName }).setType(dataTypeName).setSource(sourceMap);
      
      request.execute().actionGet();
      logger.debug(String.format("Context type '%s' created mapping in WActionStore '%s'", new Object[] { dataTypeName, getName() }));
      result = wActionStore;
    }
    return result;
  }
  
  public void flush()
  {
    Client client = getClient();
    if (client != null)
    {
      RefreshRequest request = new RefreshRequest(new String[] { this.indexName });
      client.admin().indices().refresh(request).actionGet();
      logger.debug(String.format("Flushed WActionStore '%s' to memory", new Object[] { getName() }));
    }
  }
  
  public void fsync()
  {
    Client client = getClient();
    if (client != null)
    {
      FlushRequest request = new FlushRequest(new String[] { this.indexName });
      client.admin().indices().flush(request).actionGet();
      logger.debug(String.format("Flushed WActionStore '%s' to disk", new Object[] { getName() }));
    }
  }
  
  public long getWActionCount()
  {
    long result = 0L;
    Client client = getClient();
    try
    {
      CountRequestBuilder requestBuilder = client.prepareCount(new String[] { this.indexName });
      CountResponse response = (CountResponse)requestBuilder.execute().actionGet();
      result = response.getCount();
      logger.debug(String.format("WActionStore '%s' contains %d WActions", new Object[] { getName(), Long.valueOf(result) }));
    }
    catch (IndexMissingException ignored)
    {
      logger.warn(String.format("WActionStore '%s' does not exist", new Object[] { getName() }));
    }
    return result;
  }
  
  private ImmutableOpenMap<String, MappingMetaData> getMappings(String dataTypeName)
  {
    ImmutableOpenMap<String, MappingMetaData> result = null;
    Client client = getClient();
    try
    {
      if (client != null)
      {
        GetMappingsRequestBuilder request = (GetMappingsRequestBuilder)client.admin().indices().prepareGetMappings(new String[0]).setIndices(new String[] { this.indexName });
        if (dataTypeName != null) {
          request = (GetMappingsRequestBuilder)request.setTypes(new String[] { dataTypeName });
        }
        GetMappingsResponse response = (GetMappingsResponse)request.execute().actionGet();
        result = (ImmutableOpenMap)response.getMappings().get(this.indexName);
      }
    }
    catch (IndexMissingException ignored)
    {
      logger.warn(String.format("WActionStore '%s' does not exist", new Object[] { getName() }));
    }
    return result;
  }
  
  private MappingMetaData getMapping(String dataTypeName)
  {
    MappingMetaData result = null;
    ImmutableOpenMap<String, MappingMetaData> mappings = getMappings(dataTypeName);
    if (mappings != null) {
      result = (MappingMetaData)mappings.get(dataTypeName);
    }
    return result;
  }
  
  public String getActualName()
  {
    return this.actualName;
  }
  
  public String getIndexName()
  {
    return this.indexName;
  }
  
  public String getDataTypeName()
  {
    return this.dataTypeName;
  }
  
  public JsonNode getDataTypeSchema()
  {
    return this.dataTypeSchema;
  }
  
  private Client getClient()
  {
    Client result = null;
    WActionStoreManager manager = (WActionStoreManager)getManager();
    if (manager != null) {
      result = manager.getClient();
    }
    return result;
  }
}
