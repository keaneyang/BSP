package com.bloom.wactionstore;

import com.bloom.wactionstore.exceptions.WActionStoreException;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;

public enum InternalType
{
  CHECKPOINT("$Internal.CHECKPOINT", "{  \"context\": [     { \"name\": \"$id\", \"type\": \"string\", \"nullable\": false },    { \"name\": \"$timestamp\", \"type\": \"datetime\", \"nullable\": false },    { \"name\": \"NodeName\", \"type\": \"string\", \"nullable\": false },    { \"name\": \"WActionStoreName\", \"type\": \"string\", \"nullable\": false },    { \"name\": \"WActionCount\", \"type\": \"long\", \"nullable\": false },    { \"name\": \"PathCount\", \"type\": \"long\", \"nullable\": false },    { \"name\": \"Paths\", \"type\": \"binary64\", \"nullable\": false }  ],  \"metadata\": [ \"id\", \"timestamp\" ]}"),  MONITORING("$Internal.MONITORING", "{  \"context\": [     { \"name\": \"serverID\", \"type\": \"string\", \"nullable\": false , \"index\": \"not_analyzed\" },    { \"name\": \"entityID\", \"type\": \"string\", \"nullable\": false , \"index\": \"not_analyzed\" },    { \"name\": \"valueLong\", \"type\": \"long\", \"nullable\": true , \"index\": \"no\" },    { \"name\": \"valueString\", \"type\": \"string\", \"nullable\": true , \"index\": \"no\" },    { \"name\": \"timeStamp\", \"type\": \"long\", \"nullable\": false , \"index\": \"not_analyzed\" },    { \"name\": \"type\", \"type\": \"string\", \"nullable\": false , \"index\": \"no\" }  ],  \"metadata\": [ \"id\"  , \"timestamp\"  , \"ttl\" ]}"),  HEALTH("$Internal.HEALTH", "{  \"context\": [     { \"name\": \"id\", \"type\": \"string\", \"nullable\": false , \"index\": \"not_analyzed\" }  ]}");
  
  public final String wActionStoreName;
  public final String schema;
  
  private InternalType(String wActionStoreName, String schema)
  {
    this.wActionStoreName = wActionStoreName;
    this.schema = schema;
  }
  
  public DataType getDataType(WActionStoreManager manager, Map<String, Object> properties)
  {
    WActionStore wActionStore = manager.getOrCreate(this.wActionStoreName, properties);
    String dataTypeName = name();
    JsonNode dataTypeSchema = Utility.readTree(this.schema);
    DataType result = wActionStore.setDataType(dataTypeName, dataTypeSchema, null);
    if (result == null) {
      throw new WActionStoreException(String.format("Unable to create data type '%s' in WActionStore '%s'", new Object[] { dataTypeName, this.wActionStoreName }));
    }
    return result;
  }
  
  public String getWActionStoreName()
  {
    return this.wActionStoreName;
  }
}
