package com.bloom.wactionstore.base;

import com.bloom.wactionstore.DataType;
import com.bloom.wactionstore.Utility;
import com.bloom.wactionstore.WAction;
import com.bloom.wactionstore.WActionStore;
import com.bloom.wactionstore.exceptions.WActionStoreException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonschema.core.report.ProcessingMessage;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public abstract class DataTypeBase<T extends WActionStore>
  implements DataType
{
  private static final Class<? extends Object> thisClass = DataTypeBase.class;
  private static final Logger logger = Logger.getLogger(thisClass);
  private static final Level wActionLogLevel = Level.WARN;
  private static JsonSchema wActionStoreSchema = Utility.createJsonSchema("/WActionStore.json");
  private static final Map<String, String> wActionTypeToJsonType = new HashMap(10);
  private final T wActionStore;
  private final String name;
  private final JsonNode schemaJson;
  private final ObjectNode dataSchemaJson;
  private final JsonSchema dataSchema;
  
  static
  {
    wActionTypeToJsonType.put("string", "string");
    wActionTypeToJsonType.put("text", "string");
    wActionTypeToJsonType.put("binary64", "string");
    wActionTypeToJsonType.put("date", "string");
    wActionTypeToJsonType.put("time", "string");
    wActionTypeToJsonType.put("boolean", "boolean");
    wActionTypeToJsonType.put("integer", "integer");
    wActionTypeToJsonType.put("long", "integer");
    wActionTypeToJsonType.put("datetime", "integer");
    wActionTypeToJsonType.put("double", "number");
  }
  
  protected DataTypeBase(T wActionStore, String name, JsonNode schemaJson)
  {
    this.wActionStore = wActionStore;
    this.name = name;
    this.schemaJson = schemaJson;
    this.dataSchemaJson = Utility.nodeFactory.objectNode();
    this.dataSchema = createWActionSchema();
  }
  
  private static boolean isWActionStoreSchemaValid(JsonNode schema)
  {
    return (wActionStoreSchema != null) && (schema != null) && (wActionStoreSchema.validInstanceUnchecked(schema));
  }
  
  private static void addWActionSchemaProperty(ObjectNode properties, JsonNode attribute, ArrayNode required)
  {
    String attributeName = attribute.get("name").asText();
    String wActionAttributeType = attribute.get("type").asText();
    String attributeType = translateWActionType(wActionAttributeType);
    JsonNode attributeCanBeNull = attribute.get("nullable");
    boolean attributeNullable = (attributeCanBeNull == null) || (attributeCanBeNull.asBoolean());
    
    ObjectNode attributeTypeNode = Utility.nodeFactory.objectNode();
    if (attributeNullable)
    {
      ArrayNode attributeTypes = Utility.nodeFactory.arrayNode();
      attributeTypes.add(attributeType);
      attributeTypes.add("null");
      attributeTypeNode.set("type", attributeTypes);
    }
    else
    {
      attributeTypeNode.put("type", attributeType);
      required.add(attributeName);
    }
    properties.set(attributeName, attributeTypeNode);
  }
  
  private static String translateWActionType(String wActionType)
  {
    String result = (String)wActionTypeToJsonType.get(wActionType);
    if (result == null) {
      throw new WActionStoreException(String.format("Unsupported context data type, '%s'", new Object[] { wActionType }));
    }
    return result;
  }
  
  private static void addWActionSchemaEventType(ObjectNode properties, JsonNode eventType)
  {
    ObjectNode eventTypeSchema = Utility.objectMapper.createObjectNode();
    eventTypeSchema.put("type", "array");
    
    ObjectNode eventTypeDescription = Utility.objectMapper.createObjectNode();
    eventTypeDescription.put("type", "object");
    
    ObjectNode eventTypeProperties = Utility.objectMapper.createObjectNode();
    ArrayNode required = Utility.nodeFactory.arrayNode();
    for (JsonNode attribute : eventType.get("type")) {
      addWActionSchemaProperty(eventTypeProperties, attribute, required);
    }
    eventTypeDescription.set("properties", eventTypeProperties);
    if (required.size() > 0) {
      eventTypeDescription.set("required", required);
    }
    eventTypeDescription.put("additionalProperties", false);
    
    eventTypeSchema.set("items", eventTypeDescription);
    String eventTypeName = eventType.get("name").asText();
    properties.set(eventTypeName, eventTypeSchema);
  }
  
  public T getWActionStore()
  {
    return this.wActionStore;
  }
  
  public String getName()
  {
    return this.name;
  }
  
  public JsonNode getSchemaJson()
  {
    return this.schemaJson;
  }
  
  public JsonSchema getDataSchema()
  {
    return this.dataSchema;
  }
  
  public boolean isValid()
  {
    if ((this.wActionStore == null) || (this.schemaJson == null) || (this.dataSchema == null)) {
      return false;
    }
    return (this.name != null) && (!this.name.isEmpty());
  }
  
  private JsonSchema createWActionSchema()
  {
    JsonSchema result = null;
    if (isWActionSchemaValid())
    {
      ObjectNode properties = Utility.nodeFactory.objectNode();
      ArrayNode required = Utility.nodeFactory.arrayNode();
      
      startWActionSchemaJson(this.name);
      for (JsonNode attribute : this.schemaJson.get("context")) {
        addWActionSchemaProperty(properties, attribute, required);
      }
      if (this.schemaJson.get("events") != null) {
        for (JsonNode eventType : this.schemaJson.get("events")) {
          addWActionSchemaEventType(properties, eventType);
        }
      }
      finishWActionSchemaJson(properties, required);
      result = Utility.createJsonSchema(this.dataSchemaJson);
      reportSchemaCreateResult(result != null);
    }
    return result;
  }
  
  private void startWActionSchemaJson(String title)
  {
    this.dataSchemaJson.put("$schema", "http://json-schema.org/draft-04/schema#");
    this.dataSchemaJson.put("title", title);
    this.dataSchemaJson.put("type", "object");
  }
  
  private void finishWActionSchemaJson(ObjectNode properties, ArrayNode required)
  {
    this.dataSchemaJson.set("properties", properties);
    if (required.size() > 0) {
      this.dataSchemaJson.set("required", required);
    }
    this.dataSchemaJson.put("additionalProperties", true);
  }
  
  private void reportSchemaCreateResult(boolean valid)
  {
    if (valid) {
      logger.debug(String.format("Created a schema for WActionStore '%s' from '%s'", new Object[] { this.name, this.dataSchemaJson.toString() }));
    } else {
      logger.error(String.format("Cannot create a schema for WActionStore '%s' from '%s'", new Object[] { this.name, this.dataSchemaJson.toString() }));
    }
  }
  
  private boolean isWActionSchemaValid()
  {
    if (this.schemaJson == null)
    {
      logger.error(String.format("WActionStore '%s' schema is missing", new Object[] { this.name }));
      return false;
    }
    if (!isWActionStoreSchemaValid(this.schemaJson))
    {
      logger.error(String.format("WActionStore '%s' schema is invalid: '%s'", new Object[] { this.name, this.schemaJson }));
      return false;
    }
    return true;
  }
  
  public boolean isValid(WAction wAction)
  {
    boolean result = true;
    if (wAction != null)
    {
      ProcessingReport report = this.dataSchema.validateUnchecked(wAction);
      if ((report != null) && (!report.isSuccess()))
      {
        logInvalidWAction(wAction, report);
        result = false;
      }
    }
    return result;
  }
  
  private void logInvalidWAction(WAction wAction, Iterable<ProcessingMessage> report)
  {
    int messageNumber;
    if (logger.isEnabledFor(wActionLogLevel))
    {
      logger.log(wActionLogLevel, String.format("Schema  : %s", new Object[] { this.dataSchemaJson }));
      logger.log(wActionLogLevel, String.format("WAction : %s", new Object[] { wAction }));
      messageNumber = 1;
      for (ProcessingMessage message : report)
      {
        JsonNode instanceNode = message.asJson().path("instance").path("pointer");
        String instanceName = instanceNode.isMissingNode() ? "<Unknown>" : instanceNode.asText().replace("/", "");
        String messageText = message.getMessage();
        logger.log(wActionLogLevel, String.format("Error %2d: %s : %s", new Object[] { Integer.valueOf(messageNumber), instanceName, messageText }));
        messageNumber++;
      }
    }
  }
  
  public boolean insert(WAction wAction)
  {
    List<WAction> wActions = new ArrayList(1);
    wActions.add(wAction);
    return insert(wActions, null);
  }
}
