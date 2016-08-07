package com.bloom.runtime;

import com.bloom.appmanager.AppManager;
import com.bloom.appmanager.AppManager.ComponentStatus;
import com.bloom.exceptionhandling.ExceptionType;
import com.bloom.runtime.components.EntityType;
import com.bloom.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.bloom.event.ObjectMapperFactory;
import com.bloom.event.SimpleEvent;

import org.apache.log4j.Logger;

public class ExceptionEvent
  extends SimpleEvent
{
  private static final long serialVersionUID = 4645917522060103160L;
  private static Logger logger = Logger.getLogger(ExceptionEvent.class);
  public ExceptionType type;
  public ActionType action;
  public UUID appid;
  public EntityType entityType;
  public String entityName;
  public UUID entityId;
  public AppManager.ComponentStatus componentStatus = AppManager.ComponentStatus.UNKNOWN;
  public String className;
  public String message;
  public Long epochNumber = new Long(-1L);
  public String relatedActivity;
  public String[] relatedObjects;
  
  public ExceptionEvent() {}
  
  public ExceptionEvent(UUID appid, ExceptionType exceptionType, ActionType action)
  {
    this.appid = appid;
    this.type = exceptionType;
    this.action = action;
  }
  
  public ExceptionType getType()
  {
    return this.type;
  }
  
  public void setType(ExceptionType exceptionType)
  {
    this.type = exceptionType;
  }
  
  public ActionType getAction()
  {
    return this.action;
  }
  
  public void setAction(ActionType action)
  {
    this.action = action;
  }
  
  public Long getEpochNumber()
  {
    return this.epochNumber;
  }
  
  public void setEpochNumber(Long epochNumber)
  {
    this.epochNumber = epochNumber;
  }
  
  public UUID getAppid()
  {
    return this.appid;
  }
  
  public void setAppid(UUID appid)
  {
    this.appid = appid;
  }
  
  public EntityType getEntityType()
  {
    return this.entityType;
  }
  
  public void setEntityType(EntityType entityType)
  {
    this.entityType = entityType;
  }
  
  public AppManager.ComponentStatus getComponentStatus()
  {
    return this.componentStatus;
  }
  
  public void setComponentStatus(AppManager.ComponentStatus componentStatus)
  {
    this.componentStatus = componentStatus;
  }
  
  public void setClassName(String className)
  {
    this.className = className;
  }
  
  public void setMessage(String message)
  {
    this.message = message;
  }
  
  public String toString()
  {
    ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
    jsonMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    
    StringBuilder json = new StringBuilder();
    try
    {
      json.append("\"componentName\" : ");
      json.append(jsonMapper.writeValueAsString(this.entityName));
      json.append(" , ");
      json.append("\"componentType\" : ");
      json.append(jsonMapper.writeValueAsString(this.entityType));
      json.append(" , ");
      json.append("\"exception\" : ");
      json.append(jsonMapper.writeValueAsString(this.className));
      json.append(" , ");
      json.append("\"message\" : ");
      json.append(jsonMapper.writeValueAsString(this.message));
      json.append(" , ");
      json.append("\"relatedEvents\" : ");
      json.append(jsonMapper.writeValueAsString(this.relatedObjects));
      json.append(" , ");
      json.append("\"action\" : ");
      json.append(jsonMapper.writeValueAsString(this.action));
      json.append(" , ");
      json.append("\"exceptionType\" : ");
      json.append(jsonMapper.writeValueAsString(this.type));
      json.append(" , ");
      json.append("\"epochNumber\" : ");
      json.append(jsonMapper.writeValueAsString(this.epochNumber));
    }
    catch (JsonProcessingException e)
    {
      logger.error("Failed to jsonify relatedObjects in exceptionEvent with exception " + e.getMessage());
    }
    return "ExceptionEvent : {\n" + json.toString() + "\n}";
  }
  
  public String userString()
  {
    ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
    jsonMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    
    StringBuilder json = new StringBuilder();
    try
    {
      json.append("\"componentName\" : ");
      json.append(jsonMapper.writeValueAsString(this.entityName));
      json.append(" , ");
      json.append("\"componentType\" : ");
      json.append(jsonMapper.writeValueAsString(this.entityType));
      json.append(" , ");
      json.append("\"exception\" : ");
      json.append(jsonMapper.writeValueAsString(this.className));
      json.append(" , ");
      json.append("\"message\" : ");
      json.append(jsonMapper.writeValueAsString(this.message));
      json.append(" , ");
      json.append("\"relatedEvents\" : ");
      
      json.append(jsonMapper.writeValueAsString(this.relatedObjects));
    }
    catch (JsonProcessingException e)
    {
      logger.error("Failed to jsonify relatedObjects in exceptionEvent with exception " + e.getMessage());
    }
    return "{\n" + json.toString() + "\n}";
  }
  
  public String parseRelatedObjects()
  {
    StringBuilder builder = new StringBuilder();
    for (String s : this.relatedObjects) {
      builder.append(s);
    }
    String relatedObjectsAsString = builder.toString();
    
    String removeParenthesis = new String(relatedObjectsAsString);
    removeParenthesis = removeParenthesis.substring(removeParenthesis.indexOf("(") + 1);
    removeParenthesis = removeParenthesis.substring(0, removeParenthesis.indexOf(")"));
    
    String removeBrackets = new String(removeParenthesis);
    removeBrackets = removeBrackets.substring(removeBrackets.indexOf("[") + 1);
    removeBrackets = removeBrackets.substring(0, removeBrackets.indexOf("] ["));
    return removeBrackets;
  }
  
  public void write(Kryo kryo, Output output)
  {
    super.write(kryo, output);
    kryo.writeObjectOrNull(output, this.type, ExceptionType.class);
    kryo.writeObjectOrNull(output, this.action, ActionType.class);
    kryo.writeObjectOrNull(output, this.appid, UUID.class);
    kryo.writeObjectOrNull(output, this.entityType, EntityType.class);
    kryo.writeObjectOrNull(output, this.entityId, UUID.class);
    kryo.writeObjectOrNull(output, this.componentStatus, AppManager.ComponentStatus.class);
    output.writeString(this.className);
    output.writeString(this.message);
    output.writeString(this.entityName);
    kryo.writeObjectOrNull(output, this.epochNumber, Long.class);
    output.writeString(this.relatedActivity);
    output.writeBoolean(this.relatedObjects != null);
    if (this.relatedObjects != null)
    {
      output.writeInt(this.relatedObjects.length);
      for (String o : this.relatedObjects) {
        output.writeString(o);
      }
    }
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    this.type = ((ExceptionType)kryo.readObjectOrNull(input, ExceptionType.class));
    this.action = ((ActionType)kryo.readObjectOrNull(input, ActionType.class));
    this.appid = ((UUID)kryo.readObjectOrNull(input, UUID.class));
    this.entityType = ((EntityType)kryo.readObjectOrNull(input, EntityType.class));
    this.entityId = ((UUID)kryo.readObjectOrNull(input, UUID.class));
    this.componentStatus = ((AppManager.ComponentStatus)kryo.readObjectOrNull(input, AppManager.ComponentStatus.class));
    this.className = input.readString();
    this.message = input.readString();
    this.entityName = input.readString();
    this.epochNumber = ((Long)kryo.readObjectOrNull(input, Long.class));
    this.relatedActivity = input.readString();
    
    boolean hasRelatedObjects = input.readBoolean();
    if (hasRelatedObjects)
    {
      int numRelatedObjects = input.readInt();
      this.relatedObjects = new String[numRelatedObjects];
      for (int i = 0; i < numRelatedObjects; i++) {
        this.relatedObjects[i] = input.readString();
      }
    }
  }
  
  public int hashCode()
  {
    return toString().hashCode();
  }
  
  public void setRelatedObjects(Object[] relatedObjects)
  {
    if (relatedObjects == null) {
      return;
    }
    this.relatedObjects = new String[relatedObjects.length];
    for (int i = 0; i < relatedObjects.length; i++) {
      this.relatedObjects[i] = relatedObjects[i].toString();
    }
  }
}
