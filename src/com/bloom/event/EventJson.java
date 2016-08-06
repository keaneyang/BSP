package com.bloom.event;

import com.bloom.classloading.WALoader;
import com.bloom.uuid.UUID;
import com.bloom.waction.Waction;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.bloom.event.ObjectMapperFactory;
import com.bloom.event.SimpleEvent;

import org.apache.log4j.Logger;

public class EventJson
{
  private static final long serialVersionUID = -8414735731457603959L;
  public static transient ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
  private static Logger logger = Logger.getLogger(Waction.class);
  public UUID uuid;
  public String eventType = "SimpleEvent";
  public String eventString;
  public String mapKey;
  
  public EventJson() {}
  
  public EventJson(SimpleEvent event)
  {
    this.uuid = event.get_wa_SimpleEvent_ID();
    this.eventString = event.toJSON();
    this.eventType = event.getClass().getCanonicalName();
  }
  
  public String getMapKey()
  {
    return this.mapKey;
  }
  
  public void setMapKey(String mapKey)
  {
    this.mapKey = mapKey;
  }
  
  public String getEventIDStr()
  {
    return this.uuid.getUUIDString();
  }
  
  public void setEventIDStr(String eventID)
  {
    this.uuid = new UUID(eventID);
  }
  
  public String getEventType()
  {
    return this.eventType;
  }
  
  public void setEventType(String eventType)
  {
    this.eventType = eventType;
  }
  
  public String getEventString()
  {
    return this.eventString;
  }
  
  public void setEventString(String json)
  {
    this.eventString = json;
  }
  
  public Object buildEvent()
  {
    try
    {
      if (logger.isTraceEnabled()) {
        logger.trace("classname :" + this.eventType);
      }
      if (logger.isTraceEnabled()) {
        logger.trace("classname :" + WALoader.get().loadClass(this.eventType).getCanonicalName());
      }
      if (logger.isTraceEnabled()) {
        logger.trace("json : " + this.eventString);
      }
      return jsonMapper.readValue(this.eventString, WALoader.get().loadClass(this.eventType));
    }
    catch (Exception ex)
    {
      logger.error("failed to build event object for type :" + this.eventType + "\n JSON = " + this.eventString, ex);
    }
    return "<Undeserializable>";
  }
}
