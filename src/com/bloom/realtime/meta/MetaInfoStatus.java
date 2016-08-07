package com.bloom.runtime.meta;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.bloom.event.ObjectMapperFactory;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class MetaInfoStatus
  implements Serializable
{
  private static final long serialVersionUID = -9174291694730145114L;
  private boolean isValid = true;
  private boolean isAnonymous = false;
  private boolean isDropped = false;
  private boolean isAdhoc = false;
  private boolean isGenerated = false;
  
  public boolean isValid()
  {
    return this.isValid;
  }
  
  public MetaInfoStatus setValid(boolean isValid)
  {
    this.isValid = isValid;
    return this;
  }
  
  public boolean isAnonymous()
  {
    return this.isAnonymous;
  }
  
  public MetaInfoStatus setAnonymous(boolean isAnonymous)
  {
    this.isAnonymous = isAnonymous;
    return this;
  }
  
  public boolean isDropped()
  {
    return this.isDropped;
  }
  
  public MetaInfoStatus setDropped(boolean isDropped)
  {
    this.isDropped = isDropped;
    return this;
  }
  
  public boolean isAdhoc()
  {
    return this.isAdhoc;
  }
  
  public MetaInfoStatus setAdhoc(boolean isAdhoc)
  {
    this.isAdhoc = isAdhoc;
    return this;
  }
  
  public MetaInfoStatus setGenerated(boolean isGenerated)
  {
    this.isGenerated = isGenerated;
    return this;
  }
  
  public boolean isGenerated()
  {
    return this.isGenerated;
  }
  
  public String toString()
  {
    return "{ \nisValid => " + this.isValid + " \n" + "isAnonymous => " + this.isAnonymous + " \n" + "isDropped => " + this.isDropped + " \n" + "isAdhoc => " + isAdhoc() + " \n" + "isGenerated => " + isGenerated() + " \n" + "} ";
  }
  
  public static MetaInfoStatus jsonDeserializer(Map results)
    throws JsonParseException, JsonMappingException, IOException
  {
    if ((results == null) || (results.isEmpty())) {
      return null;
    }
    ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
    jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    jsonMapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
    jsonMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    jsonMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    jsonMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    
    MetaInfoStatus mis = new MetaInfoStatus();
    if (results.get("isValid") != null) {
      mis.setValid(((Boolean)results.get("isValid")).booleanValue());
    }
    if (results.get("isAnonymous") != null) {
      mis.setAnonymous(((Boolean)results.get("isAnonymous")).booleanValue());
    }
    if (results.get("isDropped") != null) {
      mis.setDropped(((Boolean)results.get("isDropped")).booleanValue());
    }
    if (results.get("isAdhoc") != null) {
      mis.setAdhoc(((Boolean)results.get("isAdhoc")).booleanValue());
    }
    if (results.get("isGenerated") != null) {
      mis.setGenerated(((Boolean)results.get("isGenerated")).booleanValue());
    }
    return mis;
  }
}
