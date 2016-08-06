package com.bloom.wactionstore;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public final class Utility
{
  public static final JsonNodeFactory nodeFactory = new JsonNodeFactory(false);
  public static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Logger logger = Logger.getLogger(Utility.class);
  private static final JsonSchemaFactory schemaFactory = JsonSchemaFactory.byDefault();
  private static final long MILLISECONDS_PER_SECOND = 1000L;
  private static final long MESSAGE_THRESHOLD_WARN = 10000L;
  private static final long MESSAGE_THRESHOLD_INFO = 1000L;
  private static final long MESSAGE_THRESHOLD_DEBUG = 500L;
  
  static
  {
    objectMapper.registerModule(new JodaModule());
    objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
    objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
    objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
  }
  
  public static JsonSchema createJsonSchema(JsonNode jsonNode)
  {
    JsonSchema result = null;
    if (jsonNode != null) {
      try
      {
        result = schemaFactory.getJsonSchema(jsonNode);
      }
      catch (ProcessingException exception)
      {
        logger.error("JsonSchema not created", exception);
      }
    }
    return result;
  }
  
  public static JsonSchema createJsonSchema(String schemaResourceName)
  {
    JsonNode jsonNode = null;
    try
    {
      jsonNode = JsonLoader.fromResource(schemaResourceName);
    }
    catch (IOException exception)
    {
      logger.error(String.format("Schema resource, '%s', not loaded", new Object[] { schemaResourceName }), exception);
    }
    return createJsonSchema(jsonNode);
  }
  
  public static JsonNode readTree(String content)
  {
    JsonNode result = null;
    try
    {
      result = objectMapper.readTree(content);
    }
    catch (IOException exception)
    {
      logger.warn(String.format("Cannot parse JSON value '%s'", new Object[] { content }), exception);
    }
    return result;
  }
  
  public static String serializeToBase64String(Serializable object)
  {
    String result = null;
    if (object != null)
    {
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      try
      {
        ObjectOutput objectOutput = new ObjectOutputStream(stream);Throwable localThrowable2 = null;
        try
        {
          objectOutput.writeObject(object);
          objectOutput.close();
          
          byte[] encoded = Base64.encodeBase64(stream.toByteArray());
          result = new String(encoded);
        }
        catch (Throwable localThrowable1)
        {
          localThrowable2 = localThrowable1;throw localThrowable1;
        }
        finally
        {
          if (objectOutput != null) {
            if (localThrowable2 != null) {
              try
              {
                objectOutput.close();
              }
              catch (Throwable x2)
              {
                localThrowable2.addSuppressed(x2);
              }
            } else {
              objectOutput.close();
            }
          }
        }
      }
      catch (IOException exception)
      {
        logger.warn(exception.getMessage(), exception);
      }
    }
    return result;
  }
  
  public static Object serializeFromBase64String(String serialized)
  {
    Object result = null;
    if (serialized != null)
    {
      byte[] decoded = Base64.decodeBase64(serialized);
      InputStream stream = new ByteArrayInputStream(decoded);
      try
      {
        ObjectInput objectInput = new ObjectInputStream(stream);Throwable localThrowable2 = null;
        try
        {
          result = objectInput.readObject();
        }
        catch (Throwable localThrowable1)
        {
          localThrowable2 = localThrowable1;throw localThrowable1;
        }
        finally
        {
          if (objectInput != null) {
            if (localThrowable2 != null) {
              try
              {
                objectInput.close();
              }
              catch (Throwable x2)
              {
                localThrowable2.addSuppressed(x2);
              }
            } else {
              objectInput.close();
            }
          }
        }
      }
      catch (IOException|ClassNotFoundException exception)
      {
        logger.warn(exception.getMessage(), exception);
      }
    }
    return result;
  }
  
  public static void reportExecutionTime(Logger reportLogger, long milliseconds)
  {
    Level logLevel;
    if (milliseconds < 500L)
    {
      logLevel = Level.DEBUG;
    }
    else
    {
      if (milliseconds < 1000L) {
        logLevel = Level.INFO;
      } else {
        logLevel = milliseconds < 10000L ? Level.WARN : Level.ERROR;
      }
    }
    reportLogger.log(logLevel, String.format("Execution time: %d.%03d seconds", new Object[] { Long.valueOf(milliseconds / 1000L), Long.valueOf(milliseconds % 1000L) }));
    if (logger.isDebugEnabled()) {
      logger.debug(String.format("Execution time: %d.%03d seconds", new Object[] { Long.valueOf(milliseconds / 1000L), Long.valueOf(milliseconds % 1000L) }));
    }
  }
  
  public static Integer getPropertyInteger(Map<String, Object> properties, String propertyName)
  {
    Integer result = null;
    Object property = properties != null ? properties.get(propertyName) : null;
    if (property != null) {
      result = getIntegerValue(property);
    }
    return result;
  }
  
  private static Integer getIntegerValue(Object property)
  {
    Integer result = null;
    if ((property instanceof Number))
    {
      Number number = (Number)property;
      result = Integer.valueOf(number.intValue());
    }
    else
    {
      try
      {
        result = Integer.valueOf(property.toString());
      }
      catch (NumberFormatException ignored) {}
    }
    return result;
  }
  
  public static String getPropertyString(Map<String, Object> properties, String propertyName)
  {
    Object property = properties != null ? properties.get(propertyName) : null;
    return (String)property;
  }
  
  public static int extractInt(Object object)
  {
    if ((object instanceof Integer)) {
      return ((Integer)object).intValue();
    }
    return Integer.parseInt(object.toString());
  }
}
