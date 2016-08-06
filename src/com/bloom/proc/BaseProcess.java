package com.bloom.proc;

import com.bloom.exception.SourceShutdownException;
import com.bloom.intf.EventSink;
import com.bloom.intf.Process;
import com.bloom.intf.Worker;
import com.bloom.intf.Worker.WorkerType;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.components.Flow;
import com.bloom.runtime.components.ReceiptCallback;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.PropertyVariable;
import com.bloom.security.Password;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.UUID;
import com.hazelcast.core.RuntimeInterruptedException;
import com.bloom.common.exc.AdapterException;
import com.bloom.common.exc.ConnectionException;
import com.bloom.common.exc.InvalidDataException;
import com.bloom.common.exc.MetadataUnavailableException;
import com.bloom.common.exc.RecordException;
import com.bloom.common.exc.SystemException;
import com.bloom.common.exc.TargetShutdownException;
import com.bloom.event.Event;
import com.bloom.recovery.Position;
import com.bloom.recovery.SourcePosition;
import com.bloom.runtime.containers.ITaskEvent;

import java.nio.channels.ClosedSelectorException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.log4j.Logger;
import org.zeromq.ZMQException;

import zmq.ZError;
import zmq.ZError.IOException;

public abstract class BaseProcess
  implements Process
{
  private static Logger logger = Logger.getLogger(BaseProcess.class);
  protected transient List<EventSink> sinks = new ArrayList();
  boolean running = false;
  String uri;
  boolean setClassLoader = false;
  ClassLoader threadLoader = null;
  protected ReceiptCallback receiptCallback;
  
  private void replacePropVariables(Map<String, Object> properties, String key, String value)
  {
    String namespace = null;
    if ((value != null) && (!value.trim().isEmpty()))
    {
      namespace = getNameSpaceForRetrievingPropertyVariable(value, properties);
      value = getPropEnvKeyWithoutNameSpace(value);
    }
    MetaInfo.PropertyVariable propSet_from_mdr = null;
    try
    {
      propSet_from_mdr = (MetaInfo.PropertyVariable)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYVARIABLE, namespace, value, null, WASecurityManager.TOKEN);
    }
    catch (MetaDataRepositoryException e)
    {
      logger.error("The prop variable variable you are looking for is not set...Please set and use" + e.getMessage());
    }
    Map<String, Object> prop_from_mdr = propSet_from_mdr.getProperties();
    Object obj = prop_from_mdr.get(value);
    String plain_passwd = Password.getPlainStatic((String)obj);
    if ((properties.get(key) instanceof Password)) {
      properties.put(key, new Password(plain_passwd));
    } else {
      properties.put(key, plain_passwd);
    }
  }
  
  private boolean checkIfItIsPropertyVariable(String name, Map<String, Object> properties)
    throws MetadataUnavailableException
  {
    String namespace = null;
    if ((name != null) && (!name.trim().isEmpty()))
    {
      namespace = getNameSpaceForRetrievingPropertyVariable(name, properties);
      name = getPropEnvKeyWithoutNameSpace(name);
    }
    MetaInfo.PropertyVariable propSet_from_mdr = null;
    try
    {
      propSet_from_mdr = (MetaInfo.PropertyVariable)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYVARIABLE, namespace, name, null, WASecurityManager.TOKEN);
    }
    catch (MetaDataRepositoryException e)
    {
      logger.error("The prop variable variable you are looking for is not set...Please set and use" + e.getMessage());
    }
    if (propSet_from_mdr == null) {
      return false;
    }
    return true;
  }
  
  private String getNameSpaceForRetrievingPropertyVariable(String propEnvKey, Map<String, Object> properties)
  {
    String namespace = null;
    if (propEnvKey.contains("."))
    {
      String[] splitKeys = propEnvKey.split("\\.");
      namespace = splitKeys[0];
    }
    else
    {
      UUID uuid = (UUID)properties.get("UUID");
      if (uuid == null) {
        logger.warn("UUID not available in base process");
      } else {
        try
        {
          MetaInfo.MetaObject object = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
          namespace = object.getNsName();
        }
        catch (MetaDataRepositoryException e)
        {
          logger.error("Metaobject not found with the uuid : " + uuid);
        }
      }
    }
    return namespace;
  }
  
  private String getPropEnvKeyWithoutNameSpace(String propEnvKey)
  {
    if (propEnvKey.contains("."))
    {
      String[] splitKeys = propEnvKey.split("\\.");
      String propName = splitKeys[1];
      return propName;
    }
    return propEnvKey;
  }
  
  private void replaceEnvVariableProperty(Map<String, Object> properties)
    throws Exception
  {
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      if (((entry.getValue() instanceof String)) && (entry.getValue().toString().trim().startsWith("$")))
      {
        String propEnvKey = entry.getValue().toString().trim().substring(1);
        replaceEnvVariables(properties, (String)entry.getKey(), propEnvKey);
      }
    }
  }
  
  private void replaceEnvVariables(Map<String, Object> properties, String key, String value)
    throws MetaDataRepositoryException
  {
    if ((value != null) && (!value.trim().isEmpty()))
    {
      String defaultvalue = null;
      if (value.contains(":"))
      {
        String[] splitKeys = value.split(":");
        value = splitKeys[0];
        defaultvalue = splitKeys[1];
        if (defaultvalue.startsWith("$")) {
          defaultvalue = null;
        }
      }
      String propEnvValue = System.getProperty(value);
      if (propEnvValue == null) {
        propEnvValue = System.getenv(value);
      }
      if (propEnvValue != null)
      {
        if ((properties.get(key) instanceof Password)) {
          properties.put(key, new Password(propEnvValue));
        } else {
          properties.put(key, propEnvValue);
        }
      }
      else if (defaultvalue != null) {
        properties.put(key, defaultvalue);
      } else {
        throw new MetaDataRepositoryException("No object found with the name :" + value + ". It could be a property variable or environment variable. Please create before using.");
      }
    }
  }
  
  private void replacePropertyVariableOrEnvVariable(Map<String, Object> properties)
    throws Exception
  {
    for (Map.Entry<String, Object> entry : properties.entrySet())
    {
      if (((entry.getValue() instanceof String)) && (entry.getValue().toString().trim().startsWith("$")))
      {
        String propEnvKey = ((String)entry.getKey()).toString();
        String propEnvValue = entry.getValue().toString().trim().substring(1);
        boolean isPropertyVariable = checkIfItIsPropertyVariable(propEnvValue, properties);
        if (isPropertyVariable) {
          replacePropVariables(properties, propEnvKey, propEnvValue);
        } else {
          replaceEnvVariables(properties, propEnvKey, propEnvValue);
        }
      }
      if (((entry.getValue() instanceof Password)) && (((Password)entry.getValue()).getPlain().startsWith("$")))
      {
        Password p = (Password)entry.getValue();
        String propEnvKey = ((String)entry.getKey()).toString();
        String propEnvValue = p.getPlain().trim().substring(1);
        boolean isPropertyVariable = checkIfItIsPropertyVariable(propEnvValue, properties);
        if (isPropertyVariable) {
          replacePropVariables(properties, propEnvKey, propEnvValue);
        } else {
          replaceEnvVariables(properties, propEnvKey, propEnvValue);
        }
      }
    }
  }
  
  public void startWorker()
  {
    this.running = true;
    if (logger.isInfoEnabled()) {
      logger.info("Started process " + getClass().getCanonicalName());
    }
  }
  
  public void stopWorker()
  {
    this.running = false;
    if (logger.isInfoEnabled()) {
      logger.info("Stopped process " + getClass().getCanonicalName());
    }
  }
  
  public Worker.WorkerType getType()
  {
    return Worker.WorkerType.PROCESS;
  }
  
  public String getUri()
  {
    return this.uri;
  }
  
  public void setUri(String uri)
  {
    this.uri = uri;
  }
  
  public void close()
    throws Exception
  {
    if (logger.isInfoEnabled()) {
      logger.info("Closed process " + getClass().getCanonicalName());
    }
  }
  
  public void init(Map<String, Object> properties)
    throws Exception
  {
    replaceEnvVariableProperty(properties);
    if (properties.containsKey("$classLoader"))
    {
      this.threadLoader = ((ClassLoader)properties.get("$classLoader"));
      if (this.threadLoader != null)
      {
        Thread.currentThread().setContextClassLoader(this.threadLoader);
        this.setClassLoader = true;
      }
    }
  }
  
  public void init(Map<String, Object> properties, Map<String, Object> properties2)
    throws Exception
  {
    replacePropertyVariableOrEnvVariable(properties);
    replacePropertyVariableOrEnvVariable(properties2);
    init(properties);
  }
  
  public void init(Map<String, Object> properties, Map<String, Object> properties2, UUID sourceUUID, String distributionID)
    throws Exception
  {
    properties.put("UUID", sourceUUID);
    replacePropertyVariableOrEnvVariable(properties);
    replacePropertyVariableOrEnvVariable(properties2);
    init(properties, properties2);
  }
  
  public void init(Map<String, Object> properties, Map<String, Object> properties2, UUID sourceUUID, String distributionID, SourcePosition restartPosition, boolean sendPositions, Flow flow)
    throws Exception
  {
    properties.put("UUID", sourceUUID);
    replacePropertyVariableOrEnvVariable(properties);
    replacePropertyVariableOrEnvVariable(properties2);
    init(properties, properties2, sourceUUID, distributionID);
  }
  
  public void receive(int channel, Event event)
    throws Exception
  {
    if (this.threadLoader != null)
    {
      Thread.currentThread().setContextClassLoader(this.threadLoader);
      this.setClassLoader = false;
    }
    try
    {
      receiveImpl(channel, event);
    }
    catch (RecordException recoExp)
    {
      throw recoExp;
    }
    catch (InvalidDataException ide)
    {
      throw ide;
    }
    catch (AdapterException ae)
    {
      throw ae;
    }
    catch (InterruptedException e)
    {
      throw e;
    }
    catch (ZError.IOException|ClosedSelectorException|ZMQException e)
    {
      throw new InterruptedException();
    }
    catch (RuntimeInterruptedException e)
    {
      throw new InterruptedException();
    }
    catch (SourceShutdownException sse)
    {
      throw sse;
    }
    catch (TargetShutdownException tse)
    {
      throw tse;
    }
    catch (MetadataUnavailableException|ConnectionException e)
    {
      throw e;
    }
    catch (Throwable t)
    {
      logger.error(getClass().getCanonicalName() + "[" + getUri() + "] Problem processing event on channel " + channel + ": " + event, t);
      throw t;
    }
  }
  
  public void receive(int channel, Event event, Position pos)
    throws Exception
  {
    receive(channel, event);
  }
  
  public void receive(ITaskEvent batch)
    throws Exception
  {
    receive(0, null);
  }
  
  public abstract void receiveImpl(int paramInt, Event paramEvent)
    throws Exception;
  
  public void addEventSink(EventSink sink)
  {
    this.sinks.add(sink);
  }
  
  public void removeEventSink(EventSink sink)
  {
    this.sinks.remove(sink);
  }
  
  public void send(Event event, int channel)
    throws Exception
  {
    send(event, channel, null);
  }
  
  public void send(Event event, int channel, Position pos)
    throws Exception
  {
    int sinkNo = 1;
    for (EventSink sink : this.sinks) {
      if ((channel == 0) || (sinkNo++ == channel)) {
        sink.receive(0, event, pos);
      }
    }
  }
  
  public void send(ITaskEvent batch)
    throws Exception
  {
    for (EventSink sink : this.sinks) {
      sink.receive(batch);
    }
  }
  
  public UUID getNodeID()
  {
    return HazelcastSingleton.getNodeId();
  }
  
  public void onDeploy(Map<String, Object> properties1, Map<String, Object> properties2, UUID uuid)
    throws Exception
  {}
  
  public void setReceiptCallback(ReceiptCallback receiptCallback)
  {
    this.receiptCallback = receiptCallback;
  }
}
