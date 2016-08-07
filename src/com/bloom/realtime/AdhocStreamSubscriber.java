package com.bloom.runtime;

import com.bloom.distribution.WAQueue;
import com.bloom.event.QueryResultEvent;
import com.bloom.runtime.components.Subscriber;
import com.bloom.uuid.UUID;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import org.apache.log4j.Logger;

public class AdhocStreamSubscriber
  implements Subscriber
{
  private static Logger logger = Logger.getLogger(AdhocStreamSubscriber.class);
  private final String[] fieldsInfo;
  private final UUID queryMetaObjectUUID;
  private final String queueName;
  private final WAQueue consoleQueue;
  private final UUID uuid;
  
  public AdhocStreamSubscriber(String[] fieldsInfo, UUID queryMetaObjectUUID, String queueName, WAQueue consoleQueue)
  {
    this.fieldsInfo = fieldsInfo;
    this.queryMetaObjectUUID = queryMetaObjectUUID;
    this.queueName = queueName;
    this.consoleQueue = consoleQueue;
    this.uuid = new UUID(System.currentTimeMillis());
  }
  
  public UUID getUuid()
  {
    return this.uuid;
  }
  
  public String getName()
  {
    return this.queueName;
  }
  
  public void receive(Object linkID, ITaskEvent event)
    throws Exception
  {
    for (WAEvent wa : event.batch())
    {
      QueryResultEvent e = (QueryResultEvent)wa.data;
      e.setFieldsInfo(this.fieldsInfo);
    }
    event.setQueryID(this.queryMetaObjectUUID);
    if (logger.isDebugEnabled()) {
      logger.debug("Query Sending " + event.batch().size() + " # of objects to WAQueue " + this.queueName);
    }
    this.consoleQueue.put(event);
  }
}
