package com.bloom.runtime.components;

import com.bloom.appmanager.NodeManager;
import com.bloom.exceptionhandling.ExceptionType;
import com.bloom.runtime.ActionType;
import com.bloom.runtime.BaseServer;
import com.bloom.runtime.ExceptionEvent;
import com.bloom.runtime.channels.Channel;
import com.bloom.runtime.containers.TaskEvent;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.MetaObjectInfo;
import com.bloom.uuid.UUID;
import com.bloom.recovery.Position;
import com.bloom.runtime.containers.WAEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public abstract class FlowComponent
  extends MonitorableComponent
  implements IFlowComponent
{
  private static Logger logger = Logger.getLogger(FlowComponent.class);
  private final BaseServer srv;
  private final MetaInfo.MetaObject info;
  private Flow flow;
  private NodeManager nodeManager;
  
  public FlowComponent(BaseServer srv, MetaInfo.MetaObject info)
  {
    this.srv = srv;
    this.info = info;
  }
  
  public void setFlow(Flow f)
  {
    this.flow = f;
  }
  
  public Flow getFlow()
  {
    return this.flow;
  }
  
  public Flow getTopLevelFlow()
  {
    if (this.flow == null) {
      return null;
    }
    if (this.flow.getFlow() == null) {
      return this.flow;
    }
    return this.flow.getTopLevelFlow();
  }
  
  public boolean recoveryIsEnabled()
  {
    Flow f = getTopLevelFlow();
    if (f == null) {
      return false;
    }
    return f.recoveryIsEnabled();
  }
  
  public boolean isFlowInError()
  {
    if (this.nodeManager == null)
    {
      Flow topLevelflow = getTopLevelFlow();
      if (topLevelflow == null) {
        return false;
      }
      this.nodeManager = topLevelflow.getNodeManager();
      if (this.nodeManager == null) {
        return false;
      }
    }
    return this.nodeManager.isError();
  }
  
  public abstract void close()
    throws Exception;
  
  public MetaInfo.MetaObject getMetaInfo()
  {
    return this.info;
  }
  
  public MetaInfo.MetaObjectInfo getMetaObjectInfo()
  {
    return getMetaInfo().makeMetaObjectInfo();
  }
  
  public EntityType getMetaType()
  {
    return getMetaInfo().getType();
  }
  
  public UUID getMetaID()
  {
    return getMetaInfo().getUuid();
  }
  
  public String getMetaName()
  {
    return getMetaInfo().getName();
  }
  
  public String getMetaNsName()
  {
    return getMetaInfo().getNsName();
  }
  
  public String getMetaFullName()
  {
    return getMetaInfo().getFullName();
  }
  
  public String getMetaUri()
  {
    return getMetaInfo().getUri();
  }
  
  public List<UUID> getMetaDependencies()
  {
    return getMetaInfo().getDependencies();
  }
  
  public String metaToString()
  {
    return getMetaInfo().metaToString();
  }
  
  public Position getCheckpoint()
  {
    return null;
  }
  
  public BaseServer srv()
  {
    return this.srv;
  }
  
  public void notifyAppMgr(EntityType entityType, String entityName, UUID entityId, Exception exception, String relatedActivity, Object... relatedObjects)
  {
    logger.warn("received exception from :" + entityType + ", of exception type : " + exception.getClass().getCanonicalName());
    ExceptionEvent ee = new ExceptionEvent();
    Flow topLevelFlow = getTopLevelFlow();
    if (topLevelFlow == null) {
      return;
    }
    MetaInfo.Flow app = (MetaInfo.Flow)topLevelFlow.getMetaInfo();
    ee.setAppid(app.uuid);
    ee.setType(ExceptionType.getExceptionType(exception));
    ee.setEntityType(entityType);
    ee.setClassName(exception.getClass().getName());
    ee.setMessage(exception.getMessage());
    ee.entityName = entityName;
    ee.entityId = entityId;
    ee.relatedActivity = relatedActivity;
    ee.setRelatedObjects(relatedObjects);
    
    ee.setAction(getUserRequestedActionForException(exception, ee.getType(), app.getEhandlers()));
    if (logger.isInfoEnabled()) {
      logger.info("exception event created :" + ee.toString());
    }
    publishException(ee);
    if (getFlow().getNodeManager() != null) {
      getFlow().getNodeManager().recvExceptionEvent(ee);
    } else if (getFlow().getNodeManager() != null) {
      getFlow().getNodeManager().recvExceptionEvent(ee);
    } else if (getTopLevelFlow().getNodeManager() != null) {
      getTopLevelFlow().getNodeManager().recvExceptionEvent(ee);
    } else {
      logger.warn("Failed to get app manager, so NOT notifying exception. ");
    }
  }
  
  public ActionType getUserRequestedActionForException(Throwable ex, ExceptionType eType, Map<String, Object> ehandlers)
  {
    if ((ehandlers == null) || (ehandlers.isEmpty())) {
      return getDefaultAction(ex);
    }
    for (String exceptionType : ehandlers.keySet()) {
      if (eType.name().equalsIgnoreCase(exceptionType))
      {
        if (((String)ehandlers.get(exceptionType)).equalsIgnoreCase("stop")) {
          return ActionType.STOP;
        }
        if (((String)ehandlers.get(exceptionType)).equalsIgnoreCase("crash")) {
          return ActionType.CRASH;
        }
        return ActionType.IGNORE;
      }
    }
    return getDefaultAction(ex);
  }
  
  private ActionType getDefaultAction(Throwable ex)
  {
    return ActionType.CRASH;
  }
  
  protected void publishException(ExceptionEvent event)
  {
    try
    {
      if (logger.isDebugEnabled()) {
        logger.debug("publishing exception to exceptionStream.");
      }
      Stream exceptionStream = this.srv.getExceptionStream();
      if (exceptionStream != null)
      {
        Channel channel = exceptionStream.getChannel();
        if (channel != null)
        {
          if (logger.isDebugEnabled()) {
            logger.debug("channel name :" + channel.getSubscribersCount() + ", channel:" + channel);
          }
          List<WAEvent> jsonBatch = new ArrayList();
          jsonBatch.add(new WAEvent(event));
          channel.publish(TaskEvent.createStreamEvent(jsonBatch));
          logger.warn("channel to publish exceptions is not null and published to channel : " + channel.getSubscribersCount());
        }
        else
        {
          logger.warn("channel to publish exceptions is null.");
        }
      }
    }
    catch (Exception ex)
    {
      logger.error("Problem publishing exception event", ex);
    }
  }
}
