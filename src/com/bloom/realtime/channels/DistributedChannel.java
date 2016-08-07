package com.bloom.runtime.channels;

import com.bloom.distribution.WAQueue;
import com.bloom.distribution.WAQueue.ShowStreamSubscriber;
import com.bloom.exceptionhandling.WAExceptionMgr;
import com.bloom.messaging.MessagingSystem;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.runtime.AdhocStreamSubscriber;
import com.bloom.runtime.BaseServer;
import com.bloom.runtime.KeyFactory;
import com.bloom.runtime.Server;
import com.bloom.runtime.components.Flow;
import com.bloom.runtime.components.FlowComponent;
import com.bloom.runtime.components.Stream;
import com.bloom.runtime.components.Subscriber;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.monitor.MonitorModel;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.UUID;
import com.bloom.recovery.Position;
import com.bloom.recovery.SourcePosition;
import com.bloom.runtime.DistLink;

import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import scala.NotImplementedError;

public abstract class DistributedChannel
  implements Channel
{
  public MetaInfo.Stream stream_metainfo;
  public MessagingSystem messagingSystem;
  protected BaseServer srv;
  protected boolean encrypted = false;
  protected Stream owner;
  protected Status status;
  UUID thisPeerId;
  private static Logger logger = Logger.getLogger(DistributedChannel.class);
  protected KeyFactory keyFactory;
  
  public DistributedChannel(Stream owner)
  {
    this.status = Status.INITIALIZED;
    this.thisPeerId = HazelcastSingleton.getNodeId();
    this.owner = owner;
    this.stream_metainfo = owner.getStreamMeta();
    try
    {
      this.keyFactory = KeyFactory.createKeyFactory(this.stream_metainfo);
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }
  
  public static enum Status
  {
    UNKNOWN,  INITIALIZED,  RUNNING;
    
    private Status() {}
  }
  
  protected MetaInfo.Stream getStreamInfo()
  {
    return this.owner.getStreamMeta();
  }
  
  public abstract boolean isFull();
  
  public abstract Position getCheckpoint();
  
  boolean checkIfSubscriberAndStreamBelongToSameApp(UUID subscriberID)
  {
    if (!(this.srv instanceof Server)) {
      return true;
    }
    Flow owningApplication = getOwner().getTopLevelFlow();
    boolean isPresent = false;
    if (owningApplication != null)
    {
      MetaInfo.Flow owningApplicationMetaObject = owningApplication.getFlowMeta();
      try
      {
        isPresent = owningApplicationMetaObject.getAllObjects().contains(subscriberID);
      }
      catch (MetaDataRepositoryException e)
      {
        e.printStackTrace();
      }
    }
    return isPresent;
  }
  
  public String getName()
  {
    return this.owner.getMetaName();
  }
  
  public UUID getID()
  {
    return this.owner.getMetaID();
  }
  
  public abstract void stop();
  
  public abstract void start();
  
  public abstract void start(Map<UUID, Set<UUID>> paramMap);
  
  public abstract boolean verifyStart(Map<UUID, Set<UUID>> paramMap);
  
  public abstract void closeDistSub(String paramString);
  
  public abstract Set<DistLink> getSubscribers();
  
  public String getDebugId()
  {
    try
    {
      return id2name(this.thisPeerId) + "-" + this.stream_metainfo.getName();
    }
    catch (MetaDataRepositoryException e)
    {
      logger.error(e.getMessage());
    }
    return null;
  }
  
  static String id2name(UUID id)
    throws MetaDataRepositoryException
  {
    MetaInfo.MetaObject o = MetadataRepository.getINSTANCE().getMetaObjectByUUID(id, WASecurityManager.TOKEN);
    if ((o instanceof MetaInfo.Server))
    {
      MetaInfo.Server s = (MetaInfo.Server)o;
      return "srv_" + s.id;
    }
    return id.toString();
  }
  
  public MetaInfo.Stream getMetaObject()
  {
    return this.stream_metainfo;
  }
  
  public Stream getOwner()
  {
    return this.owner;
  }
  
  UUID getSubscriberID(Subscriber sub)
  {
    if ((sub instanceof FlowComponent))
    {
      FlowComponent so = (FlowComponent)sub;
      return so.getMetaID();
    }
    if ((sub instanceof MonitorModel)) {
      return MonitorModel.getMonitorModelID();
    }
    if ((sub instanceof WAQueue.ShowStreamSubscriber)) {
      return ((WAQueue.ShowStreamSubscriber)sub).uuid;
    }
    if ((sub instanceof AdhocStreamSubscriber)) {
      return ((AdhocStreamSubscriber)sub).getUuid();
    }
    if ((sub instanceof WAExceptionMgr)) {
      return WAExceptionMgr.ExceptionStreamUUID;
    }
    return null;
  }
  
  String getSubscriberName(Subscriber sub)
  {
    if ((sub instanceof FlowComponent))
    {
      FlowComponent so = (FlowComponent)sub;
      return so.getMetaName();
    }
    if ((sub instanceof MonitorModel)) {
      return "InternalMonitoring";
    }
    if ((sub instanceof WAQueue.ShowStreamSubscriber)) {
      return ((WAQueue.ShowStreamSubscriber)sub).name;
    }
    if ((sub instanceof WAExceptionMgr)) {
      return ((WAExceptionMgr)sub).getName();
    }
    if ((sub instanceof AdhocStreamSubscriber)) {
      return ((AdhocStreamSubscriber)sub).getName();
    }
    return null;
  }
  
  public static String buildReceiverName(MetaInfo.Stream stream_metainfo, UUID subID)
  {
    StringBuilder receiver_name_builder = new StringBuilder();
    receiver_name_builder.append(stream_metainfo.getNsName()).append(":").append(stream_metainfo.getName()).append(":").append(stream_metainfo.getUuid()).append(":").append(subID);
    
    return receiver_name_builder.toString();
  }
  
  public void startEmitting(SourcePosition position)
    throws Exception
  {
    throw new NotImplementedError();
  }
}
