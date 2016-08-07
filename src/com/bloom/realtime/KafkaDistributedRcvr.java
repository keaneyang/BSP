package com.bloom.runtime;

import com.bloom.kafkamessaging.KafkaSystem;
import com.bloom.messaging.Handler;
import com.bloom.messaging.UnknownObjectException;
import com.bloom.runtime.channels.DistributedChannel;
import com.bloom.runtime.components.Flow;
import com.bloom.runtime.components.FlowComponent;
import com.bloom.runtime.components.Link;
import com.bloom.runtime.components.Stream;
import com.bloom.runtime.components.Subscriber;
import com.bloom.runtime.containers.TaskEvent;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.PropertySet;
import com.bloom.uuid.UUID;
import com.bloom.proc.events.PublishableEvent;
import com.bloom.recovery.PartitionedSourcePosition;
import com.bloom.recovery.Position;
import com.bloom.runtime.DistLink;
import com.bloom.runtime.StreamEvent;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.log4j.Logger;

public class KafkaDistributedRcvr
  implements Handler
{
  private static Logger logger = Logger.getLogger(KafkaDistributedRcvr.class);
  private final KafkaSystem messagingSystem;
  private final FlowComponent owner;
  private final DistributedChannel channel;
  private final String receiverName;
  private Status status;
  private boolean recoveryEnabled;
  private Link subscriber_link;
  private DistLink subscriber_distlink;
  private List<Integer> partitions_i_own = new ArrayList();
  
  public static enum Status
  {
    UNKNOWN,  INITIALIZED,  RUNNING;
    
    private Status() {}
  }
  
  public KafkaDistributedRcvr(String receiverName, FlowComponent owner, DistributedChannel pStream, boolean encrypted, KafkaSystem messagingSystem)
  {
    this.messagingSystem = messagingSystem;
    
    this.owner = owner;
    this.channel = pStream;
    this.status = Status.INITIALIZED;
    this.receiverName = receiverName;
    
    this.recoveryEnabled = ((this.owner != null) && (this.owner.getFlow() != null) && (this.owner.getFlow().recoveryIsEnabled()));
    this.messagingSystem.createReceiver(messagingSystem.getReceiverClass(), this, receiverName, encrypted, pStream.getOwner().getMetaInfo());
  }
  
  public boolean isRecoveryEnabled()
  {
    return this.recoveryEnabled;
  }
  
  public String getName()
  {
    return this.receiverName;
  }
  
  public FlowComponent getOwner()
  {
    return this.owner;
  }
  
  public void onMessage(Object data)
  {
    if ((data instanceof StreamEvent))
    {
      StreamEvent event = (StreamEvent)data;
      if (event.getLink() == null) {
        doReceive(event.getTaskEvent());
      } else {
        doReceive(event.getLink(), event.getTaskEvent());
      }
    }
    else if ((data instanceof PublishableEvent))
    {
      PublishableEvent pubEvent = (PublishableEvent)data;
      StreamEvent event = pubEvent.getStreamEvent();
      
      doReceive(event.getLink(), event.getTaskEvent());
    }
    else if (data != null)
    {
      throw new UnknownObjectException("Expecting Stream Event at " + this.owner.getMetaName() + ", got : " + data.getClass().toString());
    }
  }
  
  private void doReceive(ITaskEvent taskEvent)
  {
    try
    {
      this.subscriber_link.subscriber.receive(Integer.valueOf(this.subscriber_link.linkID), taskEvent);
      debugEvents(this.subscriber_distlink, taskEvent);
    }
    catch (Exception e)
    {
      logger.error("Error receiving event", e);
    }
  }
  
  public void doReceive(DistLink link, ITaskEvent event)
  {
    Position batchHighPosition = null;
    if (this.recoveryEnabled)
    {
      batchHighPosition = new Position();
      for (WAEvent ev : event.batch()) {
        batchHighPosition.mergeHigherPositions(ev.position);
      }
    }
    if (this.subscriber_link != null) {
      try
      {
        if (this.recoveryEnabled)
        {
          TaskEvent sendEvent = TaskEvent.createStreamEventWithNewWAEvents(event);
          this.subscriber_link.subscriber.receive(Integer.valueOf(this.subscriber_link.linkID), sendEvent);
        }
        else
        {
          this.subscriber_link.subscriber.receive(Integer.valueOf(this.subscriber_link.linkID), event);
        }
        debugEvents(link, event);
      }
      catch (Exception e)
      {
        logger.error("Error receiving event", e);
      }
    } else if (logger.isDebugEnabled()) {
      logger.debug(this.channel.getDebugId() + " tried to pass event to unknown subscriber " + link);
    }
  }
  
  public void addSubscriber(Link link, DistLink key)
  {
    this.subscriber_link = link;
    this.subscriber_distlink = key;
  }
  
  private void debugEvents(DistLink subscriber_distlink, ITaskEvent taskEvent)
  {
    if (logger.isDebugEnabled()) {
      logger.error("I am not yet debugging this");
    }
  }
  
  public void addServerList(Map<UUID, Set<UUID>> serverToDeployedObjects)
  {
    List<UUID> servers = new ArrayList();
    for (Map.Entry<UUID, Set<UUID>> entry : serverToDeployedObjects.entrySet()) {
      if ((entry.getValue() != null) && (((Set)entry.getValue()).contains(this.subscriber_distlink.getSubID()))) {
        servers.add(entry.getKey());
      }
    }
    createPartitions(servers);
  }
  
  private void createPartitions(List<UUID> servers)
  {
    MetaInfo.Stream streamInfo = this.channel.stream_metainfo;
    MetaInfo.PropertySet streamPropset = KafkaStreamUtils.getPropertySet(streamInfo);
    int partitions = KafkaStreamUtils.getPartitionsCount(streamInfo, streamPropset);
    
    SimplePartitionManager smp = new SimplePartitionManager(this.receiverName, 1, partitions);
    smp.setServers(servers);
    UUID this_server_uuid = BaseServer.getBaseServer().getServerID();
    for (int ii = 0; ii < partitions; ii++)
    {
      UUID node_uuid = smp.getFirstPartitionOwnerForPartition(ii);
      if (this_server_uuid.equals(node_uuid)) {
        this.partitions_i_own.add(Integer.valueOf(ii));
      }
    }
  }
  
  public void startReceiver(String name, Map<Object, Object> properties)
    throws Exception
  {
    if (this.status == Status.INITIALIZED)
    {
      if (logger.isInfoEnabled()) {
        logger.info(getName() + " owns " + this.partitions_i_own.size() + " partitions: " + this.partitions_i_own);
      }
      properties.put("partitions_i_own", this.partitions_i_own);
      this.messagingSystem.startReceiver(name, properties);
      this.status = Status.RUNNING;
    }
  }
  
  public void startEmitting(String name, PartitionedSourcePosition startPosition)
    throws Exception
  {
    this.messagingSystem.startReceiverForEmitting(name, startPosition);
  }
  
  public void stop()
  {
    try
    {
      this.messagingSystem.stopReceiver(this.receiverName);
    }
    catch (Exception e) {}
    this.status = Status.INITIALIZED;
  }
  
  public Position getComponentCheckpoint()
  {
    return this.messagingSystem.getComponentCheckpoint();
  }
}
