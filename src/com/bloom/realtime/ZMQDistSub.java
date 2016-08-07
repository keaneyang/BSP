package com.bloom.runtime;

import com.bloom.kafkamessaging.KafkaSystem;
import com.bloom.messaging.InterThreadComm;
import com.bloom.messaging.MessagingSystem;
import com.bloom.messaging.Sender;
import com.bloom.messaging.SocketType;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.runtime.channels.DistributedChannel;
import com.bloom.runtime.channels.KafkaChannel;
import com.bloom.runtime.containers.TaskEvent;
import com.bloom.runtime.exceptions.NodeNotFoundException;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.uuid.UUID;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.bloom.recovery.Path;
import com.bloom.recovery.Position;
import com.bloom.recovery.PositionResponse;
import com.bloom.recovery.SourcePosition;
import com.bloom.runtime.DistLink;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.StreamEvent;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;
import org.zeromq.ZMQException;
import scala.NotImplementedError;
import zmq.ZError;
import zmq.ZError.IOException;

public abstract class ZMQDistSub
  extends DistSub
  implements ChannelEventHandler, MessageListener<PositionResponse>
{
  protected static Logger logger = Logger.getLogger(ZMQDistSub.class);
  protected final String streamSubscriberName;
  public DistLink dist_link;
  protected BaseServer srvr;
  protected DistributedChannel pStream;
  protected final boolean encrypted;
  protected ConsistentHashRing ring;
  protected DistributedRcvr distributed_rcvr;
  final Map<UUID, Map<Integer, Sender>> senderMap;
  public Status status;
  private IQueue<String> evTS = null;
  private Position highReceived = new Position();
  private Position lowReceived = new Position();
  private Position highConfirmed = new Position();
  protected long unProcessed = 0L;
  protected final boolean enableParallelSender = false;
  protected UUID firstPeer;
  int numSenderThreads = new Integer(System.getProperty("com.bloom.config.sendCapacity", "1")).intValue();
  ArrayList<InterThreadComm> queues = new ArrayList(this.numSenderThreads);
  private final String recoveryTopicName;
  private String positionRequestMessageListenerRegId = null;
  protected boolean debugEventCountMatch = false;
  protected boolean debugEventLoss = false;
  Map<UUID, Integer> peerCounts = new ConcurrentHashMap();
  
  public static enum Status
  {
    UNKNOWN,  INITIALIZED,  STARTED,  STOPPED;
    
    private Status() {}
  }
  
  public ZMQDistSub(DistLink l, DistributedChannel pStream, BaseServer srvr, boolean encrypted)
  {
    this.dist_link = l;
    this.srvr = srvr;
    this.pStream = pStream;
    this.encrypted = encrypted;
    this.streamSubscriberName = (pStream.getMetaObject().getName() + "-" + this.dist_link.getName());
    this.senderMap = new ConcurrentHashMap();
    this.status = Status.INITIALIZED;
    if (this.debugEventLoss)
    {
      logger.debug("id->name " + this.dist_link.getSubID() + " = " + this.dist_link.getName());
      this.evTS = HazelcastSingleton.get().getQueue(pStream.getMetaObject().getUuid() + "-" + this.dist_link.getSubID() + "-evTS");
    }
    this.recoveryTopicName = ("#StreamConfirm_" + this.pStream.getMetaObject().getUuid());
  }
  
  public void sendEvent(EventContainer container, int sendQueueId)
    throws InterruptedException
  {
    Sender sender = container.getDistSub().getSender(container.getPartitionId(), sendQueueId);
    if (sender == null)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("Got null Sender so dropping message for stream " + this.streamSubscriberName);
      }
      return;
    }
    WAEvent waEvent = (WAEvent)container.getEvent();
    StreamEvent streamEvent = new StreamEvent(StreamEventFactory.createStreamEvent(waEvent.data, waEvent.position), container.getDistSub().dist_link);
    try
    {
      boolean success = sender.send(streamEvent);
      if (!success) {
        if (logger.isDebugEnabled()) {
          logger.debug("Failed to send message on stream " + this.streamSubscriberName);
        }
      }
    }
    catch (Throwable t)
    {
      if ((t instanceof ZError.IOException)) {
        throw new InterruptedException();
      }
      t.printStackTrace();
    }
  }
  
  public synchronized void sendParitioned(WAEvent waEvent, RecordKey key)
    throws InterruptedException
  {
    if ((this.status != Status.STARTED) || (this.ring.size() == 0))
    {
      logDroppedEvents();
    }
    else
    {
      int partitionId = getPartitionId(key);
      try
      {
        if (waEvent.position != null)
        {
          WAEvent augmentedWaEvent = (WAEvent)waEvent.getClass().newInstance();
          augmentedWaEvent.initValues(waEvent);
          augmentedWaEvent.position = augmentedWaEvent.position.createAugmentedPosition(this.pStream.getMetaObject().getUuid(), String.valueOf(partitionId));
          this.highReceived.mergeHigherPositions(augmentedWaEvent.position);
          this.lowReceived.mergeLowerPositions(augmentedWaEvent.position);
          waEvent = augmentedWaEvent;
        }
      }
      catch (InstantiationException e)
      {
        logger.error(e);
      }
      catch (IllegalAccessException e)
      {
        logger.error(e);
      }
      EventContainer container = new EventContainer();
      container.setAllFields(waEvent, this, partitionId, this);
      sendEvent(container, 0);
      if (this.debugEventLoss) {
        this.evTS.add(waEvent.toString());
      }
      if (this.debugEventCountMatch)
      {
        UUID peerID = getPeerId(partitionId);
        Integer peerCount = (Integer)this.peerCounts.get(peerID);
        this.peerCounts.put(peerID, Integer.valueOf(peerCount == null ? 1 : peerCount.intValue() + 1));
      }
    }
  }
  
  private void logDroppedEvents()
  {
    this.unProcessed += 1L;
    if (this.ring != null)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("Unprocessed events: " + this.unProcessed + " on stream " + this.pStream.getMetaObject().getName() + " for subscriber " + this.dist_link.getName() + ", status: " + this.status.name() + ", ring size : " + this.ring.size());
      }
    }
    else if (logger.isDebugEnabled()) {
      logger.debug("Unprocessed events: " + this.unProcessed + " on stream " + this.pStream.getMetaObject().getName() + " for subscriber " + this.dist_link.getName() + ", status: " + this.status.name());
    }
  }
  
  public synchronized void sendUnpartitioned(ITaskEvent taskEvent, UUID localPeerId)
    throws InterruptedException
  {
    if ((this.status != Status.STARTED) || (this.ring.size() == 0))
    {
      logDroppedEvents();
    }
    else
    {
      boolean here = hasThisPeer(localPeerId);
      UUID peerID = here ? localPeerId : getFirstPeer();
      Sender sender = getSender(peerID, 0);
      if (sender == null)
      {
        if ((logger.isDebugEnabled()) && (peerID != null)) {
          logger.warn("Got null Sender for " + peerID + " here " + here + " localPeerId " + localPeerId + " so dropping message for stream " + this.streamSubscriberName);
        }
        return;
      }
      boolean needDupeEvent = false;
      for (WAEvent waEvent : taskEvent.batch()) {
        if ((waEvent.position != null) && (!waEvent.position.isEmpty()))
        {
          needDupeEvent = true;
          break;
        }
      }
      if (needDupeEvent)
      {
        taskEvent = TaskEvent.createStreamEventWithNewWAEvents(taskEvent);
        for (WAEvent waEvent : taskEvent.batch()) {
          if ((waEvent.position != null) && (!waEvent.position.isEmpty()))
          {
            Position augmentedPosition = waEvent.position.createAugmentedPosition(this.pStream.getMetaObject().getUuid(), null);
            this.highReceived.mergeHigherPositions(augmentedPosition);
            this.lowReceived.mergeLowerPositions(augmentedPosition);
            waEvent.position = augmentedPosition;
          }
        }
      }
      StreamEvent ae = new StreamEvent(taskEvent, this.dist_link);
      try
      {
        boolean success = sender.send(ae);
        if (!success) {
          if (logger.isDebugEnabled()) {
            logger.debug("Failed to send messages on stream " + this.streamSubscriberName);
          }
        }
      }
      catch (ZMQException t)
      {
        logger.error("Message queue error", t);
      }
      if (this.debugEventLoss) {
        for (WAEvent waEvent : taskEvent.batch()) {
          this.evTS.add(waEvent.toString());
        }
      }
      if (this.debugEventCountMatch)
      {
        Integer peerCount = (Integer)this.peerCounts.get(peerID);
        this.peerCounts.put(peerID, Integer.valueOf(peerCount == null ? new Integer(1).intValue() : peerCount.intValue() + 1));
      }
    }
  }
  
  public void stop()
  {
    if (this.status != Status.STARTED)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("stream subscriber " + this.streamSubscriberName + " is not started so not stopping");
      }
      return;
    }
    removeTopicListener();
    if (this.unProcessed > 0L)
    {
      logger.warn("Stream " + this.streamSubscriberName + " discarded " + this.unProcessed + " events before it was started");
      this.unProcessed = 0L;
    }
    for (Map<Integer, Sender> senders : this.senderMap.values())
    {
      for (Sender ss : senders.values()) {
        ss.stop();
      }
      senders.clear();
    }
    if (this.distributed_rcvr != null) {
      this.distributed_rcvr.stop();
    }
    this.senderMap.clear();
    this.status = Status.STOPPED;
    this.ring.shutDown();
    this.ring = null;
    if (this.debugEventCountMatch)
    {
      logger.warn(this.streamSubscriberName + ": Current PeerCounts: " + this.peerCounts);
      this.peerCounts.clear();
    }
    postStopHook();
    logger.info("Stopped subscriber: " + this.streamSubscriberName + " for stream: " + this.pStream.getMetaObject().getFullName());
  }
  
  public int getPartitionId(Object key)
  {
    return this.ring.getPartitionId(key);
  }
  
  public boolean hasThisPeer(UUID thisPeerId)
  {
    if (this.ring != null) {
      return this.ring.getNodes().contains(thisPeerId);
    }
    return false;
  }
  
  public UUID getPeerId(int partitionId)
  {
    return this.ring.getUUIDForPartition(partitionId, 0);
  }
  
  public Sender getSender(int partitionId, int senderThreadId)
    throws InterruptedException
  {
    UUID peerId = getPeerId(partitionId);
    return getSender(peerId, senderThreadId);
  }
  
  private Sender getSender(UUID peerId, int senderThreadId)
    throws InterruptedException
  {
    if (peerId == null) {
      return null;
    }
    Map<Integer, Sender> senders = (Map)this.senderMap.get(peerId);
    if (senders == null)
    {
      senders = new HashMap();
      this.senderMap.put(peerId, senders);
    }
    Sender sender = null;
    if (senderThreadId < senders.size()) {
      sender = (Sender)senders.get(Integer.valueOf(senderThreadId));
    }
    if (sender == null)
    {
      String receiver_name = null;
      try
      {
        if ((this.pStream.messagingSystem instanceof KafkaSystem)) {
          receiver_name = KafkaChannel.buildReceiverName(this.pStream.getMetaObject());
        } else {
          receiver_name = DistributedChannel.buildReceiverName(this.pStream.getMetaObject(), this.dist_link.getSubID());
        }
        sender = this.pStream.messagingSystem.getConnectionToReceiver(peerId, receiver_name, SocketType.PUSH, this.encrypted);
      }
      catch (InterruptedException e)
      {
        throw e;
      }
      catch (NodeNotFoundException e)
      {
        logger.warn("Could not find node with UUID: " + peerId + " on which receiver " + receiver_name + " is expected to run", e);
      }
      if (sender != null) {
        senders.put(Integer.valueOf(senderThreadId), sender);
      }
    }
    return sender;
  }
  
  public void removeDistributedRcvr()
  {
    this.distributed_rcvr.close();
  }
  
  protected void addTopicListener()
  {
    ITopic<PositionResponse> topic = HazelcastSingleton.get().getTopic(this.recoveryTopicName);
    this.positionRequestMessageListenerRegId = topic.addMessageListener(this);
  }
  
  protected void removeTopicListener()
  {
    ITopic<PositionResponse> topic = HazelcastSingleton.get().getTopic(this.recoveryTopicName);
    topic.removeMessageListener(this.positionRequestMessageListenerRegId);
    this.positionRequestMessageListenerRegId = null;
  }
  
  public boolean isFull()
  {
    Map<Integer, Sender> senders = (Map)this.senderMap.get(HazelcastSingleton.getNodeId());
    if (senders != null) {
      for (Sender sender : senders.values()) {
        if (sender.isFull()) {
          return true;
        }
      }
    }
    return false;
  }
  
  public void onMessage(Message<PositionResponse> message)
  {
    PositionResponse pr = (PositionResponse)message.getMessageObject();
    String remoteDistId = pr.toDistID;
    String localDistId = String.valueOf(this.dist_link.getLinkID());
    if ((pr.toUUID.equals(this.pStream.getMetaObject().getUuid())) && 
      (remoteDistId.equals(localDistId)))
    {
      Position confirmedPosition = ((PositionResponse)message.getMessageObject()).position;
      if (logger.isDebugEnabled()) {
        logger.debug("DistSub for " + this.pStream.getMetaObject().getName() + " received confirmation of " + confirmedPosition);
      }
      this.highConfirmed.mergeHigherPositions(confirmedPosition);
    }
  }
  
  public Position getCheckpoint()
  {
    synchronized (this.highConfirmed)
    {
      Position result = new Position(this.lowReceived);
      
      Set<Integer> processedKeys = new HashSet();
      Set<Integer> removeFromReceived = new HashSet();
      for (Integer pathHash : this.highConfirmed.keySet()) {
        if (this.lowReceived.containsKey(pathHash.intValue()))
        {
          Path confirmedPath = this.highConfirmed.get(pathHash);
          this.lowReceived.mergeHigherPath(confirmedPath);
          
          SourcePosition lowRsp = this.lowReceived.get(pathHash).getSourcePosition();
          SourcePosition highRsp = this.highReceived.get(pathHash).getSourcePosition();
          if (lowRsp.compareTo(highRsp) == 0)
          {
            removeFromReceived.add(pathHash);
          }
          else if (lowRsp.compareTo(highRsp) > 0)
          {
            logger.warn("Unexpected advanced low stream position past high stream position: in " + this.streamSubscriberName);
            this.highReceived.mergeHigherPath(this.lowReceived.get(pathHash));
          }
          processedKeys.add(pathHash);
        }
      }
      this.highConfirmed = this.highConfirmed.createPositionWithoutPaths(processedKeys);
      
      this.lowReceived = this.lowReceived.createPositionWithoutPaths(removeFromReceived);
      this.highReceived = this.highReceived.createPositionWithoutPaths(removeFromReceived);
      
      return result;
    }
  }
  
  public void startEmitting(Position position)
  {
    throw new NotImplementedError();
  }
}
