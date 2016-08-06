package com.bloom.kafkamessaging;

import com.bloom.messaging.Handler;
import com.bloom.messaging.MessagingSystem;
import com.bloom.messaging.NoDataConsumerException;
import com.bloom.messaging.Receiver;
import com.bloom.messaging.Sender;
import com.bloom.messaging.SocketType;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.runtime.KafkaStreamUtils;
import com.bloom.runtime.exceptions.NodeNotFoundException;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.uuid.UUID;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.RuntimeInterruptedException;
import com.bloom.jmqmessaging.KafkaReceiverInfo;
import com.bloom.messaging.ReceiverInfo;
import com.bloom.recovery.PartitionedSourcePosition;
import com.bloom.recovery.Position;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class KafkaSystem
  implements MessagingSystem
{
  private static Logger logger = Logger.getLogger(KafkaSystem.class);
  private final UUID serverID;
  private Map<UUID, List<ReceiverInfo>> HostToReceiverInfo;
  private Map<String, KafkaReceiver> receiversInThisSystem;
  
  public KafkaSystem(UUID serverID, String paramNotUsed)
  {
    this.serverID = serverID;
    
    this.HostToReceiverInfo = HazelcastSingleton.get().getMap("#HostToMessengerMap");
    synchronized (this.HostToReceiverInfo)
    {
      if (!this.HostToReceiverInfo.containsKey(this.serverID)) {
        this.HostToReceiverInfo.put(this.serverID, new ArrayList());
      }
    }
    this.receiversInThisSystem = new ConcurrentHashMap();
    if (logger.isInfoEnabled()) {
      logger.info("Kafka System Initialized: ID=" + serverID);
    }
  }
  
  public void createReceiver(Class clazz, Handler handler, String name, boolean encrypted, MetaInfo.Stream streamInfo)
  {
    if (clazz == null) {
      throw new NullPointerException("Can't create a Receiver out of a Null Class!");
    }
    if (name == null) {
      throw new NullPointerException("Name of a Receiver can't be Null!");
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Creating receiver for " + name);
    }
    KafkaReceiverInfo info = new KafkaReceiverInfo(name, KafkaStreamUtils.makeSafeKafkaTopicName(streamInfo.getFullName()));
    KafkaReceiver receiver = new KafkaReceiver(info, streamInfo, handler, encrypted);
    this.receiversInThisSystem.put(receiver.name, receiver);
  }
  
  public Receiver getReceiver(String name)
  {
    return (Receiver)this.receiversInThisSystem.get(name);
  }
  
  public void startReceiver(String name, Map<Object, Object> properties)
    throws Exception
  {
    KafkaReceiver kafkaReceiver = (KafkaReceiver)this.receiversInThisSystem.get(name);
    if (kafkaReceiver != null)
    {
      try
      {
        kafkaReceiver.start(properties);
      }
      catch (Exception e)
      {
        logger.error("Problem starting receiver", e);
        throw e;
      }
      updateReceiverMap(kafkaReceiver);
    }
    else
    {
      throw new NullPointerException(name + " Receiver is not created.");
    }
  }
  
  public void startReceiverForEmitting(String name, PartitionedSourcePosition startPosition)
    throws Exception
  {
    KafkaReceiver kafkaReceiver = (KafkaReceiver)this.receiversInThisSystem.get(name);
    if (kafkaReceiver != null)
    {
      try
      {
        if (startPosition != null) {
          kafkaReceiver.setPosition(startPosition);
        }
        kafkaReceiver.startForEmitting();
      }
      catch (Exception e)
      {
        throw e;
      }
      updateReceiverMap(kafkaReceiver);
    }
    else
    {
      throw new NullPointerException(name + " Receiver is not created.");
    }
  }
  
  public Sender getConnectionToReceiver(UUID peerID, String name, SocketType type, boolean isEncrypted)
    throws NodeNotFoundException, InterruptedException
  {
    if (peerID == null) {
      throw new NoDataConsumerException("PeerID is null, can't find component " + name + " on null.");
    }
    if (name == null) {
      throw new NullPointerException("Component name is null, looking for component with name null on WAServer with ID : " + peerID + " is not possible.");
    }
    if (type == null) {
      throw new NullPointerException("Socket type is null, can't create a Sender to " + name + " on  WAServer with ID : " + peerID);
    }
    ReceiverInfo rinfo = getReceiverInfo(peerID, name);
    if (!(rinfo instanceof KafkaReceiverInfo)) {
      throw new NodeNotFoundException(name + " is not a KafkaReceiverInfo, it is a " + rinfo.getClass());
    }
    KafkaReceiverInfo info = (KafkaReceiverInfo)rinfo;
    if (logger.isTraceEnabled()) {
      logger.trace("Sender for " + name + " on peer: " + peerID + " created");
    }
    return null;
  }
  
  public void updateReceiverMap(KafkaReceiver rcvr)
  {
    if (rcvr == null) {
      throw new NullPointerException("Receiver can't be null!");
    }
    synchronized (this.HostToReceiverInfo)
    {
      List<ReceiverInfo> kafkaReceiverInfoList = (List)this.HostToReceiverInfo.get(this.serverID);
      
      KafkaReceiverInfo info = new KafkaReceiverInfo(rcvr.name, KafkaStreamUtils.makeSafeKafkaTopicName(rcvr.name));
      if (validateCorrectness(kafkaReceiverInfoList, info))
      {
        kafkaReceiverInfoList.add(info);
        this.HostToReceiverInfo.put(this.serverID, kafkaReceiverInfoList);
        if (logger.isTraceEnabled()) {
          logger.trace("Kafka Messengers on current(" + this.serverID.toString() + ") Kafka System : \n" + this.receiversInThisSystem.keySet());
        }
      }
      else if (logger.isDebugEnabled())
      {
        logger.debug("Receiver " + rcvr.name + " already exists");
      }
    }
  }
  
  public boolean validateCorrectness(List<ReceiverInfo> kafkaReceiverInfoList, KafkaReceiverInfo info)
  {
    if (info == null) {
      throw new NullPointerException("Can't validate correctness of a Null Object!");
    }
    if (kafkaReceiverInfoList.isEmpty()) {
      return true;
    }
    for (ReceiverInfo mInfo : kafkaReceiverInfoList) {
      if (mInfo.equals(info)) {
        return false;
      }
    }
    return true;
  }
  
  private ReceiverInfo getReceiverInfo(UUID uuid, String name)
    throws InterruptedException
  {
    ReceiverInfo info = null;
    int count = 0;
    while (info == null)
    {
      synchronized (this.HostToReceiverInfo)
      {
        List<ReceiverInfo> returnInfo = null;
        try
        {
          returnInfo = (List)this.HostToReceiverInfo.get(uuid);
        }
        catch (RuntimeInterruptedException e)
        {
          throw new InterruptedException(e.getMessage());
        }
        if ((returnInfo != null) && (!returnInfo.isEmpty())) {
          for (ReceiverInfo anInfo : returnInfo) {
            if (name.equals(anInfo.getName())) {
              info = anInfo;
            }
          }
        }
      }
      if ((info != null) || (count >= 10)) {
        break;
      }
      Thread.sleep(500L);
      count++;
    }
    return info;
  }
  
  public boolean stopReceiver(String name)
    throws Exception
  {
    if (name == null) {
      throw new NullPointerException("Can't Undeploy a Receiver with Null Value!");
    }
    boolean isAlive = killReceiver(name);
    synchronized (this.HostToReceiverInfo)
    {
      ArrayList<ReceiverInfo> rcvrs = (ArrayList)this.HostToReceiverInfo.get(this.serverID);
      boolean removed = false;
      for (ReceiverInfo rcvrInfo : rcvrs) {
        if (name.equalsIgnoreCase(rcvrInfo.getName()))
        {
          removed = rcvrs.remove(rcvrInfo);
          break;
        }
      }
      if ((!isAlive) && (removed))
      {
        if (logger.isDebugEnabled()) {
          logger.debug("Stopped Receiver : " + name);
        }
      }
      else {
        throw new RuntimeException("Receiver : " + name + " not found");
      }
      this.HostToReceiverInfo.put(this.serverID, rcvrs);
      this.receiversInThisSystem.remove(name);
    }
    if (logger.isTraceEnabled())
    {
      logger.trace("HostToReceiver : " + this.HostToReceiverInfo.entrySet());
      logger.trace("All receievers : " + this.receiversInThisSystem.entrySet());
    }
    return isAlive;
  }
  
  public boolean killReceiver(String name)
    throws Exception
  {
	  boolean isAlive;
    KafkaReceiver messenger = (KafkaReceiver)this.receiversInThisSystem.get(name);
    if (messenger != null)
    {
       isAlive = messenger.stop();
      while (isAlive)
      {
        isAlive = messenger.stop();
        if (logger.isDebugEnabled()) {
          logger.debug("Trying to stop receiver : " + name);
        }
        Thread.sleep(100L);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Stopped Receiver : " + name);
      }
    }
    else
    {
      throw new NullPointerException(name + " Receiver does not exist");
    }
   
    return isAlive;
  }
  
  public boolean stopAllReceivers()
  {
    boolean isAlive = false;
    for (String name : this.receiversInThisSystem.keySet()) {
      try
      {
        isAlive = stopReceiver(name);
      }
      catch (Exception e)
      {
        logger.debug(e.getMessage());
      }
    }
    return isAlive;
  }
  
  public void shutdown()
  {
    boolean isAlive = stopAllReceivers();
    while (isAlive)
    {
      try
      {
        Thread.sleep(200L);
        isAlive = stopAllReceivers();
      }
      catch (InterruptedException e) {}
      logger.debug("Tryin to Stop All Receivers");
    }
    synchronized (this.HostToReceiverInfo)
    {
      this.HostToReceiverInfo.remove(this.serverID.toString());
      this.receiversInThisSystem.clear();
    }
  }
  
  public boolean cleanupAllReceivers()
  {
    boolean isAlive = true;
    for (String name : this.receiversInThisSystem.keySet()) {
      try
      {
        isAlive = killReceiver(name);
      }
      catch (Exception e)
      {
        logger.error(e.getMessage());
      }
    }
    return isAlive;
  }
  
  public Map.Entry<UUID, ReceiverInfo> searchStreamName(String streamName)
  {
	   UUID peer;
    synchronized (this.HostToReceiverInfo)
    {
      Set<UUID> peerUUID = this.HostToReceiverInfo.keySet();
      for (Iterator i$ = peerUUID.iterator(); i$.hasNext();)
      {
        peer = (UUID)i$.next();
        List<ReceiverInfo> receiverList = (List)this.HostToReceiverInfo.get(peer);
        final UUID pUuid = peer;
        for (final ReceiverInfo receiverInfo : receiverList) {
          if (receiverInfo.getName().startsWith(streamName)) {
            new Map.Entry()
            {
              public ReceiverInfo setValue(ReceiverInfo value)
              {
                return null;
              }
              
              public ReceiverInfo getValue()
              {
                return receiverInfo;
              }
              
              public UUID getKey()
              {
                return pUuid;
              }
            };
          }
        }
      }
      
      return null;
    }
  }
  
  public Class getReceiverClass()
  {
    return KafkaReceiver.class;
  }
  
  public Position getComponentCheckpoint()
  {
    Position result = new Position();
    for (KafkaReceiver r : this.receiversInThisSystem.values()) {
      result.mergeHigherPositions(r.getComponentCheckpoint());
    }
    return result;
  }
}
