package com.bloom.runtime;

import com.bloom.kafkamessaging.KafkaSystem;
import com.bloom.messaging.MessagingSystem;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.runtime.channels.DistributedChannel;
import com.bloom.runtime.channels.KafkaChannel;
import com.bloom.runtime.components.Link;
import com.bloom.runtime.components.Stream;
import com.bloom.uuid.UUID;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISet;
import com.bloom.runtime.DistLink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.log4j.Logger;

public class ManagedZMQDistSub
  extends ZMQDistSub
{
  private ISet<UUID> peersFoundByAddingToISet;
  private Set<UUID> peersPassedDownFromAppManager;
  
  public ManagedZMQDistSub(DistLink key, DistributedChannel distributedChannelInterface, BaseServer srv, boolean encrypted)
  {
    super(key, distributedChannelInterface, srv, encrypted);
    this.peersFoundByAddingToISet = HazelcastSingleton.get().getSet("#StreamSubsPeersList-" + this.pStream.getMetaObject().getUuid() + "-" + this.dist_link.getHash());
    this.peersPassedDownFromAppManager = new HashSet();
  }
  
  protected UUID getFirstPeer()
  {
    if (this.firstPeer == null)
    {
      Iterator it;
      if (!(Server.getBaseServer() instanceof Server)) {
        it = this.peersFoundByAddingToISet.iterator();
      } else {
        it = this.peersPassedDownFromAppManager.iterator();
      }
      if (it.hasNext()) {
        this.firstPeer = ((UUID)it.next());
      }
    }
    return this.firstPeer;
  }
  
  public void addDistributedRcvr(String receiverName, Stream owner, DistributedChannel d_channel, boolean encrypted, MessagingSystem messagingSystem, Link link, Map<UUID, Set<UUID>> thisisimportant)
  {
    this.distributed_rcvr = new DistributedRcvr(receiverName, owner, d_channel, encrypted, messagingSystem);
    this.distributed_rcvr.addSubscriber(link, this.dist_link);
    this.distributed_rcvr.addServerList(thisisimportant);
  }
  
  public void start()
  {
    if (this.status != ZMQDistSub.Status.STARTED) {
      if (!(Server.getBaseServer() instanceof Server))
      {
        this.ring = new ConsistentHashRing("STREAM: " + this.streamSubscriberName, 1);
        List<UUID> servers = new ArrayList(this.peersFoundByAddingToISet);
        this.ring.set(servers);
        
        this.status = ZMQDistSub.Status.STARTED;
        super.addTopicListener();
        if (logger.isInfoEnabled()) {
          logger.info("Starting subscriber: " + this.streamSubscriberName + " for stream: " + this.pStream.getName() + " on AGENT");
        }
      }
      else
      {
        logger.warn("Unexpected point while starting subscriber: " + this.streamSubscriberName + " for stream: " + this.pStream.getName());
        Thread.dumpStack();
      }
    }
  }
  
  public void start(Map<UUID, Set<UUID>> serverToDeployedObjects)
  {
    if (this.status != ZMQDistSub.Status.STARTED)
    {
      List<UUID> servers = new ArrayList();
      for (Map.Entry<UUID, Set<UUID>> entry : serverToDeployedObjects.entrySet()) {
        if ((entry.getValue() != null) && (((Set)entry.getValue()).contains(this.dist_link.getSubID()))) {
          servers.add(entry.getKey());
        }
      }
      this.peersPassedDownFromAppManager.addAll(servers);
      this.ring = new ConsistentHashRing("STREAM: " + this.streamSubscriberName, 1);
      this.ring.set(servers);
      
      this.status = ZMQDistSub.Status.STARTED;
      super.addTopicListener();
      if (logger.isInfoEnabled()) {
        logger.info("Started subscriber: " + this.streamSubscriberName + " for stream: " + this.pStream.getName());
      }
    }
  }
  
  public void startDistributedReceiver(Map<UUID, Set<UUID>> thisisimportant)
  {
    if (this.distributed_rcvr != null)
    {
      String receiver_name;
      if ((this.pStream.messagingSystem instanceof KafkaSystem)) {
        receiver_name = KafkaChannel.buildReceiverName(this.pStream.getMetaObject());
      } else {
        receiver_name = DistributedChannel.buildReceiverName(this.pStream.getMetaObject(), this.dist_link.getSubID());
      }
      try
      {
        this.distributed_rcvr.startReceiver(receiver_name, new HashMap());
      }
      catch (Exception e)
      {
        logger.error(e.getMessage(), e);
      }
    }
  }
  
  public void postStopHook() {}
  
  public void close(UUID peerID)
  {
    stop();
    this.peersFoundByAddingToISet.remove(peerID);
    this.firstPeer = null;
    this.pStream = null;
    this.dist_link = null;
    this.srvr = null;
  }
  
  public boolean verifyStart(Map<UUID, Set<UUID>> serverToDeployedObjects)
  {
    return true;
  }
  
  public void addPeer(UUID thisPeerId)
  {
    this.peersFoundByAddingToISet.add(thisPeerId);
    if (!(Server.getBaseServer() instanceof Server)) {
      resetRing();
    }
  }
  
  private void resetRing()
  {
    List<UUID> servers = new ArrayList(this.peersFoundByAddingToISet);
    if (this.ring != null) {
      this.ring.set(servers);
    }
  }
  
  public int getPeerCount()
  {
    return 0;
  }
}
