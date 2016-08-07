package com.bloom.runtime;

import com.bloom.messaging.MessagingSystem;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.runtime.channels.DistributedChannel;
import com.bloom.runtime.components.Link;
import com.bloom.runtime.components.Stream;
import com.bloom.runtime.exceptions.NodeNotFoundException;
import com.bloom.uuid.UUID;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.bloom.runtime.DistLink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.log4j.Logger;

public class UnManagedZMQDistSub
  extends ZMQDistSub
  implements ItemListener<UUID>, MembershipListener
{
  private static Logger logger = Logger.getLogger(UnManagedZMQDistSub.class);
  private ISet<UUID> peers;
  private String memberShipId;
  private String itemListenerRegisteredId;
  
  public UnManagedZMQDistSub(DistLink l, DistributedChannel pStream, BaseServer srvr, boolean encrypted)
  {
    super(l, pStream, srvr, encrypted);
    String ringName = "STREAM: " + this.streamSubscriberName;
    this.memberShipId = HazelcastSingleton.get().getCluster().addMembershipListener(this);
    this.peers = HazelcastSingleton.get().getSet("#StreamSubsPeersList-" + pStream.getMetaObject().getUuid() + "-" + this.dist_link.getHash());
    if (logger.isDebugEnabled()) {
      logger.debug("--> DistSub Created: " + ringName);
    }
  }
  
  protected UUID getFirstPeer()
  {
    if (this.firstPeer == null)
    {
      Iterator it = this.peers.iterator();
      if (it.hasNext()) {
        this.firstPeer = ((UUID)it.next());
      }
    }
    return this.firstPeer;
  }
  
  public void addPeer(UUID peer)
  {
    addToPeersList(peer);
    if (logger.isDebugEnabled()) {
      logger.debug("Sending peer request for peer " + peer + " stream " + this.streamSubscriberName);
    }
    registerServer(peer);
  }
  
  public void addToPeersList(UUID peer)
  {
    this.peers.add(peer);
    this.firstPeer = null;
  }
  
  public int getPeerCount()
  {
    return this.peers.size();
  }
  
  public String toString()
  {
    return this.dist_link + " " + Arrays.toString(this.peers.toArray());
  }
  
  public void addDistributedRcvr(String receiverName, Stream owner, DistributedChannel d_channel, boolean encrypted, MessagingSystem messagingSystem, Link link, Map<UUID, Set<UUID>> thisisimportant)
  {
    this.distributed_rcvr = new DistributedRcvr(receiverName, owner, d_channel, encrypted, messagingSystem);
    try
    {
      this.distributed_rcvr.startReceiver(receiverName, new HashMap());
    }
    catch (Exception e)
    {
      logger.error(e.getMessage(), e);
    }
    this.distributed_rcvr.addSubscriber(link, this.dist_link);
  }
  
  public void close(UUID peerId)
  {
    stop();
    removeFromPeersList(peerId);
    HazelcastSingleton.get().getCluster().removeMembershipListener(this.memberShipId);
    if (this.peers.size() == 0)
    {
      this.peers.destroy();
      this.peers = null;
    }
    this.pStream = null;
    this.dist_link = null;
    this.srvr = null;
  }
  
  public synchronized void start()
  {
    this.ring = new ConsistentHashRing("STREAM: " + this.streamSubscriberName, 1);
    this.status = ZMQDistSub.Status.STARTED;
    this.itemListenerRegisteredId = this.peers.addItemListener(this, true);
    Iterator it = this.peers.iterator();
    if (logger.isDebugEnabled()) {
      logger.debug("Peers in the list : " + Arrays.toString(this.peers.toArray()));
    }
    while (it.hasNext())
    {
      UUID peer = (UUID)it.next();
      
      registerServer(peer);
    }
    super.addTopicListener();
    if (logger.isInfoEnabled()) {
      logger.info("Started subscriber: " + this.streamSubscriberName + " for stream: " + this.pStream.getName());
    }
  }
  
  public void start(Map<UUID, Set<UUID>> servers)
  {
    start();
  }
  
  public void startDistributedReceiver(Map<UUID, Set<UUID>> thisisimportant) {}
  
  public void postStopHook()
  {
    this.peers.removeItemListener(this.itemListenerRegisteredId);
  }
  
  public void itemAdded(ItemEvent<UUID> uuidItemEvent)
  {
    UUID peer = (UUID)uuidItemEvent.getItem();
    registerServer(peer);
  }
  
  public void itemRemoved(ItemEvent<UUID> uuidItemEvent) {}
  
  private void registerServer(UUID peer)
  {
    try
    {
      if (this.status != ZMQDistSub.Status.STARTED)
      {
        logger.debug("Stream " + this.streamSubscriberName + " is not started so cannot register peer " + peer);
        return;
      }
      if (this.ring.getNodes().contains(peer))
      {
        if (logger.isDebugEnabled()) {
          logger.debug("node " + peer + " is already registered to stream " + this.streamSubscriberName);
        }
        return;
      }
      this.ring.add(peer);
      if (logger.isDebugEnabled()) {
        logger.debug("Adding node " + peer + " to stream " + this.streamSubscriberName);
      }
    }
    catch (NodeNotFoundException e)
    {
      removeFromPeersList(peer);
    }
  }
  
  private void removeFromPeersList(UUID peer)
  {
    this.peers.remove(peer);
    this.firstPeer = null;
  }
  
  private synchronized void deregisterServer(ItemEvent<UUID> uuidItemEvent)
  {
    if (this.status == ZMQDistSub.Status.STARTED) {
      return;
    }
    UUID peer = (UUID)uuidItemEvent.getItem();
    if (this.ring != null) {
      try
      {
        this.ring.remove(peer);
      }
      catch (NodeNotFoundException e)
      {
        removeFromPeersList(peer);
      }
    }
  }
  
  public boolean verifyStart(Map<UUID, Set<UUID>> serverToDeployedObjects)
  {
    List<UUID> servers = new ArrayList();
    for (Map.Entry<UUID, Set<UUID>> entry : serverToDeployedObjects.entrySet()) {
      if ((entry.getValue() != null) && (((Set)entry.getValue()).contains(this.dist_link.getSubID()))) {
        servers.add(entry.getKey());
      }
    }
    if (this.ring.getNodes().containsAll(servers)) {
      return true;
    }
    servers.removeAll(this.ring.getNodes());
    if (logger.isInfoEnabled()) {
      logger.info("Failed verifying all the subscription for stream " + this.streamSubscriberName + ". It is still waiting to get subscription from " + servers + ". Peers contain " + Arrays.toString(this.peers.toArray()));
    }
    return false;
  }
  
  public void memberAdded(MembershipEvent membershipEvent) {}
  
  public void memberRemoved(MembershipEvent membershipEvent)
  {
    removeFromPeersList(new UUID(membershipEvent.getMember().getUuid()));
  }
  
  public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {}
}
