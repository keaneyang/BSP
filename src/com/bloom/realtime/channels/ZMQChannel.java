package com.bloom.runtime.channels;

import com.bloom.exception.RuntimeInterruptedException;
import com.bloom.jmqmessaging.ZMQSystem;
import com.bloom.messaging.MessagingProvider;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepositoryUtils;
import com.bloom.runtime.BaseServer;
import com.bloom.runtime.DistSub;
import com.bloom.runtime.KeyFactory;
import com.bloom.runtime.ManagedZMQDistSub;
import com.bloom.runtime.Server;
import com.bloom.runtime.UnManagedZMQDistSub;
import com.bloom.runtime.ZMQDistSub;
import com.bloom.runtime.components.Link;
import com.bloom.runtime.components.Stream;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.monitor.MonitorEvent;
import com.bloom.uuid.UUID;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Member;
import com.bloom.recovery.Position;
import com.bloom.runtime.DistLink;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class ZMQChannel
  extends DistributedChannel
  implements ItemListener<DistLink>
{
  private static Logger logger = Logger.getLogger(ZMQChannel.class);
  private ISet<DistLink> subscribers;
  public final ConcurrentHashMap<DistLink, ZMQDistSub> subscriptions = new ConcurrentHashMap();
  private Channel.NewSubscriberAddedCallback callback;
  private String itemListenerId;
  private Map<UUID, Set<UUID>> thisisimportant;
  
  public ZMQChannel(Stream owner)
  {
    super(owner);
    if (owner == null) {
      throw new RuntimeException("DistributedKryoChannel has no owner");
    }
    if (isCurrentStreamEncrypted()) {
      this.encrypted = true;
    }
    this.messagingSystem = MessagingProvider.getMessagingSystem(MessagingProvider.ZMQ_SYSTEM);
    this.subscribers = HazelcastSingleton.get().getSet("#sharedStreamSubs-" + getID());
    this.itemListenerId = this.subscribers.addItemListener(this, true);
    
    Iterator it = this.subscribers.iterator();
    while (it.hasNext())
    {
      DistLink distLink = (DistLink)it.next();
      if ((!distLink.getName().equals("Global.ExceptionManager")) && (!distLink.getName().equals("MonitoringCQ"))) {
        if (logger.isInfoEnabled()) {
          logger.info("Found subscriber: " + distLink.getName() + "for stream: " + owner.getMetaFullName());
        }
      }
    }
    if (logger.isInfoEnabled()) {
      logger.info(getDebugId() + " distributed channel created");
    }
  }
  
  public ZMQChannel(BaseServer srvr, Stream owner)
  {
    super(owner);
    if (owner == null) {
      throw new RuntimeException("DistributedKryoChannel has no owner");
    }
    this.srv = srvr;
    if (isCurrentStreamEncrypted()) {
      this.encrypted = true;
    }
    this.messagingSystem = MessagingProvider.getMessagingSystem(MessagingProvider.ZMQ_SYSTEM);
    this.subscribers = HazelcastSingleton.get().getSet("#sharedStreamSubs-" + getID());
    this.itemListenerId = this.subscribers.addItemListener(this, true);
    
    Iterator it = this.subscribers.iterator();
    while (it.hasNext())
    {
      DistLink distLink = (DistLink)it.next();
      if ((!distLink.getName().equals("Global.ExceptionManager")) && (!distLink.getName().equals("MonitoringCQ"))) {
        if (logger.isInfoEnabled()) {
          logger.info("Found subscriber: " + distLink.getName() + "for stream: " + owner.getMetaFullName());
        }
      }
      if (!(srvr instanceof Server)) {
        createGetDistSub(distLink, true);
      } else {
        createGetDistSub(distLink);
      }
    }
    if (logger.isInfoEnabled()) {
      logger.info(getDebugId() + " distributed channel created");
    }
  }
  
  public Set<DistLink> getSubscribers()
  {
    return this.subscriptions.keySet();
  }
  
  public void closeDistSub(String uuid)
  {
    Map.Entry<DistLink, ZMQDistSub> current = null;
    for (Map.Entry<DistLink, ZMQDistSub> entry : this.subscriptions.entrySet()) {
      if (((ZMQDistSub)entry.getValue()).hasThisPeer(new UUID(uuid)))
      {
        current = entry;
        break;
      }
    }
    if (current != null)
    {
      ((ZMQDistSub)current.getValue()).stop();
      boolean removed = this.subscribers.remove(current.getKey());
      this.subscriptions.remove(current.getKey());
      if (removed) {
        if (logger.isInfoEnabled()) {
          logger.info("Tungsten subscription to showStream removed, current count in Subscriber ISet: " + this.subscribers.size() + " and count is subscription local map: " + this.subscriptions.size());
        } else if (logger.isInfoEnabled()) {
          logger.info("Tunsgten subscription to showStream was not removed, current count in Subscriber ISet: " + this.subscribers.size() + " and count is subscription local map: " + this.subscriptions.size());
        }
      }
    }
  }
  
  public Stream getOwner()
  {
    return this.owner;
  }
  
  private boolean isCurrentStreamEncrypted()
  {
    MetaInfo.Stream currentStreamInfo = getStreamInfo();
    MetaInfo.Flow appInfo = MetadataRepositoryUtils.getAppMetaObjectBelongsTo(currentStreamInfo);
    if (appInfo != null)
    {
      if (logger.isInfoEnabled()) {
        logger.info(currentStreamInfo.name + " belongs to app:" + appInfo.name + ", encryption:" + appInfo.encrypted);
      }
      return appInfo.encrypted;
    }
    return false;
  }
  
  public void publish(ITaskEvent event)
    throws Exception
  {
    if (this.status != DistributedChannel.Status.RUNNING) {
      return;
    }
    try
    {
      Iterator i$;
      WAEvent waEvent;
      RecordKey key;
      if (this.keyFactory != null)
      {
        IBatch<WAEvent> waEventIBatch = event.batch();
        for (i$ = waEventIBatch.iterator(); i$.hasNext();)
        {
          waEvent = (WAEvent)i$.next();
          
          key = this.keyFactory.makeKey(waEvent.data);
          for (ZMQDistSub s : this.subscriptions.values()) {
            s.sendParitioned(waEvent, key);
          }
        }
      }
      else
      {
        for (ZMQDistSub s : this.subscriptions.values()) {
          s.sendUnpartitioned(event, this.thisPeerId);
        }
      }
    }
    catch (InterruptedException e)
    {
      throw new RuntimeInterruptedException(e);
    }
  }
  
  private void showSubscriptions()
  {
    if (logger.isDebugEnabled())
    {
      logger.debug("--> Remote Subscriptions for " + getName() + " - " + getID());
      for (DistSub sub : this.subscriptions.values()) {
        logger.debug("    -- " + sub);
      }
    }
  }
  
  public Collection<MonitorEvent> getMonitorEvents(long ts)
  {
    return Collections.emptyList();
  }
  
  public void addCallback(Channel.NewSubscriberAddedCallback callback)
  {
    this.callback = callback;
  }
  
  public int getSubscribersCount()
  {
    return this.subscriptions.size();
  }
  
  public void addSubscriber(Link link)
  {
    if (logger.isDebugEnabled()) {
      logger.debug("--> " + getName() + " add subscriber " + link);
    }
    if (this.callback != null) {
      this.callback.notifyMe(link);
    }
    UUID subID = getSubscriberID(link.subscriber);
    String name = getSubscriberName(link.subscriber);
    DistLink key = new DistLink(subID, link.linkID, name);
    if (logger.isInfoEnabled()) {
      logger.info(getDebugId() + " add subscription from " + key);
    }
    boolean isPresent = checkIfSubscriberAndStreamBelongToSameApp(subID);
    DistSub dist_sub = createGetDistSub(key, isPresent);
    if (isPresent) {
      dist_sub.addPeer(this.thisPeerId);
    } else {
      dist_sub.addPeer(this.thisPeerId);
    }
    String receiverName = DistributedChannel.buildReceiverName(this.stream_metainfo, subID);
    dist_sub.addDistributedRcvr(receiverName, this.owner, this, this.encrypted, this.messagingSystem, link, this.thisisimportant);
    if (this.status == DistributedChannel.Status.RUNNING)
    {
      if (this.thisisimportant == null) {
        dist_sub.start();
      } else {
        dist_sub.start(this.thisisimportant);
      }
      try
      {
        dist_sub.startDistributedReceiver(this.thisisimportant);
      }
      catch (Exception e)
      {
        logger.error(e.getMessage(), e);
      }
    }
    this.subscribers.add(key);
  }
  
  private synchronized DistSub createGetDistSub(DistLink key)
  {
    return createGetDistSub(key, false);
  }
  
  public void removeSubscriber(Link link)
  {
    if (logger.isDebugEnabled()) {
      logger.debug("--> " + getName() + " remove subscriber " + link);
    }
    UUID subID = getSubscriberID(link.subscriber);
    String name = getSubscriberName(link.subscriber);
    DistLink key = new DistLink(subID, link.linkID, name);
    if (key != null)
    {
      if (logger.isInfoEnabled()) {
        logger.info(getDebugId() + " remove subscription from " + key);
      }
      ZMQDistSub sub = (ZMQDistSub)this.subscriptions.get(key);
      if (sub != null)
      {
        sub.removeDistributedRcvr();
        sub.close(this.thisPeerId);
        this.subscriptions.remove(key);
      }
      this.subscribers.remove(key);
    }
    showSubscriptions();
  }
  
  public void itemAdded(ItemEvent<DistLink> distLinkItemEvent)
  {
    if (logger.isInfoEnabled()) {
      logger.info("New susbscriber added to " + getName() + " on remote node(" + distLinkItemEvent.getMember().getUuid() + ") so adding on this node as well : " + "(" + BaseServer.getServerName() + ", " + BaseServer.getBaseServer().getServerID() + ")");
    }
    DistLink key = (DistLink)distLinkItemEvent.getItem();
    if ((this.messagingSystem instanceof ZMQSystem))
    {
      boolean isPresent = checkIfSubscriberAndStreamBelongToSameApp(key.getSubID());
      DistSub dist_sub = createGetDistSub(key, isPresent);
      if (this.status == DistributedChannel.Status.RUNNING) {
        dist_sub.start(this.thisisimportant);
      }
    }
    else
    {
      boolean isPresent = checkIfSubscriberAndStreamBelongToSameApp(key.getSubID());
      DistSub dist_sub = createGetDistSub(key, isPresent);
      if (this.status == DistributedChannel.Status.RUNNING) {
        dist_sub.start(this.thisisimportant);
      }
    }
    showSubscriptions();
  }
  
  public void itemRemoved(ItemEvent<DistLink> distLinkItemEvent)
  {
    if (logger.isInfoEnabled()) {
      logger.info("Removing susbscriber to " + getName() + " on : " + BaseServer.getServerName() + " because it was " + "removed on remote node(" + distLinkItemEvent.getMember().getUuid() + ")");
    }
    DistLink key = (DistLink)distLinkItemEvent.getItem();
    this.subscriptions.remove(key);
    showSubscriptions();
  }
  
  public synchronized void stop()
  {
    if (this.status != DistributedChannel.Status.RUNNING)
    {
      if (logger.isInfoEnabled()) {
        logger.info("Stream " + getName() + " is not running so nothing to stop");
      }
      return;
    }
    for (ZMQDistSub s : this.subscriptions.values()) {
      s.stop();
    }
    if (logger.isInfoEnabled()) {
      logger.info("Stream " + getName() + " stopped");
    }
    this.status = DistributedChannel.Status.INITIALIZED;
  }
  
  public synchronized void start()
  {
    if (this.status != DistributedChannel.Status.INITIALIZED)
    {
      if (logger.isInfoEnabled()) {
        logger.info("ZMQChannel " + getName() + " is already running so cannot start it again.");
      }
      return;
    }
    for (DistSub s : this.subscriptions.values()) {
      s.start();
    }
    this.status = DistributedChannel.Status.RUNNING;
    if (this.subscriptions.isEmpty()) {
      if (logger.isInfoEnabled()) {
        logger.info("Started ZMQChannel " + getName() + " without subscribers");
      } else if (logger.isInfoEnabled()) {
        logger.info("Started ZMQChannel " + getName() + " with " + this.subscriptions.size() + " subscribers");
      }
    }
  }
  
  public synchronized void start(Map<UUID, Set<UUID>> servers)
  {
    if (this.status != DistributedChannel.Status.INITIALIZED)
    {
      if (logger.isInfoEnabled()) {
        logger.info("ZMQChannel " + getName() + " is already running so cannot start it again.");
      }
      return;
    }
    this.thisisimportant = servers;
    for (DistSub s : this.subscriptions.values()) {
      s.start(servers);
    }
    this.status = DistributedChannel.Status.RUNNING;
    if (this.subscriptions.isEmpty()) {
      if (logger.isInfoEnabled()) {
        logger.info("Started ZMQChannel " + getName() + " without subscribers");
      } else if (logger.isInfoEnabled()) {
        logger.info("Started ZMQChannel " + getName() + " with " + this.subscriptions.size() + " subscribers");
      }
    }
  }
  
  public boolean verifyStart(Map<UUID, Set<UUID>> serverToDeployedObjects)
  {
    if (this.status != DistributedChannel.Status.RUNNING) {
      return false;
    }
    if (this.subscriptions.size() < this.subscribers.size()) {
      return false;
    }
    for (Map.Entry<DistLink, ZMQDistSub> s : this.subscriptions.entrySet()) {
      if (!((ZMQDistSub)s.getValue()).verifyStart(serverToDeployedObjects)) {
        return false;
      }
    }
    return true;
  }
  
  public void close()
    throws IOException
  {
    stop();
    this.subscribers.removeItemListener(this.itemListenerId);
    this.itemListenerId = null;
    if ((BaseServer.getBaseServer() instanceof Server))
    {
      this.subscribers.destroy();
      this.subscribers = null;
    }
    if (logger.isInfoEnabled()) {
      try
      {
        logger.info(getDebugId() + " close " + getName() + " on " + id2name(this.thisPeerId));
      }
      catch (MetaDataRepositoryException e)
      {
        logger.error("Could not print message? " + e.getMessage(), e);
      }
    }
    showSubscriptions();
    try
    {
      KeyFactory.removeKeyFactory(getStreamInfo(), this.srv);
    }
    catch (Exception e)
    {
      logger.error("Error while removing key factory", e);
    }
    this.thisPeerId = null;
    this.owner = null;
    this.srv = null;
  }
  
  public Position getCheckpoint()
  {
    Position result = new Position();
    for (ZMQDistSub s : this.subscriptions.values()) {
      result.mergeLowerPositions(s.getCheckpoint());
    }
    return result;
  }
  
  public boolean isFull()
  {
    for (ZMQDistSub s : this.subscriptions.values()) {
      if (s.isFull()) {
        return true;
      }
    }
    return false;
  }
  
  private synchronized DistSub createGetDistSub(DistLink key, boolean isManaged)
  {
    ZMQDistSub s = (ZMQDistSub)this.subscriptions.get(key);
    if (s == null) {
      if (isManaged)
      {
        s = new ManagedZMQDistSub(key, this, this.srv, this.encrypted);
        this.subscriptions.put(key, s);
      }
      else
      {
        s = new UnManagedZMQDistSub(key, this, this.srv, this.encrypted);
        this.subscriptions.put(key, s);
      }
    }
    return s;
  }
}
