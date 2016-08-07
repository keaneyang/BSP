package com.bloom.runtime.channels;

import com.bloom.kafkamessaging.KafkaSender;
import com.bloom.messaging.MessagingProvider;
import com.bloom.metaRepository.MetadataRepositoryUtils;
import com.bloom.runtime.DistSub;
import com.bloom.runtime.KafkaDistSub;
import com.bloom.runtime.KafkaManagedDistSub;
import com.bloom.runtime.KafkaStreamUtils;
import com.bloom.runtime.KeyFactory;
import com.bloom.runtime.components.Link;
import com.bloom.runtime.components.Stream;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.monitor.MonitorEvent;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.uuid.UUID;
import com.bloom.recovery.PartitionedSourcePosition;
import com.bloom.recovery.Position;
import com.bloom.recovery.SourcePosition;
import com.bloom.runtime.DistLink;
import com.bloom.runtime.containers.ITaskEvent;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;
import scala.NotImplementedError;

public class KafkaChannel
  extends DistributedChannel
{
  private static Logger logger = Logger.getLogger(KafkaChannel.class);
  private KafkaSender kafkaSender;
  public final ConcurrentHashMap<DistLink, KafkaManagedDistSub> subscriptions = new ConcurrentHashMap();
  private Map<UUID, Set<UUID>> app_nodes;
  
  public KafkaChannel(Stream stream_runtime)
    throws Exception
  {
    super(stream_runtime);
    this.messagingSystem = MessagingProvider.getMessagingSystem(MessagingProvider.KAFKA_SYSTEM);
  }
  
  public Set<DistLink> getSubscribers()
  {
    return this.subscriptions.keySet();
  }
  
  public synchronized void publish(ITaskEvent event)
    throws Exception
  {
    if (this.status != DistributedChannel.Status.RUNNING) {
      return;
    }
    if (this.status == DistributedChannel.Status.RUNNING)
    {
      if (this.kafkaSender == null)
      {
        boolean is_encrypted = isCurrentStreamEncrypted();
        boolean recoveryEnabled = this.owner.isRecoveryEnabled();
        this.kafkaSender = new KafkaSender(is_encrypted, this.owner, this.keyFactory, recoveryEnabled);
      }
      this.kafkaSender.send(event);
    }
  }
  
  public void addSubscriber(Link link)
    throws Exception
  {
    UUID subID = getSubscriberID(link.subscriber);
    String name = getSubscriberName(link.subscriber);
    DistLink key = new DistLink(subID, link.linkID, name);
    KafkaManagedDistSub dist_sub = createGetDistSub(key);
    String receiver_name = DistributedChannel.buildReceiverName(this.stream_metainfo, subID);
    if (Logger.getLogger("KafkaStreams").isInfoEnabled()) {
      logger.info("Added subscriber " + name + " with receiver_name: " + receiver_name);
    }
    dist_sub.addDistributedRcvr(receiver_name, this.owner, this, this.encrypted, this.messagingSystem, link, this.app_nodes);
    if (this.status != DistributedChannel.Status.RUNNING) {
      throw new RuntimeException("Wrong status when adding subscriber to " + this.stream_metainfo.name + ": expected RUNNING but found " + this.status);
    }
    try
    {
      dist_sub.startDistributedReceiver(this.app_nodes);
    }
    catch (Exception e)
    {
      throw e;
    }
    this.subscriptions.put(key, dist_sub);
  }
  
  public void removeSubscriber(Link link)
  {
    UUID subID = getSubscriberID(link.subscriber);
    String name = getSubscriberName(link.subscriber);
    DistLink key = new DistLink(subID, link.linkID, name);
    if (key != null) {
      this.subscriptions.remove(key);
    }
  }
  
  public void addCallback(Channel.NewSubscriberAddedCallback callback)
  {
    throw new NotImplementedError();
  }
  
  public int getSubscribersCount()
  {
    return this.subscriptions.size();
  }
  
  public void close()
    throws IOException
  {
    try
    {
      KeyFactory.removeKeyFactory(getStreamInfo(), this.srv);
    }
    catch (Exception e)
    {
      logger.error(e.getMessage(), e);
    }
  }
  
  public Collection<MonitorEvent> getMonitorEvents(long ts)
  {
    return null;
  }
  
  public boolean isFull()
  {
    return false;
  }
  
  public Map getStats()
  {
    if (this.kafkaSender != null) {
      return this.kafkaSender.getStats();
    }
    return null;
  }
  
  public void addSpecificMonitorEvents(MonitorEventsCollection monEvs)
  {
    if (this.kafkaSender != null) {
      this.kafkaSender.addSpecificMonitorEvents(monEvs);
    }
  }
  
  public synchronized Position getCheckpoint()
  {
    Position result = new Position();
    if (this.kafkaSender != null) {
      result.mergeHigherPositions(this.kafkaSender.getComponentCheckpoint());
    }
    for (KafkaDistSub kds : this.subscriptions.values()) {
      result.mergeHigherPositions(kds.getComponentCheckpoint());
    }
    return result;
  }
  
  public synchronized void stop()
  {
    if (this.status != DistributedChannel.Status.RUNNING)
    {
      if (logger.isInfoEnabled()) {
        logger.info("KafkaChannel " + getName() + " is not running so nothing to stop");
      }
      return;
    }
    if (this.kafkaSender != null)
    {
      this.kafkaSender.stop();
      this.kafkaSender = null;
    }
    if ((this.subscriptions != null) && (!this.subscriptions.isEmpty())) {
      for (DistSub sub : this.subscriptions.values()) {
        sub.stop();
      }
    }
    this.status = DistributedChannel.Status.INITIALIZED;
    if (this.subscriptions.isEmpty()) {
      if (logger.isInfoEnabled()) {
        logger.info("Started KafkaChannel " + getName() + " without subscribers");
      } else if (logger.isInfoEnabled()) {
        logger.info("Started KafkaChannel " + getName() + " with " + this.subscriptions.size() + " subscribers");
      }
    }
  }
  
  public void start()
  {
    start(null);
  }
  
  public void start(Map<UUID, Set<UUID>> servers)
  {
    if (this.status != DistributedChannel.Status.INITIALIZED)
    {
      if (logger.isInfoEnabled()) {
        logger.info("KafkaChannel " + getName() + " is already running so cannot start it again.");
      }
      return;
    }
    this.app_nodes = servers;
    this.status = DistributedChannel.Status.RUNNING;
    if (this.subscriptions.isEmpty()) {
      if (logger.isInfoEnabled()) {
        logger.info("Started KafkaChannel " + getName() + " without subscribers");
      } else if (logger.isInfoEnabled()) {
        logger.info("Started KafkaChannel " + getName() + " with " + this.subscriptions.size() + " subscribers");
      }
    }
  }
  
  public boolean verifyStart(Map<UUID, Set<UUID>> servers)
  {
    return true;
  }
  
  public void closeDistSub(String uuid) {}
  
  private boolean isCurrentStreamEncrypted()
  {
    MetaInfo.MetaObject currentStreamInfo = this.owner.getMetaInfo();
    MetaInfo.Flow appInfo = MetadataRepositoryUtils.getAppMetaObjectBelongsTo(currentStreamInfo);
    if (appInfo != null) {
      return appInfo.encrypted;
    }
    return false;
  }
  
  public static String buildReceiverName(MetaInfo.Stream stream_metainfo)
  {
    return KafkaStreamUtils.createTopicName(stream_metainfo);
  }
  
  public void startEmitting(SourcePosition startPosition)
    throws Exception
  {
    assert ((startPosition instanceof PartitionedSourcePosition));
    for (KafkaDistSub distSub : this.subscriptions.values()) {
      distSub.startEmitting((PartitionedSourcePosition)startPosition);
    }
  }
  
  private synchronized KafkaManagedDistSub createGetDistSub(DistLink key)
  {
    KafkaManagedDistSub s = (KafkaManagedDistSub)this.subscriptions.get(key);
    if (s == null)
    {
      s = new KafkaManagedDistSub(key, this, this.srv, this.encrypted);
      this.subscriptions.put(key, s);
    }
    return s;
  }
}
