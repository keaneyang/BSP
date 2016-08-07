package com.bloom.runtime;

import com.bloom.kafkamessaging.KafkaSystem;
import com.bloom.messaging.MessagingSystem;
import com.bloom.messaging.Sender;
import com.bloom.runtime.channels.DistributedChannel;
import com.bloom.runtime.components.Link;
import com.bloom.runtime.components.Stream;
import com.bloom.uuid.UUID;
import com.bloom.runtime.DistLink;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

public class KafkaManagedDistSub
  extends KafkaDistSub
{
  private String receiver_name;
  
  public KafkaManagedDistSub(DistLink key, DistributedChannel distributedChannelInterface, BaseServer srv, boolean encrypted)
  {
    super(key, distributedChannelInterface, srv, encrypted);
  }
  
  protected UUID getFirstPeer()
  {
    return new UUID(System.currentTimeMillis());
  }
  
  public void addDistributedRcvr(String receiverName, Stream owner, DistributedChannel d_channel, boolean encrypted, MessagingSystem messagingSystem, Link link, Map<UUID, Set<UUID>> thisisimportant)
  {
    assert ((messagingSystem instanceof KafkaSystem));
    this.receiver_name = receiverName;
    this.distributedRcvr = new KafkaDistributedRcvr(receiverName, owner, d_channel, encrypted, (KafkaSystem)messagingSystem);
    this.distributedRcvr.addSubscriber(link, this.distLink);
    this.distributedRcvr.addServerList(thisisimportant);
  }
  
  public Sender getSender(int partitionId, int sendQueueId)
    throws InterruptedException
  {
    return null;
  }
  
  public void start()
  {
    if (this.status != KafkaDistSub.Status.STARTED) {
      this.status = KafkaDistSub.Status.STARTED;
    }
    if (logger.isInfoEnabled()) {
      logger.info("Started subscriber " + this.streamSubscriberName + " for stream: " + this.parentStream.getName());
    }
  }
  
  public void start(Map<UUID, Set<UUID>> serverToDeployedObjects)
  {
    if (this.status != KafkaDistSub.Status.STARTED) {
      this.status = KafkaDistSub.Status.STARTED;
    }
    if (logger.isInfoEnabled()) {
      logger.info("Started subscriber " + this.streamSubscriberName + " for stream: " + this.parentStream.getName());
    }
  }
  
  public void startDistributedReceiver(Map<UUID, Set<UUID>> thisisimportant)
    throws Exception
  {
    if (this.distributedRcvr != null) {
      this.distributedRcvr.startReceiver(this.receiver_name, new HashMap());
    }
  }
  
  public void postStopHook() {}
  
  public void close(UUID peerID)
  {
    stop();
    this.parentStream = null;
    this.distLink = null;
    this.srvr = null;
  }
  
  public boolean verifyStart(Map<UUID, Set<UUID>> serverToDeployedObjects)
  {
    return true;
  }
  
  public void addPeer(UUID thisPeerId) {}
  
  public int getPeerCount()
  {
    return 0;
  }
}
