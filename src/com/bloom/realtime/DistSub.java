package com.bloom.runtime;

import com.bloom.messaging.MessagingSystem;
import com.bloom.messaging.Sender;
import com.bloom.runtime.channels.DistributedChannel;
import com.bloom.runtime.components.Link;
import com.bloom.runtime.components.Stream;
import com.bloom.uuid.UUID;
import com.bloom.recovery.Position;
import com.bloom.runtime.DistLink;

import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import scala.NotImplementedError;

public abstract class DistSub
  implements ChannelEventHandler
{
  protected static Logger logger = Logger.getLogger(DistSub.class);
  public DistLink dist_link;
  protected ConsistentHashRing ring;
  protected abstract UUID getFirstPeer();
  
  public abstract void start();
  
  public abstract void start(Map<UUID, Set<UUID>> paramMap);
  
  public abstract void startDistributedReceiver(Map<UUID, Set<UUID>> paramMap)
    throws Exception;
  
  public abstract void stop();
  
  public abstract void postStopHook();
  
  public abstract void close(UUID paramUUID);
  
  public abstract boolean verifyStart(Map<UUID, Set<UUID>> paramMap);
  
  public abstract void addPeer(UUID paramUUID);
  
  public abstract int getPeerCount();
  
  public abstract void addDistributedRcvr(String paramString, Stream paramStream, DistributedChannel paramDistributedChannel, boolean paramBoolean, MessagingSystem paramMessagingSystem, Link paramLink, Map<UUID, Set<UUID>> paramMap);
  
  public abstract Sender getSender(int paramInt1, int paramInt2)
    throws InterruptedException;
  
  public static enum Status
  {
    UNKNOWN,  INITIALIZED,  STARTED,  Status,  STOPPED;
    
    private Status() {}
  }
  
  public void startEmitting(Position position)
  {
    throw new NotImplementedError();
  }
}
