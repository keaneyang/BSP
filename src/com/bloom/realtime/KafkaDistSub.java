package com.bloom.runtime;

import com.bloom.runtime.channels.DistributedChannel;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.recovery.PartitionedSourcePosition;
import com.bloom.recovery.Position;
import com.bloom.runtime.DistLink;

import org.apache.log4j.Logger;

public abstract class KafkaDistSub
  extends DistSub
{
  protected static Logger logger = Logger.getLogger(KafkaDistSub.class);
  protected final String streamSubscriberName;
  public DistLink distLink;
  protected BaseServer srvr;
  protected DistributedChannel parentStream;
  protected final boolean encrypted;
  protected ConsistentHashRing ring;
  protected KafkaDistributedRcvr distributedRcvr;
  public Status status;
  
  public static enum Status
  {
    UNKNOWN,  INITIALIZED,  STARTED,  STOPPED;
    
    private Status() {}
  }
  
  public KafkaDistSub(DistLink l, DistributedChannel pStream, BaseServer srvr, boolean encrypted)
  {
    this.distLink = l;
    this.srvr = srvr;
    this.parentStream = pStream;
    this.encrypted = encrypted;
    this.streamSubscriberName = (pStream.getMetaObject().getName() + "-" + this.distLink.getName());
    this.status = Status.INITIALIZED;
  }
  
  public void stop()
  {
    if (this.status != Status.STARTED)
    {
      if (logger.isDebugEnabled()) {
        if (logger.isDebugEnabled()) {
          logger.debug("Stream subscriber " + this.streamSubscriberName + " is not started so not stopping");
        }
      }
      return;
    }
    if (this.distributedRcvr != null) {
      this.distributedRcvr.stop();
    }
    this.distributedRcvr = null;
    this.status = Status.STOPPED;
    if (logger.isInfoEnabled()) {
      logger.info("Stopped stream subscriber " + this.streamSubscriberName + " for stream: " + this.parentStream.getName());
    }
    postStopHook();
  }
  
  public boolean isFull()
  {
    return false;
  }
  
  public void sendEvent(EventContainer event, int sendQueueId)
    throws InterruptedException
  {}
  
  public void startEmitting(PartitionedSourcePosition position)
    throws Exception
  {
    if (Logger.getLogger("KafkaStreams").isDebugEnabled()) {
      Logger.getLogger("KafkaStreams").debug("Starting KafkaDistSub " + this.streamSubscriberName + " at " + position);
    }
    if (this.status != Status.STARTED)
    {
      this.distributedRcvr.startEmitting(this.distributedRcvr.getName(), position);
      start();
    }
  }
  
  public Position getComponentCheckpoint()
  {
    return this.distributedRcvr.getComponentCheckpoint();
  }
}
