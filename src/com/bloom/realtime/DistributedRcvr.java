package com.bloom.runtime;

import com.bloom.messaging.Handler;
import com.bloom.messaging.MessagingSystem;
import com.bloom.messaging.UnknownObjectException;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.runtime.channels.DistributedChannel;
import com.bloom.runtime.components.Flow;
import com.bloom.runtime.components.FlowComponent;
import com.bloom.runtime.components.Link;
import com.bloom.runtime.components.Stream;
import com.bloom.runtime.components.Subscriber;
import com.bloom.runtime.containers.TaskEvent;
import com.bloom.uuid.UUID;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.bloom.proc.events.PublishableEvent;
import com.bloom.recovery.Position;
import com.bloom.recovery.PositionResponse;
import com.bloom.runtime.DistLink;
import com.bloom.runtime.StreamEvent;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class DistributedRcvr
  implements Handler
{
  private static Logger logger = Logger.getLogger(DistributedRcvr.class);
  public final Map<DistLink, Link> subscribers = new HashMap();
  protected String receiverName;
  private ScheduledFuture<?> scheduledFuture = null;
  private DistributedChannel channel;
  private boolean debugEventLoss = false;
  private boolean debugEventCountMatch = false;
  private final ConcurrentHashMap<Integer, Position> highDeliveredPerLink = new ConcurrentHashMap();
  private boolean recoveryEnabled = false;
  public Status status;
  public FlowComponent owner;
  long countt = -1L;
  long lastCount;
  long delta;
  long stime = 0L;
  long etime = 0L;
  long ttime = 0L;
  long tdiff;
  private final BaseServer srv;
  final MessagingSystem messagingSystem;
  
  public static enum Status
  {
    UNKNOWN,  INITIALIZED,  RUNNING;
    
    private Status() {}
  }
  
  public DistributedRcvr(String receiverName, FlowComponent owner, DistributedChannel pStream, boolean encrypted, MessagingSystem messagingSystem)
  {
    this.messagingSystem = messagingSystem;
    this.messagingSystem.createReceiver(messagingSystem.getReceiverClass(), this, receiverName, encrypted, pStream.getOwner().getMetaInfo());
    
    this.owner = owner;
    this.channel = pStream;
    this.status = Status.INITIALIZED;
    this.receiverName = receiverName;
    
    this.recoveryEnabled = ((this.owner != null) && (this.owner.getFlow() != null) && (this.owner.getFlow().recoveryIsEnabled()));
    this.srv = Server.getServer();
    if (this.recoveryEnabled) {
      this.scheduledFuture = this.srv.getScheduler().scheduleWithFixedDelay(this.positionConfirmationThread, 0L, 1L, TimeUnit.SECONDS);
    }
  }
  
  public void startReceiver(String name, Map<Object, Object> properties)
    throws Exception
  {
    if (this.status == Status.INITIALIZED)
    {
      this.messagingSystem.startReceiver(name, properties);
      start();
    }
  }
  
  private final Runnable positionConfirmationThread = new Runnable()
  {
    public void run()
    {
      if (DistributedRcvr.this.highDeliveredPerLink.isEmpty()) {
        return;
      }
      synchronized (DistributedRcvr.this.highDeliveredPerLink)
      {
        try
        {
          if (DistributedRcvr.logger.isDebugEnabled()) {
            DistributedRcvr.logger.debug("Starting DistRcvr for " + DistributedRcvr.this.channel.getMetaObject().getName());
          }
          Map<Integer, Position> highMap = DistributedRcvr.this.highDeliveredPerLink;
          for (Integer linkID : highMap.keySet())
          {
            Position highDelivered = (Position)DistributedRcvr.this.highDeliveredPerLink.get(linkID);
            if (!highDelivered.isEmpty())
            {
              if (DistributedRcvr.logger.isDebugEnabled()) {
                DistributedRcvr.logger.debug("DistRcvr for " + DistributedRcvr.this.channel.getMetaObject().getName() + " sending confirmation of " + highDelivered);
              }
              String distID = String.valueOf(linkID);
              PositionResponse response = new PositionResponse(DistributedRcvr.this.owner.getMetaID(), DistributedRcvr.this.srv.getServerID(), DistributedRcvr.this.owner.getMetaID(), distID, highDelivered);
              HazelcastSingleton.get().getTopic("#StreamConfirm_" + DistributedRcvr.this.owner.getMetaID()).publish(response);
              DistributedRcvr.this.highDeliveredPerLink.remove(linkID);
              if (DistributedRcvr.logger.isDebugEnabled()) {
                DistributedRcvr.logger.debug("Finished DistRcvr for " + DistributedRcvr.this.channel.getMetaObject().getName() + " sending confirmation of " + highDelivered);
              }
            }
          }
        }
        catch (Throwable e)
        {
          DistributedRcvr.logger.error("Distributed Received failed to confirm position for stream " + DistributedRcvr.this.channel.getMetaObject().getName(), e);
        }
        finally
        {
          if (DistributedRcvr.logger.isDebugEnabled()) {
            DistributedRcvr.logger.debug("leaving for stream " + DistributedRcvr.this.channel.getMetaObject().getName());
          }
        }
      }
    }
  };
  
  public static float ns2s(long nsecs)
  {
    float f = (float)nsecs;
    float t = 1.0E9F;
    return f / t;
  }
  
  public void stats2()
  {
    if (logger.isDebugEnabled()) {
      logger.debug("Stream Stats @" + this.owner.getMetaName() + " : \n" + "Total messages : " + this.countt + "\n" + "Total time :  " + ns2s(this.ttime) + " seconds , \n" + "Rate(nrOfItems/sec) : " + this.delta * 1000000000L / this.tdiff + "\n" + "-----------------------------------------------------------------");
    }
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
    if (logger.isDebugEnabled())
    {
      this.countt += 1L;
      if (this.countt == 0L) {
        this.stime = System.nanoTime();
      }
      if ((this.countt % 500000L == 0L) && (this.countt != 0L))
      {
        this.etime = System.nanoTime();
        this.tdiff = (this.etime - this.stime);
        this.ttime += this.tdiff;
        this.stime = this.etime;
        this.delta = (this.countt - this.lastCount);
        this.lastCount = this.countt;
        stats2();
      }
    }
  }
  
  public String getName()
  {
    return this.receiverName;
  }
  
  public FlowComponent getOwner()
  {
    return this.owner;
  }
  
  public void stop()
  {
    if (this.debugEventCountMatch) {
      logger.warn(this.owner.getMetaName() + " Events received " + this.peerCounts);
    }
    this.status = Status.INITIALIZED;
  }
  
  public void start()
  {
    this.status = Status.RUNNING;
  }
  
  public void close()
  {
    try
    {
      MessagingSystem messagingSystem = this.channel.messagingSystem;
      messagingSystem.stopReceiver(this.receiverName);
      this.owner = null;
      this.channel = null;
      this.receiverName = null;
      if (this.scheduledFuture != null)
      {
        this.scheduledFuture.cancel(true);
        this.scheduledFuture = null;
      }
    }
    catch (Exception e)
    {
      logger.error("Unable to Stop Stream Receiver: " + this.receiverName, e);
    }
  }
  
  public void addSubscriber(Link link, DistLink key)
  {
    this.subscribers.put(key, link);
    if (logger.isDebugEnabled()) {
      showSubscriptions();
    }
  }
  
  public void showSubscriptions()
  {
    if (logger.isDebugEnabled())
    {
      logger.debug("--> Local Subscriptions for " + this.channel.getMetaObject().getName() + " - " + this.channel.getMetaObject().getUuid());
      for (Link sub : this.subscribers.values()) {
        logger.debug("    -- " + sub);
      }
    }
  }
  
  public void addServerList(Map<UUID, Set<UUID>> serverToDeployedObjects)
  {
    if (serverToDeployedObjects != null)
    {
      List<UUID> servers = new ArrayList();
      DistLink dist_link = (DistLink)this.subscribers.keySet().iterator().next();
      for (Map.Entry<UUID, Set<UUID>> entry : serverToDeployedObjects.entrySet()) {
        if ((entry.getValue() != null) && (((Set)entry.getValue()).contains(dist_link.getSubID()))) {
          servers.add(entry.getKey());
        }
      }
      if (logger.isInfoEnabled()) {
        logger.info("Servers where the receiver for " + dist_link.getName() + " is running: (" + servers + ")");
      }
    }
  }
  
  Map<UUID, Integer> peerCounts = new ConcurrentHashMap();
  
  private void doReceive(ITaskEvent taskEvent)
  {
    assert (this.subscribers.entrySet().size() == 0);
    Map.Entry<DistLink, Link> entry = (Map.Entry)this.subscribers.entrySet().iterator().next();
    Link l = (Link)entry.getValue();
    DistLink link = (DistLink)entry.getKey();
    try
    {
      l.subscriber.receive(Integer.valueOf(l.linkID), taskEvent);
      debugEvents(link, taskEvent);
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
    Link l = (Link)this.subscribers.get(link);
    if (l != null) {
      try
      {
        if (this.recoveryEnabled)
        {
          TaskEvent sendEvent = TaskEvent.createStreamEventWithNewWAEvents(event);
          l.subscriber.receive(Integer.valueOf(l.linkID), sendEvent);
        }
        else
        {
          l.subscriber.receive(Integer.valueOf(l.linkID), event);
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
    if (this.recoveryEnabled) {
      synchronized (this.highDeliveredPerLink)
      {
        Position highDelivered = null;
        if (link != null)
        {
          if (logger.isDebugEnabled()) {
            logger.debug("Setting highDeliveredPerLink for linkId " + link.getLinkID() + " channel " + this.channel.getMetaObject().getName());
          }
          highDelivered = (Position)this.highDeliveredPerLink.get(Integer.valueOf(link.getLinkID()));
          if (highDelivered == null) {
            highDelivered = new Position();
          }
          highDelivered.mergeHigherPositions(batchHighPosition);
          this.highDeliveredPerLink.put(Integer.valueOf(link.getLinkID()), highDelivered);
          if (logger.isDebugEnabled()) {
            logger.debug("Completed Setting highDeliveredPerLink for linkId " + link.getLinkID() + " " + this.highDeliveredPerLink + " channel " + this.channel.getMetaObject().getName());
          }
        }
      }
    }
  }
  
  private void debugEvents(DistLink link, ITaskEvent event)
  {
    if (this.debugEventCountMatch)
    {
      Integer count = (Integer)this.peerCounts.get(link.getSubID());
      if (count == null) {
        count = new Integer(0);
      }
      this.peerCounts.put(link.getSubID(), Integer.valueOf(count.intValue() + 1));
    }
    IQueue<String> evTS;
    if (this.debugEventLoss)
    {
      evTS = HazelcastSingleton.get().getQueue(this.channel.getMetaObject().getUuid() + "-" + link.getSubID() + "-evTS");
      for (WAEvent waEvent : event.batch())
      {
        String next = null;
        int count = 0;
        while (count < 10)
        {
          next = waitForEvent(evTS, next);
          next = matchEvent(evTS, waEvent, next);
          if (next != null) {
            break;
          }
          count++;
        }
        if (next == null) {
          logger.warn("Stream: " + this.owner.getMetaName() + "Did not find a matching event at all");
        }
      }
    }
  }
  
  private String matchEvent(IQueue<String> evTS, WAEvent waEvent, String next)
  {
    while ((next != null) && (!next.equals(waEvent.toString())))
    {
      logger.warn("Stream: " + this.owner.getMetaName() + "Missing event " + next);
      try
      {
        next = (String)evTS.remove();
      }
      catch (NoSuchElementException e)
      {
        return null;
      }
    }
    return next;
  }
  
  private String waitForEvent(IQueue<String> evTS, String next)
  {
    while (next == null) {
      try
      {
        next = (String)evTS.remove();
      }
      catch (Exception e) {}
    }
    return next;
  }
}
