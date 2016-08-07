package com.bloom.runtime.components;

import com.bloom.proc.BaseProcess;
import com.bloom.runtime.BaseServer;
import com.bloom.runtime.meta.MetaInfo.Target;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.runtime.monitor.MonitorEvent.Type;
import com.bloom.utility.Utility;
import com.bloom.common.exc.AdapterException;
import com.bloom.event.Event;
import com.bloom.recovery.Acknowledgeable;
import com.bloom.recovery.Position;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import org.apache.log4j.Logger;

public class Target
  extends FlowComponent
  implements Subscriber, Restartable, Compound, ReceiptCallback
{
  private static Logger logger = Logger.getLogger(Target.class);
  private final MetaInfo.Target targetInfo;
  private final BaseProcess adapter;
  private Publisher dataSource;
  private volatile boolean running = false;
  private volatile long eventsInput = 0L;
  private volatile long eventsAcked = 0L;
  private boolean useDeliveryCallback = false;
  private Position checkpoint = new Position();
  
  public Target(BaseProcess adapter, MetaInfo.Target targetInfo, BaseServer srv)
    throws Exception
  {
    super(srv, targetInfo);
    this.adapter = adapter;
    this.targetInfo = targetInfo;
  }
  
  public void connectParts(Flow flow)
    throws Exception
  {
    this.dataSource = flow.getPublisher(this.targetInfo.inputStream);
  }
  
  public void receive(Object linkID, ITaskEvent event)
    throws Exception
  {
    if (!this.running)
    {
      if (Logger.getLogger("Recovery").isDebugEnabled()) {
        Logger.getLogger("Recovery").debug(getMetaName() + " is stopped but received a batch of " + event.batch().size() + " events");
      }
      return;
    }
    try
    {
      if (isFlowInError()) {
        return;
      }
      setProcessThread();
      for (WAEvent ev : event.batch()) {
        if (ev.position == null)
        {
          Event actualEvent = (Event)ev.data;
          this.adapter.receive(0, actualEvent);
          this.eventsInput += 1L;
        }
        else
        {
          Position dataPosition = ev.position.createAugmentedPosition(getMetaID(), null);
          if (dataPosition.precedes(this.checkpoint))
          {
            if (Logger.getLogger("Recovery").isDebugEnabled()) {
              Logger.getLogger("Recovery").debug(getMetaName() + " dropping duplicate: " + ev);
            }
          }
          else
          {
            Event actualEvent = (Event)ev.data;
            this.adapter.receive(0, actualEvent, dataPosition);
            this.eventsInput += 1L;
            if (this.useDeliveryCallback) {
              this.checkpoint.mergeLowerPositions(dataPosition);
            }
          }
        }
      }
    }
    catch (Exception e)
    {
      logger.error("Got exception " + e.getMessage(), e);
      notifyAppMgr(EntityType.TARGET, getMetaName(), getMetaID(), e, "target receive", new Object[] { event });
      throw new AdapterException("Exception in Target Adapter " + getMetaName(), e);
    }
  }
  
  public void close()
    throws Exception
  {
    resetProcessThread();
  }
  
  public synchronized void start()
    throws Exception
  {
    if (this.running) {
      return;
    }
    if ((recoveryIsEnabled()) && (!this.useDeliveryCallback) && 
      ((this.adapter instanceof Acknowledgeable)))
    {
      this.adapter.setReceiptCallback(this);
      this.useDeliveryCallback = true;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("starting target : " + this.targetInfo.name);
    }
    try
    {
      this.adapter.init(this.targetInfo.properties, this.targetInfo.formatterProperties, this.targetInfo.inputStream, null);
    }
    catch (Exception e)
    {
      throw new AdapterException("Initialization exception in Target Adapter " + getMetaName(), e);
    }
    this.adapter.startWorker();
    srv().subscribe(this.dataSource, this);
    this.running = true;
  }
  
  public synchronized void stop()
    throws Exception
  {
    if (!this.running) {
      return;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("stopping target : " + this.targetInfo.name);
    }
    srv().unsubscribe(this.dataSource, this);
    this.adapter.stopWorker();
    this.adapter.close();
    this.running = false;
  }
  
  public boolean isRunning()
  {
    return this.running;
  }
  
  private Long prevIn = null;
  private Long prevAck = null;
  private Long prevOutput = null;
  private Long prevRate = null;
  
  public void addSpecificMonitorEvents(MonitorEventsCollection monEvs)
  {
    Long latestActivity = null;
    
    Long in = Long.valueOf(this.eventsInput);
    if (!in.equals(this.prevIn))
    {
      monEvs.add(MonitorEvent.Type.INPUT, in);
      monEvs.add(MonitorEvent.Type.OUTPUT, in);
      latestActivity = Long.valueOf(monEvs.getTimeStamp());
    }
    Long ack = Long.valueOf(this.eventsAcked);
    if (!ack.equals(this.prevAck))
    {
      monEvs.add(MonitorEvent.Type.TARGET_ACKED, ack);
      latestActivity = Long.valueOf(monEvs.getTimeStamp());
    }
    Long output = ack.longValue() != 0L ? ack : in;
    if (!output.equals(this.prevOutput))
    {
      monEvs.add(MonitorEvent.Type.TARGET_OUTPUT, output);
      latestActivity = Long.valueOf(monEvs.getTimeStamp());
    }
    Long rate = monEvs.getRate(output, this.prevOutput);
    if ((rate != null) && (!rate.equals(this.prevRate)))
    {
      monEvs.add(MonitorEvent.Type.INPUT_RATE, rate);
      monEvs.add(MonitorEvent.Type.SOURCE_RATE, rate);
      monEvs.add(MonitorEvent.Type.RATE, rate);
      latestActivity = Long.valueOf(monEvs.getTimeStamp());
    }
    if (latestActivity != null) {
      monEvs.add(MonitorEvent.Type.LATEST_ACTIVITY, latestActivity);
    }
    this.prevIn = in;
    this.prevAck = ack;
    this.prevOutput = output;
    this.prevRate = rate;
  }
  
  public String toString()
  {
    return getMetaUri() + " - " + getMetaID() + " RUNTIME";
  }
  
  public Position getCheckpoint()
  {
    return new Position(this.checkpoint);
  }
  
  public void setRestartPosition(Position restartPosition)
  {
    this.checkpoint = new Position(restartPosition);
    if (logger.isDebugEnabled())
    {
      logger.debug("Setting restart position for target " + getMetaName() + " to:");
      Utility.prettyPrint(this.checkpoint);
    }
  }
  
  public void onDeploy()
    throws Exception
  {
    this.adapter.onDeploy(this.targetInfo.properties, this.targetInfo.formatterProperties, this.targetInfo.inputStream);
  }
  
  public void ack(Position eventPosition)
  {
    this.checkpoint.mergeHigherPositions(eventPosition);
    this.checkpoint.removeEqualPaths(eventPosition);
    this.eventsAcked += 1L;
    if (logger.isInfoEnabled()) {
      logger.info("Got Acknowlegdement for position " + eventPosition + ".\nNo of Events Acked is - " + this.eventsAcked);
    }
  }
}
