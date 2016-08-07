package com.bloom.runtime.components;

import com.bloom.distribution.WAQueue;
import com.bloom.exceptionhandling.ExceptionType;
import com.bloom.intf.EventSink;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.proc.SourceProcess;
import com.bloom.runtime.BaseServer;
import com.bloom.runtime.StreamEventFactory;
import com.bloom.runtime.channels.Channel;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.meta.MetaInfo.Source;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.runtime.monitor.MonitorEvent.Type;
import com.bloom.uuid.UUID;
import com.bloom.event.Event;
import com.bloom.recovery.Position;
import com.bloom.recovery.SourcePosition;
import com.bloom.runtime.containers.ITaskEvent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.log4j.Logger;

public class Source
  extends FlowComponent
  implements Publisher, Runnable, EventSink, Restartable, Compound
{
  private static Logger logger = Logger.getLogger(Source.class);
  private volatile boolean running = false;
  private final Channel output;
  private final SourceProcess adapter;
  private final MetaInfo.Source sourceInfo;
  private Subscriber dataSink;
  private Future<?> process;
  private volatile long input = 0L;
  private SourcePosition currentSourcePosition;
  private boolean sendPositions;
  private Flow ownerFlow;
  private MetaInfo.Flow curApp = null;
  private WAQueue ehQueue = null;
  
  public Source(SourceProcess adapter, MetaInfo.Source sourceInfo, BaseServer srv)
    throws Exception
  {
    super(srv, sourceInfo);
    this.adapter = adapter;
    this.sourceInfo = sourceInfo;
    this.output = srv.createSimpleChannel();
    adapter.addEventSink(this);
  }
  
  public void connectParts(Flow flow)
    throws Exception
  {
    this.dataSink = flow.getSubscriber(this.sourceInfo.outputStream);
  }
  
  public Stream connectStream()
    throws Exception
  {
    Stream stream = srv().getStream(this.sourceInfo.outputStream, this.ownerFlow);
    if (this.curApp != null)
    {
      boolean isRecoveryEnabled = this.curApp.recoveryType == 2;
      stream.setRecoveryEnabled(isRecoveryEnabled);
    }
    this.dataSink = stream;
    return stream;
  }
  
  public Channel getChannel()
  {
    return this.output;
  }
  
  public void close()
    throws Exception
  {
    stop();
    this.output.close();
  }
  
  public void run()
  {
    while (this.running)
    {
      if (isFlowInError()) {
        return;
      }
      try
      {
        this.adapter.receive(0, null);
      }
      catch (Exception ex)
      {
        logger.error("Exception thrown by Source Adapter " + getMetaName(), ex);
        notifyAppMgr(EntityType.SOURCE, getMetaName(), getMetaID(), ex, "source run", new Object[0]);
        ExceptionType exceptionType = ExceptionType.getExceptionType(ex);
        if (exceptionType.name().equalsIgnoreCase(ExceptionType.UnknownException.name())) {
          try
          {
            stop();
          }
          catch (Exception e)
          {
            logger.error(e, e);
          }
        }
      }
    }
  }
  
  public void receive(int channel, Event event)
    throws Exception
  {
    setProcessThread();
    this.output.publish(StreamEventFactory.createStreamEvent(event));
    this.input += 1L;
  }
  
  public void receive(int channel, Event event, Position pos)
    throws Exception
  {
    setProcessThread();
    this.output.publish(StreamEventFactory.createStreamEvent(event, pos));
    this.input += 1L;
  }
  
  public void receive(ITaskEvent batch)
    throws Exception
  {
    setProcessThread();
    this.output.publish(batch);
    this.input += 1L;
  }
  
  public synchronized void start()
    throws Exception
  {
    if (this.running) {
      return;
    }
    this.running = true;
    String distId = BaseServer.getServerName();
    if (this.ownerFlow != null) {
      distId = this.ownerFlow.getDistributionId();
    }
    this.adapter.init(this.sourceInfo.properties, this.sourceInfo.parserProperties, getMetaID(), distId, this.currentSourcePosition, this.sendPositions, this.ownerFlow);
    srv().subscribe(this, this.dataSink);
    ExecutorService pool = srv().getThreadPool();
    this.process = pool.submit(this);
  }
  
  public synchronized void stop()
    throws Exception
  {
    if (!this.running) {
      return;
    }
    resetProcessThread();
    this.running = false;
    srv().unsubscribe(this, this.dataSink);
    if (this.adapter != null) {
      this.adapter.close();
    }
    if (this.process != null)
    {
      this.process.cancel(true);
      this.process = null;
    }
  }
  
  public boolean isRunning()
  {
    return this.running;
  }
  
  private Long prevIn = null;
  Long prevInRate = null;
  
  public void addSpecificMonitorEvents(MonitorEventsCollection monEvs)
  {
    Long in = Long.valueOf(this.input);
    Long rate = null;
    long timeStamp = monEvs.getTimeStamp();
    if (!in.equals(this.prevIn))
    {
      monEvs.add(MonitorEvent.Type.INPUT, in);
      monEvs.add(MonitorEvent.Type.SOURCE_INPUT, in);
      monEvs.add(MonitorEvent.Type.LATEST_ACTIVITY, Long.valueOf(System.currentTimeMillis()));
    }
    long delta = this.prevIn == null ? 0L : in.longValue() - this.prevIn.longValue();
    rate = Long.valueOf(delta == 0L ? 0L : Math.ceil(1000L * delta / (timeStamp - this.prevTimeStamp.longValue())));
    if (!rate.equals(this.prevInRate))
    {
      monEvs.add(MonitorEvent.Type.INPUT_RATE, rate);
      monEvs.add(MonitorEvent.Type.SOURCE_RATE, rate);
      monEvs.add(MonitorEvent.Type.RATE, rate);
    }
    this.prevIn = in;
    this.prevInRate = rate;
  }
  
  public UUID getNodeID()
  {
    return HazelcastSingleton.getNodeId();
  }
  
  public SourceProcess getAdapter()
  {
    return this.adapter;
  }
  
  public void setPosition(SourcePosition sourcePosition)
  {
    this.currentSourcePosition = sourcePosition;
  }
  
  public void setSendPositions(boolean sendPositions)
  {
    this.sendPositions = sendPositions;
  }
  
  public void setOwnerFlow(Flow flow)
  {
    this.ownerFlow = flow;
  }
  
  public boolean requiresPartitionedSourcePosition()
  {
    return this.adapter.requiresPartitionedSourcePosition();
  }
  
  public void onDeploy()
    throws Exception
  {
    this.adapter.onDeploy(this.sourceInfo.properties, this.sourceInfo.parserProperties, getMetaID());
  }
}
