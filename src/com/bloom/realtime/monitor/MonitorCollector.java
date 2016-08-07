package com.bloom.runtime.monitor;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.proc.SourceProcess;
import com.bloom.runtime.BaseServer;
import com.bloom.runtime.Server;
import com.bloom.runtime.channels.Channel;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.components.FlowComponent;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.StatusInfo;
import com.bloom.runtime.meta.MetaInfo.StatusInfo.Status;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.UUID;
import com.bloom.common.exc.SystemException;
import com.bloom.event.Event;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.log4j.Logger;

@PropertyTemplate(name="MonitoringDataSource", type=AdapterType.source, properties={}, inputType=MonitorEvent.class, outputType=MonitorBatchEvent.class)
public class MonitorCollector
  extends SourceProcess
{
  private static Logger logger = Logger.getLogger(MonitorCollector.class);
  private static final long periodMillis = 5000L;
  private BaseServer srv = null;
  private static Queue<MonitorEvent> monEventQueue = new ConcurrentLinkedQueue();
  private double maxAllowedHeapUsagePercent;
  
  public void collectEvents()
    throws InterruptedException
  {
    long ts = System.currentTimeMillis();
    try
    {
      logger.debug("Collecting monitoring events");
      List<MonitorEvent> allEvents = new ArrayList();
      Collection<Channel> channels = this.srv.getAllChannelsView();
      for (Channel channel : channels)
      {
        Collection<MonitorEvent> monitorEvents = channel.getMonitorEvents(ts);
        if (monitorEvents != null) {
          allEvents.addAll(monitorEvents);
        }
      }
      Collection<FlowComponent> allObjects = this.srv.getAllObjectsView();
      for (FlowComponent theObject : allObjects)
      {
        Collection<MonitorEvent> monitorEvents = theObject.getMonitorEvents(ts);
        if (monitorEvents != null) {
          allEvents.addAll(monitorEvents);
        }
      }
      Collection<MonitorEvent> monitorEvents = this.srv.getMonitorEvents(ts);
      long freeMemory;
      long maxMemory;
      if (monitorEvents != null)
      {
        freeMemory = -1L;
        maxMemory = -1L;
        
        Iterator<MonitorEvent> iter = monitorEvents.iterator();
        for (MonitorEvent mEvent : monitorEvents)
        {
          if (mEvent.type == MonitorEvent.Type.MEMORY_FREE) {
            freeMemory = mEvent.valueLong.longValue();
          } else if (mEvent.type == MonitorEvent.Type.MEMORY_MAX) {
            maxMemory = mEvent.valueLong.longValue();
          }
          allEvents.add(mEvent);
        }
        if ((freeMemory != -1L) && (maxMemory != -1L)) {
          if ((maxMemory - freeMemory) / maxMemory > this.maxAllowedHeapUsagePercent / 100.0D)
          {
            Collection<FlowComponent> flComps = Server.server.getAllObjectsView();
            for (FlowComponent comp : flComps) {
              if ((comp.getMetaType() == EntityType.APPLICATION) && (!comp.getMetaNsName().equals("Global")))
              {
                MetaInfo.StatusInfo statusInfo = MetadataRepository.getINSTANCE().getStatusInfo(comp.getMetaID(), WASecurityManager.TOKEN);
                if (statusInfo.getStatus() == MetaInfo.StatusInfo.Status.RUNNING)
                {
                  logger.warn("Crashing component " + comp.getMetaFullName() + " due to not enough memory. Freememory - " + freeMemory + ", MaxMemory - " + maxMemory + ", AllowedHeapPercent - " + this.maxAllowedHeapUsagePercent);
                  
                  comp.notifyAppMgr(comp.getMetaType(), comp.getMetaFullName(), comp.getMetaID(), new SystemException("Server " + Server.getServerName() + " exceeded heap usage threshold of " + this.maxAllowedHeapUsagePercent), null, null);
                }
              }
            }
          }
        }
      }
      for (MonitorEvent monEvent = (MonitorEvent)monEventQueue.poll(); monEvent != null; monEvent = (MonitorEvent)monEventQueue.poll()) {
        allEvents.add(monEvent);
      }
      MonitorBatchEvent batchEvent = new MonitorBatchEvent(System.currentTimeMillis(), allEvents);
      send(batchEvent, 0);
    }
    catch (Throwable e)
    {
      logger.error("Problem running Monitor", e);
    }
    try
    {
      Thread.sleep(5000L);
    }
    catch (InterruptedException e) {}
  }
  
  public synchronized void init(Map<String, Object> properties, Map<String, Object> parserProperties, UUID uuid, String distributionID)
    throws Exception
  {
    super.init(properties, parserProperties, uuid, distributionID);
    this.srv = BaseServer.getBaseServer();
    try
    {
      this.maxAllowedHeapUsagePercent = Double.parseDouble(System.getProperty("com.bloom.maxAllowedHeapUsagePercent", "90"));
      if (this.maxAllowedHeapUsagePercent > 100.0D) {
        this.maxAllowedHeapUsagePercent = 90.0D;
      }
    }
    catch (Exception e)
    {
      logger.warn("Invalid setting for maxAllowedHeapUsagePercent, defaulting to 90");
      this.maxAllowedHeapUsagePercent = 90.0D;
    }
  }
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    collectEvents();
  }
  
  public void close()
    throws Exception
  {
    super.close();
  }
  
  public static void reportIssue(UUID objectID, String message)
  {
    MonitorEvent monEvent = new MonitorEvent(Server.server.getServerID(), objectID, MonitorEvent.Type.LOG_ERROR, message, Long.valueOf(System.currentTimeMillis()));
    monEventQueue.add(monEvent);
  }
  
  public static void reportStateChange(UUID objectID, String statusChangeString)
  {
    MonitorEvent monEvent = new MonitorEvent(BaseServer.getBaseServer().getServerID(), objectID, MonitorEvent.Type.STATUS_CHANGE, statusChangeString, Long.valueOf(System.currentTimeMillis()));
    monEventQueue.add(monEvent);
  }
}
