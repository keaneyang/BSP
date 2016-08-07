package com.bloom.runtime.components;

import com.bloom.runtime.BaseServer;
import com.bloom.runtime.monitor.MonitorEvent;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.runtime.monitor.Monitorable;
import com.bloom.runtime.monitor.MonitorEvent.Type;
import com.bloom.uuid.UUID;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.log4j.Logger;

public abstract class MonitorableComponent
  implements Monitorable
{
  private static Logger logger = Logger.getLogger(MonitorableComponent.class);
  private Thread processThread = null;
  public Long prevTimeStamp = null;
  private Long prevCpuTime;
  
  public abstract void addSpecificMonitorEvents(MonitorEventsCollection paramMonitorEventsCollection);
  
  public abstract UUID getMetaID();
  
  public abstract BaseServer srv();
  
  public void setProcessThread()
  {
    if (this.processThread == null) {
      this.processThread = Thread.currentThread();
    }
  }
  
  public void resetProcessThread()
  {
    this.processThread = null;
  }
  
  public Collection<MonitorEvent> getMonitorEvents(long ts)
  {
    if ((this.prevTimeStamp != null) && (this.prevTimeStamp.longValue() == ts))
    {
      logger.warn("Unexpected MonitorableComponent called for events with same previous timestamp: " + ts);
      return new ArrayList();
    }
    BaseServer srv = srv();
    if (srv != null)
    {
      UUID serverID = srv.getServerID();
      UUID entityID = getMetaID();
      MonitorEventsCollection monEvs = new MonitorEventsCollection(ts, this.prevCpuTime, serverID, entityID);
      addSpecificMonitorEvents(monEvs);
      if (this.processThread != null) {
        try
        {
          long cpuTime = ManagementFactory.getThreadMXBean().getThreadCpuTime(this.processThread.getId());
          if (this.prevCpuTime != null)
          {
            long cpuDelta = Math.abs(cpuTime - this.prevCpuTime.longValue());
            long cpuRate = 1000L * cpuDelta / (ts - this.prevTimeStamp.longValue());
            monEvs.add(MonitorEvent.Type.CPU_RATE, Long.valueOf(cpuRate));
            monEvs.add(MonitorEvent.Type.CPU_THREAD, this.processThread.getName());
          }
          this.prevCpuTime = Long.valueOf(cpuTime);
        }
        catch (UnsupportedOperationException e)
        {
          logger.info("MXBean does not support CPU on this operating system", e);
          monEvs.add(MonitorEvent.Type.CPU_RATE, Long.valueOf(-1L));
        }
        catch (Exception e)
        {
          logger.error("Error getting thread CPU usage", e);
          monEvs.add(MonitorEvent.Type.CPU_RATE, Long.valueOf(-1L));
        }
      }
      this.prevTimeStamp = Long.valueOf(ts);
      return monEvs.getEvents();
    }
    return new ArrayList();
  }
}
