package com.bloom.runtime.monitor;

import java.util.ArrayList;
import java.util.Collection;

import com.bloom.uuid.UUID;

public class MonitorEventsCollection
{
  private final Collection<MonitorEvent> monEvs;
  private final long timeStamp;
  private final Long prevTimeStamp;
  private final UUID serverID;
  private final UUID entityID;
  
  public MonitorEventsCollection(long timeStamp, Long prevTimeStamp, UUID serverID, UUID entityID)
  {
    this.monEvs = new ArrayList();
    this.timeStamp = timeStamp;
    this.prevTimeStamp = prevTimeStamp;
    this.serverID = serverID;
    this.entityID = entityID;
  }
  
  public void add(MonitorEvent.Type type, Long value)
  {
    this.monEvs.add(new MonitorEvent(this.serverID, this.entityID, type, value, Long.valueOf(this.timeStamp)));
  }
  
  public void add(MonitorEvent.Type type, String value)
  {
    this.monEvs.add(new MonitorEvent(this.serverID, this.entityID, type, value, Long.valueOf(this.timeStamp)));
  }
  
  public Collection<MonitorEvent> getEvents()
  {
    return this.monEvs;
  }
  
  public long getTimeStamp()
  {
    return this.timeStamp;
  }
  
  public Long getPrevTimeStamp()
  {
    return this.prevTimeStamp;
  }
  
  public Long getTimeSpanSecs()
  {
    if (this.prevTimeStamp == null) {
      return null;
    }
    return Long.valueOf((this.timeStamp - this.prevTimeStamp.longValue()) / 1000L);
  }
  
  public Long getRate(Long valNow, Long valPrev)
  {
    if ((valNow == null) || (valPrev == null)) {
      return null;
    }
    Long timeSpan = getTimeSpanSecs();
    if (timeSpan == null) {
      return null;
    }
    return Long.valueOf((long)Math.ceil((valNow.longValue() - valPrev.longValue()) / timeSpan.longValue()));
  }
}
