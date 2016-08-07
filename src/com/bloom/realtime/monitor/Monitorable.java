package com.bloom.runtime.monitor;

import java.util.Collection;

public abstract interface Monitorable
{
  public abstract Collection<MonitorEvent> getMonitorEvents(long paramLong);
}
