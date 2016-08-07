package com.bloom.runtime.components;

import java.util.List;

import com.bloom.uuid.UUID;

public abstract interface FlowStateApi
{
  public abstract void startFlow(List<UUID> paramList, Long paramLong)
    throws Exception;
  
  public abstract void startFlow(Long paramLong)
    throws Exception;
  
  public abstract void stopFlow(Long paramLong)
    throws Exception;
  
  public abstract void startCaches(List<UUID> paramList, Long paramLong)
    throws Exception;
  
  public abstract void stopCaches(Long paramLong)
    throws Exception;
}
