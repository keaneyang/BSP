package com.bloom.intf;

import com.bloom.runtime.components.Flow;
import com.bloom.uuid.UUID;
import com.bloom.event.Event;
import com.bloom.recovery.Position;
import com.bloom.recovery.SourcePosition;
import com.bloom.runtime.containers.ITaskEvent;

import java.util.Map;

public abstract interface Process
  extends EventSink, EventSource, Worker
{
  public abstract void init(Map<String, Object> paramMap)
    throws Exception;
  
  public abstract void init(Map<String, Object> paramMap1, Map<String, Object> paramMap2)
    throws Exception;
  
  public abstract void init(Map<String, Object> paramMap1, Map<String, Object> paramMap2, UUID paramUUID, String paramString)
    throws Exception;
  
  public abstract void init(Map<String, Object> paramMap1, Map<String, Object> paramMap2, UUID paramUUID, String paramString, SourcePosition paramSourcePosition, boolean paramBoolean, Flow paramFlow)
    throws Exception;
  
  public abstract void send(Event paramEvent, int paramInt)
    throws Exception;
  
  public abstract void send(Event paramEvent, int paramInt, Position paramPosition)
    throws Exception;
  
  public abstract void send(ITaskEvent paramITaskEvent)
    throws Exception;
}

