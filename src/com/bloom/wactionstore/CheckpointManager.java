package com.bloom.wactionstore;

import com.bloom.recovery.Position;

public abstract interface CheckpointManager
{
  public abstract Position get(String paramString);
  
  public abstract void add(String paramString, WAction paramWAction, Position paramPosition);
  
  public abstract void start(String paramString);
  
  public abstract void flush(WActionStore paramWActionStore);
  
  public abstract void remove(String paramString);
  
  public abstract void removeInvalidWActions(String paramString);
  
  public abstract void closeWactionStore(String paramString);
  
  public abstract void writeBlankCheckpoint(String paramString);
}

