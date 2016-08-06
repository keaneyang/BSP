package com.bloom.intf;

public abstract interface Worker
{
  public abstract void startWorker();
  
  public abstract void stopWorker();
  
  public abstract WorkerType getType();
  
  public abstract void close()
    throws Exception;
  
  public abstract void setUri(String paramString);
  
  public abstract String getUri();
  
  public static enum WorkerType
  {
    Q,  THREAD,  PROCESS;
    
    private WorkerType() {}
  }
}
