package com.bloom.runtime;

import org.apache.log4j.Logger;

import com.bloom.uuid.UUID;

public class ShowStreamManager
{
  private static ShowStreamManager INSTANCE = new ShowStreamManager();
  private static Logger logger = Logger.getLogger(ShowStreamManager.class);
  public static final UUID ShowStreamUUID = new UUID("3624C7DG-2292-4535-BBE5-E376C5F5BC42");
  
  public static ShowStreamManager getInstance()
  {
    return INSTANCE;
  }
}