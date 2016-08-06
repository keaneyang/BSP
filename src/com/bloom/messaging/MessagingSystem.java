package com.bloom.messaging;

import com.bloom.runtime.exceptions.NodeNotFoundException;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.uuid.UUID;

import java.util.Map;

public abstract interface MessagingSystem
{
  public abstract void createReceiver(Class paramClass, Handler paramHandler, String paramString, boolean paramBoolean, MetaInfo.Stream paramStream);
  
  public abstract Receiver getReceiver(String paramString);
  
  public abstract void startReceiver(String paramString, Map<Object, Object> paramMap)
    throws Exception;
  
  public abstract Sender getConnectionToReceiver(UUID paramUUID, String paramString, SocketType paramSocketType, boolean paramBoolean)
    throws NodeNotFoundException, InterruptedException;
  
  public abstract boolean stopReceiver(String paramString)
    throws Exception;
  
  public abstract void shutdown();
  
  public abstract boolean cleanupAllReceivers();
  
  public abstract Class getReceiverClass();
}

