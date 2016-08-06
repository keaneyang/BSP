package com.bloom.distribution;

import com.bloom.proc.events.WAQueueEvent;
import com.bloom.runtime.Server;
import com.bloom.runtime.StreamEventFactory;
import com.bloom.runtime.channels.Channel;
import com.bloom.runtime.components.Link;
import com.bloom.runtime.components.Stream;
import com.bloom.runtime.components.Subscriber;
import com.bloom.tungsten.Tungsten;
import com.bloom.uuid.UUID;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class WAQueue
{
  private static Logger logger = Logger.getLogger(WAQueue.class);
  private static volatile WAQueueManager instance = null;
  private final String name;
  public final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList();
  public volatile boolean isActive;
  private BlockingQueue blockingQueue = new LinkedBlockingQueue(50000);
  private volatile boolean dequerReady = false;
  private Dequer dequer = null;
  private final int timeToPoll = 100;
  
  private static WAQueueManager getWAQueueManager()
  {
    if (instance == null) {
      synchronized (WAQueueManager.class)
      {
        if (instance == null) {
          instance = new WAQueueManager();
        }
      }
    }
    return instance;
  }
  
  public String getName()
  {
    return this.name;
  }
  
  public static WAQueue getQueue(String queueName)
  {
    return getWAQueueManager().getQueue(queueName);
  }
  
  public static void removeQueue(String queueName)
  {
    getWAQueueManager().removeQueue(queueName);
  }
  
  private WAQueue(String name)
  {
    this.name = name;
  }
  
  public void subscribeIfNotSubscribed(Listener listener)
    throws Exception
  {
    if (!this.listeners.contains(listener)) {
      subscribe(listener);
    }
  }
  
  public void subscribe(Listener listener)
    throws Exception
  {
    this.listeners.add(listener);
    getWAQueueManager().subscribeForStream();
    if (logger.isDebugEnabled())
    {
      logger.debug("New Listener added for: " + this.name + ", total listeners: " + this.listeners.size());
      logger.debug("-------------------------");
    }
  }
  
  public void subscribeForTungsten(Listener listener)
    throws Exception
  {
    this.listeners.add(listener);
    getWAQueueManager().subscribeForTungsten();
    if (logger.isDebugEnabled()) {
      logger.debug("Tungsten listener added");
    }
  }
  
  private void stopDequer()
  {
    this.dequerReady = false;
  }
  
  public void unsubscribe(Listener listener)
  {
    this.listeners.remove(listener);
    stopDequer();
    this.dequer = null;
    this.blockingQueue.clear();
    if (this.listeners.size() == 0)
    {
      this.isActive = false;
      removeQueue(this.name);
    }
    getWAQueueManager().unsubscribe();
    if (logger.isDebugEnabled()) {
      logger.debug("Removed Listener on: " + this.name + ", total listeners: " + this.listeners.size());
    }
  }
  
  public void put(Object item)
  {
    getWAQueueManager().sendMessage(this.name, item);
  }
  
  public String toString()
  {
    return "WAQueue(" + this.name + " has " + this.listeners.size() + " listeners)";
  }
  
  public static abstract interface Listener
  {
    public abstract void onItem(Object paramObject);
  }
  
  public static class ShowStreamSubscriber
    implements Subscriber
  {
    public final UUID uuid;
    public final String name;
    
    public ShowStreamSubscriber()
    {
      this.uuid = new UUID(System.currentTimeMillis());
      this.name = ("ShowStreamSubscriber-" + this.uuid.toString());
    }
    
    public void receive(Object linkID, ITaskEvent event)
      throws Exception
    {
      for (WAEvent waEvent : event.batch())
      {
        WAQueueEvent queueEvent = (WAQueueEvent)waEvent.data;
        
        String qKey = queueEvent.getKey();
        WAQueue waQueue = (WAQueue)WAQueue.instance.instances.get(qKey);
        if (waQueue != null) {
          synchronized (waQueue)
          {
            if (!waQueue.dequerReady) {
              waQueue.startDequer();
            }
            waQueue.putInQueue(queueEvent.getPayload());
          }
        }
      }
    }
  }
  
  private void startDequer()
  {
    this.dequer = new Dequer(this.blockingQueue);
    this.dequerReady = true;
    this.dequer.start();
    Thread.yield();
  }
  
  private void putInQueue(Object object)
    throws InterruptedException
  {
    this.blockingQueue.put(object);
  }
  
  private class Dequer
    extends Thread
  {
    private final BlockingQueue<Object> blockingQueue;
    
    public Dequer(BlockingQueue blockingQueue)
    {
      super();
      this.blockingQueue = blockingQueue;
    }
    
    public void run()
    {
      while (WAQueue.this.dequerReady) {
    	Object data;
        try
        {
          data = this.blockingQueue.poll(100L, TimeUnit.MILLISECONDS);
          for (WAQueue.Listener listener : WAQueue.this.listeners) {
            if (data != null) {
              listener.onItem(data);
            }
          }
        }
        catch (InterruptedException e)
        {
          
          WAQueue.logger.error(e.getMessage());
        }
      }
    }
  }
  
  private static class WAQueueManager
  {
    private volatile ConcurrentMap<String, WAQueue> instances = new ConcurrentHashMap();
    private Link streamSubscriber = null;
    private Stream showStream = null;
    
    WAQueueManager()
    {
      if (Server.server != null) {
        this.showStream = Server.server.getShowStream();
      } else if (WAQueue.logger.isDebugEnabled()) {
        WAQueue.logger.debug("Server is NULL");
      }
    }
    
    public void sendToClient(String key, Object o)
    {
      WAQueueEvent queueEvent = new WAQueueEvent(key, o);
      if (this.showStream != null)
      {
        publish(queueEvent);
      }
      else if (Server.server != null)
      {
        this.showStream = Server.server.getShowStream();
        if (this.showStream != null) {
          publish(queueEvent);
        } else if (WAQueue.logger.isDebugEnabled()) {
          WAQueue.logger.debug("showStream is NULL, which is unexpected at this point)");
        }
      }
      else if (WAQueue.logger.isDebugEnabled())
      {
        WAQueue.logger.debug("Server is NULLshowStream is NULL, which is unexpected at this point)");
      }
    }
    
    void sendMessage(String key, Object o)
    {
      sendToClient(key, o);
    }
    
    public void publish(WAQueueEvent queueEvent)
    {
      try
      {
        this.showStream.getChannel().publish(StreamEventFactory.createStreamEvent(queueEvent));
      }
      catch (Exception e)
      {
        WAQueue.logger.error(e.getMessage());
      }
    }
    
    WAQueue getQueue(String queueName)
    {
      WAQueue queue = (WAQueue)this.instances.get(queueName);
      if (queue == null)
      {
        WAQueue newqueue = new WAQueue(queueName);
        queue = (WAQueue)this.instances.putIfAbsent(queueName, newqueue);
        if (queue == null) {
          queue = newqueue;
        }
      }
      if (WAQueue.logger.isDebugEnabled()) {
        WAQueue.logger.debug("Get WAQueue:" + queueName);
      }
      return queue;
    }
    
    void removeQueue(String queueName)
    {
      WAQueue queue = (WAQueue)this.instances.remove(queueName);
      if (queue != null)
      {
        queue.stopDequer();
        if (WAQueue.logger.isDebugEnabled()) {
          WAQueue.logger.debug("Removed queue:" + queueName);
        }
      }
    }
    
    public void subscribeForStream()
      throws Exception
    {
      if (this.showStream == null) {
        if (Server.server != null) {
          this.showStream = Server.server.getShowStream();
        } else if (WAQueue.logger.isDebugEnabled()) {
          WAQueue.logger.debug("Server is NULL");
        }
      }
      if (this.showStream != null)
      {
        if (this.streamSubscriber == null)
        {
          this.streamSubscriber = new Link(new WAQueue.ShowStreamSubscriber());
          this.showStream.getChannel().addSubscriber(this.streamSubscriber);
        }
        if (WAQueue.logger.isDebugEnabled()) {
          WAQueue.logger.debug("New subscriber added");
        }
      }
    }
    
    public void subscribeForTungsten()
      throws Exception
    {
      if (this.showStream == null) {
        if (Tungsten.showStream != null) {
          this.showStream = Tungsten.showStream;
        }
      }
      if (this.showStream != null) {
        if (this.streamSubscriber == null)
        {
          this.streamSubscriber = new Link(new WAQueue.ShowStreamSubscriber());
          this.showStream.getChannel().addSubscriber(this.streamSubscriber);
          if (WAQueue.logger.isDebugEnabled()) {
            WAQueue.logger.debug("New subscriber added");
          }
        }
      }
    }
    
    public void unsubscribe()
    {
      if (this.instances.size() == 0)
      {
        this.showStream.getChannel().removeSubscriber(this.streamSubscriber);
        this.streamSubscriber = null;
        this.showStream = null;
      }
      if (WAQueue.logger.isDebugEnabled()) {
        WAQueue.logger.debug("Removed Stream Subscriber");
      }
    }
  }
}
