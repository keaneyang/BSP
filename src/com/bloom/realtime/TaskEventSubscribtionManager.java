package com.bloom.runtime;

import com.bloom.runtime.containers.ITaskEvent;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class TaskEventSubscribtionManager
{
  private final List<TaskEventSubscribtion> subscribers = new CopyOnWriteArrayList();
  
  public void addSubscribtion(TaskEventSubscribtion s)
  {
    this.subscribers.add(s);
  }
  
  public boolean removeSubscribtion(TaskEventSubscribtion s)
  {
    return this.subscribers.remove(s);
  }
  
  public void putTaskEvent(ITaskEvent e)
    throws InterruptedException
  {
    Iterator<TaskEventSubscribtion> it = this.subscribers.iterator();
    while (it.hasNext())
    {
      TaskEventSubscribtion sub = (TaskEventSubscribtion)it.next();
      sub.put(e);
    }
  }
  
  public int offerTaskEvent(ITaskEvent e)
  {
    int notReceived = 0;
    Iterator<TaskEventSubscribtion> it = this.subscribers.iterator();
    while (it.hasNext())
    {
      TaskEventSubscribtion sub = (TaskEventSubscribtion)it.next();
      if (!sub.offer(e)) {
        notReceived++;
      }
    }
    return notReceived;
  }
}
