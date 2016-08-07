package com.bloom.runtime;

import com.bloom.runtime.containers.ITaskEvent;
import java.util.concurrent.BlockingQueue;

public class TaskEventSubscribtion
{
  private final int userStreamID;
  private final BlockingQueue<TaskEventWithStreamID> channel;
  
  public TaskEventSubscribtion(int id, BlockingQueue<TaskEventWithStreamID> channel)
  {
    this.userStreamID = id;
    this.channel = channel;
  }
  
  public boolean offer(ITaskEvent e)
  {
    return this.channel.offer(new TaskEventWithStreamID(this.userStreamID, e));
  }
  
  public void put(ITaskEvent e)
    throws InterruptedException
  {
    this.channel.put(new TaskEventWithStreamID(this.userStreamID, e));
  }
}
