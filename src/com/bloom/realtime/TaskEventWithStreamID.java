package com.bloom.runtime;

import com.bloom.runtime.containers.ITaskEvent;

public class TaskEventWithStreamID
{
  public final int userStreamID;
  public final ITaskEvent event;
  
  public TaskEventWithStreamID(int id, ITaskEvent e)
  {
    this.userStreamID = id;
    this.event = e;
  }
}
