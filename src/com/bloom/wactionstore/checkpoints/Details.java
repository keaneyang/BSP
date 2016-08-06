package com.bloom.wactionstore.checkpoints;

import com.bloom.recovery.Position;

final class Details
{
  public long wActions;
  public long timestamp;
  public Position position;
  
  Details()
  {
    start();
    this.position = new Position();
  }
  
  Details(Details copy)
  {
    this.wActions = copy.wActions;
    this.timestamp = copy.timestamp;
    this.position = new Position(copy.position);
  }
  
  public void start()
  {
    this.timestamp = System.currentTimeMillis();
    this.wActions = 0L;
  }
}
