package com.bloom.runtime.window;

import com.bloom.runtime.containers.Batch;
import com.bloom.runtime.containers.Range;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;
import java.util.List;
import java.util.NoSuchElementException;

abstract class WBuffer
{
  RecordKey key;
  private ScalaWindow owner;
  
  void setOwner(ScalaWindow owner, RecordKey key)
  {
    this.owner = owner;
    this.key = key;
  }
  
  final void publish(List<WAEvent> added, List<WAEvent> removed)
    throws Exception
  {
    Range snapshot = makeSnapshot();
    this.owner.publish(Batch.asBatch(added), removed, snapshot);
  }
  
  final void publish(Batch added, List<WAEvent> removed)
    throws Exception
  {
    Range snapshot = makeSnapshot();
    this.owner.publish(added, removed, snapshot);
  }
  
  abstract void update(Batch paramBatch, long paramLong)
    throws Exception;
  
  abstract Range makeSnapshot();
  
  void removeTill(long now)
    throws Exception
  {
    throw new UnsupportedOperationException();
  }
  
  void removeAll()
    throws Exception
  {
    throw new UnsupportedOperationException();
  }
  
  abstract void addEvent(WAEvent paramWAEvent);
  
  abstract WAEvent getFirstEvent()
    throws NoSuchElementException;
  
  abstract int getSize();
  
  final void addEvents(IBatch added)
  {
    IBatch<WAEvent> waEventIBatch = added;
    for (WAEvent e : waEventIBatch) {
      addEvent(e);
    }
  }
  
  final boolean countFull()
  {
    return getSize() == this.owner.count;
  }
  
  final boolean countOverflow()
  {
    return getSize() > this.owner.count;
  }
  
  final boolean notExpiredYet(long eventTimestamp, long now)
  {
    return eventTimestamp + this.owner.interval > now;
  }
  
  final boolean inRange(WAEvent a, WAEvent b)
  {
    return this.owner.attrComparator.inRange(a, b);
  }
}
