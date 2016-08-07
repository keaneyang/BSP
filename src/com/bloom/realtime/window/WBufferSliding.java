package com.bloom.runtime.window;

import com.bloom.runtime.containers.Range;
import com.bloom.runtime.containers.WAEvent;
import java.util.Collection;
import java.util.NoSuchElementException;
import scala.Tuple2;
import scala.collection.immutable.Queue;
import scala.collection.immutable.Queue.;

abstract class WBufferSliding
  extends WBuffer
{
  private Queue<WAEvent> buffer = Queue..MODULE$.empty();
  
  final void addEvent(WAEvent e)
  {
    this.buffer = this.buffer.enqueue(e);
  }
  
  final WAEvent getFirstEvent()
    throws NoSuchElementException
  {
    return (WAEvent)this.buffer.front();
  }
  
  final int getSize()
  {
    return this.buffer.size();
  }
  
  Range makeSnapshot()
  {
    return Range.createRange(this.key, this.buffer);
  }
  
  final void removeEvent(Collection<WAEvent> removed)
  {
    Tuple2<WAEvent, Queue<WAEvent>> ret = this.buffer.dequeue();
    this.buffer = ((Queue)ret._2);
    removed.add(ret._1);
  }
}
