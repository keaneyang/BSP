package com.bloom.runtime.window;

import com.bloom.runtime.containers.Range;
import com.bloom.runtime.containers.WAEvent;
import java.util.List;
import java.util.NoSuchElementException;
import scala.collection.JavaConverters.;
import scala.collection.convert.Decorators.AsJava;
import scala.collection.immutable.Queue;
import scala.collection.immutable.Queue.;

abstract class WBufferJumping1
  extends WBuffer
{
  private Queue<WAEvent> buffer = Queue..MODULE$.empty();
  private Queue<WAEvent> oldbuf = Queue..MODULE$.empty();
  
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
    return Range.createRange(this.key, this.oldbuf);
  }
  
  void doJump()
    throws Exception
  {
    List<WAEvent> newrec = (List)JavaConverters..MODULE$.seqAsJavaListConverter(this.buffer).asJava();
    List<WAEvent> oldrec = (List)JavaConverters..MODULE$.seqAsJavaListConverter(this.oldbuf).asJava();
    this.oldbuf = this.buffer;
    this.buffer = Queue..MODULE$.empty();
    publish(newrec, oldrec);
  }
}
