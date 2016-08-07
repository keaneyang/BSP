package com.bloom.runtime.window;

import com.bloom.runtime.containers.Range;
import com.bloom.runtime.containers.WAEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

abstract class WBufferJumping
  extends WBuffer
{
  private List<WAEvent> buffer = new ArrayList();
  private List<WAEvent> oldbuf = new ArrayList();
  
  final void addEvent(WAEvent e)
  {
    this.buffer.add(e);
  }
  
  final WAEvent getFirstEvent()
    throws NoSuchElementException
  {
    try
    {
      return (WAEvent)this.buffer.get(0);
    }
    catch (IndexOutOfBoundsException e)
    {
      throw new NoSuchElementException();
    }
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
    List<WAEvent> newrec = this.buffer;
    List<WAEvent> oldrec = this.oldbuf;
    this.oldbuf = this.buffer;
    this.buffer = new ArrayList();
    publish(newrec, oldrec);
  }
}
