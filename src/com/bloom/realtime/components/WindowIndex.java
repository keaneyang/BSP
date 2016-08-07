package com.bloom.runtime.components;

import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.WAEvent;
import java.util.Collections;
import java.util.Iterator;

public class WindowIndex
{
  public Iterator<WAEvent> createIterator(RecordKey key)
  {
    return Collections.emptyIterator();
  }
  
  public void update(RecordKey key, WAEvent event, boolean doadd) {}
}
