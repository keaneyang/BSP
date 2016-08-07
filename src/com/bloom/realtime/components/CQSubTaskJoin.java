package com.bloom.runtime.components;

import com.bloom.recovery.Position;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.IRange;
import com.bloom.runtime.containers.WAEvent;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Iterator;

public abstract class CQSubTaskJoin
  extends CQSubTask
{
  private WAEvent[] rows;
  private Iterator<WAEvent>[] iterators;
  private IRange[] state;
  
  public boolean isUpdateIndexesCodeGenerated()
  {
    return true;
  }
  
  private void set(int index, WAEvent r)
  {
    if (this.context.isTracingExecution())
    {
      PrintStream out = this.context.getTracingStream();
      out.println("set(" + index + ", " + r + ")");
    }
    this.rows[index] = r;
  }
  
  public void init(CQTask context)
  {
    this.context = context;
    int dscount = context.getDataSetCount();
    this.rows = new WAEvent[dscount];
    this.iterators = new Iterator[dscount];
  }
  
  public void updateState()
  {
    this.state = this.context.copyState();
    for (IRange range : this.state) {
      range.beginTransaction();
    }
    boolean isUpdateIndexesCodeGenerated = isUpdateIndexesCodeGenerated();
    if (isUpdateIndexesCodeGenerated)
    {
      for (WAEvent e : getAdded()) {
        updateIndexes(e, true);
      }
      for (WAEvent e : getRemoved()) {
        updateIndexes(e, false);
      }
    }
  }
  
  public void cleanState()
  {
    for (IRange range : this.state) {
      range.endTransaction();
    }
  }
  
  public Object getRowData(int ds)
  {
    Object row = this.rows[ds] == null ? null : this.rows[ds].data;
    if (this.context.isTracingExecution())
    {
      PrintStream out = this.context.getTracingStream();
      out.println("getRowData(" + ds + ") returned " + row);
    }
    return row;
  }
  
  public Position getRowPosition(int ds)
  {
    Position row = this.rows[ds] == null ? null : this.rows[ds].position;
    if (this.context.isTracingExecution())
    {
      PrintStream out = this.context.getTracingStream();
      out.println("getRowPosition(" + ds + ") returned " + row);
    }
    return row;
  }
  
  public Object getWindowRow(int ds, int index)
  {
    Object row = getRowData(ds);
    if (this.context.isTracingExecution())
    {
      PrintStream out = this.context.getTracingStream();
      out.println("getWindowRow(" + ds + ", " + index + ") returned " + row);
    }
    return row;
  }
  
  public void createEmptyIterator(int ds)
  {
    this.iterators[ds] = Collections.emptyIterator();
  }
  
  public void createWindowIterator(int ds)
  {
    IRange rng = this.state[ds];
    assert (rng != null);
    Iterator<WAEvent> i = rng.all().iterator();
    if (this.context.isTracingExecution())
    {
      PrintStream out = this.context.getTracingStream();
      out.println("createWindowIterator(" + ds + ")");
      while (i.hasNext()) {
        out.println("  " + i.next());
      }
      i = rng.all().iterator();
    }
    this.iterators[ds] = i;
  }
  
  public void createIndexIterator(int ds, int index, RecordKey key)
  {
    this.iterators[ds] = this.context.createIndexIterator(index, key);
  }
  
  public boolean fetch(int ds)
  {
    Iterator<WAEvent> it = this.iterators[ds];
    boolean ret = it.hasNext();
    WAEvent e = ret ? (WAEvent)it.next() : null;
    set(ds, e);
    if (this.context.isTracingExecution())
    {
      PrintStream out = this.context.getTracingStream();
      out.println("fetch(" + ds + ") returned " + ret);
    }
    return ret;
  }
  
  public void updateIndexes(WAEvent event, boolean doadd) {}
  
  public void setRow(WAEvent event)
  {
    if (this.context.isTracingExecution())
    {
      PrintStream out = this.context.getTracingStream();
      out.println("setRow(" + event + ")");
    }
    set(getThisDS(), event);
  }
  
  public void createWindowLookupIterator(int ds, int index, RecordKey key)
  {
    this.iterators[ds] = this.state[ds].lookup(index, key).iterator();
  }
  
  public Object[] getSourceEvents()
  {
    Object[] result = new Object[this.rows.length];
    for (int i = 0; i < this.rows.length; i++) {
      result[i] = this.rows[i].data;
    }
    return result;
  }
  
  public void createCollectionIterator(int ds, final Object o)
  {
    this.iterators[ds = new Iterator()
    {
      final Iterator<Object> it = ((Iterable)o).iterator();
      
      public boolean hasNext()
      {
        return this.it.hasNext();
      }
      
      public WAEvent next()
      {
        return new WAEvent(this.it.next());
      }
      
      public void remove() {}
    };
  }
}
