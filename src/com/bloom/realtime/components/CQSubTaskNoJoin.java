package com.bloom.runtime.components;

import com.bloom.recovery.Position;
import com.bloom.runtime.containers.IRange;
import com.bloom.runtime.containers.WAEvent;

public abstract class CQSubTaskNoJoin
  extends CQSubTask
{
  private WAEvent waRows;
  
  public void init(CQTask context)
  {
    this.context = context;
  }
  
  public void setRow(WAEvent event)
  {
    this.waRows = event;
  }
  
  public Object getRowData(int ds)
  {
    assert (ds == 0);
    Object row = this.waRows.data;
    if (row == null)
    {
      String streamName = this.context.getSourceName(ds);
      throw new RuntimeException("null data in stream <" + streamName + ">");
    }
    return row;
  }
  
  public Position getRowPosition(int ds)
  {
    assert (ds == 0);
    return this.waRows == null ? null : this.waRows.position;
  }
  
  public Object getWindowRow(int ds, int index)
  {
    return getRowData(ds);
  }
  
  public Object[] getSourceEvents()
  {
    return new Object[] { this.waRows.data };
  }
  
  public void updateState()
  {
    IRange[] state = this.context.copyState();
    for (IRange range : state) {
      if (range != null) {
        range.beginTransaction();
      }
    }
  }
  
  public void cleanState()
  {
    IRange[] state = this.context.copyState();
    for (IRange range : state) {
      if (range != null) {
        range.endTransaction();
      }
    }
  }
}
