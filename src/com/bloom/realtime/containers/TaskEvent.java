package com.bloom.runtime.containers;

import com.bloom.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.IRange;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.log4j.Logger;

public abstract class TaskEvent
  implements ITaskEvent
{
  public static final int HAS_DATA = 1;
  public static final int RESET_AGGREGATES = 2;
  private static final long serialVersionUID = 5836478948970080038L;
  private static Logger logger = Logger.getLogger(TaskEvent.class);
  private UUID queryID = null;
  
  public boolean snapshotUpdate()
  {
    return (batch().isEmpty()) && (removedBatch().isEmpty());
  }
  
  public IBatch batch()
  {
    return Batch.emptyBatch();
  }
  
  public IBatch filterBatch(int indexID, RecordKey key)
  {
    return Batch.emptyBatch();
  }
  
  public IBatch removedBatch()
  {
    return Batch.emptyBatch();
  }
  
  public IRange snapshot()
  {
    return Range.emptyRange();
  }
  
  public int getFlags()
  {
    return 1;
  }
  
  public UUID getQueryID()
  {
    return this.queryID;
  }
  
  public void setQueryID(UUID queryID)
  {
    this.queryID = queryID;
  }
  
  public String toString()
  {
    return "TE(" + batch() + " " + removedBatch() + " " + snapshot() + ")";
  }
  
  public static TaskEvent createWindowEvent(IBatch added, final IBatch removed, final IRange range)
  {
    new TaskEvent()
    {
      private static final long serialVersionUID = 5776239061208773283L;
      IBatch xadded = this.val$added;
      IBatch xremoved = removed;
      IRange xrange = range;
      
      public IBatch batch()
      {
        return this.xadded;
      }
      
      public IBatch removedBatch()
      {
        return this.xremoved;
      }
      
      public IRange snapshot()
      {
        return this.xrange;
      }
      
      public void write(Kryo kryo, Output output)
      {
        kryo.writeClassAndObject(output, this.xadded);
        kryo.writeClassAndObject(output, this.xremoved);
        kryo.writeClassAndObject(output, this.xrange);
      }
      
      public void read(Kryo kryo, Input input)
      {
        this.xadded = ((Batch)kryo.readClassAndObject(input));
        this.xremoved = ((Batch)kryo.readClassAndObject(input));
        this.xrange = ((Range)kryo.readClassAndObject(input));
      }
    };
  }
  
  public static TaskEvent createStreamEvent(List<WAEvent> added)
  {
    new TaskEvent()
    {
      private static final long serialVersionUID = 5260812394346021244L;
      List<WAEvent> xadded = this.val$added;
      
      public Batch batch()
      {
        return Batch.asBatch(this.xadded);
      }
      
      public void write(Kryo kr, Output output)
      {
        if ((this.val$added != null) && (this.xadded.size() > 0))
        {
          if (this.val$added.size() == 1)
          {
            output.writeByte(1);
            kr.writeObject(output, this.xadded.get(0));
          }
          else
          {
            output.writeByte(2);
            kr.writeClassAndObject(output, this.val$added);
          }
        }
        else {
          output.writeByte(0);
        }
      }
      
      public void read(Kryo kr, Input input)
      {
        byte hasAdded = input.readByte();
        if (hasAdded == 0)
        {
          this.xadded = Collections.EMPTY_LIST;
        }
        else if (hasAdded == 1)
        {
          WAEvent item = (WAEvent)kr.readObject(input, WAEvent.class);
          this.xadded = Collections.singletonList(item);
        }
        else
        {
          this.xadded = ((List)kr.readClassAndObject(input));
        }
      }
    };
  }
  
  private static void writeList(List<WAEvent> data, Kryo kr, Output output)
  {
    if ((data != null) && (data.size() > 0))
    {
      output.writeByte((byte)1);
      int size = data.size();
      output.writeInt(size);
      for (int i = 0; i < size; i++) {
        kr.writeObject(output, data.get(i));
      }
    }
    else
    {
      output.writeByte((byte)0);
    }
  }
  
  private static List<WAEvent> readList(Kryo kr, Input input)
  {
    byte isEmpty = input.readByte();
    if (isEmpty == 0) {
      return Collections.emptyList();
    }
    int size = input.readInt();
    assert (size >= 0);
    List<WAEvent> ret = new ArrayList();
    for (int i = 0; i < size; i++)
    {
      WAEvent item = (WAEvent)kr.readObject(input, WAEvent.class);
      ret.add(item);
    }
    return ret;
  }
  
  public static TaskEvent createStreamEvent(List<WAEvent> added, final List<WAEvent> removed, final boolean isStateful)
  {
    new TaskEvent()
    {
      private static final long serialVersionUID = 8121755941701609319L;
      List<WAEvent> xadded = this.val$added;
      List<WAEvent> xremoved = removed;
      boolean stateful = isStateful;
      
      public Batch batch()
      {
        return Batch.asBatch(this.xadded);
      }
      
      public IBatch removedBatch()
      {
        return Batch.asBatch(this.xremoved);
      }
      
      public IRange snapshot()
      {
        if (this.stateful) {
          return Range.createRange(null, this.val$added);
        }
        return Range.emptyRange();
      }
      
      public void write(Kryo kr, Output output)
      {
        output.writeBoolean(this.stateful);
        TaskEvent.writeList(this.xadded, kr, output);
        TaskEvent.writeList(this.xremoved, kr, output);
        UUID test = getQueryID();
        if (test == null)
        {
          output.writeByte((byte)0);
        }
        else
        {
          output.writeByte((byte)1);
          output.writeString(test.getUUIDString());
        }
      }
      
      public void read(Kryo kr, Input input)
      {
        this.stateful = input.readBoolean();
        this.xadded = TaskEvent.readList(kr, input);
        this.xremoved = TaskEvent.readList(kr, input);
        byte result = input.readByte();
        if (result == 1)
        {
          String uuidString = input.readString();
          if (uuidString != null) {
            setQueryID(new UUID(uuidString));
          }
        }
      }
    };
  }
  
  public static TaskEvent createWindowStateEvent(Range snapshot)
  {
    new TaskEvent()
    {
      private static final long serialVersionUID = 3621467538164019186L;
      Range xsnapshot = this.val$snapshot;
      
      public IBatch batch()
      {
        return this.xsnapshot.all();
      }
      
      public IBatch filterBatch(int indexID, RecordKey key)
      {
        return this.xsnapshot.lookup(indexID, key);
      }
      
      public Range snapshot()
      {
        return this.xsnapshot;
      }
      
      public void write(Kryo kryo, Output output)
      {
        kryo.writeClassAndObject(output, this.xsnapshot);
      }
      
      public void read(Kryo kryo, Input input)
      {
        this.xsnapshot = ((Range)kryo.readClassAndObject(input));
      }
    };
  }
  
  public static TaskEvent createWAStoreQueryEvent(Range snapshot)
  {
    new TaskEvent()
    {
      private static final long serialVersionUID = 4601388050386280658L;
      Range xsnapshot = this.val$snapshot;
      
      public IBatch batch()
      {
        return this.xsnapshot.all();
      }
      
      public IBatch filterBatch(int indexID, RecordKey key)
      {
        return this.xsnapshot.lookup(indexID, key);
      }
      
      public Range snapshot()
      {
        return this.xsnapshot;
      }
      
      public void write(Kryo kryo, Output output)
      {
        kryo.writeClassAndObject(output, this.xsnapshot);
      }
      
      public void read(Kryo kryo, Input input)
      {
        this.xsnapshot = ((Range)kryo.readClassAndObject(input));
      }
      
      public int getFlags()
      {
        return 3;
      }
    };
  }
  
  public static TaskEvent createStreamEventWithNewWAEvents(final ITaskEvent event)
  {
    List<WAEvent> added = new ArrayList();
    for (WAEvent original : event.batch()) {
      try
      {
        WAEvent newInstance = (WAEvent)original.getClass().newInstance();
        newInstance.initValues(original);
        added.add(newInstance);
      }
      catch (InstantiationException e)
      {
        logger.error(e);
      }
      catch (IllegalAccessException e)
      {
        logger.error(e);
      }
    }
    final List<WAEvent> removed = new ArrayList();
    for (WAEvent original : event.removedBatch()) {
      try
      {
        WAEvent newInstance = (WAEvent)original.getClass().newInstance();
        newInstance.initValues(original);
        removed.add(newInstance);
      }
      catch (InstantiationException e)
      {
        logger.error(e);
      }
      catch (IllegalAccessException e)
      {
        logger.error(e);
      }
    }
    new TaskEvent()
    {
      List<WAEvent> _added = this.val$added;
      List<WAEvent> _removed = removed;
      TaskEvent _event = (TaskEvent)event;
      private static final long serialVersionUID = -1246997384610275229L;
      
      public Batch batch()
      {
        return Batch.asBatch(this._added);
      }
      
      public IBatch removedBatch()
      {
        return Batch.asBatch(this._removed);
      }
      
      public IRange snapshot()
      {
        return this._event.snapshot();
      }
      
      public void write(Kryo kr, Output output)
      {
        if ((this._added != null) && (this._added.size() > 0))
        {
          if (this._added.size() == 1)
          {
            output.writeByte(1);
            kr.writeObject(output, this._added.get(0));
          }
          else
          {
            output.writeByte(2);
            kr.writeClassAndObject(output, this._added);
          }
        }
        else {
          output.writeByte(0);
        }
        if ((this._removed != null) && (this._removed.size() > 0))
        {
          if (this._removed.size() == 1)
          {
            output.writeByte(1);
            kr.writeObject(output, this._removed.get(0));
          }
          else
          {
            output.writeByte(2);
            kr.writeClassAndObject(output, this._removed);
          }
        }
        else {
          output.writeByte(0);
        }
        if (this._event != null)
        {
          output.writeByte(1);
          kr.writeClassAndObject(output, this._event);
        }
      }
      
      public void read(Kryo kr, Input input)
      {
        this._added = new ArrayList();
        this._removed = new ArrayList();
        byte hasAdded = input.readByte();
        if (hasAdded != 0) {
          if (hasAdded == 1)
          {
            WAEvent item = (WAEvent)kr.readObject(input, WAEvent.class);
            this._added.add(item);
          }
          else
          {
            this._added.addAll((List)kr.readClassAndObject(input));
          }
        }
        byte hasRemoved = input.readByte();
        if (hasRemoved != 0) {
          if (hasRemoved == 1)
          {
            WAEvent item = (WAEvent)kr.readObject(input, WAEvent.class);
            this._removed.add(item);
          }
          else
          {
            this._removed.addAll((List)kr.readClassAndObject(input));
          }
        }
        byte hasEvent = input.readByte();
        if (hasEvent != 0) {
          this._event = ((TaskEvent)kr.readClassAndObject(input));
        }
      }
    };
  }
}
