package com.bloom.runtime.window;

import com.bloom.runtime.KeyFactory;
import com.bloom.runtime.containers.Batch;
import com.bloom.runtime.containers.Range;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;
import java.util.Map;
import java.util.Map.Entry;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.immutable.HashMap;

class PartitionedBufferPolicy
  implements WBufferPolicy
{
  final KeyFactory keyFactory;
  scala.collection.immutable.HashMap<RecordKey, WBuffer> buffers = HashMap.MODULE$.empty();
  
  PartitionedBufferPolicy(KeyFactory kf)
    throws Exception
  {
    this.keyFactory = kf;
  }
  
  private WBuffer getWindowByKey(ScalaWindow w, RecordKey key)
  {
    Option<WBuffer> entry = this.buffers.get(key);
    WBuffer b;
    if (entry.isEmpty())
    {
       b = w.createBuffer(key);
      this.buffers = this.buffers.$plus(new Tuple2(key, b));
    }
    else
    {
      b = (WBuffer)entry.get();
    }
    return b;
  }
  
  public void updateBuffer(ScalaWindow w, Batch batch, long now)
    throws Exception
  {
    Map<RecordKey, IBatch<WAEvent>> map = Batch.partition(this.keyFactory, batch);
    for (Map.Entry<RecordKey, IBatch<WAEvent>> e : map.entrySet())
    {
      RecordKey key = (RecordKey)e.getKey();
      IBatch keybatch = (IBatch)e.getValue();
      WBuffer b = getWindowByKey(w, key);
      w.updateBuffer(b, (Batch)keybatch, now);
    }
  }
  
  public void initBuffer(ScalaWindow w)
  {
    this.buffers = HashMap.MODULE$.empty();
  }
  
  public void onJumpingTimer()
    throws Exception
  {
    Iterator<Tuple2<RecordKey, WBuffer>> it = this.buffers.iterator();
    while (it.hasNext())
    {
      Tuple2<RecordKey, WBuffer> t = (Tuple2)it.next();
      try
      {
        ((WBuffer)t._2).removeAll();
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    }
  }
  
  public Range createSnapshot()
  {
    Map<RecordKey, IBatch> map = new java.util.HashMap();
    Iterator<Tuple2<RecordKey, WBuffer>> it = this.buffers.iterator();
    while (it.hasNext())
    {
      Tuple2<RecordKey, WBuffer> t = (Tuple2)it.next();
      map.put(t._1, ((WBuffer)t._2).makeSnapshot().all());
    }
    Range range = (Range)Range.createRange(map);
    return range;
  }
}
