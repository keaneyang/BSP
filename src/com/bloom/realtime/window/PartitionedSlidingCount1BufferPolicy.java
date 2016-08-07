package com.bloom.runtime.window;

import com.bloom.runtime.KeyFactory;
import com.bloom.runtime.containers.Batch;
import com.bloom.runtime.containers.Range;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;
import java.util.ArrayList;
import java.util.List;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.HashMap;

class PartitionedSlidingCount1BufferPolicy
  implements WBufferPolicy
{
  final KeyFactory keyFactory;
  HashMap<RecordKey, WAEvent> buffer = HashMap.MODULE$.empty();
  
  PartitionedSlidingCount1BufferPolicy(KeyFactory kf)
  {
    this.keyFactory = kf;
  }
  
  public void updateBuffer(ScalaWindow w, Batch batch, long now)
    throws Exception
  {
    List<WAEvent> removed = new ArrayList();
    IBatch<WAEvent> waEventIBatch = batch;
    for (WAEvent rec : waEventIBatch)
    {
      RecordKey key = this.keyFactory.makeKey(rec.data);
      Option<WAEvent> prev = this.buffer.get(key);
      if (prev.nonEmpty()) {
        removed.add(prev.get());
      }
      this.buffer = this.buffer.$plus(new Tuple2(key, rec));
    }
    w.publish(batch, removed, createSnapshot());
  }
  
  public void initBuffer(ScalaWindow w)
  {
    this.buffer = HashMap.MODULE$.empty();
  }
  
  public void onJumpingTimer()
    throws Exception
  {
    throw new UnsupportedOperationException();
  }
  
  public Range createSnapshot()
  {
    return Range.createRange(this.buffer);
  }
}
