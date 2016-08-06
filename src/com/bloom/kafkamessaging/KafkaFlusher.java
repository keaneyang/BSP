package com.bloom.kafkamessaging;

import java.util.ArrayList;
import java.util.List;

import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.components.Stream;

public class KafkaFlusher
  implements Runnable
{
  private final Stream streamRuntime;
  List<KafkaSender.PositionedBuffer> partitions;
  
  public KafkaFlusher(Stream streamRuntime)
  {
    this.partitions = new ArrayList();
    this.streamRuntime = streamRuntime;
  }
  
  public void run()
  {
    try
    {
      for (KafkaSender.PositionedBuffer dataBuffer : this.partitions) {
        dataBuffer.flushToKafka();
      }
    }
    catch (Exception e)
    {
      this.streamRuntime.notifyAppMgr(EntityType.STREAM, this.streamRuntime.getMetaFullName(), this.streamRuntime.getMetaID(), e, null, new Object[0]);
    }
  }
  
  public void add(KafkaSender.PositionedBuffer dataBuffer)
  {
    this.partitions.add(dataBuffer);
  }
}
