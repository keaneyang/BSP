package com.bloom.kafkamessaging;

public class KafkaPartitionSendStats
{
  long sentCount;
  long time;
  long bytes;
  long waitCount;
  long maxLatency;
  long iterations;
  
  public KafkaPartitionSendStats()
  {
    resetValues();
  }
  
  public void resetValues()
  {
    this.sentCount = (this.time = this.bytes = this.waitCount = this.maxLatency = this.iterations = 0L);
  }
  
  public void record(long time, long bytes, long sentCount, long waitCount)
  {
    this.iterations += 1L;
    this.time += time;
    this.maxLatency = Math.max(this.maxLatency, time);
    this.bytes += bytes;
    this.sentCount += sentCount;
    this.waitCount += waitCount;
  }
}
