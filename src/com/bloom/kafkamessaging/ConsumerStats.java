package com.bloom.kafkamessaging;

import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.log4j.Logger;

public class ConsumerStats
{
  private static final Logger logger = Logger.getLogger(ConsumerStats.class);
  private SimpleConsumer simpleConsumer;
  private long startTime = 0L;
  private long endTime = 0L;
  private long size = 0L;
  private long count = 0L;
  private long totalTime = 0L;
  private long maxLatency = 0L;
  private long iterations = 0L;
  
  public ConsumerStats(SimpleConsumer simpleConsumer)
  {
    this.simpleConsumer = simpleConsumer;
  }
  
  public void setStartTime(long startTime)
  {
    this.startTime = startTime;
  }
  
  public long getStartTime()
  {
    return this.startTime;
  }
  
  public void setEndTime(long endTime)
  {
    this.endTime = endTime;
    this.totalTime += endTime - this.startTime;
    this.maxLatency = Math.max(this.maxLatency, this.totalTime);
  }
  
  public long getEndTime()
  {
    return this.endTime;
  }
  
  public void addMessageSize(int i)
  {
    this.size += i;
  }
  
  public void incrementMessageCount(int waEventIndex)
  {
    this.count += waEventIndex;
  }
  
  public void increaseIterations()
  {
    this.iterations += 1L;
  }
  
  public void reset()
  {
    this.startTime = 0L;
    this.endTime = 0L;
    this.size = 0L;
    this.count = 0L;
    this.totalTime = 0L;
    this.maxLatency = 0L;
  }
  
  public void printStats()
  {
    if (logger.isDebugEnabled()) {
      logger.debug("Stats for Consumer to broker: " + this.simpleConsumer.host() + ":" + this.simpleConsumer.port());
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Total Events: " + this.count + ", " + "Event rate: " + this.count / (this.totalTime / 1000.0D) + " events/sec, " + "Read throughput: " + this.size / 1000000.0D / (this.totalTime / 1000.0D) + " MB/sec, " + "Max latency: " + this.maxLatency + " ms, " + "Iterations : " + this.iterations);
    }
  }
}
