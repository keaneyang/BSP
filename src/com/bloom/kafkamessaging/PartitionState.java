package com.bloom.kafkamessaging;

import com.bloom.recovery.Position;
import kafka.cluster.Broker;
import kafka.javaapi.consumer.SimpleConsumer;

public class PartitionState
{
  public final int partitionId;
  public long kafkaReadOffset = 0L;
  public SimpleConsumer kafkaTopicConsumer = null;
  public Broker kafkaTopicBroker = null;
  public OffsetPosition partitionCheckpoint = null;
  public SimpleConsumer checkpointTopicConsumer = null;
  public Broker checkpointTopicBroker = null;
  public long lastCheckpointWriteOffset = -1L;
  public Position lastEmittedPosition;
  
  public PartitionState(int partitionId)
  {
    this.partitionId = partitionId;
  }
  
  public String toString()
  {
    return "PartitionState{" + this.partitionId + "@" + this.kafkaReadOffset + " con=" + this.kafkaTopicConsumer + " bro=" + this.kafkaTopicBroker + " part=" + this.partitionCheckpoint + " chCon=" + this.checkpointTopicConsumer + " chBro=" + this.checkpointTopicBroker + " lastWrite=" + this.lastCheckpointWriteOffset + "}";
  }
}
