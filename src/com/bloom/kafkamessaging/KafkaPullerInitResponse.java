package com.bloom.kafkamessaging;

import java.util.Map;

public class KafkaPullerInitResponse
{
  final boolean shouldReInitialize;
  final String kakfaTopicName;
  final Integer partitionId;
  final Object response;
  final Map<Integer, PartitionState> partitions;
  
  public KafkaPullerInitResponse(boolean shouldReInitialize, String kafkaTopicName, Integer partitionId, Object second, Map<Integer, PartitionState> partitions)
  {
    this.shouldReInitialize = shouldReInitialize;
    this.kakfaTopicName = kafkaTopicName;
    this.partitionId = partitionId;
    this.response = second;
    this.partitions = partitions;
  }
}
