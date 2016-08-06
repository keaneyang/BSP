package com.bloom.kafkamessaging;

import java.util.List;
import kafka.cluster.Broker;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class ConsumerBrokerAndPartitionStates
{
  SimpleConsumer consumer;
  Broker broker;
  KafkaProducer<byte[], byte[]> producer;
  List<PartitionState> partitionStates;
  
  public ConsumerBrokerAndPartitionStates(SimpleConsumer simpleConsumer, Broker broker, KafkaProducer<byte[], byte[]> kafkaProducer, List<PartitionState> partitionStates)
  {
    this.consumer = simpleConsumer;
    this.broker = broker;
    this.producer = kafkaProducer;
    this.partitionStates = partitionStates;
  }
  
  public void setPartitionStates(List<PartitionState> startingOffset)
  {
    this.partitionStates = startingOffset;
  }
}
