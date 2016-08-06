package com.bloom.kafkamessaging;

import kafka.cluster.Broker;
import kafka.javaapi.consumer.SimpleConsumer;

public class SimpleConsumerBroker
{
  SimpleConsumer simpleConsumer;
  Broker broker;
  
  public SimpleConsumerBroker(SimpleConsumer simpleConsumer, Broker broker)
  {
    this.simpleConsumer = simpleConsumer;
    this.broker = broker;
  }
}
