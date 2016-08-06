package com.bloom.kafkamessaging;

import com.bloom.messaging.Handler;
import com.bloom.messaging.Receiver;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.KafkaStreamUtils;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.jmqmessaging.KafkaReceiverInfo;
import com.bloom.recovery.PartitionedSourcePosition;
import com.bloom.recovery.Position;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class KafkaReceiver
  implements Receiver
{
  private static Logger logger = Logger.getLogger(KafkaReceiver.class);
  protected final KafkaReceiverInfo info;
  protected MetaInfo.Stream streamInfo;
  protected final String name;
  protected final boolean isEncrypted;
  Map<Object, Object> properties;
  KafkaPuller kafkaPuller;
  ExecutorService executorService;
  
  public KafkaReceiver(KafkaReceiverInfo info, MetaInfo.Stream streamInfo, Handler rcvr, boolean encrypted)
  {
    this.info = info;
    this.name = info.getRcvr_name();
    this.isEncrypted = encrypted;
    this.streamInfo = streamInfo;
    
    this.kafkaPuller = new KafkaPuller(this, rcvr);
    if (logger.isInfoEnabled()) {
      logger.info("Created KafkaReceiver info.name=" + info.getName());
    }
  }
  
  public KafkaReceiver(KafkaReceiverInfo info, Handler rcvr, boolean encrypted, MetaInfo.Stream streamInfo, boolean isTest)
    throws MetaDataRepositoryException
  {
    this.info = info;
    this.name = info.getRcvr_name();
    this.isEncrypted = encrypted;
    this.streamInfo = streamInfo;
    if (isTest)
    {
      this.properties = new HashMap();
    }
    else
    {
      this.kafkaPuller = new KafkaPuller(this, rcvr);
      if (logger.isInfoEnabled()) {
        logger.info("Created KafkaReceiver info.name=" + info.getName());
      }
    }
  }
  
  public void start(Map<Object, Object> properties)
    throws Exception
  {
    this.properties = properties;
    List<Integer> partitions = (List)this.properties.get("partitions_i_own");
    if (partitions.isEmpty()) {
      throw new KafkaException("Cannot start Kafka Puller for " + this.info.getTopic_name() + " with zero partitions");
    }
    List<String> kafkaBrokers = (List)properties.get(KafkaConstants.broker_address_list);
    if ((kafkaBrokers == null) || (kafkaBrokers.isEmpty())) {
      kafkaBrokers = KafkaStreamUtils.getBrokerAddress(this.streamInfo);
    }
    for (int ii = 0; ii < KafkaConstants.maxAttemptsToInitKafka; ii++) {
      try
      {
        this.kafkaPuller.init(partitions, kafkaBrokers);
        ii = KafkaConstants.maxAttemptsToInitKafka + 1;
        if (logger.isInfoEnabled()) {
          logger.info("Initializing Kafka Puller for " + this.info.getRcvr_name());
        }
      }
      catch (Exception e)
      {
        if (ii < KafkaConstants.maxAttemptsToInitKafka)
        {
          if (logger.isInfoEnabled()) {
            logger.info("Initializing Kafka Puller for " + this.info.getRcvr_name() + " " + "failed, while retry in 2 seconds");
          }
          this.kafkaPuller.doCleanUp();
          this.kafkaPuller.cleanupConsumers();
          this.kafkaPuller.cleanupLocalTasks();
          KafkaStreamUtils.retryBackOffMillis(2000L);
        }
        else
        {
          throw e;
        }
      }
    }
  }
  
  public boolean stop()
    throws InterruptedException
  {
    this.kafkaPuller.stop();
    this.executorService.shutdown();
    this.kafkaPuller.doCleanUp();
    while (!this.executorService.awaitTermination(2L, TimeUnit.SECONDS)) {
      if (logger.isInfoEnabled()) {
        logger.info("Waiting for 2 seconds to shutdown executor service fro Kafka Puller");
      }
    }
    if (logger.isInfoEnabled()) {
      logger.info("Stopped Kafka Receiver : " + this.name);
    }
    return false;
  }
  
  public void setPosition(PartitionedSourcePosition position)
  {
    this.kafkaPuller.setStartPosition(position);
  }
  
  public void startForEmitting()
    throws Exception
  {
    this.executorService = Executors.newSingleThreadExecutor();
    this.executorService.submit(this.kafkaPuller);
  }
  
  public Position getComponentCheckpoint()
  {
    return this.kafkaPuller.getComponentCheckpoint();
  }
}
