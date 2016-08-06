package com.bloom.kafkamessaging;

import com.bloom.messaging.Handler;
import com.bloom.runtime.KafkaDistributedRcvr;
import com.bloom.runtime.KafkaStreamUtils;
import com.bloom.runtime.Pair;
import com.bloom.runtime.Server;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.components.FlowComponent;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.utility.Utility;
import com.bloom.uuid.UUID;
import com.bloom.jmqmessaging.KafkaReceiverInfo;
import com.bloom.recovery.KafkaSourcePosition;
import com.bloom.recovery.PartitionedSourcePosition;
import com.bloom.recovery.Position;
import com.bloom.recovery.SourcePosition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import kafka.cluster.Broker;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;

public class KafkaPuller
  implements Runnable
{
  private static Logger logger = Logger.getLogger(KafkaPuller.class);
  private static final long STATS_PERIOD_MILLIS = 10000L;
  final String kafkaTopicName;
  final UUID streamUuid;
  final Handler rcvr;
  final String clientName;
  final KafkaReceiver kafkaReceiver;
  private final boolean isSingleThreaded = true;
  final boolean isRecoveryEnabled;
  final Set<PartitionState> partitions = new HashSet();
  final Map<SimpleConsumer, Set<PartitionState>> consumerPartitions = new HashMap();
  private final Set<SimpleConsumer> checkpointConsumers = new HashSet();
  private Pair<KafkaPullerTask, Future>[] kafkaPullerTasks;
  private ExecutorService dataExecutorService;
  Pair<StreamCheckpointWriterTask, Future> kafkaCheckpointTasks;
  private ScheduledThreadPoolExecutor checkpointExecutorService;
  Pair<KafkaPullerStatsTask, Future> kafkaStatsTasks;
  private ScheduledThreadPoolExecutor statsExecutorService;
  private boolean isStarted;
  private PartitionedSourcePosition startPosition = null;
  KafkaProducer<byte[], byte[]> kafkaProducer;
  private List<String> boostrapKafkaBrokers;
  private List<Integer> boostrapKafkaPartitions;
  private volatile boolean dontStop = true;
  
  public KafkaPuller(KafkaReceiver kafkaReceiver, Handler rcvr)
  {
    this.kafkaReceiver = kafkaReceiver;
    this.kafkaTopicName = kafkaReceiver.info.getTopic_name();
    this.streamUuid = kafkaReceiver.streamInfo.getUuid();
    this.rcvr = rcvr;
    this.clientName = ("Client_" + this.kafkaTopicName + "_" + getServerId());
    this.isRecoveryEnabled = ((KafkaDistributedRcvr)rcvr).isRecoveryEnabled();
    createProducer();
    if (logger.isInfoEnabled()) {
      logger.info("Kafka Puller for " + kafkaReceiver.name + " created to read from " + this.kafkaTopicName);
    }
  }
  
  private void createProducer()
  {
    Properties producerProperties = new Properties();
    
    List<String> kafka_bootstrap_brokers = KafkaStreamUtils.getBrokerAddress(this.kafkaReceiver.streamInfo);
    producerProperties.put("bootstrap.servers", kafka_bootstrap_brokers);
    producerProperties.put("acks", KafkaConstants.producer_acks);
    producerProperties.put("key.serializer", "com.bloom.kafkamessaging.KafkaEncoder");
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProperties.put("buffer.memory", Integer.valueOf(KafkaConstants.producer_buffer_memory));
    producerProperties.put("batch.size", Integer.valueOf(KafkaConstants.producer_batchsize));
    producerProperties.put("max.request.size", Integer.valueOf(KafkaConstants.producer_max_request_size));
    this.kafkaProducer = new KafkaProducer(producerProperties);
  }
  
  public static UUID getServerId()
  {
    if (Server.getServer() != null) {
      return Server.getServer().getServerID();
    }
    return new UUID(System.currentTimeMillis());
  }
  
  public void init(List<Integer> kafkaPartitions, List<String> kafkaBrokers)
    throws Exception
  {
    this.boostrapKafkaPartitions = kafkaPartitions;
    this.boostrapKafkaBrokers = kafkaBrokers;
    Map<Integer, PartitionState> kpir = initConsumersForKafka(kafkaPartitions, kafkaBrokers);
    if (this.isRecoveryEnabled) {
      initRecoveryRelatedMetadata(kafkaBrokers, kafkaPartitions, kpir);
    }
  }
  
  void reInitKafka()
    throws Exception
  {
    createProducer();
    init(this.boostrapKafkaPartitions, this.boostrapKafkaBrokers);
  }
  
  public Map<Integer, PartitionState> initConsumersForKafka(List<Integer> kafkaPartitions, List<String> kafkaBrokers)
    throws Exception
  {
    Map<Integer, PartitionState> result = new HashMap();
    
    Map<Broker, List<Integer>> kafkaPartitionLeaders = KafkaStreamUtils.getBrokerPartitions(kafkaBrokers, this.kafkaTopicName, kafkaPartitions, 20);
    if (Logger.getLogger("KafkaStreams").isInfoEnabled()) {
      logger.info(kafkaPartitionLeaders);
    }
    if ((kafkaPartitionLeaders == null) || (kafkaPartitionLeaders.isEmpty())) {
      throw new KafkaException("Failed to communicate with Kafka Brokers [" + kafkaBrokers + "] to find Leader for [" + this.kafkaTopicName + ", with partitions " + kafkaPartitions + "]");
    }
    try
    {
      for (Map.Entry<Broker, List<Integer>> entry : kafkaPartitionLeaders.entrySet())
      {
        Broker broker = (Broker)entry.getKey();
        SimpleConsumer simpleConsumer = new SimpleConsumer(broker.host(), broker.port(), 100000, 65536, this.clientName);
        Set<PartitionState> partitionsForConsumer = new HashSet();
        
        List<Integer> partitionIds = (List)entry.getValue();
        for (Integer partitionId : partitionIds)
        {
          PartitionState ps = new PartitionState(partitionId.intValue());
          ps.kafkaTopicBroker = broker;
          ps.kafkaTopicConsumer = simpleConsumer;
          
          SourcePosition sp = this.startPosition == null ? null : this.startPosition.get(String.valueOf(partitionId));
          if (sp == null)
          {
            OffsetResponse offsetResponse = KafkaStreamUtils.getFirstOffset(this.kafkaTopicName, partitionId.intValue(), ps.kafkaTopicConsumer);
            ps.kafkaReadOffset = offsetResponse.offsets(this.kafkaTopicName, partitionId.intValue())[0];
          }
          else if ((sp instanceof KafkaSourcePosition))
          {
            KafkaSourcePosition ksp = (KafkaSourcePosition)sp;
            ps.kafkaReadOffset = ksp.getKafkaReadOffset();
          }
          else
          {
            logger.error("Unexpected Kafka stream initialized with a non-KafkaSourcePosition; starting from beginning: " + sp);
            OffsetResponse offsetResponse = KafkaStreamUtils.getFirstOffset(this.kafkaTopicName, partitionId.intValue(), ps.kafkaTopicConsumer);
            ps.kafkaReadOffset = offsetResponse.offsets(this.kafkaTopicName, partitionId.intValue())[0];
          }
          result.put(partitionId, ps);
          partitionsForConsumer.add(ps);
          this.partitions.add(ps);
        }
        this.consumerPartitions.put(simpleConsumer, partitionsForConsumer);
      }
    }
    catch (Exception e)
    {
      throw e;
    }
    return result;
  }
  
  private void initRecoveryRelatedMetadata(List<String> kafkaBrokers, List<Integer> kafkaPartitions, Map<Integer, PartitionState> partitions)
    throws Exception
  {
    String checkpointTopicName = KafkaStreamUtils.getCheckpointTopicName(this.kafkaTopicName);
    Map<Broker, List<Integer>> checkpointTopicLeaders = KafkaStreamUtils.getBrokerPartitions(kafkaBrokers, checkpointTopicName, kafkaPartitions, 20);
    if (Logger.getLogger("KafkaStreams").isInfoEnabled()) {
      logger.info(checkpointTopicLeaders);
    }
    if ((checkpointTopicLeaders == null) || (checkpointTopicLeaders.isEmpty())) {
      throw new KafkaException("Failed to communicate with Kafka Brokers [" + kafkaBrokers + "] to find Leader for [" + checkpointTopicName + ", with partitions " + kafkaPartitions + "]");
    }
    try
    {
    	Broker broker;
      for (Map.Entry<Broker, List<Integer>> entry : checkpointTopicLeaders.entrySet())
      {
        broker = (Broker)entry.getKey();
        List<Integer> partitionIds = (List)entry.getValue();
        for (Integer partitionId : partitionIds)
        {
          PartitionState ps = (PartitionState)partitions.get(partitionId);
          if (ps == null)
          {
            logger.error("Kafka partition information not found when trying to add checkpoint information: " + this.kafkaTopicName + ":" + partitionId);
          }
          else
          {
            ps.checkpointTopicBroker = broker;
            SimpleConsumer checkpointConsumer = new SimpleConsumer(broker.host(), broker.port(), 100000, 65536, this.clientName);
            ps.checkpointTopicConsumer = checkpointConsumer;
            
            Object rawObject = KafkaStreamUtils.getLastObjectFromTopic(checkpointTopicName, partitionId.intValue(), ps.checkpointTopicConsumer, 2);
            OffsetPosition partitionCheckpoint = null;
            if (rawObject != null) {
              if ((rawObject instanceof OffsetPosition)) {
                partitionCheckpoint = (OffsetPosition)rawObject;
              } else {
                throw new KafkaException("Unexpected class type: " + rawObject.getClass().getSimpleName() + " while reading" + " last object from Kafka topic: " + checkpointTopicName + "[" + partitionId + "]");
              }
            }
            if (partitionCheckpoint == null)
            {
              OffsetResponse offsetResponse = KafkaStreamUtils.getFirstOffset(this.kafkaTopicName, partitionId.intValue(), ps.kafkaTopicConsumer);
              ps.lastCheckpointWriteOffset = offsetResponse.offsets(this.kafkaTopicName, partitionId.intValue())[0];
              partitionCheckpoint = new OffsetPosition(ps.lastCheckpointWriteOffset, System.currentTimeMillis());
            }
            if (partitionCheckpoint.getOffset() < ps.kafkaReadOffset)
            {
              KafkaStreamUtils.updateCheckpoint(partitionCheckpoint, this.kafkaTopicName, partitionId.intValue(), ps.kafkaReadOffset, ps.checkpointTopicConsumer);
              MetaInfo.Stream streamInfo = this.kafkaReceiver.streamInfo;
              KafkaStreamUtils.writeCheckpoint(this.kafkaTopicName, partitionId.intValue(), partitionCheckpoint, this.kafkaProducer, streamInfo, ps.checkpointTopicConsumer, 3);
            }
            ps.partitionCheckpoint = partitionCheckpoint;
            
            this.checkpointConsumers.add(checkpointConsumer);
          }
        }
      }
    }
    catch (Exception e)
    {
      
      throw e;
    }
  }
  
  public synchronized void stop()
    throws InterruptedException
  {
    assert (this.isStarted);
    this.dontStop = false;
    
    this.isStarted = false;
    if (logger.isInfoEnabled()) {
      logger.info("Kafka Puller for " + this.kafkaReceiver.name + " stopped");
    }
  }
  
  public void startReadingFromTopic()
    throws Exception
  {
    assert (!this.isStarted);
    if (Logger.getLogger("KafkaStreams").isInfoEnabled()) {
      logger.info("Creating puller task for all consumers");
    }
    KafkaPullerTask kafkaPullerTask = new KafkaPullerTask(this, this.consumerPartitions);
    this.dataExecutorService = Executors.newFixedThreadPool(1);
    Future future = this.dataExecutorService.submit(kafkaPullerTask);
    Pair pair = new Pair(kafkaPullerTask, future);
    this.kafkaPullerTasks = new Pair[1];
    this.kafkaPullerTasks[0] = pair;
    if (logger.isDebugEnabled())
    {
      KafkaPullerStatsTask kafkaPullerStats = new KafkaPullerStatsTask(this.kafkaPullerTasks, this.kafkaReceiver);
      this.statsExecutorService = new ScheduledThreadPoolExecutor(1);
      Future statsFuture = this.statsExecutorService.scheduleAtFixedRate(kafkaPullerStats, 10000L, 10000L, TimeUnit.MILLISECONDS);
      this.kafkaStatsTasks = new Pair(kafkaPullerStats, statsFuture);
    }
    if (this.isRecoveryEnabled)
    {
      StreamCheckpointWriterTask writerTask = new StreamCheckpointWriterTask(this);
      this.checkpointExecutorService = new ScheduledThreadPoolExecutor(1);
      Future checkpointFuture = this.checkpointExecutorService.scheduleAtFixedRate(writerTask, 10000L, 10000L, TimeUnit.MILLISECONDS);
      
      this.kafkaCheckpointTasks = new Pair(writerTask, checkpointFuture);
    }
    this.isStarted = true;
    if (logger.isInfoEnabled()) {
      logger.info("Kafka Puller started reading from topic " + this.kafkaReceiver.name);
    }
  }
  
  public void setStartPosition(PartitionedSourcePosition position)
  {
    assert (!this.isStarted);
    if (position == null) {
      return;
    }
    if (Logger.getLogger("KafkaStreams").isDebugEnabled()) {
      Logger.getLogger("KafkaStreams").debug("Set Kafka puller position " + position);
    }
    for (PartitionState ps : this.partitions)
    {
      String valueOf = String.valueOf(ps.partitionId);
      SourcePosition sp = position.get(valueOf);
      if ((sp instanceof KafkaSourcePosition))
      {
        KafkaSourcePosition ksp = (KafkaSourcePosition)sp;
        ps.kafkaReadOffset = ksp.getKafkaReadOffset();
      }
    }
    this.startPosition = position;
  }
  
  public Position getComponentCheckpoint()
  {
    Position result = new Position();
    for (PartitionState ps : this.partitions) {
      result.mergeHigherPositions(ps.lastEmittedPosition);
    }
    if (Logger.getLogger("KafkaStreams").isDebugEnabled())
    {
      Logger.getLogger("KafkaStreams").debug("Component checkpoint for " + this.kafkaTopicName + ":");
      Utility.prettyPrint(result);
    }
    return result;
  }
  
  public void run()
  {
    try
    {
      startReadingFromTopic();
      while (this.dontStop)
      {
        checkTasks();
        Thread.sleep(5000L);
      }
    }
    catch (InterruptedException ie)
    {
      ie = 
      
        ie;logger.error(ie.getMessage(), ie);FlowComponent flowComponent = this.rcvr.getOwner();flowComponent.notifyAppMgr(EntityType.STREAM, this.clientName, this.streamUuid, ie, null, new Object[0]);
    }
    catch (Exception e)
    {
      e = 
      
        e;logger.error(e.getMessage(), e);FlowComponent flowComponent = this.rcvr.getOwner();flowComponent.notifyAppMgr(EntityType.STREAM, this.clientName, this.streamUuid, e, null, new Object[0]);
    }
    finally {}
  }
  
  private synchronized void checkTasks()
    throws Exception
  {
    checkDataConsumptionTask();
    
    checkCheckpointTask();
    
    checkStatsTask();
  }
  
  private void checkDataConsumptionTask()
    throws Exception
  {
    for (Pair<KafkaPullerTask, Future> taskFuturePair : this.kafkaPullerTasks) {
      if ((taskFuturePair.second != null) && ((((Future)taskFuturePair.second).isDone()) || (((Future)taskFuturePair.second).isCancelled()))) {
        if (!((KafkaPullerTask)taskFuturePair.first).isUserTriggered)
        {
          for (int ii = 0; ii < KafkaConstants.maxAttemptsToReInitKafka; ii++) {
            try
            {
              if (logger.isInfoEnabled()) {
                logger.info("Error in data consumption task for Kafka Puller: " + this.kafkaReceiver.info.getRcvr_name() + ", will try to reinitialize");
              }
              KafkaStreamUtils.retryBackOffMillis(2000L);
              reInitKafka();
              ii = KafkaConstants.maxAttemptsToReInitKafka + 1;
            }
            catch (Exception e)
            {
              if (ii < KafkaConstants.maxAttemptsToReInitKafka)
              {
                doCleanUp();
                KafkaStreamUtils.retryBackOffMillis(2000L);
              }
              else
              {
                throw e;
              }
            }
          }
          startReadingFromTopic();
        }
      }
    }
  }
  
  private void checkCheckpointTask()
    throws Exception
  {
    if ((this.kafkaCheckpointTasks != null) && ((((Future)this.kafkaCheckpointTasks.second).isDone()) || (((Future)this.kafkaCheckpointTasks.second).isCancelled()))) {
      if (!((StreamCheckpointWriterTask)this.kafkaCheckpointTasks.first).isUserTriggered)
      {
        for (int ii = 0; ii < KafkaConstants.maxAttemptsToReInitKafka; ii++) {
          try
          {
            if (logger.isInfoEnabled()) {
              logger.info("Error in checkpoint task for Kafka Puller: " + this.kafkaReceiver.info.getRcvr_name() + ", will try to reinitialize");
            }
            reInitKafka();
            ii = KafkaConstants.maxAttemptsToReInitKafka + 1;
          }
          catch (Exception e)
          {
            if (ii < KafkaConstants.maxAttemptsToReInitKafka)
            {
              doCleanUp();
              KafkaStreamUtils.retryBackOffMillis(2000L);
            }
            else
            {
              throw e;
            }
          }
        }
        startReadingFromTopic();
      }
    }
  }
  
  private void checkStatsTask()
  {
    if ((this.kafkaStatsTasks != null) && ((((Future)this.kafkaStatsTasks.second).isDone()) || (((Future)this.kafkaStatsTasks.second).isCancelled()))) {
      if (!((KafkaPullerStatsTask)this.kafkaStatsTasks.first).isUserTriggered)
      {
        KafkaPullerStatsTask kafkaPullerStats = new KafkaPullerStatsTask(this.kafkaPullerTasks, this.kafkaReceiver);
        this.statsExecutorService = new ScheduledThreadPoolExecutor(1);
        Future statsFuture = this.statsExecutorService.scheduleAtFixedRate(kafkaPullerStats, 10000L, 10000L, TimeUnit.MILLISECONDS);
        this.kafkaStatsTasks = new Pair(kafkaPullerStats, statsFuture);
      }
    }
  }
  
  void doCleanUp()
    throws InterruptedException
  {
    cleanupProducer();
    cleanupConsumers();
    cleanupLocalTasks();
  }
  
  void cleanupLocalTasks()
    throws InterruptedException
  {
    if (this.kafkaPullerTasks != null) {
      for (Pair<KafkaPullerTask, Future> taskFuturePair : this.kafkaPullerTasks)
      {
        ((KafkaPullerTask)taskFuturePair.first).stop();
        
        ((Future)taskFuturePair.second).cancel(false);
      }
    }
    shutdownExceutorService(this.dataExecutorService, 2L);
    shutdownExceutorService(this.checkpointExecutorService, 2L);
    shutdownExceutorService(this.statsExecutorService, 2L);
    this.partitions.clear();
    this.consumerPartitions.clear();
    this.checkpointConsumers.clear();
  }
  
  private void shutdownExceutorService(ExecutorService execService, long timeout)
    throws InterruptedException
  {
    if (execService != null)
    {
      execService.shutdown();
      while (!execService.awaitTermination(timeout, TimeUnit.SECONDS)) {}
    }
  }
  
  void cleanupConsumers()
  {
    if (this.consumerPartitions != null)
    {
      SimpleConsumer consumer;
      for (Iterator i$ = this.consumerPartitions.keySet().iterator(); i$.hasNext(); consumer.close()) {
        consumer = (SimpleConsumer)i$.next();
      }
    }
    if (this.checkpointConsumers != null)
    {
      SimpleConsumer consumer;
      for (Iterator i$ = this.checkpointConsumers.iterator(); i$.hasNext(); consumer.close()) {
        consumer = (SimpleConsumer)i$.next();
      }
    }
  }
  
  void cleanupProducer()
  {
    if (this.kafkaProducer != null) {
      this.kafkaProducer.close();
    }
  }
}
