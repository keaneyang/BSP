package com.bloom.kafkamessaging;

import com.bloom.classloading.WALoader;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.runtime.KafkaStreamUtils;
import com.bloom.runtime.KeyFactory;
import com.bloom.runtime.components.Stream;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.PropertySet;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.monitor.MonitorEvent;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.security.WASecurityManager;
import com.bloom.ser.KryoSingleton;
import com.bloom.utility.Utility;
import com.bloom.uuid.UUID;
import com.esotericsoftware.kryo.KryoException;
import com.bloom.intf.Formatter;
import com.bloom.recovery.Path;
import com.bloom.recovery.Position;
import com.bloom.recovery.SourcePosition;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import kafka.cluster.Broker;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;

public class KafkaSender
{
  private static final Logger logger = Logger.getLogger(KafkaSender.class);
  private final MDRepository mdRepository = MetadataRepository.getINSTANCE();
  private final String kafkaTopicName;
  private final KeyFactory key_factory;
  final MetaInfo.Stream streamInfo;
  protected final boolean isEncrypted;
  final PositionedBuffer[] dataBuffers;
  private final short maxAttemptsToStartKafka = 5;
  private final long maxSleepMillis = 500L;
  private final boolean isRecoveryEnabled;
  private final Stream streamRuntime;
  private ScheduledThreadPoolExecutor flushProcess;
  private volatile boolean isStarted = false;
  private final int num_partitions;
  private final Properties producer_properties;
  KafkaFlusher[] flushers;
  KafkaProducer<byte[], byte[]>[] kafka_producers;
  private String format;
  private Formatter formatter;
  boolean isAvro;
  
  public KafkaSender(boolean isEncrypted, Stream streamRuntime, KeyFactory keyFactory, boolean recoveryEnabled)
    throws Exception
  {
    this.isEncrypted = isEncrypted;
    this.streamInfo = streamRuntime.getMetaInfo();
    this.streamRuntime = streamRuntime;
    this.key_factory = keyFactory;
    this.isRecoveryEnabled = recoveryEnabled;
    
    this.producer_properties = new Properties();
    List<String> bootstrapBrokers = KafkaStreamUtils.getBrokerAddress(this.streamInfo);
    this.producer_properties.put("bootstrap.servers", bootstrapBrokers);
    this.producer_properties.put("acks", KafkaConstants.producer_acks);
    this.producer_properties.put("key.serializer", "com.bloom.kafkamessaging.KafkaEncoder");
    this.producer_properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    this.producer_properties.put("buffer.memory", Integer.valueOf(KafkaConstants.producer_buffer_memory));
    this.producer_properties.put("batch.size", Integer.valueOf(KafkaConstants.producer_batchsize));
    this.producer_properties.put("max.request.size", Integer.valueOf(KafkaConstants.producer_max_request_size));
    this.producer_properties.put("retries", Integer.valueOf(0));
    this.producer_properties.put("linger.ms", Integer.valueOf(0));
    
    this.kafkaTopicName = KafkaStreamUtils.createTopicName(this.streamInfo);
    
    MetaInfo.PropertySet streamPropset = KafkaStreamUtils.getPropertySet(this.streamInfo);
    
    this.num_partitions = KafkaStreamUtils.getPartitionsCount(this.streamInfo, streamPropset);
    this.dataBuffers = new PositionedBuffer[this.num_partitions];
    for (int ii = 0; ii < this.num_partitions; ii++) {
      this.dataBuffers[ii] = new PositionedBuffer(ii, KafkaConstants.send_buffer_size);
    }
    if (streamPropset.properties != null)
    {
      this.format = ((String)streamPropset.properties.get("dataformat"));
      if ((this.format != null) && (this.format.equalsIgnoreCase("avro")))
      {
        this.formatter = getFormatterInstance(this.format);
        if (this.formatter == null) {
          throw new Exception("Allowed dataformat is only avro or leave it blank to default to Striim format. Format passed in : " + this.format);
        }
        this.isAvro = true;
      }
    }
    setupKafkaFlushers();
  }
  
  private Formatter getFormatterInstance(String format)
    throws Exception
  {
    if (format.equalsIgnoreCase("avro"))
    {
      Map<String, Object> avroFormatter_Properties = new HashMap();
      String schemFileName = this.streamInfo.getFullName().concat("_schema.avsc");
      avroFormatter_Properties.put("schemaFileName", schemFileName);
      UUID type_uuid = this.streamInfo.getDataType();
      if (type_uuid != null)
      {
        MetaInfo.Type type = (MetaInfo.Type)this.mdRepository.getMetaObjectByUUID(type_uuid, WASecurityManager.TOKEN);
        avroFormatter_Properties.put("TypeName", type.getName());
        Class<?> typeClass = WALoader.get().loadClass(type.className);
        avroFormatter_Properties.put("EventType", "ContainerEvent");
        Field[] fields = typeClass.getDeclaredFields();
        Field[] typedEventFields = new Field[fields.length - 1];
        int i = 0;
        for (Field field : fields) {
          if ((Modifier.isPublic(field.getModifiers())) && 
            (!"mapper".equals(field.getName())))
          {
            typedEventFields[i] = field;
            i++;
          }
        }
        fields = typedEventFields;
        
        String formatterClassName = "com.bloom.proc.AvroFormatter";
        Class<?> formatterClass = Class.forName(formatterClassName);
        Formatter avroFormatter = (Formatter)formatterClass.getConstructor(new Class[] { Map.class, Field[].class }).newInstance(new Object[] { avroFormatter_Properties, fields });
        return avroFormatter;
      }
    }
    return null;
  }
  
  private void setupKafkaFlushers()
    throws Exception
  {
    assert (!this.isStarted);
    
    this.flushers = new KafkaFlusher[KafkaConstants.flush_threads];
    this.kafka_producers = new KafkaProducer[KafkaConstants.flush_threads];
    for (int ij = 0; ij < this.num_partitions; ij++)
    {
      int partition = ij % KafkaConstants.flush_threads;
      if (this.flushers[partition] == null) {
        this.flushers[partition] = new KafkaFlusher(this.streamRuntime);
      }
      if (this.kafka_producers[partition] == null) {
        this.kafka_producers[partition] = new KafkaProducer(this.producer_properties);
      }
      this.flushers[partition].add(this.dataBuffers[ij]);
      this.dataBuffers[ij].setKafkaProducer(this.kafka_producers[partition]);
    }
    this.flushProcess = new ScheduledThreadPoolExecutor(1);
    for (KafkaFlusher flusher : this.flushers) {
      this.flushProcess.scheduleAtFixedRate(flusher, KafkaConstants.flush_period, KafkaConstants.flush_period, TimeUnit.MILLISECONDS);
    }
    Object response_to_set_wait_positions = null;
    Exception ee = null;
    String ss = null;
    if (this.isRecoveryEnabled)
    {
      int currentAttempts = 0;
      do
      {
        try
        {
          setPartitionWaitPositions();
        }
        catch (Exception e)
        {
          if (((e instanceof MetaDataRepositoryException)) || ((e instanceof InterruptedException)) || ((e instanceof KryoException))) {
            throw e;
          }
          logger.error(e.getMessage(), e);
          ee = e;
          logger.error("Will retry getting kakfa topic metadata for upto 2.5 seconds");
        }
      } while (currentAttempts++ < 5);
    }
    if (response_to_set_wait_positions != null)
    {
      if (ee != null) {
        throw new Exception(ee.getMessage(), ee);
      }
      if (ss != null) {
        throw new Exception(ss);
      }
    }
    this.isStarted = true;
    if (logger.isInfoEnabled()) {
      logger.info("Created KafkaSender to write to topic: " + this.kafkaTopicName + " for stream : " + this.streamInfo.getFullName());
    }
  }
  
  public synchronized void stop()
  {
    assert (this.isStarted);
    this.isStarted = false;
    for (PositionedBuffer positionedBuffer : this.dataBuffers) {
      try
      {
        positionedBuffer.flushToKafka();
      }
      catch (Exception e)
      {
        logger.warn("Error while trying to stopping Kafka sender, it can be ignored");
        logger.error(e.getMessage(), e);
      }
    }
    if (this.flushProcess != null) {
      try
      {
        this.flushProcess.shutdown();
        if (!this.flushProcess.awaitTermination(KafkaConstants.flush_period, TimeUnit.MILLISECONDS)) {
          logger.warn("Unable to terminate flush process in 5000 milliseconds for " + this.kafkaTopicName);
        }
      }
      catch (InterruptedException e)
      {
        logger.error("Interrupted while trying to terminate flush process for " + this.kafkaTopicName, e);
      }
    }
    for (KafkaProducer kafkaProducer : this.kafka_producers) {
      kafkaProducer.close();
    }
    if (logger.isInfoEnabled()) {
      logger.info("Kafka sender for topic: " + this.kafkaTopicName + " stopped");
    }
  }
  
  public synchronized boolean send(ITaskEvent data)
    throws Exception
  {
    if (!this.isStarted) {
      return false;
    }
    for (WAEvent event : data.batch())
    {
      int partitionId = getPartitionId(event);
      this.dataBuffers[partitionId].put(event);
    }
    return true;
  }
  
  public Position getComponentCheckpoint()
  {
    Position result = new Position();
    for (PositionedBuffer b : this.dataBuffers) {
      result.mergeHigherPositions(b.eventsInMemory);
    }
    return result;
  }
  
  private void setPartitionWaitPositions()
    throws Exception
  {
    String checkpointTopicName = KafkaStreamUtils.getCheckpointTopicName(this.kafkaTopicName);
    
    List<String> kafkaBrokers = KafkaConstants.broker_address_list;
    if ((kafkaBrokers == null) || (kafkaBrokers.isEmpty())) {
      kafkaBrokers = KafkaStreamUtils.getBrokerAddress(this.streamInfo);
    }
    String[] hostPort = ((String)kafkaBrokers.get(0)).split(":");
    assert (hostPort.length == 2);
    String clientName = "Client_" + this.kafkaTopicName + "_" + KafkaPuller.getServerId() + "_CHECKPOINT";
    
    MetaInfo.PropertySet streamPropset = KafkaStreamUtils.getPropertySet(this.streamInfo);
    
    int numPartitions = KafkaStreamUtils.getPartitionsCount(this.streamInfo, streamPropset);
    List<Integer> kafkaTopicPartitions;
    for (kafkaTopicPartitions = new ArrayList(); kafkaTopicPartitions.size() < numPartitions; kafkaTopicPartitions.add(Integer.valueOf(kafkaTopicPartitions.size()))) {}
    Map<Broker, List<Integer>> kafkaTopicLeaders = KafkaStreamUtils.getBrokerPartitions(kafkaBrokers, this.kafkaTopicName, kafkaTopicPartitions, 20);
    
    Map<Integer, SimpleConsumer> kafkaTopicConsumers = new HashMap();
    SimpleConsumer consumer;
    for (Map.Entry<Broker, List<Integer>> entry : kafkaTopicLeaders.entrySet())
    {
      Broker broker = (Broker)entry.getKey();
      List<Integer> partitionIds = (List)entry.getValue();
      consumer = new SimpleConsumer(broker.host(), broker.port(), 100000, 65536, clientName);
      for (Integer partitionId : partitionIds) {
        kafkaTopicConsumers.put(partitionId, consumer);
      }
    }
    List<Integer> checkpointTopicPartitions;
    for (checkpointTopicPartitions = new ArrayList(); checkpointTopicPartitions.size() < numPartitions; checkpointTopicPartitions.add(Integer.valueOf(checkpointTopicPartitions.size()))) {}
    Map<Broker, List<Integer>> checkpointTopicLeaders = KafkaStreamUtils.getBrokerPartitions(kafkaBrokers, checkpointTopicName, checkpointTopicPartitions, 20);
    
    Map<Integer, SimpleConsumer> checkpointTopicConsumers = new HashMap();
    for (Map.Entry<Broker, List<Integer>> entry : checkpointTopicLeaders.entrySet())
    {
      Broker broker = (Broker)entry.getKey();
      List<Integer> partitionIds = (List)entry.getValue();
      consumer = new SimpleConsumer(broker.host(), broker.port(), 100000, 65536, clientName);
      for (Integer partitionId : partitionIds) {
        checkpointTopicConsumers.put(partitionId, consumer);
      }
    }
    for (PositionedBuffer positionedBuffer : this.dataBuffers)
    {
      SimpleConsumer kafkaConsumer = (SimpleConsumer)kafkaTopicConsumers.get(Integer.valueOf(positionedBuffer.partitionId));
      if (kafkaConsumer == null)
      {
        logger.warn("No broker/consumer found for Kafka partition " + this.kafkaTopicName + ":" + positionedBuffer.partitionId);
      }
      else
      {
        SimpleConsumer checkpointConsumer = (SimpleConsumer)checkpointTopicConsumers.get(Integer.valueOf(positionedBuffer.partitionId));
        if (checkpointConsumer == null)
        {
          logger.warn("No broker/consumer found for Kafka checkpoint partition " + checkpointTopicName + ":" + positionedBuffer.partitionId);
        }
        else
        {
          Object rawObject = KafkaStreamUtils.getLastObjectFromTopic(checkpointTopicName, positionedBuffer.partitionId, checkpointConsumer, 2);
          OffsetPosition partitionCheckpoint = null;
          if (rawObject != null) {
            if ((rawObject instanceof OffsetPosition)) {
              partitionCheckpoint = (OffsetPosition)rawObject;
            }
          }
          if (partitionCheckpoint == null)
          {
            OffsetResponse offsetResponse_f = KafkaStreamUtils.getFirstOffset(this.kafkaTopicName, positionedBuffer.partitionId, kafkaConsumer);
            long startingOffset = offsetResponse_f.offsets(this.kafkaTopicName, positionedBuffer.partitionId)[0];
            partitionCheckpoint = new OffsetPosition(startingOffset, System.currentTimeMillis());
          }
          OffsetResponse offsetResponse_n = KafkaStreamUtils.getNextOffset(this.kafkaTopicName, positionedBuffer.partitionId, kafkaConsumer);
          long kafkaOffset = offsetResponse_n.offsets(this.kafkaTopicName, positionedBuffer.partitionId)[0];
          if (partitionCheckpoint.getOffset() < kafkaOffset)
          {
            KafkaStreamUtils.updateCheckpoint(partitionCheckpoint, this.kafkaTopicName, positionedBuffer.partitionId, kafkaOffset, kafkaConsumer);
            KafkaStreamUtils.writeCheckpoint(this.kafkaTopicName, positionedBuffer.partitionId, partitionCheckpoint, positionedBuffer.kafkaProducer, this.streamInfo, checkpointConsumer, 3);
          }
          if (!partitionCheckpoint.isEmpty())
          {
            positionedBuffer.waitPosition = partitionCheckpoint;
            if (Logger.getLogger("KafkaStreams").isDebugEnabled())
            {
              Logger.getLogger("KafkaStreams").debug("Kafka partition checkpoint for " + this.kafkaTopicName + ":" + positionedBuffer.partitionId + "...");
              Utility.prettyPrint(positionedBuffer.waitPosition);
            }
          }
        }
      }
    }
  }
  
  private int getPartitionId(WAEvent event)
  {
    if (this.key_factory != null)
    {
      RecordKey record_key = this.key_factory.makeKey(event.data);
      int result = Math.abs(record_key.hashCode() % this.dataBuffers.length);
      return result;
    }
    return 0;
  }
  
  public void close()
  {
    this.formatter = null;
  }
  
  class PositionedBuffer
  {
    final int partitionId;
    ByteBuffer byteBuffer;
    Position eventsInMemory = new Position();
    Position waitPosition = null;
    private long lastSuccessfulWriteOffset = 0L;
    public volatile boolean isFull = false;
    private KafkaProducer<byte[], byte[]> kafkaProducer;
    int pendingCount = 0;
    long waitStats = 0L;
    KafkaPartitionSendStats stats = new KafkaPartitionSendStats();
    
    public PositionedBuffer(int partition_id, int sendBufferSize)
      throws MetaDataRepositoryException
    {
      this.partitionId = partition_id;
      this.byteBuffer = ByteBuffer.allocate(sendBufferSize);
    }
    
    private synchronized void put(WAEvent data)
      throws Exception
    {
      if (data.position != null)
      {
        data.position = data.position.createAugmentedPosition(KafkaSender.this.streamInfo.uuid, String.valueOf(this.partitionId));
        if (isBeforeWaitPosition(data.position))
        {
          if (Logger.getLogger("Recovery").isDebugEnabled()) {
            Logger.getLogger("Recovery").debug(KafkaSender.this.streamInfo.name + " dropping input which precedes the checkpoint: " + data.position);
          }
          return;
        }
      }
      byte[] bytes = convertToBytes(data);
      if (this.byteBuffer.position() + 4 + bytes.length > this.byteBuffer.limit())
      {
        this.isFull = true;
        while (this.isFull) {
          wait(KafkaConstants.flush_period);
        }
        this.waitStats += 1L;
      }
      this.byteBuffer.putInt(bytes.length);
      this.byteBuffer.put(bytes);
      this.pendingCount += 1;
      if (KafkaSender.this.isRecoveryEnabled) {
        this.eventsInMemory.mergeHigherPositions(data.position);
      }
      if (data.position != null) {
        this.eventsInMemory.mergeHigherPositions(data.position);
      }
      if (KafkaSender.logger.isDebugEnabled()) {
        KafkaSender.logger.debug(KafkaSender.this.kafkaTopicName + ":" + this.partitionId + " put " + data.position);
      }
    }
    
    private byte[] convertToBytes(WAEvent data)
      throws Exception
    {
      if (KafkaSender.this.isAvro) {
        return KafkaSender.this.formatter.format(data);
      }
      return KryoSingleton.write(data, KafkaSender.this.isEncrypted);
    }
    
    public synchronized void flushToKafka()
      throws Exception
    {
      if (this.byteBuffer.position() == 0) {
        return;
      }
      if (Logger.getLogger("KafkaStreams").isDebugEnabled())
      {
        Logger.getLogger("KafkaStreams").debug("Flushing to kafka buffer partition " + KafkaSender.this.kafkaTopicName + ":" + this.partitionId);
        Utility.prettyPrint(this.eventsInMemory);
      }
      byte[] kafkaBytes = new byte[this.byteBuffer.position()];
      System.arraycopy(this.byteBuffer.array(), 0, kafkaBytes, 0, this.byteBuffer.position());
      
      long tmp_pos = this.byteBuffer.position();
      long tmp_count = this.pendingCount;
      long currenTime = System.currentTimeMillis();
      long writeOffset = KafkaStreamUtils.write(KafkaSender.this.kafkaTopicName, this.partitionId, kafkaBytes, this.kafkaProducer, KafkaSender.this.streamInfo, this.lastSuccessfulWriteOffset, 3);
      long endTime = System.currentTimeMillis();
      this.stats.record(endTime - currenTime, tmp_pos, tmp_count, this.waitStats);
      
      this.lastSuccessfulWriteOffset = writeOffset;
      
      this.byteBuffer.clear();
      this.pendingCount = 0;
      this.eventsInMemory = new Position();
      
      this.isFull = false;
      notifyAll();
    }
    
    private synchronized boolean isBeforeWaitPosition(Position position)
    {
      if ((this.waitPosition == null) || (position == null)) {
        return false;
      }
      for (Path wactionPath : position.values()) {
        if (this.waitPosition.containsKey(wactionPath.getPathHash()))
        {
          SourcePosition sp = this.waitPosition.get(Integer.valueOf(wactionPath.getPathHash())).getSourcePosition();
          if (wactionPath.getSourcePosition().compareTo(sp) <= 0) {
            return true;
          }
          this.waitPosition = this.waitPosition.createPositionWithoutPath(wactionPath);
        }
      }
      if (this.waitPosition.isEmpty()) {
        this.waitPosition = null;
      }
      return false;
    }
    
    public void setKafkaProducer(KafkaProducer<byte[], byte[]> kafkaProducer)
    {
      this.kafkaProducer = kafkaProducer;
    }
  }
  
  public Map getStats()
  {
    long total_event_count = 0L;
    long total_bytes_count = 0L;
    long total_time = 0L;
    long max_latency = 0L;
    long total_iterations = 0L;
    long total_wait_count = 0L;
    Map sMap = new HashMap();
    for (PositionedBuffer positionedBuffer : this.dataBuffers)
    {
      total_event_count += positionedBuffer.stats.sentCount;
      total_bytes_count += positionedBuffer.stats.bytes;
      total_time += positionedBuffer.stats.time;
      max_latency = Math.max(max_latency, positionedBuffer.stats.maxLatency);
      total_iterations += positionedBuffer.stats.iterations;
      total_wait_count += positionedBuffer.stats.waitCount;
    }
    if (total_time != 0L)
    {
      long total_time_seconds = total_time / 1000L;
      if (total_time_seconds != 0L)
      {
        long events_per_sec = total_event_count / total_time_seconds;
        long megs_per_sec = total_bytes_count / 1000000L / total_time_seconds;
        sMap.put("events_per_sec", Long.valueOf(events_per_sec));
        sMap.put("megs_per_sec", Long.valueOf(megs_per_sec));
      }
      else
      {
        sMap.put("events_per_sec", Integer.valueOf(0));
        sMap.put("megs_per_sec", Integer.valueOf(0));
      }
    }
    else
    {
      sMap.put("events_per_sec", Integer.valueOf(0));
      sMap.put("megs_per_sec", Integer.valueOf(0));
    }
    if (total_iterations != 0L)
    {
      long avg_latency = total_time / total_iterations;
      sMap.put("avg_latency", Long.valueOf(avg_latency));
    }
    else
    {
      sMap.put("avg_latency", Integer.valueOf(0));
    }
    sMap.put("total_event_count", Long.valueOf(total_event_count));
    sMap.put("total_bytes_count", Long.valueOf(total_bytes_count));
    sMap.put("max_latency", Long.valueOf(max_latency));
    sMap.put("total_wait_count", Long.valueOf(total_wait_count));
    
    return sMap;
  }
  
  public void addSpecificMonitorEvents(MonitorEventsCollection monEvs)
  {
    MetaInfo.PropertySet propertySet = KafkaStreamUtils.getPropertySet(this.streamInfo);
    if (propertySet != null)
    {
      Map<String, Object> properties = propertySet.getProperties();
      if ((properties != null) && (properties.containsKey("jmx.broker"))) {
        monEvs.add(MonitorEvent.Type.KAFKA_BROKERS, properties.get("jmx.broker").toString());
      }
    }
  }
}
