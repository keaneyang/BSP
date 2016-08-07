package com.bloom.runtime;

import com.bloom.kafkamessaging.KafkaConstants;
import com.bloom.kafkamessaging.KafkaException;
import com.bloom.kafkamessaging.OffsetPosition;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.metaRepository.RemoteCall;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.containers.WAEvent;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.PropertySet;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.security.WASecurityManager;
import com.bloom.ser.KryoSingleton;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Future;
import kafka.admin.AdminUtils;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.common.TopicExistsException;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.utils.ZKStringSerializer;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

public class KafkaStreamUtils
  implements Serializable
{
  private static final long serialVersionUID = 6880318751354053803L;
  private static Logger logger = Logger.getLogger(KafkaStreamUtils.class);
  public static final MDRepository md_repository = MetadataRepository.getINSTANCE();
  
  public static class CreateTopic
    implements RemoteCall
  {
    private static final long serialVersionUID = 1164757976316951507L;
    public MetaInfo.Stream streamObject;
    
    public CreateTopic(MetaInfo.Stream streamObject)
    {
      this.streamObject = streamObject;
    }
    
    public Object call()
      throws Exception
    {
      return Boolean.valueOf(KafkaStreamUtils.createTopic(this.streamObject));
    }
  }
  
  public static class DeleteTopic
    implements RemoteCall
  {
    private static final long serialVersionUID = -5529822347005672810L;
    public MetaInfo.Stream streamObject;
    
    public DeleteTopic(MetaInfo.Stream streamObject)
    {
      this.streamObject = streamObject;
    }
    
    public Object call()
      throws Exception
    {
      return Boolean.valueOf(KafkaStreamUtils.deleteTopic(this.streamObject));
    }
  }
  
  public static MetaInfo.PropertySet getPropertySet(MetaInfo.Stream streamInfo)
  {
    String name;
    String namespace;
    String name;
    if (streamInfo.pset.indexOf(".") == -1)
    {
      String namespace = streamInfo.getNsName();
      name = streamInfo.pset;
    }
    else
    {
      namespace = streamInfo.pset.split("\\.")[0];
      name = streamInfo.pset.split("\\.")[1];
    }
    try
    {
      MetaInfo.PropertySet kakfka_props = (MetaInfo.PropertySet)md_repository.getMetaObjectByName(EntityType.PROPERTYSET, namespace, name, null, WASecurityManager.TOKEN);
      streamInfo.propertySet = kakfka_props;
      return kakfka_props;
    }
    catch (MetaDataRepositoryException e)
    {
      Logger.getLogger("KafkaStreams").error("Unable to find Property Set for stream " + streamInfo.name + ", not expected to happen");
    }
    return null;
  }
  
  public static String getZkAddress(MetaInfo.Stream streamObject)
    throws MetaDataRepositoryException
  {
    MetaInfo.PropertySet kafka_props = getPropertySet(streamObject);
    if (kafka_props == null) {
      throw new MetaDataRepositoryException("No Kafka property set found for : " + streamObject.pset + ", can't proceed");
    }
    String zk_address_list = (String)kafka_props.getProperties().get("zk.address");
    return zk_address_list;
  }
  
  public static String getZkAddress(MetaInfo.PropertySet propertySet)
  {
    String zk_address_list = (String)propertySet.getProperties().get("zk.address");
    return zk_address_list;
  }
  
  public static int getNumReplicas(MetaInfo.PropertySet propertySet)
  {
    String zk_num_replicas = (String)propertySet.getProperties().get("replication.factor");
    if (zk_num_replicas == null) {
      return Integer.parseInt("1");
    }
    return Integer.parseInt(zk_num_replicas);
  }
  
  public static List<String> getBrokerAddress(MetaInfo.Stream streamObject)
  {
    MetaInfo.PropertySet kafka_props = getPropertySet(streamObject);
    
    String[] broker_list = ((String)kafka_props.getProperties().get("bootstrap.brokers")).split(",");
    List<String> broker_address_list = Arrays.asList(broker_list);
    return broker_address_list;
  }
  
  public static boolean createTopic(MetaInfo.Stream streamObject)
    throws Exception
  {
    boolean isCheckpointTopicCreated = false;
    MetaInfo.PropertySet kafka_propset = getPropertySet(streamObject);
    if (kafka_propset == null) {
      throw new Exception("PropertySet " + streamObject.pset + " does not exist. Please create the Property Set and try again.");
    }
    String zk_address_list = getZkAddress(kafka_propset);
    ZkClient zkClient = null;
    boolean isDataTopicCreated;
    String topicName;
    try
    {
      zkClient = new ZkClient(zk_address_list, KafkaConstants.session_timeout, KafkaConstants.connection_timeout, ZKStringSerializer..MODULE$);
      int numPartitions = getPartitionsCount(streamObject, kafka_propset);
      if (logger.isInfoEnabled()) {
        logger.info("Creating kafka topic with " + numPartitions + " partitions");
      }
      topicName = createTopicName(streamObject);
      int numReplicas = getNumReplicas(kafka_propset);
      isDataTopicCreated = createTopic(zkClient, topicName, numPartitions, numReplicas);
      if (isDataTopicCreated)
      {
        String checkpointName = getCheckpointTopicName(topicName);
        isCheckpointTopicCreated = createTopic(zkClient, checkpointName, numPartitions, numReplicas);
        if (Logger.getLogger("KafkaStreams").isDebugEnabled()) {
          Logger.getLogger("KafkaStreams").debug("Created kafka topic pair " + topicName + " & " + checkpointName);
        }
      }
    }
    catch (TopicExistsException e)
    {
      
      return 0;
    }
    catch (Exception e)
    {
      throw e;
    }
    finally
    {
      if (zkClient != null) {
        zkClient.close();
      }
    }
    return (isDataTopicCreated) && (isCheckpointTopicCreated);
  }
  
  public static int getPartitionsCount(MetaInfo.Stream streamInfo, MetaInfo.PropertySet streamPropset)
  {
    if ((streamInfo.partitioningFields == null) || (streamInfo.partitioningFields.isEmpty())) {
      return 1;
    }
    try
    {
      if (streamPropset.getProperties().containsKey("partitions"))
      {
        int userRequest = Integer.parseInt((String)streamPropset.getProperties().get("partitions"));
        if (userRequest > 0) {
          return userRequest;
        }
      }
      else if (System.getProperties().contains("com.bloom.config.kafka.topic.partitions"))
      {
        int userRequest = Integer.parseInt(System.getProperty("com.bloom.config.kafka.topic.partitions"));
        if (userRequest > 0) {
          return userRequest;
        }
      }
    }
    catch (NumberFormatException e) {}
    return KafkaConstants.default_number_of_partition;
  }
  
  public static boolean createTopic(ZkClient zkClient, String topicName, int numPartitions, int numReplicas)
    throws Exception
  {
    boolean isCreated = false;
    try
    {
      while (!isCreated)
      {
        AdminUtils.createTopic(zkClient, topicName, numPartitions, numReplicas, new Properties());
        short maxRetries = 100;
        while (!isCreated)
        {
          maxRetries = (short)(maxRetries - 1);
          if (maxRetries <= 0) {
            break;
          }
          Thread.sleep(500L);
          isCreated = AdminUtils.topicExists(zkClient, topicName);
        }
      }
    }
    catch (Exception e)
    {
      throw e;
    }
    return isCreated;
  }
  
  public static String createTopicName(MetaInfo.Stream streamObject)
  {
    StringBuilder topic_name_builder = new StringBuilder();
    topic_name_builder.append(streamObject.getNsName()).append(":").append(streamObject.getName());
    String topic_name = topic_name_builder.toString();
    return makeSafeKafkaTopicName(topic_name);
  }
  
  public static String makeSafeKafkaTopicName(String unsafeTopicName)
  {
    String result = unsafeTopicName.replaceAll("[^a-zA-Z0-9_\\-]", "_");
    return result;
  }
  
  public static boolean deleteTopic(MetaInfo.Stream streamInfo)
    throws MetaDataRepositoryException
  {
    MetaInfo.PropertySet propertySet = streamInfo.propertySet;
    if (propertySet == null)
    {
      if (logger.isInfoEnabled()) {
        logger.info("Kafka Stream requested delete on topic with no propertySet: " + streamInfo);
      }
      return false;
    }
    String zk_address = getZkAddress(propertySet);
    ZkClient zkClient = null;
    try
    {
      zkClient = new ZkClient(zk_address, KafkaConstants.session_timeout, KafkaConstants.connection_timeout, ZKStringSerializer..MODULE$);
      String topic_name = createTopicName(streamInfo);
      AdminUtils.deleteTopic(zkClient, topic_name);
      String checkpoint_name = getCheckpointTopicName(topic_name);
      AdminUtils.deleteTopic(zkClient, checkpoint_name);
      return true;
    }
    catch (Exception e)
    {
      throw e;
    }
    finally
    {
      if (zkClient != null) {
        zkClient.close();
      }
    }
  }
  
  public static CreateTopic getCreateTopicExecutor(MetaInfo.Stream streamObject)
  {
    return new CreateTopic(streamObject);
  }
  
  public static DeleteTopic getDeleteTopicExecutor(MetaInfo.Stream metaObject)
  {
    return new DeleteTopic(metaObject);
  }
  
  public static String getCheckpointTopicName(String kafkaTopicName)
  {
    return kafkaTopicName + "_CHECKPOINT";
  }
  
  private static boolean isIgnorableError(short error_code)
  {
    return error_code == ErrorMapping.ReplicaNotAvailableCode();
  }
  
  public static Map<Broker, List<Integer>> getBrokerPartitions(List<String> kafkaBrokers, String kafkaTopicName, List<Integer> kafkaPartitions, int maxRetries)
    throws InterruptedException, KafkaException
  {
    assert (maxRetries >= 1);
    int retries = 1;
    Map<Broker, List<Integer>> result = new HashMap();
    do
    {
      boolean gotAllMetadata = true;
      TopicMetadata topicMetadata = getBrokerPartitions(kafkaBrokers, kafkaTopicName, kafkaPartitions);
      if ((topicMetadata != null) && (topicMetadata.errorCode() == ErrorMapping.NoError()))
      {
        for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
          if ((partitionMetadata.errorCode() == ErrorMapping.NoError()) || (isIgnorableError(partitionMetadata.errorCode())))
          {
            if (kafkaPartitions.contains(Integer.valueOf(partitionMetadata.partitionId())))
            {
              List<Integer> all_partitions = (List)result.get(partitionMetadata.leader());
              if (all_partitions == null) {
                all_partitions = new ArrayList();
              }
              all_partitions.add(Integer.valueOf(partitionMetadata.partitionId()));
              result.put(partitionMetadata.leader(), all_partitions);
            }
          }
          else
          {
            result.clear();
            logger.error("Error fetching metadata for Partition: " + partitionMetadata.partitionId() + " on Topic: " + kafkaTopicName + " with error code: " + partitionMetadata.errorCode() + ". Don't panic we'll retry a few more times to get topic metadata");
            
            gotAllMetadata = false;
            break;
          }
        }
        if (gotAllMetadata) {
          retries = maxRetries + 1;
        }
        if (logger.isInfoEnabled()) {
          logger.info("Took " + retries * 500 / 1000.0D + " seconds to get topic/partition metadata from kafka brokers");
        }
      }
      else if (topicMetadata == null)
      {
        logger.error("Error fetching metadata for Topic: " + kafkaTopicName + " from all the brokers in [" + kafkaBrokers + "]. Will retry a few more times");
      }
      else
      {
        logger.error("Error fetching metadata for Topic: " + kafkaTopicName + " from all the brokers in [" + kafkaBrokers + "], Reason -> error code: " + topicMetadata.errorCode() + ". Will retry a few more times");
      }
      retryBackOffMillis(5000L);
    } while (retries++ <= maxRetries);
    if (result.isEmpty()) {
      throw new KafkaException("Could not get Topic/Partition metadata for topic: " + kafkaTopicName + " with partitions : " + kafkaPartitions + " after trying for " + maxRetries * 5000 / 1000.0D + " seconds using the Broker list: " + kafkaBrokers);
    }
    return result;
  }
  
  private static TopicMetadata getBrokerPartitions(List<String> kafkaBrokers, String kafkaTopicName, List<Integer> kafkaPartitions)
    throws InterruptedException
  {
    TopicMetadata result = null;
    for (Iterator i$ = kafkaBrokers.iterator(); i$.hasNext(); maxRetries > 0)
    {
      String seed = (String)i$.next();
      
      String[] ip_port = seed.split(":");
      String host = ip_port[0];
      int port = Integer.parseInt(ip_port[1]);
      SimpleConsumer consumer = null;
      short maxRetries = 3;
      try
      {
        consumer = new SimpleConsumer(host, port, 100000, 65536, kafkaTopicName);
        List<String> topics = Collections.singletonList(kafkaTopicName);
        TopicMetadataRequest request = new TopicMetadataRequest(topics);
        
        TopicMetadataResponse response = consumer.send(request);
        List<TopicMetadata> metaData = response.topicsMetadata();
        if (Logger.getLogger("KafkaStreams").isInfoEnabled()) {
          logger.info(metaData);
        }
        if ((metaData != null) && (!metaData.isEmpty()))
        {
          result = (TopicMetadata)metaData.get(0);
          if (consumer == null) {
            break;
          }
          consumer.close(); break;
        }
      }
      catch (Exception e)
      {
        logger.error("Error communicating with Broker [" + seed + "] to find Leader for [" + kafkaTopicName + "] with partitions " + kafkaPartitions + "]" + " Reason: " + e.getMessage() + ", will continue try with other brokers that may have been configured as bootstrap brokers");
      }
      finally
      {
        if (consumer != null) {
          consumer.close();
        }
      }
      retryBackOffMillis(2000L);
      
      maxRetries = (short)(maxRetries - 1);
    }
    return result;
  }
  
  public static OffsetPosition getLastCheckpoint(String checkpointTopicName, int partitionId, SimpleConsumer consumer)
    throws Exception
  {
    Object rawObject = getLastObject(checkpointTopicName, partitionId, consumer);
    if (rawObject == null) {
      return null;
    }
    if ((rawObject instanceof OffsetPosition))
    {
      OffsetPosition result = (OffsetPosition)rawObject;
      return result;
    }
    throw new RuntimeException("Cannot return last object as an OffsetPosition because it is a " + rawObject.getClass());
  }
  
  public static OffsetPosition updateCheckpoint(OffsetPosition partitionCheckpoint, String kafkaTopicName, int partitionId, long kafkaStopHereOffset, SimpleConsumer consumer)
    throws Exception
  {
    long this_fetch_offset = partitionCheckpoint.getOffset();
    while (this_fetch_offset < kafkaStopHereOffset) {
      try
      {
        FetchRequestBuilder builder = new FetchRequestBuilder();
        builder.clientId(consumer.clientId());
        builder.addFetch(kafkaTopicName, partitionId, this_fetch_offset, KafkaConstants.fetch_size);
        FetchRequest req = builder.build();
        short maxAttempts = 2;
        boolean hasError = true;
        FetchResponse fetchResponse;
        do
        {
          fetchResponse = consumer.fetch(req);
          if (fetchResponse.hasError())
          {
            logger.error("Error fetching data from " + kafkaTopicName + " on partition " + partitionId + " reason -> error code: " + fetchResponse.errorCode(kafkaTopicName, partitionId));
          }
          else
          {
            hasError = false;
            maxAttempts = 0;
          }
          maxAttempts = (short)(maxAttempts - 1);
        } while (maxAttempts > 0);
        if (hasError) {
          throw new KafkaException("Error fetching data from " + kafkaTopicName + " on partition " + partitionId + " reason -> error code: " + fetchResponse.errorCode(kafkaTopicName, partitionId));
        }
        ByteBufferMessageSet messageSet = fetchResponse.messageSet(kafkaTopicName, partitionId);
        for (MessageAndOffset messageAndOffset : messageSet) {
          if (messageAndOffset.offset() <= kafkaStopHereOffset)
          {
            ByteBuffer payload = messageAndOffset.message().payload();
            while (payload.hasRemaining())
            {
              int size = payload.getInt();
              if ((payload.position() + size > payload.limit()) || (size == 0)) {
                break;
              }
              byte[] current_bytes = new byte[size];
              payload.get(current_bytes, 0, size);
              Object rawObject = KryoSingleton.read(current_bytes, false);
              if ((rawObject instanceof WAEvent))
              {
                WAEvent e = (WAEvent)rawObject;
                if (Logger.getLogger("KafkaStreams").isDebugEnabled()) {
                  Logger.getLogger("KafkaStreams").debug("Scanning for checkpoint " + kafkaTopicName + ":" + partitionId + "@" + messageAndOffset.offset() + " ~ " + e.position);
                }
                partitionCheckpoint.mergeHigherPositions(e.position);
              }
              else
              {
                logger.error("Unexpected object type found in kafka stream while updating checkpoint: " + rawObject.getClass() + "... " + rawObject);
              }
              this_fetch_offset = messageAndOffset.nextOffset();
            }
          }
        }
      }
      catch (Exception e)
      {
        throw e;
      }
    }
    return partitionCheckpoint;
  }
  
  public static long write(String kafkaTopicName, int partitionId, byte[] kafkaBytes, KafkaProducer<byte[], byte[]> producer, MetaInfo.Stream streamInfo, long offsetPrecedingWrite, int maxAttempts)
    throws Exception
  {
    assert (maxAttempts > 0);
    
    ProducerRecord<byte[], byte[]> record = new ProducerRecord(kafkaTopicName, Integer.valueOf(partitionId), null, kafkaBytes);
    try
    {
      RecordMetadata rm = (RecordMetadata)producer.send(record).get();
      long result = rm.offset();
      if (Logger.getLogger("KafkaStreams").isDebugEnabled()) {
        Logger.getLogger("KafkaStreams").debug("Kafka write to " + kafkaTopicName + ":" + partitionId + " succeded normally for " + kafkaBytes.length + " bytes at offset " + result + " on partition " + rm.partition());
      }
      return result;
    }
    catch (Exception e)
    {
      logger.error(e.getMessage(), e);
      do
      {
        Thread.sleep(2000L);
        SimpleConsumer simple_consumer = null;
        try
        {
          List<String> broker_address_list = getBrokerAddress(streamInfo);
          String clientName = "Client_" + kafkaTopicName + "_" + partitionId + "_" + BaseServer.getBaseServer().getServerID();
          List<Integer> kafka_partition = new ArrayList(1);
          kafka_partition.add(Integer.valueOf(partitionId));
          Map<Broker, List<Integer>> brokers_map = getBrokerPartitions(broker_address_list, kafkaTopicName, kafka_partition, 20);
          Broker current_broker_for_partition = null;
          for (Map.Entry<Broker, List<Integer>> entry : brokers_map.entrySet()) {
            if (((List)entry.getValue()).contains(Integer.valueOf(partitionId)))
            {
              current_broker_for_partition = (Broker)entry.getKey();
              break;
            }
          }
          if (current_broker_for_partition != null)
          {
            String host = current_broker_for_partition.host();
            int port = current_broker_for_partition.port();
            simple_consumer = new SimpleConsumer(host, port, 100000, 65536, clientName);
          }
          if (simple_consumer == null)
          {
            if (simple_consumer != null)
            {
              simple_consumer.close();
              simple_consumer = null;
            }
          }
          else
          {
            OffsetResponse offsetResponse = getNextOffset(kafkaTopicName, partitionId, simple_consumer);
            long[] offsetAfterOrAtWriteArray = offsetResponse.offsets(kafkaTopicName, partitionId);
            if (offsetAfterOrAtWriteArray.length > 0)
            {
              long offsetAfterOrAtWrite = offsetAfterOrAtWriteArray[0];
              long foundOffset = findBytesInRecentKafka(kafkaTopicName, partitionId, simple_consumer, kafkaBytes, offsetPrecedingWrite, offsetAfterOrAtWrite);
              if (foundOffset < 0L) {}
              offsetPrecedingWrite = offsetAfterOrAtWrite;
              return foundOffset;
            }
            logger.warn("Number offsets returned is less than 0: " + offsetAfterOrAtWriteArray.length + " will retry " + maxAttempts + " times");
          }
        }
        catch (KafkaException ke)
        {
          logger.error(e.getMessage(), e);
          logger.error("Will retry " + maxAttempts + " time(s)");
        }
        catch (Exception fe)
        {
          throw fe;
        }
        finally
        {
          if (simple_consumer != null)
          {
            simple_consumer.close();
            simple_consumer = null;
          }
        }
      } while (maxAttempts-- > 0);
      throw new KafkaException("Couldn't validate if the last write to Kafka topic: " + kafkaTopicName + "[" + partitionId + "] was successful");
    }
  }
  
  public static long writeCheckpoint(String kafkaTopicName, int partitionId, OffsetPosition partitionCheckpoint, KafkaProducer<byte[], byte[]> producer, MetaInfo.Stream streamInfo, SimpleConsumer checkpointTopicConsumer, int maxAttempts)
    throws Exception
  {
    byte[] bytes = KryoSingleton.write(partitionCheckpoint, false);
    String checkpointTopicName = getCheckpointTopicName(kafkaTopicName);
    OffsetResponse offsetResponse = getNextOffset(checkpointTopicName, partitionId, checkpointTopicConsumer);
    long offsetPrecedingWrite = offsetResponse.offsets(checkpointTopicName, partitionId)[0];
    return write(checkpointTopicName, partitionId, bytes, producer, streamInfo, offsetPrecedingWrite, maxAttempts);
  }
  
  public static long findBytesInRecentKafka(String kafkaTopicName, int partitionId, SimpleConsumer consumer, byte[] findBytes, long lowOffset, long highOffset)
    throws KafkaException
  {
    for (long this_fetch_offset = lowOffset; this_fetch_offset <= highOffset; this_fetch_offset += 1L)
    {
      FetchRequestBuilder builder = new FetchRequestBuilder();
      builder.clientId(consumer.clientId());
      builder.addFetch(kafkaTopicName, partitionId, this_fetch_offset, KafkaConstants.fetch_size);
      FetchRequest req = builder.build();
      try
      {
        FetchResponse fetchResponse = consumer.fetch(req);
        if (fetchResponse.hasError()) {
          throw new KafkaException("Error when trying to fetch top eight bytes in checkpoint stream for " + kafkaTopicName + ":" + partitionId + ": code=" + fetchResponse.errorCode(kafkaTopicName, partitionId));
        }
        ByteBufferMessageSet messageSet = fetchResponse.messageSet(kafkaTopicName, partitionId);
        for (MessageAndOffset messageAndOffset : messageSet) {
          if (messageAndOffset.offset() <= highOffset)
          {
            byte[] superset = messageAndOffset.message().payload().array();
            byte[] subset = findBytes;
            
            int subsetIndex = 0;
            for (int supersetIndex = 0; supersetIndex < superset.length; supersetIndex++) {
              if (superset[supersetIndex] == subset[subsetIndex])
              {
                subsetIndex++;
                if (subsetIndex >= subset.length) {
                  return messageAndOffset.offset();
                }
              }
              else
              {
                if (superset.length - supersetIndex <= subset.length) {
                  break;
                }
                subsetIndex = 0;
              }
            }
            this_fetch_offset = messageAndOffset.nextOffset();
          }
        }
      }
      catch (Exception e)
      {
        throw new KafkaException(e.getMessage(), e);
      }
    }
    return -1L;
  }
  
  public static String getKafkaStreamLockName(String kafkaTopicName, int partitionId)
  {
    return "KafkaStreamLock_" + kafkaTopicName + "_" + partitionId;
  }
  
  public static OffsetResponse getNextOffset(String kafkaTopicName, int partitionId, SimpleConsumer consumer)
    throws KafkaException, InterruptedException
  {
    OffsetResponse offsetResponse = getLastOffsets(kafkaTopicName, partitionId, consumer, 1);
    return offsetResponse;
  }
  
  public static Object getLastObject(String kafkaTopicName, int partitionId, SimpleConsumer consumer)
    throws Exception
  {
    OffsetResponse offsetResponse = getLastOffsets(kafkaTopicName, partitionId, consumer, 2);
    long[] kafkaOffsets = offsetResponse.offsets(kafkaTopicName, partitionId);
    Object result = getObjectAtGreatestOffset(kafkaTopicName, partitionId, kafkaOffsets, consumer);
    return result;
  }
  
  public static Object getLastObjectFromTopic(String kafkaTopicName, int partitionId, SimpleConsumer consumer, int num_records)
    throws Exception
  {
    OffsetResponse offsetResponse = getLastOffsets(kafkaTopicName, partitionId, consumer, num_records);
    long[] kafkaOffsets = offsetResponse.offsets(kafkaTopicName, partitionId);
    Object result = getObjectAtGreatestOffset(kafkaTopicName, partitionId, kafkaOffsets, consumer);
    return result;
  }
  
  public static Object getObjectAtGreatestOffset(String kafkaTopicName, int partitionId, long[] kafkaOffsets, SimpleConsumer consumer)
    throws Exception
  {
    try
    {
      FetchRequestBuilder builder = new FetchRequestBuilder();
      builder.clientId(consumer.clientId());
      for (long kafkaOffset : kafkaOffsets) {
        builder.addFetch(kafkaTopicName, partitionId, kafkaOffset, KafkaConstants.fetch_size);
      }
      FetchRequest req = builder.build();
      short maxAttempts = 2;
      boolean hasError = true;
      FetchResponse fetchResponse;
      do
      {
        fetchResponse = consumer.fetch(req);
        if (fetchResponse.hasError())
        {
          logger.error("Error fetching data from " + kafkaTopicName + " on partition " + partitionId + " reason -> error code: " + fetchResponse.errorCode(kafkaTopicName, partitionId));
        }
        else
        {
          hasError = false;
          maxAttempts = 0;
        }
        maxAttempts = (short)(maxAttempts - 1);
      } while (maxAttempts > 0);
      if (hasError) {
        throw new KafkaException("Error fetching data from " + kafkaTopicName + " on partition " + partitionId + " reason -> error code: " + fetchResponse.errorCode(kafkaTopicName, partitionId));
      }
      MessageAndOffset foundMessage = null;
      ByteBufferMessageSet messageSet = fetchResponse.messageSet(kafkaTopicName, partitionId);
      for (MessageAndOffset messageAndOffset : messageSet) {
        if (!messageAndOffset.message().isNull()) {
          if ((foundMessage == null) || (messageAndOffset.offset() > foundMessage.offset())) {
            foundMessage = messageAndOffset;
          }
        }
      }
      if (foundMessage == null) {
        return null;
      }
      ByteBuffer payload = foundMessage.message().payload();
      byte[] bytes = new byte[payload.limit()];
      payload.get(bytes);
      return KryoSingleton.read(bytes, false);
    }
    catch (Exception e)
    {
      throw e;
    }
  }
  
  public static OffsetResponse getLastOffsets(String kafkaTopicName, int partitionId, SimpleConsumer consumer, int count)
    throws KafkaException, InterruptedException
  {
    OffsetResponse lastOffset = getOffset(kafkaTopicName, partitionId, kafka.api.OffsetRequest.LatestTime(), consumer.clientId(), count, consumer);
    return lastOffset;
  }
  
  public static OffsetResponse getFirstOffset(String kafkaTopicName, int partitionId, SimpleConsumer consumer)
    throws KafkaException, InterruptedException
  {
    OffsetResponse firstOffset = getOffset(kafkaTopicName, partitionId, kafka.api.OffsetRequest.EarliestTime(), consumer.clientId(), 1, consumer);
    return firstOffset;
  }
  
  private static OffsetResponse getOffset(String topic, int partition, long whichTime, String clientName, int count, SimpleConsumer simpleConsumer)
    throws KafkaException, InterruptedException
  {
    try
    {
      TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
      Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap();
      requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, count));
      kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
      short maxTries = 2;
      OffsetResponse response;
      do
      {
        response = simpleConsumer.getOffsetsBefore(request);
        if (response.hasError()) {
          logger.error("Error getting offset for topic: " + topic + " on partition: " + partition + " reason -> error code: " + response.errorCode(topic, partition) + ", will retry");
        } else {
          return response;
        }
        if (maxTries != 0) {
          retryBackOffMillis(3000L);
        }
        maxTries = (short)(maxTries - 1);
      } while (maxTries > 0);
      throw new KafkaException("Error getting offset for topic: " + topic + " on partition: " + partition + " reason -> error code: " + response.errorCode(topic, partition));
    }
    catch (Exception e)
    {
      throw new KafkaException(e.getMessage(), e);
    }
  }
  
  public static boolean retryBackOffMillis(long milliseconds)
  {
    try
    {
      Thread.sleep(milliseconds);
      return true;
    }
    catch (InterruptedException e)
    {
      Logger.getLogger("KafkaStreams").warn("Kafka thread interrupted while sleeping", e);
      e.printStackTrace();
    }
    return false;
  }
}
