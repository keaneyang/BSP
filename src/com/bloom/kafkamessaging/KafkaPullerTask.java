package com.bloom.kafkamessaging;

import com.bloom.classloading.WALoader;
import com.bloom.messaging.Handler;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.runtime.KafkaStreamUtils;
import com.bloom.runtime.StreamEventFactory;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.PropertySet;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.UUID;
import com.bloom.event.SimpleEvent;
import com.bloom.recovery.KafkaSourcePosition;
import com.bloom.recovery.Position;
import com.bloom.runtime.StreamEvent;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

class KafkaPullerTask
  implements Runnable
{
  private static Logger logger = Logger.getLogger(KafkaPuller.class);
  private final Handler rcvr;
  private final KafkaPuller kafkaPuller;
  private final KafkaDecoder kd = new KafkaDecoder();
  protected Integer[] emittedCountPerPartition;
  public volatile boolean dontStop;
  long min_fetch_size;
  long avg_fetch_size;
  long max_fetch_size;
  long total_fetch_reqs = 0L;
  private final Map<SimpleConsumer, Set<PartitionState>> consumerPartitions;
  FetchResponse fetchResponse = null;
  Exception ex = null;
  boolean isUserTriggered = false;
  Map<SimpleConsumer, ConsumerStats> consumerStats;
  private boolean isAvro;
  private SpecificDatumReader avroReader;
  private MDRepository mdr = MetadataRepository.getINSTANCE();
  private WALoader wal = WALoader.get();
  private MetaInfo.Type streamDataTye;
  private Object streamDataTypeInstance;
  private Class streamDataTypeClass;
  
  public KafkaPullerTask(KafkaPuller kafkaPuller, Map<SimpleConsumer, Set<PartitionState>> consumerPartitions)
    throws Exception
  {
    this.kafkaPuller = kafkaPuller;
    this.consumerPartitions = consumerPartitions;
    this.rcvr = kafkaPuller.rcvr;
    this.emittedCountPerPartition = new Integer[kafkaPuller.partitions.size()];
    for (int ii = 0; ii < this.emittedCountPerPartition.length; ii++) {
      this.emittedCountPerPartition[ii] = new Integer(0);
    }
    this.consumerStats = new HashMap(this.consumerPartitions.size());
    for (SimpleConsumer simpleConsumer : consumerPartitions.keySet()) {
      this.consumerStats.put(simpleConsumer, new ConsumerStats(simpleConsumer));
    }
    MetaInfo.PropertySet streamPropset = KafkaStreamUtils.getPropertySet(kafkaPuller.kafkaReceiver.streamInfo);
    if (streamPropset.properties != null)
    {
      String format = (String)streamPropset.properties.get("dataformat");
      if ((format != null) && (format.equalsIgnoreCase("avro")))
      {
        this.streamDataTypeClass = getStreamDataTypeClass();
        this.streamDataTypeInstance = this.streamDataTypeClass.newInstance();
        this.avroReader = getAvroReader();
        this.isAvro = true;
        this.streamDataTye = getStreamDataType();
      }
    }
    this.dontStop = Boolean.TRUE.booleanValue();
  }
  
  private SpecificDatumReader getAvroReader()
    throws Exception
  {
    boolean foundSchema = false;
    MetaInfo.Stream streamMetaObject = null;
    while (!foundSchema)
    {
      streamMetaObject = (MetaInfo.Stream)this.mdr.getMetaObjectByUUID(this.kafkaPuller.streamUuid, WASecurityManager.TOKEN);
      if (streamMetaObject.avroSchema != null) {
        foundSchema = true;
      }
      Thread.sleep(1000L);
    }
    Schema schema = new Schema.Parser().parse(streamMetaObject.avroSchema);
    SpecificDatumReader avroReader = new SpecificDatumReader(schema);
    return avroReader;
  }
  
  private MetaInfo.Type getStreamDataType()
    throws MetaDataRepositoryException
  {
    UUID uuid = this.kafkaPuller.kafkaReceiver.streamInfo.getDataType();
    if (uuid != null)
    {
      MetaInfo.Type dataType = (MetaInfo.Type)this.mdr.getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
      return dataType;
    }
    return null;
  }
  
  private Class<?> getStreamDataTypeClass()
    throws MetaDataRepositoryException, ClassNotFoundException
  {
    UUID uuid = this.kafkaPuller.kafkaReceiver.streamInfo.getDataType();
    if (uuid != null)
    {
      MetaInfo.Type dataType = (MetaInfo.Type)this.mdr.getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
      return this.wal.loadClass(dataType.className);
    }
    return null;
  }
  
  public void run()
  {
    if (logger.isInfoEnabled()) {
      logger.info("Started Kafka Puller Task for : " + this.kafkaPuller.clientName);
    }
    while (this.dontStop) {
      try
      {
        for (SimpleConsumer simpleConsumer : this.consumerPartitions.keySet())
        {
          FetchRequestBuilder builder = new FetchRequestBuilder();
          builder.maxWait(1000);
          builder.minBytes(1048576);
          builder.clientId(this.kafkaPuller.clientName);
          for (PartitionState ps : (Set)this.consumerPartitions.get(simpleConsumer)) {
            builder.addFetch(this.kafkaPuller.kafkaTopicName, ps.partitionId, ps.kafkaReadOffset, KafkaConstants.fetch_size);
          }
          FetchRequest req = builder.build();
          ((ConsumerStats)this.consumerStats.get(simpleConsumer)).setStartTime(System.currentTimeMillis());
          this.fetchResponse = simpleConsumer.fetch(req);
          ((ConsumerStats)this.consumerStats.get(simpleConsumer)).setEndTime(System.currentTimeMillis());
          if (this.fetchResponse.hasError())
          {
            this.dontStop = false;
            this.isUserTriggered = false;
          }
          doProcess(simpleConsumer, this.fetchResponse);
        }
      }
      catch (Exception e)
      {
        this.ex = e;
        this.isUserTriggered = false;
        this.dontStop = false;
        logger.error(e);
      }
    }
  }
  
  private void doProcess(SimpleConsumer simpleConsumer, FetchResponse fetchResponse)
    throws Exception
  {
	  PartitionState partitionState;
    for (Iterator i$ = ((Set)this.consumerPartitions.get(simpleConsumer)).iterator(); i$.hasNext();)
    {
      partitionState = (PartitionState)i$.next();
      
      ByteBufferMessageSet bbms = fetchResponse.messageSet(this.kafkaPuller.kafkaTopicName, partitionState.partitionId);
      buildPullerStats(bbms);
      ((ConsumerStats)this.consumerStats.get(simpleConsumer)).addMessageSize(bbms.sizeInBytes());
      for (MessageAndOffset messageAndOffset : bbms) {
        if (messageAndOffset.offset() < partitionState.kafkaReadOffset)
        {
          logger.warn("Kafka requested data at offset " + partitionState.kafkaReadOffset + " but received data from earlier offset " + messageAndOffset.offset());
        }
        else
        {
          int waEventIndex = 0;
          ByteBuffer payload = messageAndOffset.message().payload();
          while (payload.hasRemaining()) {
            try
            {
              int size = payload.getInt();
              if ((payload.position() + size > payload.limit()) || (size == 0))
              {
                waEventIndex++;
                ((ConsumerStats)this.consumerStats.get(simpleConsumer)).increaseIterations(); break;
              }
              byte[] current_bytes = new byte[size];
              payload.get(current_bytes, 0, size);
              
              Object rawObject = deserialize(current_bytes);
              Integer localInteger1;
              if ((rawObject instanceof com.bloom.runtime.containers.WAEvent))
              {
                com.bloom.runtime.containers.WAEvent e = (com.bloom.runtime.containers.WAEvent)rawObject;
                if (e.position != null)
                {
                  partitionState.partitionCheckpoint.mergeHigherPositions(e.position);
                  partitionState.partitionCheckpoint.setOffset(messageAndOffset.nextOffset());
                  if (Logger.getLogger("KafkaStreams").isDebugEnabled()) {
                    Logger.getLogger("KafkaStreams").debug("Pulled  " + this.kafkaPuller.kafkaTopicName + ":" + partitionState.partitionId + "@" + partitionState.kafkaReadOffset + "/" + waEventIndex + ": " + e.position);
                  }
                  KafkaSourcePosition sp = new KafkaSourcePosition(this.kafkaPuller.kafkaTopicName, partitionState.partitionId, waEventIndex, messageAndOffset.offset());
                  e.position = Position.from(this.kafkaPuller.streamUuid, String.valueOf(partitionState.partitionId), sp);
                  partitionState.lastEmittedPosition = e.position;
                }
                StreamEvent streamEvent = new StreamEvent(StreamEventFactory.createStreamEvent(e.data, e.position), null);
                this.rcvr.onMessage(streamEvent);
                
                Integer[] arrayOfInteger = this.emittedCountPerPartition;int i = partitionState.partitionId;localInteger1 = arrayOfInteger[i];Integer localInteger2 = arrayOfInteger[i] = Integer.valueOf(arrayOfInteger[i].intValue() + 1);
              }
              else
              {
                logger.error("Unexpected object type found in kafka stream: " + rawObject.getClass() + "... " + rawObject);
              }
            }
            catch (Exception e)
            {
              throw e;
            }
            finally
            {
              waEventIndex++;
              ((ConsumerStats)this.consumerStats.get(simpleConsumer)).increaseIterations();
            }
          }
          partitionState.kafkaReadOffset = messageAndOffset.nextOffset();
          ((ConsumerStats)this.consumerStats.get(simpleConsumer)).incrementMessageCount(waEventIndex);
        }
      }
    }
    
  }
  
  private Object deserialize(byte[] current_bytes)
    throws Exception
  {
    if (this.isAvro)
    {
      BinaryDecoder recordDecoder = DecoderFactory.get().binaryDecoder(current_bytes, null);
      GenericData.Record oo = (GenericData.Record)this.avroReader.read(null, recordDecoder);
      com.bloom.runtime.containers.WAEvent pWAEvent;
      if ((this.streamDataTypeInstance instanceof com.bloom.proc.events.WAEvent))
      {
        GenericData.Array dataArray = (GenericData.Array)oo.get("data");
        Object[] data = null;
        int ii;
        if (dataArray != null)
        {
          data = new Object[dataArray.size()];
          ii = 0;
          for (Object utf8 : dataArray) {
            data[(ii++)] = utf8.toString();
          }
        }
        System.out.println(Arrays.asList(data));
        Map metadataMap = (Map)oo.get("metadata");
        HashMap mmap = new HashMap();
        for (Object ss : metadataMap.keySet()) {
          mmap.put(ss, metadataMap.get(ss).toString());
        }
        GenericData.Array beforeObject = (GenericData.Array)oo.get("before");
        Object[] before = null;
        int jj;
        if (beforeObject != null)
        {
          before = new Object[beforeObject.size()];
          jj = 0;
          for (Object utf8 : beforeObject) {
            before[(jj++)] = utf8.toString();
          }
        }
        Object dataPBMObject = oo.get("dataPresenceBitMap");
        byte[] dataPBMBytes = Base64.decodeBase64(dataPBMObject.toString());
        
        Object beforePBMObject = oo.get("beforePresenceBitMap");
        byte[] beforePBMBytes = Base64.decodeBase64(beforePBMObject.toString());
        
        Object uuidObject = oo.get("typeUUID");
        UUID typeUUID = null;
        if (uuidObject != null) {
          typeUUID = new UUID(uuidObject.toString());
        }
        com.bloom.proc.events.WAEvent waEvent = (com.bloom.proc.events.WAEvent)this.streamDataTypeClass.newInstance();
        waEvent.setPayload(data);
        waEvent.before = before;
        waEvent.metadata = mmap;
        waEvent.dataPresenceBitMap = dataPBMBytes;
        waEvent.beforePresenceBitMap = beforePBMBytes;
        waEvent.typeUUID = typeUUID;
        pWAEvent = new com.bloom.runtime.containers.WAEvent(waEvent);
      }
      else
      {
        Object[] vals = new Object[this.streamDataTye.fields.size()];
        int ip = 0;
        for (Map.Entry<String, String> entry : this.streamDataTye.fields.entrySet())
        {
          Object ood = oo.get((String)entry.getKey());
          if (((String)entry.getValue()).contains("String"))
          {
            String sp = null;
            if (sp != null) {
              sp = ood.toString();
            }
            vals[ip] = sp;
          }
          else if (((String)entry.getValue()).contains("DateTime"))
          {
            DateTime dt = null;
            if (ood != null) {
              dt = DateTime.parse(ood.toString());
            }
            vals[ip] = dt;
          }
          else if (((String)entry.getValue()).contains("UUID"))
          {
            if (ood != null) {
              vals[ip] = new UUID(ood.toString());
            } else {
              vals[ip] = ood;
            }
          }
          else if (((String)entry.getValue()).contains("byte"))
          {
            byte[] bytes = Base64.decodeBase64(ood.toString());
            Object result = this.kd.deserialize("not used", bytes);
            vals[ip] = result;
          }
          else
          {
            vals[ip] = ood;
          }
          ip++;
        }
        SimpleEvent simpleEvent = (SimpleEvent)this.streamDataTypeClass.newInstance();
        simpleEvent.setPayload(vals);
        
        pWAEvent = new com.bloom.runtime.containers.WAEvent(simpleEvent);
      }
      Object posObject = oo.get("position");
      byte[] posBytes = Base64.decodeBase64(posObject.toString());
      Position position = (Position)this.kd.deserialize("not used", posBytes);
      pWAEvent.position = position;
      return pWAEvent;
    }
    return this.kd.deserialize("not used", current_bytes);
  }
  
  public void stop()
  {
    this.dontStop = false;
    this.isUserTriggered = true;
  }
  
  private void buildPullerStats(ByteBufferMessageSet bbms)
  {
    int current_fetch_size = bbms.sizeInBytes();
    if (current_fetch_size != 0)
    {
      if ((this.min_fetch_size == 0L) && (this.max_fetch_size == 0L) && (this.avg_fetch_size == 0L))
      {
        this.max_fetch_size = current_fetch_size;
        this.min_fetch_size = current_fetch_size;
      }
      else
      {
        if (current_fetch_size < this.min_fetch_size) {
          this.min_fetch_size = current_fetch_size;
        }
        if (current_fetch_size > this.max_fetch_size) {
          this.max_fetch_size = current_fetch_size;
        }
      }
      this.total_fetch_reqs += 1L;
      this.avg_fetch_size = ((this.avg_fetch_size + current_fetch_size) / this.total_fetch_reqs);
    }
  }
}
