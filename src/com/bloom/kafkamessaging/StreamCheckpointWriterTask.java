package com.bloom.kafkamessaging;

import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.runtime.KafkaStreamUtils;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;

import java.util.Set;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;

public class StreamCheckpointWriterTask
  implements Runnable
{
  private static Logger logger = Logger.getLogger(StreamCheckpointWriterTask.class);
  private final KafkaPuller kafkaPuller;
  private final Set<PartitionState> partitions;
  private final KafkaProducer producer;
  private final String kafkaTopicName;
  static final int checkpointPeriodMillis = 10000;
  Exception ex = null;
  boolean isUserTriggered = false;
  
  public StreamCheckpointWriterTask(KafkaPuller kafkaPuller)
  {
    this.kafkaPuller = kafkaPuller;
    this.partitions = kafkaPuller.partitions;
    this.kafkaTopicName = kafkaPuller.kafkaTopicName;
    this.producer = kafkaPuller.kafkaProducer;
  }
  
  public void run()
  {
    long currentTimeMillis = System.currentTimeMillis();
    for (PartitionState partitionState : this.partitions)
    {
      ILock lock = null;
      try
      {
        lock = HazelcastSingleton.get().getLock(KafkaStreamUtils.getKafkaStreamLockName(this.kafkaTopicName, partitionState.partitionId));
        if (!lock.tryLock())
        {
          if (Logger.getLogger("KafkaStreams").isDebugEnabled()) {
            Logger.getLogger("KafkaStreams").debug(this.kafkaTopicName + ":" + partitionState.partitionId + " could not get a lock for writing a checkpoint");
          }
          if ((lock != null) && (lock.isLockedByCurrentThread())) {
            lock.unlock();
          }
        }
        else if ((partitionState.partitionCheckpoint != null) && ((partitionState.partitionCheckpoint.isEmpty()) || (partitionState.partitionCheckpoint.getTimestamp() + 10000L > currentTimeMillis)))
        {
          if ((lock != null) && (lock.isLockedByCurrentThread())) {
            lock.unlock();
          }
        }
        else
        {
          String checkpointTopicName = KafkaStreamUtils.getCheckpointTopicName(this.kafkaTopicName);
          Object rawObject = KafkaStreamUtils.getLastObjectFromTopic(checkpointTopicName, partitionState.partitionId, partitionState.checkpointTopicConsumer, 2);
          if (rawObject != null) {
            if ((rawObject instanceof OffsetPosition))
            {
              OffsetPosition mostRecentCheckpoint = (OffsetPosition)rawObject;
              if ((mostRecentCheckpoint != null) && ((mostRecentCheckpoint.getTimestamp() + 10000L > currentTimeMillis) || (partitionState.partitionCheckpoint.getOffset() <= mostRecentCheckpoint.getOffset())))
              {
                if ((lock == null) || (!lock.isLockedByCurrentThread())) {
                  continue;
                }
                lock.unlock(); continue;
              }
            }
            else
            {
              throw new KafkaException("Unexpected class type: " + rawObject.getClass().getSimpleName() + " while reading" + " last object from Kafka topic: " + checkpointTopicName + "[" + partitionState.partitionId + "]");
            }
          }
          OffsetPosition safeLocalCopy;
          synchronized (partitionState.partitionCheckpoint)
          {
            partitionState.partitionCheckpoint.setTimestamp(currentTimeMillis);
            safeLocalCopy = new OffsetPosition(partitionState.partitionCheckpoint);
          }
          MetaInfo.Stream streamInfo = this.kafkaPuller.kafkaReceiver.streamInfo;
          long writeOffset = KafkaStreamUtils.writeCheckpoint(this.kafkaTopicName, partitionState.partitionId, safeLocalCopy, this.producer, streamInfo, partitionState.checkpointTopicConsumer, 3);
          
          partitionState.lastCheckpointWriteOffset = writeOffset;
          if (logger.isDebugEnabled()) {
            logger.debug("Updated checkpoint for " + this.kafkaTopicName + " at checkpoint buffer offset " + writeOffset);
          }
        }
      }
      catch (Exception e)
      {
        logger.error("Error while checkpointing Kafka stream", e);
        this.ex = e;
        this.isUserTriggered = false;
      }
      finally
      {
        if ((lock != null) && (lock.isLockedByCurrentThread())) {
          lock.unlock();
        }
      }
    }
  }
  
  public void stop()
  {
    this.isUserTriggered = true;
  }
}
