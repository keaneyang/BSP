package com.bloom.kafkamessaging;

import java.util.Map;
import java.util.concurrent.Future;
import org.apache.log4j.Logger;

import com.bloom.runtime.Pair;

public class KafkaPullerStatsTask
  implements Runnable
{
  private static final Logger logger = Logger.getLogger(KafkaPullerStatsTask.class);
  private final Pair<KafkaPullerTask, Future>[] consumerThreads;
  private final KafkaReceiver kafkaReceiver;
  int total_events_processed = 0;
  private boolean showDetail = false;
  boolean isUserTriggered = false;
  
  public KafkaPullerStatsTask(Pair<KafkaPullerTask, Future>[] consumerThreads, KafkaReceiver kafkaReceiver)
  {
    this.consumerThreads = consumerThreads;
    this.kafkaReceiver = kafkaReceiver;
  }
  
  public void run()
  {
    if (!this.showDetail)
    {
      for (Pair<KafkaPullerTask, Future> taskFuturePair : this.consumerThreads)
      {
        for (Integer cc : ((KafkaPullerTask)taskFuturePair.first).emittedCountPerPartition) {
          this.total_events_processed += cc.intValue();
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Fetch stats for : Avg: " + ((KafkaPullerTask)taskFuturePair.first).avg_fetch_size + ", Max: " + ((KafkaPullerTask)taskFuturePair.first).max_fetch_size + ", Min: " + ((KafkaPullerTask)taskFuturePair.first).min_fetch_size);
        }
        for (ConsumerStats consumerStats : ((KafkaPullerTask)taskFuturePair.first).consumerStats.values()) {
          consumerStats.printStats();
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Kafka puller for " + this.kafkaReceiver.name + " consumed " + this.total_events_processed);
      }
      this.total_events_processed = 0;
    }
  }
  
  public void stop()
  {
    this.isUserTriggered = true;
  }
}
