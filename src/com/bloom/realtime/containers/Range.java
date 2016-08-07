package com.bloom.runtime.containers;

import com.bloom.historicalcache.Cache;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.runtime.NodeStartUp.InitializerParams.Params;
import com.hazelcast.core.HazelcastInstance;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.IRange;
import com.bloom.runtime.containers.WAEvent;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import org.apache.log4j.Logger;
import scala.Option;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Queue;

public abstract class Range
  implements Serializable, IRange
{
  private static Logger logger = Logger.getLogger(Range.class);
  private static final long serialVersionUID = 1646284409392129520L;
  private HazelcastInstance hazelcast;
  
  public Range()
  {
    this.hazelcast = HazelcastSingleton.get(NodeStartUp.InitializerParams.Params.getWAClusterName());
  }
  
  public abstract IBatch all();
  
  public IBatch lookup(int indexID, RecordKey key)
  {
    return Batch.emptyBatch();
  }
  
  public abstract String toString();
  
  public IRange update(IRange r)
  {
    return r;
  }
  
  static abstract class SubRange
    extends Range
  {
    private static final long serialVersionUID = 1346179739415210631L;
    final RecordKey key;
    
    SubRange(RecordKey key)
    {
      this.key = key;
    }
  }
  
  private static final Range emptyRange = new Range()
  {
    private static final long serialVersionUID = 3638589221501577212L;
    
    public Batch all()
    {
      return Batch.emptyBatch();
    }
    
    public String toString()
    {
      return "<empty range>";
    }
  };
  
  public static Range emptyRange()
  {
    return emptyRange;
  }
  
  public static Range createRange(Cache cache)
  {
    new Range()
    {
      private static final long serialVersionUID = -3860895434812640102L;
      private final long TRANSACTION_CLOSED_ID = -1L;
      private Long transactionCount = Long.valueOf(0L);
      private long transactionId = -1L;
      
      public IBatch all()
      {
        if (Range.logger.isDebugEnabled()) {
          Range.logger.debug("Get all() for cache " + this.val$cache.getMetaFullName() + " " + this.transactionId);
        }
        return this.val$cache.get(Long.valueOf(this.transactionId));
      }
      
      public IBatch lookup(int indexID, RecordKey key)
      {
        if (Range.logger.isDebugEnabled()) {
          Range.logger.debug("Get lookup(K) for cache " + this.val$cache.getMetaFullName() + " " + this.transactionId);
        }
        return this.val$cache.get(key, Long.valueOf(this.transactionId));
      }
      
      public String toString()
      {
        return "(cache range ->" + this.val$cache + ") with transactionId id " + this.transactionId;
      }
      
      public synchronized void beginTransaction()
      {
        if (this.transactionId == -1L) {
          this.transactionId = this.val$cache.beginTransaction();
        }
        this.transactionCount = Long.valueOf(this.transactionCount.longValue() + 1L);
      }
      
      public synchronized void endTransaction()
      {
        if (this.transactionId == -1L) {
          throw new RuntimeException("This transaction is already closed !");
        }
        this.transactionCount = Long.valueOf(this.transactionCount.longValue() - 1L);
        if (this.transactionCount.longValue() == 0L)
        {
          this.val$cache.endTransaction(this.transactionId);
          this.transactionId = -1L;
        }
      }
    };
  }
  
  public static Range createRange(RecordKey key, final Collection<WAEvent> buf)
  {
    new SubRange(key)
    {
      private static final long serialVersionUID = 8217289990303717304L;
      
      public Batch all()
      {
        return Batch.asBatch(buf);
      }
      
      public String toString()
      {
        return "(collection range for key:" + this.key + ")";
      }
    };
  }
  
  public static Range createRange(RecordKey key, final Queue<WAEvent> buf)
  {
    new SubRange(key)
    {
      private static final long serialVersionUID = 5437575933413950070L;
      
      public Batch all()
      {
        return Batch.asBatch(buf);
      }
      
      public String toString()
      {
        return "(scala queue range for key:" + this.key + ")";
      }
    };
  }
  
  public static Range createRange(HashMap<RecordKey, WAEvent> map)
  {
    new Range()
    {
      private static final long serialVersionUID = 6264708067898281018L;
      
      public Batch all()
      {
        return Batch.asBatch(this.val$map);
      }
      
      public IBatch lookup(int indexID, RecordKey key)
      {
        Option<WAEvent> ret = this.val$map.get(key);
        if (ret.nonEmpty()) {
          return Batch.asBatch((WAEvent)ret.get());
        }
        return Batch.emptyBatch();
      }
      
      public String toString()
      {
        return "(scala map range " + this.val$map + ")";
      }
    };
  }
  
  public static IRange createRange(Map<RecordKey, IBatch> map)
  {
    new Range()
    {
      private static final long serialVersionUID = -5114607566419634291L;
      
      public Batch all()
      {
        return Batch.batchesAsBatch(this.val$map.values());
      }
      
      public IBatch lookup(int indexID, RecordKey key)
      {
        IBatch sn = (IBatch)this.val$map.get(key);
        if (sn != null) {
          return sn;
        }
        return Batch.emptyBatch();
      }
      
      public IRange update(IRange r)
      {
        if ((r instanceof Range.SubRange))
        {
          Range.SubRange sr = (Range.SubRange)r;
          this.val$map.put(sr.key, sr.all());
          return this;
        }
        return r;
      }
      
      public String toString()
      {
        return "(map of subranges " + this.val$map + ")";
      }
    };
  }
  
  public void beginTransaction() {}
  
  public void endTransaction() {}
}
