package com.bloom.runtime.containers;

import com.bloom.runtime.KeyFactory;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.StreamEvent;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import scala.Tuple2;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Queue;

public abstract class Batch<T>
  implements IBatch
{
  private static final long serialVersionUID = 3418639571053079192L;
  
  public abstract java.util.Iterator<T> iterator();
  
  public abstract int size();
  
  public boolean isEmpty()
  {
    return size() == 0;
  }
  
  public T first()
  {
    Iterable<T> it = this;
    java.util.Iterator i$ = it.iterator();
    if (i$.hasNext())
    {
      T e = i$.next();
      return e;
    }
    return null;
  }
  
  public T last()
  {
    Iterable<T> it = this;
    T ret = null;
    for (T e : it) {
      ret = e;
    }
    return ret;
  }
  
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    String sep = "";
    sb.append("[");
    Iterable<T> it = this;
    for (T e : it)
    {
      sb.append(sep).append(e);
      sep = ",";
    }
    sb.append("]");
    return sb.toString();
  }
  
  private static final Batch emptyBatch = new Batch()
  {
    private static final long serialVersionUID = -7983831581625863170L;
    
    public java.util.Iterator<WAEvent> iterator()
    {
      return Collections.emptyIterator();
    }
    
    public int size()
    {
      return 0;
    }
  };
  
  public static Batch emptyBatch()
  {
    return emptyBatch;
  }
  
  public static Batch asBatch(HashMap<RecordKey, WAEvent> b)
  {
    new Batch()
    {
      private static final long serialVersionUID = 4242550963548968535L;
      
      public java.util.Iterator<WAEvent> iterator()
      {
        new java.util.Iterator()
        {
          scala.collection.Iterator<Tuple2<RecordKey, WAEvent>> it = Batch.2.this.val$b.iterator();
          
          public boolean hasNext()
          {
            return this.it.hasNext();
          }
          
          public WAEvent next()
          {
            return (WAEvent)((Tuple2)this.it.next())._2;
          }
          
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        };
      }
      
      public int size()
      {
        return this.val$b.size();
      }
    };
  }
  
  private static class ColBatch<WAEvent>
    extends Batch
  {
    private static final long serialVersionUID = -6646847362508352538L;
    final Collection<WAEvent> col;
    
    ColBatch(Collection<WAEvent> col)
    {
      this.col = col;
    }
    
    public java.util.Iterator<WAEvent> iterator()
    {
      return this.col.iterator();
    }
    
    public int size()
    {
      return this.col.size();
    }
  }
  
  private static class StreamColBatch<StreamEvent>
    extends Batch
  {
    private static final long serialVersionUID = -6646847362508352538L;
    final Collection<StreamEvent> col;
    
    StreamColBatch(Collection<StreamEvent> col)
    {
      this.col = col;
    }
    
    public java.util.Iterator<StreamEvent> iterator()
    {
      return this.col.iterator();
    }
    
    public int size()
    {
      return this.col.size();
    }
  }
  
  public static Batch asBatch(Collection<WAEvent> col)
  {
    return new ColBatch(col);
  }
  
  public static Batch asStreamBatch(Collection<StreamEvent> col)
  {
    return new StreamColBatch(col);
  }
  
  public static Batch asBatch(WAEvent e)
  {
    new Batch()
    {
      private static final long serialVersionUID = 7274470955311509658L;
      
      public java.util.Iterator<WAEvent> iterator()
      {
        new java.util.Iterator()
        {
          private boolean hasNext = true;
          
          public boolean hasNext()
          {
            return this.hasNext;
          }
          
          public WAEvent next()
          {
            if (this.hasNext)
            {
              this.hasNext = false;
              return Batch.3.this.val$e;
            }
            throw new NoSuchElementException();
          }
          
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        };
      }
      
      public int size()
      {
        return 1;
      }
    };
  }
  
  public static Batch batchesAsBatch(Collection<IBatch> col)
  {
    new Batch()
    {
      private static final long serialVersionUID = -1796558864997411339L;
      
      public java.util.Iterator<WAEvent> iterator()
      {
        new java.util.Iterator()
        {
          private java.util.Iterator<IBatch> sit = Batch.4.this.val$col.iterator();
          private java.util.Iterator<WAEvent> it = Collections.emptyIterator();
          private WAEvent _next = null;
          
          private WAEvent getNext()
          {
            if (this._next == null)
            {
              while (!this.it.hasNext())
              {
                if (!this.sit.hasNext()) {
                  break label77;
                }
                this.it = ((IBatch)this.sit.next()).iterator();
              }
              this._next = ((WAEvent)this.it.next());
            }
            label77:
            return this._next;
          }
          
          public boolean hasNext()
          {
            return getNext() != null;
          }
          
          public WAEvent next()
          {
            WAEvent ret = getNext();
            this._next = null;
            return ret;
          }
          
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        };
      }
      
      public int size()
      {
        int size = 0;
        for (IBatch b : this.val$col) {
          size += b.size();
        }
        return size;
      }
      
      public boolean isEmpty()
      {
        if (!this.val$col.isEmpty()) {
          for (IBatch b : this.val$col) {
            if (!b.isEmpty()) {
              return false;
            }
          }
        }
        return true;
      }
    };
  }
  
  public static Batch asBatch(Queue<WAEvent> buf)
  {
    new Batch()
    {
      private static final long serialVersionUID = 4716758572600013367L;
      
      public java.util.Iterator<WAEvent> iterator()
      {
        new java.util.Iterator()
        {
          private scala.collection.Iterator<WAEvent> it = Batch.5.this.val$buf.iterator();
          
          public boolean hasNext()
          {
            return this.it.hasNext();
          }
          
          public WAEvent next()
          {
            return (WAEvent)this.it.next();
          }
          
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        };
      }
      
      public int size()
      {
        return this.val$buf.size();
      }
    };
  }
  
  public static Map<RecordKey, IBatch<WAEvent>> partition(KeyFactory keyFactory, IBatch<WAEvent> batch)
  {
    if (batch.size() == 1)
    {
      WAEvent rec = (WAEvent)batch.first();
      RecordKey key = keyFactory.makeKey(rec.data);
      return Collections.singletonMap(key, batch);
    }
    Map<RecordKey, IBatch<WAEvent>> map = new LinkedHashMap();
    for (WAEvent rec : batch)
    {
      RecordKey key = keyFactory.makeKey(rec.data);
      IBatch values = (IBatch)map.get(key);
      if (values == null)
      {
        values = new ColBatch(new ArrayList());
        map.put(key, values);
      }
      ((ColBatch)values).col.add(rec);
    }
    return map;
  }
}
