package com.bloom.distribution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.bloom.runtime.IPartitionManager;

public class WAIndex<I, K, V>
{
  Type type;
  Map<I, List<WAIndex<I, K, V>.Entry>> index;
  FieldAccessor<I, V> accessor;
  IPartitionManager pm;
  
  public static enum Type
  {
    hash,  tree;
    
    private Type() {}
  }
  
  public class Entry
  {
    final int partId;
    final K key;
    final V value;
    
    public Entry(int partId, K key, V value)
    {
      this.partId = partId;
      this.key = key;
      this.value = value;
    }
    
    public boolean equals(Object obj)
    {
      return this.key.equals(obj);
    }
    
    public int hashCode()
    {
      return this.key.hashCode();
    }
  }
  
  public WAIndex(Type type, FieldAccessor<I, V> accessor, IPartitionManager pm)
  {
    this.type = type;
    this.accessor = accessor;
    this.pm = pm;
    switch (type)
    {
    case hash: 
      this.index = new HashMap(); break;
    case tree: 
      this.index = new TreeMap();
    }
  }
  
  public void add(int partId, K key, V value)
  {
    I indexVal = this.accessor.getField(value);
    if (indexVal != null) {
      synchronized (this.index)
      {
        List<WAIndex<I, K, V>.Entry> entries = (List)this.index.get(indexVal);
        if (entries == null) {
          entries = new ArrayList();
        }
        synchronized (entries)
        {
          entries.add(new Entry(partId, key, value));
        }
        this.index.put(indexVal, entries);
      }
    }
  }
  
  public void remove(K key, V value)
  {
    I indexVal = this.accessor.getField(value);
    if (indexVal != null) {
      synchronized (this.index)
      {
        List<WAIndex<I, K, V>.Entry> entries = (List)this.index.get(indexVal);
        if (entries != null)
        {
          synchronized (entries)
          {
            Iterator<WAIndex<I, K, V>.Entry> it = entries.iterator();
            while (it.hasNext())
            {
              WAIndex<I, K, V>.Entry entry = (Entry)it.next();
              if (key.equals(entry.key))
              {
                it.remove();
                break;
              }
            }
          }
          if (entries.size() == 0) {
            this.index.remove(indexVal);
          }
        }
      }
    }
  }
  
  public Map<K, V> getEqual(I indexVal)
  {
    Map<K, V> res = new HashMap();
    
    List<WAIndex<I, K, V>.Entry> entries = (List)this.index.get(indexVal);
    if (entries != null) {
      synchronized (entries)
      {
        for (WAIndex<I, K, V>.Entry entry : entries) {
          if (this.pm.isLocalPartitionById(entry.partId, 0)) {
            res.put(entry.key, entry.value);
          }
        }
      }
    }
    return res;
  }
  
  public int size()
  {
    return this.index.size();
  }
  
  public Map<K, V> getRange(I startVal, I endVal)
  {
    if (this.type != Type.tree) {
      throw new RuntimeException("Range index queries only possible on tree indexes");
    }
    Map<K, V> res = new LinkedHashMap();
    if (this.index.isEmpty()) {
      return res;
    }
    TreeMap<I, List<WAIndex<I, K, V>.Entry>> tree = (TreeMap)this.index;
    if (startVal == null) {
      startVal = tree.firstKey();
    } else {
      startVal = tree.ceilingKey(startVal);
    }
    if (endVal == null) {
      endVal = tree.lastKey();
    } else {
      endVal = tree.floorKey(endVal);
    }
    if ((startVal == null) || (endVal == null)) {
      return res;
    }
    I hashVal = startVal;
    boolean done = false;
    while (!done)
    {
      List<WAIndex<I, K, V>.Entry> entries = (List)tree.get(hashVal);
      if (entries != null) {
        synchronized (entries)
        {
          for (WAIndex<I, K, V>.Entry entry : entries) {
            if (this.pm.isLocalPartitionById(entry.partId, 0)) {
              res.put(entry.key, entry.value);
            }
          }
        }
      }
      if (endVal.equals(hashVal))
      {
        done = true;
      }
      else
      {
        hashVal = tree.higherKey(hashVal);
        if (hashVal == null) {
          done = true;
        }
      }
    }
    return res;
  }
  
  public void clear()
  {
    this.index.clear();
  }
  
  public static abstract interface FieldAccessor<I, V>
  {
    public abstract I getField(V paramV);
  }
}
