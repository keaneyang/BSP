package com.bloom.runtime.monitor;

import com.bloom.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.distribution.Partitionable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MonitorData
  implements Serializable, KryoSerializable
{
  private static final long serialVersionUID = 3204834325566246466L;
  public static int HISTORY_EXPIRE_TIME = 300000;
  Key key;
  Map<String, Object> values;
  List<MonitorEvent> history;
  public MonitorData() {}
  
  public static class Key
    implements Serializable, KryoSerializable, Partitionable
  {
    private static final long serialVersionUID = -8753566499697728394L;
    UUID entityID;
    UUID serverID;
    int hash;
    
    public Key() {}
    
    public Key(UUID entityID, UUID serverID)
    {
      this.entityID = entityID;
      this.serverID = serverID;
      this.hash = Objects.hash(new Object[] { serverID, entityID });
    }
    
    public void write(Kryo kryo, Output output)
    {
      this.entityID.write(kryo, output);
      this.serverID.write(kryo, output);
      output.writeInt(this.hash);
    }
    
    public void read(Kryo kryo, Input input)
    {
      this.entityID = new UUID();
      this.entityID.read(kryo, input);
      this.serverID = new UUID();
      this.serverID.read(kryo, input);
      this.hash = input.readInt();
    }
    
    public int hashCode()
    {
      return this.hash;
    }
    
    public boolean equals(Object obj)
    {
      if ((obj instanceof Key))
      {
        Key other = (Key)obj;
        return (this.entityID.equals(other.entityID)) && (this.serverID.equals(other.serverID));
      }
      return false;
    }
    
    public String toString()
    {
      return "{ entityID: " + this.entityID.getUUIDString() + ", serverID: " + this.serverID.getUUIDString() + " }";
    }
    
    public boolean usePartitionId()
    {
      return false;
    }
    
    public Object getPartitionKey()
    {
      return this.entityID;
    }
    
    public int getPartitionId()
    {
      return 0;
    }
  }
  
  public MonitorData(UUID entityID, UUID serverID)
  {
    this.key = new Key(entityID, serverID);
    this.values = new HashMap();
    this.history = new ArrayList();
  }
  
  public Key getKey()
  {
    return this.key;
  }
  
  public void addValues(Map<String, Object> vAdd)
  {
    synchronized (this.values)
    {
      this.values.putAll(vAdd);
    }
  }
  
  public Map<String, Object> getValues()
  {
    Map<String, Object> vRet = new HashMap();
    synchronized (this.values)
    {
      vRet.putAll(this.values);
    }
    return vRet;
  }
  
  public void addSingleValue(String k, Object value)
  {
    this.values.put(k, value);
  }
  
  public void addSingleEvent(MonitorEvent event)
  {
    this.history.add(event);
  }
  
  public void addHistory(List<MonitorEvent> hAdd)
  {
    if (!hAdd.isEmpty()) {
      synchronized (this.history)
      {
        this.history.addAll(hAdd);
        Collections.sort(this.history, new Comparator()
        {
        	
          public int compare(MonitorEvent o1, MonitorEvent o2)
          {
            return Long.compare(o1.timeStamp, o2.timeStamp);
          }
        });
        if (!this.history.isEmpty())
        {
          MonitorEvent lastEvent = (MonitorEvent)this.history.get(this.history.size() - 1);
          long historyCutoff = lastEvent.timeStamp - HISTORY_EXPIRE_TIME;
          MonitorEvent candidate = (MonitorEvent)this.history.get(0);
          while ((candidate != null) && (candidate.timeStamp < historyCutoff))
          {
            this.history.remove(0);
            if (!this.history.isEmpty()) {
              candidate = (MonitorEvent)this.history.get(0);
            } else {
              candidate = null;
            }
          }
        }
      }
    }
  }
  
  public void addLatest(MonitorData latest)
  {
    addValues(latest.values);
    addHistory(latest.history);
  }
  
  public List<MonitorEvent> getHistory()
  {
    List<MonitorEvent> hRet = new ArrayList();
    synchronized (this.history)
    {
      hRet.addAll(this.history);
    }
    return hRet;
  }
  
  public void write(Kryo kryo, Output output)
  {
    this.key.write(kryo, output);
    synchronized (this.values)
    {
      kryo.writeObject(output, this.values);
    }
    synchronized (this.history)
    {
      kryo.writeObject(output, this.history);
    }
  }
  
  public void read(Kryo kryo, Input input)
  {
    this.key = new Key();
    this.key.read(kryo, input);
    this.values = ((Map)kryo.readObject(input, HashMap.class));
    this.history = ((List)kryo.readObject(input, ArrayList.class));
  }
}
