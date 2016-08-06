package com.bloom.waction;

import com.bloom.uuid.UUID;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.bloom.distribution.Partitionable;

import flexjson.JSON;
import java.io.Serializable;
import javax.jdo.annotations.Persistent;
import org.apache.log4j.Logger;

public class WactionKey
  implements Partitionable, Serializable, Comparable<WactionKey>
{
  private static Logger logger = Logger.getLogger(WactionKey.class);
  private static final long serialVersionUID = 250311883592330044L;
  @Persistent
  public UUID id;
  @Persistent
  public Object key;
  
  public WactionKey() {}
  
  public WactionKey(UUID id, Object key)
  {
    this.id = id;
    this.key = key;
  }
  
  public void setId(String id)
  {
    this.id = new UUID(id);
  }
  
  public String getId()
  {
    return this.id.toString();
  }
  
  public void setWactionKey(String wactionKeyStr)
  {
    if (wactionKeyStr == null) {
      return;
    }
    String[] idAndKey = wactionKeyStr.split(":", 2);
    this.id = new UUID(idAndKey[0]);
    this.key = (idAndKey.length == 2 ? idAndKey[1] : this.id);
  }
  
  @JSON(include=false)
  @JsonIgnore
  public String getWactionKeyStr()
  {
    if ((this.id != null) && (this.key != null)) {
      return this.id.getUUIDString() + ":" + this.key;
    }
    return null;
  }
  
  public boolean usePartitionId()
  {
    return false;
  }
  
  @JSON(include=false)
  @JsonIgnore
  public Object getPartitionKey()
  {
    return this.key;
  }
  
  @JSON(include=false)
  @JsonIgnore
  public int getPartitionId()
  {
    return 0;
  }
  
  public int hashCode()
  {
    return this.id.hashCode();
  }
  
  public boolean equals(Object obj)
  {
    if ((obj instanceof WactionKey)) {
      return this.id.equals(((WactionKey)obj).id);
    }
    return false;
  }
  
  public String toString()
  {
    return "{\"id\":\"" + this.id + "\",\"key\":\"" + this.key + "\"}";
  }
  
  public int compareTo(WactionKey o)
  {
    return this.id.compareTo(o.id);
  }
}
