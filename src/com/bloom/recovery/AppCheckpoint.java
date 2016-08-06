package com.bloom.recovery;

import com.bloom.uuid.UUID;
import com.bloom.recovery.Path;
import com.bloom.recovery.SourcePosition;

import java.io.Serializable;
import java.sql.Timestamp;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class AppCheckpoint
  implements Serializable
{
  private static final long serialVersionUID = -1850611944670314876L;
  public int id = 0;
  public final String flowUuid;
  public Path.ItemList pathItems;
  public final SourcePosition sourcePosition;
  public Timestamp updated;
  
  private AppCheckpoint()
  {
    this.flowUuid = null;
    this.pathItems = null;
    this.sourcePosition = null;
  }
  
  public AppCheckpoint(UUID flowUuid, Path.ItemList pathItems, SourcePosition sourcePosition)
  {
    this.flowUuid = flowUuid.getUUIDString();
    this.pathItems = pathItems;
    this.sourcePosition = sourcePosition;
  }
  
  public boolean equals(Object obj)
  {
    if (!(obj instanceof AppCheckpoint)) {
      return false;
    }
    AppCheckpoint that = (AppCheckpoint)obj;
    if ((this.flowUuid == null ? 1 : 0) != (that.flowUuid == null ? 1 : 0)) {}
    if (!this.flowUuid.equals(that.flowUuid)) {
      return false;
    }
    if ((this.pathItems == null ? 1 : 0) != (that.pathItems == null ? 1 : 0)) {}
    if (!this.pathItems.equals(that.pathItems)) {
      return false;
    }
    if ((this.sourcePosition == null ? 1 : 0) != (that.sourcePosition == null ? 1 : 0)) {}
    if (this.sourcePosition.compareTo(that.sourcePosition) != 0) {
      return false;
    }
    return true;
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(toString()).toHashCode();
  }
  
  public String toString()
  {
    return "[" + this.flowUuid + ", " + this.pathItems + ", " + this.sourcePosition + "]";
  }
  
  public String getPathItemsAsString()
  {
    return this.pathItems.toString();
  }
  
  public void setPathItemsAsString(String st)
  {
    this.pathItems = Path.ItemList.fromString(st);
  }
  
  public void prePersist()
  {
    this.updated = new Timestamp(System.currentTimeMillis());
  }
  
  public void preUpdate()
  {
    this.updated = new Timestamp(System.currentTimeMillis());
  }
}
