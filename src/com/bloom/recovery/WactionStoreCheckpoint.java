package com.bloom.recovery;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.log4j.Logger;

import com.bloom.recovery.Path;
import com.bloom.recovery.SourcePosition;

public class WactionStoreCheckpoint
  implements Serializable
{
  private static final long serialVersionUID = -1850611944670314876L;
  private static Logger logger = Logger.getLogger(WactionStoreCheckpoint.class);
  public String id = null;
  public Path.ItemList pathItems;
  public SourcePosition sourcePosition;
  public Timestamp updated;
  
  private WactionStoreCheckpoint()
  {
    this.pathItems = null;
    this.sourcePosition = null;
  }
  
  public static String getHash(Path.ItemList items)
  {
    String retStr = null;
    try
    {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(items.getBytes());
      retStr = DatatypeConverter.printHexBinary(digest);
    }
    catch (NoSuchAlgorithmException e)
    {
      logger.error("Unable to store checkpoint");
    }
    return retStr;
  }
  
  public static String getHash(String wActionStoreName, Long checkpointSequence, Path.ItemList items)
  {
    String retStr = null;
    try
    {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(wActionStoreName.getBytes());
      md.update(checkpointSequence.toString().getBytes());
      md.update(items.getBytes());
      byte[] digest = md.digest();
      retStr = DatatypeConverter.printHexBinary(digest);
    }
    catch (NoSuchAlgorithmException e)
    {
      logger.error("Unable to store checkpoint");
    }
    return retStr;
  }
  
  public WactionStoreCheckpoint(Path.ItemList pathUuids, SourcePosition sourcePosition)
  {
    this.pathItems = pathUuids;
    this.sourcePosition = sourcePosition;
  }
  
  public String getIdString()
  {
    if (this.id == null) {
      this.id = getHash(this.pathItems);
    }
    return this.id;
  }
  
  public void setIdString(String id)
  {
    this.id = id;
  }
  
  public String getPathItemsAsString()
  {
    return this.pathItems.toString();
  }
  
  public void setPathItemsAsString(String st)
  {
    this.pathItems = Path.ItemList.fromString(st);
  }
  
  public boolean equals(Object obj)
  {
    if (!(obj instanceof WactionStoreCheckpoint)) {
      return false;
    }
    WactionStoreCheckpoint that = (WactionStoreCheckpoint)obj;
    if (this.pathItems.size() != that.pathItems.size()) {
      return false;
    }
    for (int i = 0; i < this.pathItems.size(); i++) {
      if (!this.pathItems.get(i).equals(that.pathItems.get(i))) {
        return false;
      }
    }
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
    return "[" + this.pathItems.toString() + ", " + this.sourcePosition + "]";
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
