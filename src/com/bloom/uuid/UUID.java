 package com.bloom.uuid;
 
 import com.esotericsoftware.kryo.Kryo;
 import com.esotericsoftware.kryo.KryoSerializable;
 import com.esotericsoftware.kryo.io.Input;
 import com.esotericsoftware.kryo.io.Output;
 import com.fasterxml.jackson.annotation.JsonIgnore;
import com.bloom.uuid.Hex;
import com.bloom.uuid.UUID;
import com.bloom.uuid.UUIDGen;

import flexjson.JSON;
 import java.io.IOException;
 import java.io.Serializable;
 import java.nio.ByteBuffer;
 import java.security.MessageDigest;
 import java.security.NoSuchAlgorithmException;
 import javax.jdo.annotations.Persistent;
 import org.apache.log4j.Logger;
 
 
 
 public class UUID
   implements Comparable<UUID>, KryoSerializable, Serializable, Cloneable
 {
   private static Logger logger = Logger.getLogger(UUID.class);
   
   static final long serialVersionUID = 7435962790062944603L;
 
   @JSON(include=false)
   @JsonIgnore
   @Persistent
   public long time;
   
   @JSON(include=false)
   @JsonIgnore
   @Persistent
   public long clockSeqAndNode;
 
   public UUID() {}
  
   public UUID(long currentTimeMillis)
   {
     this(UUIDGen.createTime(currentTimeMillis), UUIDGen.getClockSeqAndNode());
   }

 
   public UUID(long time, long clockSeqAndNode)
   {
     this.time = time;
     this.clockSeqAndNode = clockSeqAndNode;
   }
 
 
   public UUID(UUID u)
   {
     this(u.time, u.clockSeqAndNode);
   }
   
   public UUID(String uidstr) {
     setUUIDString(uidstr);
   }
   
 
 
   public UUID(CharSequence s)
   {
     this(Hex.parseLong(s.subSequence(0, 18)), Hex.parseLong(s.subSequence(19, 36)));
   }
   
 
 
   public int compareTo(UUID t)
   {
     if (this == t) {
       return 0;
     }
     if (this.time > t.time) {
       return 1;
     }
     if (this.time < t.time) {
       return -1;
     }
     if (this.clockSeqAndNode > t.clockSeqAndNode) {
       return 1;
     }
     if (this.clockSeqAndNode < t.clockSeqAndNode) {
       return -1;
     }
     return 0;
   }
   
   private String stringRep = null;
   
 
 
 
 
 
 
   public final String toString()
   {
     if (this.stringRep == null) {
       this.stringRep = toAppendable(null).toString();
     }
     return this.stringRep;
   }
   
 
 
 
 
 
 
 
   public StringBuffer toStringBuffer(StringBuffer in)
   {
     StringBuffer out = in;
     if (out == null) {
       out = new StringBuffer(36);
     }
     else {
       out.ensureCapacity(out.length() + 36);
     }
     return (StringBuffer)toAppendable(out);
   }
   
 
 
 
 
 
 
 
 
 
 
   public Appendable toAppendable(Appendable a)
   {
     Appendable out = a;
     if (out == null) {
       out = new StringBuilder(36);
     }
     try {
       Hex.append(out, (int)(this.time >> 32)).append('-');
       Hex.append(out, (short)(int)(this.time >> 16)).append('-');
       Hex.append(out, (short)(int)this.time).append('-');
       Hex.append(out, (short)(int)(this.clockSeqAndNode >> 48)).append('-');
       Hex.append(out, this.clockSeqAndNode, 12);
     }
     catch (IOException ex) {}
     
 
     return out;
   }
   
 
 
 
 
 
 
 
 
   public int hashCode()
   {
     return (int)(this.time >> 32 ^ this.time ^ this.clockSeqAndNode >> 32 ^ this.clockSeqAndNode);
   }
   
 
 
 
 
   public Object clone()
   {
     try
     {
       return super.clone();
     }
     catch (CloneNotSupportedException ex) {}
     
     return null;
   }
   
 
 
 
 
 
   public final long getTime()
   {
     return this.time;
   }
   
 
 
   @JSON(include=false)
   @JsonIgnore
   public final long getSystemTime()
   {
     return (this.time - 122192928000000000L) / 10000L;
   }
   
 
 
 
   public final long getClockSeqAndNode()
   {
     return this.clockSeqAndNode;
   }
   
 
 
 
 
 
 
 
 
   public boolean equals(Object obj)
   {
     if (this == obj) {
       return true;
     }
     if (!(obj instanceof UUID)) {
       return false;
     }
     UUID that = (UUID)obj;
     return (this.time == that.time) && (this.clockSeqAndNode == that.clockSeqAndNode);
   }
   
 
 
 
 
 
 
 
 
 
   public static UUID nilUUID()
   {
     return new UUID(0L, 0L);
   }
   
   public void read(Kryo kr, Input input)
   {
     this.time = input.readLong();
     this.clockSeqAndNode = input.readLong();
   }
   
   public void write(Kryo kr, Output output) {
     output.writeLong(this.time);
     output.writeLong(this.clockSeqAndNode);
   }
   
 
 
 
   public String getUUIDString()
   {
     return toString();
   }
   
   public void setUUIDString(String uuid) {
     this.time = Hex.parseLong(uuid.subSequence(0, 18));
     this.clockSeqAndNode = Hex.parseLong(uuid.subSequence(19, 36));
   }
   
   public byte[] toBytes() {
     ByteBuffer buffer = ByteBuffer.allocate(16);
     buffer.putLong(this.clockSeqAndNode);
     buffer.putLong(this.time);
     return buffer.array();
   }
   
   public byte[] toEightBytes() { ByteBuffer buffer = ByteBuffer.allocate(8);
     buffer.putLong(this.clockSeqAndNode ^ this.time);
     return buffer.array();
   }
   
 
 
 
 
 
 
   public UUID combine(UUID that)
   {
     byte[] arbitraryBytes = ByteBuffer.allocate(256).putLong(this.time).putLong(this.clockSeqAndNode).putLong(that.time).putLong(that.clockSeqAndNode).array();
     
 
 
 
 
     return combine(arbitraryBytes);
   }
   
 
 
 
 
 
 
   public UUID combine(String arbitraryString)
   {
     if (arbitraryString == null) {
       arbitraryString = "";
     }
     byte[] stringBytes = arbitraryString.getBytes();
     byte[] arbitraryBytes = ByteBuffer.allocate(128 + stringBytes.length).putLong(this.time).putLong(this.clockSeqAndNode).put(stringBytes, 0, stringBytes.length).array();
     
 
 
 
     return combine(arbitraryBytes);
   }
   
 
 
 
 
 
 
   private UUID combine(byte[] arbitraryBytes)
   {
     try
     {
       MessageDigest md = MessageDigest.getInstance("MD5");
       byte[] md5bytes = md.digest(arbitraryBytes);
       if (md5bytes.length != 16) {
         logger.error("MD5 algorithm returned " + md5bytes.length + " bytes (expected 16)");
         return null;
       }
       long time = (md5bytes[0] << 56) + ((md5bytes[1] & 0xFF) << 48) + ((md5bytes[2] & 0xFF) << 40) + ((md5bytes[3] & 0xFF) << 32) + ((md5bytes[4] & 0xFF) << 24) + ((md5bytes[5] & 0xFF) << 16) + ((md5bytes[6] & 0xFF) << 8) + ((md5bytes[7] & 0xFF) << 0);
       
 
 
 
 
 
 
 
       long clockSeqAndNode = (md5bytes[0] << 120) + ((md5bytes[1] & 0xFF) << 112) + ((md5bytes[2] & 0xFF) << 104) + ((md5bytes[3] & 0xFF) << 96) + ((md5bytes[4] & 0xFF) << 88) + ((md5bytes[5] & 0xFF) << 80) + ((md5bytes[6] & 0xFF) << 72) + ((md5bytes[7] & 0xFF) << 64);
       
 
 
 
 
 
 
 
       return new UUID(time, clockSeqAndNode);
     }
     catch (NoSuchAlgorithmException e) {
       logger.error(e); }
     return null;
   }
   
   public static UUID genCurTimeUUID()
   {
     return new UUID(System.currentTimeMillis());
   }
 }

