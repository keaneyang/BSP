 package com.bloom.messaging;
 
 import com.bloom.ser.KryoSingleton;
import com.bloom.uuid.UUID;
import com.bloom.messaging.ReceiverInfo;
 
 
 public abstract class Sender
 {
   private final UUID serverID;
   private final ReceiverInfo info;
   private final TransportMechanism mechanism;
   private final KryoSingleton serializer;
   
   public Sender(UUID serverID, ReceiverInfo info, TransportMechanism mechansim)
   {
     this.serverID = serverID;
     this.info = info;
     this.mechanism = mechansim;
     this.serializer = KryoSingleton.get();
   }
   
   protected Sender()
   {
     this.serverID = null;
     this.info = null;
     this.mechanism = null;
     this.serializer = null;
   }
 
   public ReceiverInfo getInfo()
   {
     return this.info;
   }
 
   public UUID getServerID()
   {
     return this.serverID;
   }
   
 
   public KryoSingleton getSerializer()
   {
     return this.serializer;
   }
   
   public TransportMechanism getMechansim()
   {
     return this.mechanism;
   }
   
   public abstract void start()
     throws InterruptedException;
   
   public abstract void stop();
   
   public abstract boolean send(Object paramObject);
   
   public abstract boolean isFull();
 }

