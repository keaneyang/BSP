package com.bloom.ser;

import com.bloom.cache.CacheAccessor;
import com.bloom.cache.CacheAccessor.CacheEntry;
import com.bloom.cache.CacheAccessor.CacheIterator;
import com.bloom.cache.CacheAccessor.Clear;
import com.bloom.cache.CacheAccessor.ContainsKey;
import com.bloom.cache.CacheAccessor.Get;
import com.bloom.cache.CacheAccessor.GetAll;
import com.bloom.cache.CacheAccessor.GetAndPut;
import com.bloom.cache.CacheAccessor.GetAndRemove;
import com.bloom.cache.CacheAccessor.GetAndReplace;
import com.bloom.cache.CacheAccessor.GetCacheInfo;
import com.bloom.cache.CacheAccessor.GetKeys;
import com.bloom.cache.CacheAccessor.Invoke;
import com.bloom.cache.CacheAccessor.Put;
import com.bloom.cache.CacheAccessor.PutIfAbsent;
import com.bloom.cache.CacheAccessor.Query;
import com.bloom.cache.CacheAccessor.QueryStats;
import com.bloom.cache.CacheAccessor.Remove;
import com.bloom.cache.CacheAccessor.RemoveWithValue;
import com.bloom.cache.CacheAccessor.Replace;
import com.bloom.cache.CacheAccessor.SimpleQuery;
import com.bloom.cache.CacheAccessor.Size;
import com.bloom.classloading.WALoader;
import com.bloom.distribution.ReloadableClassResolver;
import com.bloom.distribution.WAQueue;
import com.bloom.event.EventJson;
import com.bloom.historicalcache.Cache;
import com.bloom.historicalcache.QueryManager;
import com.bloom.historicalcache.QueryManager.InsertCacheRecord;
import com.bloom.jmqmessaging.Executor;
import com.bloom.jmqmessaging.Executor.ExecutionRequest;
import com.bloom.jmqmessaging.Executor.ExecutionResponse;
import com.bloom.proc.events.AlertEvent;
import com.bloom.proc.events.AvroEvent;
import com.bloom.proc.events.ClusterTestEvent;
import com.bloom.proc.events.CollectdEvent;
import com.bloom.proc.events.DynamicEvent;
import com.bloom.proc.events.JsonNodeEvent;
import com.bloom.proc.events.Log4JEvent;
import com.bloom.proc.events.ObjectArrayEvent;
import com.bloom.proc.events.ShowStreamEvent;
import com.bloom.proc.events.StringArrayEvent;
import com.bloom.proc.events.WAQueueEvent;
import com.bloom.proc.events.WindowsLogEvent;
import com.bloom.runtime.DistributedSubscriber;
import com.bloom.runtime.ExceptionEvent;
import com.bloom.runtime.StreamTaskEvent;
import com.bloom.runtime.channels.ZMQChannel;
import com.bloom.runtime.containers.TaskEvent;
import com.bloom.runtime.monitor.MonitorBatchEvent;
import com.bloom.runtime.monitor.MonitorEvent;
import com.bloom.waction.Waction;
import com.bloom.waction.WactionContext;
import com.bloom.waction.WactionKey;
import com.esotericsoftware.kryo.ClassResolver;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ReferenceResolver;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.runtime.StreamEvent;
import com.bloom.ser.CommonObjectSpace;

import java.io.IOException;
import java.util.Random;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.persistence.criteria.From;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
import org.objenesis.strategy.StdInstantiatorStrategy;

public class KryoSingleton
{
  public static final int PLATFORM_REGISTRATION_START_INDEX = 150;
  public static final int PLATFORM_REGISTRATION_MAX_SIZE = 200;
  private static Logger logger = Logger.getLogger(KryoSingleton.class);
  private static ThreadLocal<KryoThreadLocal> threadKpes;
  private static KryoSingleton INSTANCE = new KryoSingleton();
  private static ReloadableClassResolver resolver;
  private static KryoThreadLocal defaultKryo;
  static final int nrOfEvents = 1;
  static final String characters = "abcdefghijklmnopqrstuvwxyz";
  
  public static class ThreadSafeKryo
    extends Kryo
  {
    public Serializer getDefaultSerializer(Class type)
    {
      Serializer s = new KryoSingleton.ThreadSafeSerializer(type);
      
      return s;
    }
    
    public Serializer getSuperDefaultSerializer(Class type)
    {
      return super.getDefaultSerializer(type);
    }
    
    public ThreadSafeKryo() {}
    
    public ThreadSafeKryo(ClassResolver classResolver, ReferenceResolver referenceResolver)
    {
      super(referenceResolver);
    }
    
    public ThreadSafeKryo(ReferenceResolver referenceResolver)
    {
      super();
    }
  }
  
  public static class ThreadSafeSerializer
    extends Serializer
  {
    final Class type;
    
    public ThreadSafeSerializer(Class type)
    {
      this.type = type;
    }
    
    private final ThreadLocal<Serializer> threadSerializers = new ThreadLocal();
    
    private Serializer getThreadSerializer(KryoSingleton.ThreadSafeKryo kryo)
    {
      Serializer s = (Serializer)this.threadSerializers.get();
      if (s == null)
      {
        s = kryo.getSuperDefaultSerializer(this.type);
        this.threadSerializers.set(s);
      }
      return s;
    }
    
    public void write(Kryo kryo, Output output, Object object)
    {
      Serializer s = getThreadSerializer((KryoSingleton.ThreadSafeKryo)kryo);
      s.write(kryo, output, object);
    }
    
    public Object read(Kryo kryo, Input input, Class type)
    {
      Serializer s = getThreadSerializer((KryoSingleton.ThreadSafeKryo)kryo);
      return s.read(kryo, input, type);
    }
  }
  
  public static class KryoThreadLocal
  {
    public KryoSingleton.ThreadSafeKryo kryo;
    public Input input;
    public Output output;
    public boolean isUsed = false;
    public Cipher cipherEncrypt = null;
    public Cipher cipherDecrypt = null;
    public Object encryptLock = new Object();
    public Object decryptLock = new Object();
    private static final byte[] AES_KEYS = "GunturuMirapakai".getBytes();
    private static final byte[] IV_BYTES = "MarathaSrikhanda".getBytes();
    private static final SecretKeySpec keySpec = new SecretKeySpec(AES_KEYS, "AES");
    private static final IvParameterSpec ivSpec = new IvParameterSpec(IV_BYTES);
    private static final String ENCRY_STD = "AES/CBC/PKCS5Padding";
    private static Logger logger = Logger.getLogger(KryoThreadLocal.class);
    
    public KryoThreadLocal(ReloadableClassResolver resolver)
    {
      this.kryo = new KryoSingleton.ThreadSafeKryo(resolver, null);
      this.kryo.setReferences(false);
      this.kryo.setRegistrationRequired(false);
      this.kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
      this.kryo.setClassLoader(WALoader.get());
      
      this.output = new Output(8192, -1);
      this.input = new Input();
    }
    
    public void initializeCiphers()
    {
      try
      {
        this.cipherEncrypt = Cipher.getInstance("AES/CBC/PKCS5Padding");
        this.cipherEncrypt.init(1, keySpec, ivSpec);
        
        this.cipherDecrypt = Cipher.getInstance("AES/CBC/PKCS5Padding");
        this.cipherDecrypt.init(2, keySpec, ivSpec);
      }
      catch (Exception ex)
      {
        logger.error("Failed to create cipher for AES/CBC/PKCS5Padding", ex);
        throw new RuntimeException("Failed to create cipher for encryption.", ex);
      }
    }
    
    public byte[] getEncryptedBytes(byte[] bytes)
    {
      if ((bytes == null) || (bytes.length == 0)) {
        return bytes;
      }
      synchronized (this.encryptLock)
      {
        try
        {
          if (logger.isTraceEnabled()) {
            logger.trace("encrypted bytes : " + new String(this.cipherEncrypt.doFinal(bytes)));
          }
          return this.cipherEncrypt.doFinal(bytes);
        }
        catch (Exception ex)
        {
          logger.error("Exception while encrypting data ", ex);
          throw new RuntimeException("Exception while encrypting data ", ex);
        }
      }
    }
    
    public byte[] getDecryptedBytes(byte[] bytes)
    {
      if ((bytes == null) || (bytes.length == 0)) {
        return bytes;
      }
      synchronized (this.decryptLock)
      {
        try
        {
          if (logger.isTraceEnabled()) {
            logger.trace("decrypted bytes : " + new String(this.cipherDecrypt.doFinal(bytes)));
          }
          return this.cipherDecrypt.doFinal(bytes);
        }
        catch (Exception ex)
        {
          logger.error("Exception while decrypting data ", ex);
          throw new RuntimeException("Exception while decrypting data ", ex);
        }
      }
    }
    
    public String toString()
    {
      try
      {
        return "input.size:" + this.input.available() + " , output:" + this.output.total() + ", kryo:" + (this.kryo != null);
      }
      catch (IOException e) {}
      return "";
    }
  }
  
  public static synchronized KryoSingleton get()
  {
    return INSTANCE;
  }
  
  private KryoSingleton()
  {
    if (INSTANCE != null) {
      throw new RuntimeException("Not a valid Initialization of Kryo");
    }
    threadKpes = new ThreadLocal()
    {
      protected KryoSingleton.KryoThreadLocal initialValue()
      {
        KryoSingleton.KryoThreadLocal ktl = new KryoSingleton.KryoThreadLocal(KryoSingleton.resolver);
        ktl.initializeCiphers();
        return ktl;
      }
    };
    resolver = new ReloadableClassResolver();
    defaultKryo = (KryoThreadLocal)threadKpes.get();
    synchronized (defaultKryo)
    {
      initKryo(defaultKryo.kryo);
    }
  }
  
  public void removeClassRegistration(Class<?> clazz)
  {
    synchronized (defaultKryo)
    {
      ((ReloadableClassResolver)defaultKryo.kryo.getClassResolver()).removeClassRegistration(clazz);
    }
  }
  
  public void addClassRegistration(Class<?> clazz, int id)
  {
    synchronized (defaultKryo)
    {
      if (id != -1) {
        defaultKryo.kryo.register(clazz, id);
      } else {
        defaultKryo.kryo.register(clazz);
      }
    }
  }
  
  public void dumpClassIds()
  {
    synchronized (defaultKryo)
    {
      ((ReloadableClassResolver)defaultKryo.kryo.getClassResolver()).dumpClassIds();
    }
  }
  
  public boolean isClassRegistered(Class<?> clazz)
  {
    synchronized (defaultKryo)
    {
      if (defaultKryo.kryo.getClassResolver().getRegistration(clazz) != null) {
        return true;
      }
      return false;
    }
  }
  
  public void initKryo(Kryo kryo)
  {
    CommonObjectSpace.registerClasses(kryo);
    
    int index = 150;
    kryo.register(Cache.class, index++);
    kryo.register(WAQueue.class, index++);
    kryo.register(DistributedSubscriber.class, index++);
    kryo.register(ZMQChannel.class, index++);
    kryo.register(TaskEvent.class, index++);
    kryo.register(StreamEvent.class, index++);
    kryo.register(StreamTaskEvent.class, index++);
    kryo.register(WAQueueEvent.class, index++);
    kryo.register(ShowStreamEvent.class, index++);
    kryo.register(From.class, index++);
    kryo.register(AlertEvent.class, index++);
    kryo.register(ClusterTestEvent.class, index++);
    kryo.register(JsonNodeEvent.class, index++);
    kryo.register(Log4JEvent.class, index++);
    kryo.register(ObjectArrayEvent.class);
    kryo.register(StringArrayEvent.class, index++);
    kryo.register(ClusterTestEvent.class, index++);
    kryo.register(DynamicEvent.class, index++);
    kryo.register(Waction.class, index++);
    kryo.register(WactionKey.class, index++);
    kryo.register(WactionContext.class, index++);
    kryo.register(EventJson.class, index++);
    kryo.register(ExceptionEvent.class, index++);
    kryo.register(MonitorEvent.class, index++);
    kryo.register(MonitorBatchEvent.class, index++);
    kryo.register(WindowsLogEvent.class, index++);
    kryo.register(CollectdEvent.class, index++);
    kryo.register(Executor.ExecutionRequest.class, index++);
    kryo.register(Executor.ExecutionResponse.class, index++);
    kryo.register(CacheAccessor.CacheEntry.class, index++);
    kryo.register(CacheAccessor.CacheIterator.class, index++);
    kryo.register(CacheAccessor.Clear.class, index++);
    kryo.register(CacheAccessor.ContainsKey.class, index++);
    kryo.register(CacheAccessor.Get.class, index++);
    kryo.register(CacheAccessor.GetAll.class, index++);
    kryo.register(CacheAccessor.GetAndPut.class, index++);
    kryo.register(CacheAccessor.GetAndRemove.class, index++);
    kryo.register(CacheAccessor.GetAndReplace.class, index++);
    kryo.register(CacheAccessor.GetCacheInfo.class, index++);
    kryo.register(CacheAccessor.ContainsKey.class, index++);
    kryo.register(CacheAccessor.GetKeys.class, index++);
    kryo.register(CacheAccessor.Invoke.class, index++);
    kryo.register(CacheAccessor.Put.class, index++);
    kryo.register(CacheAccessor.PutIfAbsent.class, index++);
    kryo.register(CacheAccessor.Query.class, index++);
    kryo.register(CacheAccessor.QueryStats.class, index++);
    kryo.register(CacheAccessor.Remove.class, index++);
    kryo.register(CacheAccessor.RemoveWithValue.class, index++);
    kryo.register(CacheAccessor.Replace.class, index++);
    kryo.register(CacheAccessor.SimpleQuery.class, index++);
    kryo.register(CacheAccessor.Size.class, index++);
    kryo.register(QueryManager.InsertCacheRecord.class, index++);
    kryo.register(AvroEvent.class, index++);
    kryo.register(GenericRecord.class, index++);
    kryo.register(GenericData.Record.class, new AbstractAvroSerializer(), index++);
  }
  
  public static Object read(byte[] bytes, boolean isEncrypted)
  {
    KryoThreadLocal entry = (KryoThreadLocal)threadKpes.get();
    entry.isUsed = true;
    if (isEncrypted) {
      bytes = entry.getDecryptedBytes(bytes);
    }
    entry.input.setBuffer(bytes);
    
    Object currentReadResult = entry.kryo.readClassAndObject(entry.input);
    
    entry.isUsed = false;
    return currentReadResult;
  }
  
  public static byte[] write(Object obj, boolean isEncrypted)
  {
    KryoThreadLocal entry = (KryoThreadLocal)threadKpes.get();
    entry.isUsed = true;
    byte[] currentBytes = null;
    entry.output.clear();
    
    entry.kryo.writeClassAndObject(entry.output, obj);
    
    currentBytes = entry.output.toBytes();
    if (isEncrypted) {
      currentBytes = entry.getEncryptedBytes(currentBytes);
    }
    entry.isUsed = false;
    return currentBytes;
  }
  
  public static String generateString(int nrOfBytes)
  {
    Random random = new Random();
    char[] text = new char[nrOfBytes];
    for (int i = 0; i < nrOfBytes; i++) {
      text[i] = "abcdefghijklmnopqrstuvwxyz".charAt(random.nextInt("abcdefghijklmnopqrstuvwxyz".length()));
    }
    return new String(text);
  }
}
