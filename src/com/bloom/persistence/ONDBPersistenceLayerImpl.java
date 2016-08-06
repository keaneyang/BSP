package com.bloom.persistence;

import com.bloom.classloading.WALoader;
import com.bloom.intf.PersistenceLayer;
import com.bloom.intf.PersistenceLayer.Range;
import com.bloom.waction.Waction;
import com.bloom.waction.WactionKey;
import com.bloom.recovery.Position;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.CountDownLatch;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;
import oracle.kv.OperationFactory;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

public class ONDBPersistenceLayerImpl
  implements PersistenceLayer
{
  private static Logger logger = Logger.getLogger(ONDBPersistenceLayerImpl.class);
  public Map<String, Object> prop = null;
  public String storeName = null;
  public int MAX_CONN = 0;
  public String recovery_key = null;
  private String userName = null;
  private String password = null;
  private KVStore kvstore = null;
  private String url = null;
  private String kvschema = "kvstore";
  private KVStoreConfig config = null;
  
  public ONDBPersistenceLayerImpl() {}
  
  public ONDBPersistenceLayerImpl(String kvStoreName, Map<String, Object> properties)
  {
    this.storeName = kvStoreName;
    this.recovery_key = (this.storeName + "_recovery");
    this.prop = properties;
  }
  
  public void init()
  {
    if (this.prop == null)
    {
      logger.error("No properties are set. Set required properties for ONDB");
      return;
    }
    if (this.prop.get("NOSQL_PROPERTY") == null)
    {
      logger.error("Set either NOSQL_PROPERTY with ONDB url. e.g. localhost:5000");
      return;
    }
    this.url = ((String)this.prop.get("NOSQL_PROPERTY"));
    if (this.storeName == null)
    {
      logger.error("Set storeName. ");
      return;
    }
    if (this.prop.get("DB_NAME") == null) {
      logger.warn("kvstore database is not set. taking kvstore as default one. ");
    } else {
      this.kvschema = ((String)this.prop.get("DB_NAME"));
    }
    if (this.prop.get("ONDB_USER") != null) {
      this.userName = ((String)this.prop.get("ONDB_USER"));
    }
    if (this.prop.get("ONDB_PASSWD") != null) {
      this.password = ((String)this.prop.get("ONDB_PASSWD"));
    }
    if (this.prop.get("MAX_CONN") != null)
    {
      String mc = (String)this.prop.get("MAX_CONN");
      try
      {
        this.MAX_CONN = Integer.parseInt(mc);
      }
      catch (Exception ex) {}
    }
    this.config = new KVStoreConfig(this.kvschema, new String[] { this.url });
    if (this.kvstore == null) {
      this.kvstore = KVStoreFactory.getStore(this.config);
    }
  }
  
  private KVStore[] getConnectionsForRead(int num)
  {
    if (num < 1) {
      return null;
    }
    KVStore[] kvs = new KVStore[num];
    for (int ii = 0; ii < num; ii++) {
      kvs[ii] = KVStoreFactory.getStore(this.config);
    }
    return kvs;
  }
  
  public void init(String storeName)
  {
    init();
  }
  
  public int delete(Object object)
  {
    throw new NotImplementedException();
  }
  
  public PersistenceLayer.Range[] persist(Object object)
  {
    int numWritten = 0;
    PersistenceLayer.Range[] rangeArray = new PersistenceLayer.Range[1];
    int batchsize = 1000;
    if (object == null) {
      throw new IllegalArgumentException("Cannot persist null object");
    }
    if ((object instanceof List))
    {
      if (logger.isDebugEnabled()) {
        logger.debug("Number of wactions to persist :" + ((List)object).size());
      }
      OperationFactory of = this.kvstore.getOperationFactory();
      ArrayList<Operation> opList = new ArrayList();
      
      Waction trackWacPos = null;
      int ii = 0;
      for (Object o1 : (List)object) {
        if (ii < batchsize)
        {
          Waction waction = (Waction)o1;
          trackWacPos = waction;
          String mapKey = waction.getMapKeyString();
          
          long ts = waction.getWactionTs();
          List<String> minorPath = Arrays.asList(new String[] { mapKey, String.valueOf(ts) });
          Key key = Key.createKey(this.storeName, minorPath);
          
          Value value = Value.createValue(serializeObj(waction));
          opList.add(of.createPut(key, value));
          ii++;
          numWritten++;
        }
        else
        {
          try
          {
            this.kvstore.execute(opList);
          }
          catch (OperationExecutionException|FaultException e)
          {
            logger.error("error doing bulk insert to ONDB database", e);
          }
          ii = 0;
          opList = new ArrayList();
        }
      }
      if (opList.size() > 0) {
        try
        {
          this.kvstore.execute(opList);
        }
        catch (OperationExecutionException|FaultException e)
        {
          logger.error("error doing bulk insert to ONDB database", e);
        }
      }
    }
    else
    {
      insert((Waction)object);
      
      numWritten++;
    }
    rangeArray[0] = new PersistenceLayer.Range(0, numWritten);
    rangeArray[0].setSuccessful(true);
    return rangeArray;
  }
  
  private void insert(Waction waction)
  {
    String mapKey = waction.getMapKeyString();
    
    long ts = waction.getWactionTs();
    List<String> minorPath = Arrays.asList(new String[] { mapKey, String.valueOf(ts) });
    Key key = Key.createKey(this.storeName, minorPath);
    
    Value value = Value.createValue(serializeObj(waction));
    this.kvstore.put(key, value);
  }
  
  private static byte[] serializeObj(Object object)
  {
    bytes = null;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutput out = null;
    try
    {
      out = new ObjectOutputStream(bos);
      out.writeObject(object);
      return bos.toByteArray();
    }
    catch (IOException iex)
    {
      logger.error("exception serializing object:", iex);
    }
    finally
    {
      try
      {
        if (out != null) {
          out.close();
        }
        if (bos != null) {
          bos.close();
        }
      }
      catch (IOException iex) {}
    }
  }
  
  private static Object deSerizlizeObj(byte[] bytes)
  {
    Thread.currentThread().setContextClassLoader(WALoader.get());
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    ObjectInput in = null;
    object = null;
    try
    {
      in = new ObjectInputStream(bis)
      {
        protected Class<?> resolveClass(ObjectStreamClass desc)
          throws IOException, ClassNotFoundException
        {
          try
          {
            return super.resolveClass(desc);
          }
          catch (ClassNotFoundException cx)
          {
            if (ONDBPersistenceLayerImpl.logger.isTraceEnabled()) {
              ONDBPersistenceLayerImpl.logger.trace("** runtime class :" + desc.getName());
            }
          }
          return WALoader.get().loadClass(desc.getName());
        }
      };
      return in.readObject();
    }
    catch (IOException|ClassNotFoundException iex)
    {
      logger.error("exception de-serializing bytes:", iex);
    }
    finally
    {
      try
      {
        if (in != null) {
          in.close();
        }
        if (bis != null) {
          bis.close();
        }
      }
      catch (IOException iex) {}
    }
  }
  
  public void merge(Object object)
  {
    logger.warn("merge on ONDB is not implemented");
  }
  
  public void setStoreName(String storeName, String tableName)
  {
    this.storeName = storeName;
  }
  
  public Object get(Class<?> objectClass, Object objectId)
  {
    return null;
  }
  
  public List<?> runQuery(String query, Map<String, Object> params, Integer maxResults)
  {
    return null;
  }
  
  public Object runNativeQuery(String query)
  {
    return null;
  }
  
  public int executeUpdate(String query, Map<String, Object> params)
  {
    return 0;
  }
  
  public void close()
  {
    if (this.kvstore != null) {
      this.kvstore.close();
    }
  }
  
  public Position getWSPosition(String namespaceName, String wactionStoreName)
  {
    throw new NotImplementedException("This method is not supported for ONDBPersistenceLayerImpl");
  }
  
  public boolean clearWSPosition(String namespaceName, String wactionStoreName)
  {
    throw new NotImplementedException("This method is not supported for ONDBPersistenceLayerImpl");
  }
  
  public <T extends Waction> List<T> getResults(Class<T> objectClass, Map<String, Object> filter, Set<WactionKey> excludeKeys)
  {
    if (logger.isDebugEnabled()) {
      logger.debug("trying to get results from table :" + this.storeName + ", and create objects of type : " + objectClass.getCanonicalName());
    }
    List<T> results = null;
    long startTime = 0L;
    long endTime = 0L;
    String filterKey = null;
    boolean cacheonly = false;
    if (logger.isTraceEnabled()) {
      logger.trace("filters : " + filter);
    }
    if ((filter != null) && (filter.size() > 0))
    {
      Object startTimeVal = filter.get("startTime");
      if (startTimeVal != null)
      {
        startTime = new Double(startTimeVal.toString()).longValue();
        startTime *= 1000L;
      }
      Object endTimeVal = filter.get("endTime");
      if (endTimeVal != null)
      {
        endTime = new Double(endTimeVal.toString()).longValue();
        endTime *= 1000L;
      }
      filterKey = (String)filter.get("key");
      Object ocfilter = filter.get("cacheOnly");
      String cfilter = "false";
      if (ocfilter != null) {
        cfilter = ocfilter.toString();
      }
      if (cfilter != null) {
        cacheonly = cfilter.equalsIgnoreCase("true");
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("filter:" + filter + ", cacheOnly:" + cacheonly);
    }
    if (cacheonly) {
      return new ArrayList();
    }
    if ((startTime > 0L) && (endTime > 0L))
    {
      Key key = Key.createKey(this.storeName);
      SortedSet<Key> set = this.kvstore.multiGetKeys(key, null, null);
      
      Set<Key> tobeQueried = new HashSet();
      for (Key k1 : set)
      {
        String ts = (String)k1.getMinorPath().get(1);
        long tsLong = Long.parseLong(ts);
        if ((tsLong >= startTime) && (tsLong <= endTime)) {
          tobeQueried.add(k1);
        }
      }
      List<Key> tobeQueriedList = new ArrayList();
      tobeQueriedList.addAll(tobeQueried);
      int totalReads = tobeQueried.size();
      results = new ArrayList(totalReads);
      if ((this.MAX_CONN > 1) && (totalReads > 0))
      {
        int numThreads = this.MAX_CONN - 1;
        int querySize = totalReads / numThreads;
        ONDBQuery<T>[] fetchTask = new ONDBQuery[numThreads];
        Thread[] t = new Thread[numThreads];
        CountDownLatch ctl = new CountDownLatch(numThreads);
        
        int fromIndex = 0;
        int toIndex = querySize - 1;
        if (toIndex < 0) {
          toIndex = 0;
        }
        for (int ii = 0; ii < numThreads; ii++)
        {
          fetchTask[ii] = new ONDBQuery(tobeQueriedList.subList(fromIndex, toIndex), ctl);
          fromIndex = toIndex + 1;
          toIndex = fromIndex + querySize - 1;
          if (toIndex >= totalReads - 1) {
            toIndex = totalReads - 1;
          }
          t[ii] = new Thread(fetchTask[ii], "ONDBQuery@" + this.storeName);
          t[ii].start();
        }
        try
        {
          ctl.await();
        }
        catch (InterruptedException iex)
        {
          logger.warn("waction read thread was interrupted due to " + iex.getLocalizedMessage());
        }
        for (int ii = 0; ii < numThreads; ii++) {
          if (fetchTask[ii].getResults() != null) {
            results.addAll(fetchTask[ii].getResults());
          }
        }
      }
      else
      {
        for (Key k1 : tobeQueried)
        {
          byte[] valueBytes = this.kvstore.get(k1).getValue().getValue();
          
          T waction = (Waction)deSerizlizeObj(valueBytes);
          results.add(waction);
        }
      }
    }
    else if (filterKey != null)
    {
      Key key = Key.createKey(this.storeName);
      SortedSet<Key> set = this.kvstore.multiGetKeys(key, null, null);
      
      Set<Key> tobeQueried = new HashSet();
      for (Key k1 : set)
      {
        String wkey = (String)k1.getMinorPath().get(0);
        if (filterKey.equals(wkey)) {
          tobeQueried.add(k1);
        }
      }
      results = new ArrayList(tobeQueried.size());
      for (Key k1 : tobeQueried)
      {
        byte[] valueBytes = this.kvstore.get(k1).getValue().getValue();
        
        T waction = (Waction)deSerizlizeObj(valueBytes);
        results.add(waction);
      }
    }
    else
    {
      results = new ArrayList();
      Key key = Key.createKey(this.storeName);
      SortedMap<Key, ValueVersion> kvResult = this.kvstore.multiGet(key, null, null);
      for (Map.Entry<Key, ValueVersion> entry : kvResult.entrySet())
      {
        byte[] valueBytes = ((ValueVersion)entry.getValue()).getValue().getValue();
        T waction = (Waction)deSerizlizeObj(valueBytes);
        results.add(waction);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("no of results : " + results.size());
    }
    return results;
  }
  
  private class ONDBQuery<T>
    implements Runnable
  {
    List<Key> keys = null;
    List<T> results = null;
    CountDownLatch cLatch = null;
    
    ONDBQuery(CountDownLatch keys)
    {
      this.keys = keys;
      this.cLatch = ctl;
      this.results = new ArrayList(keys.size());
    }
    
    public void run()
    {
      for (Key key : this.keys)
      {
        byte[] valueBytes = ONDBPersistenceLayerImpl.this.kvstore.get(key).getValue().getValue();
        T waction = ONDBPersistenceLayerImpl.deSerizlizeObj(valueBytes);
        this.results.add(waction);
      }
      this.cLatch.countDown();
    }
    
    public List<T> getResults()
    {
      return this.results;
    }
  }
  
  public <T extends Waction> List<T> getResults(Class<T> objectClass, String wactionKey, Map<String, Object> filter)
  {
    if (logger.isDebugEnabled()) {
      logger.debug("trying to get results for store :" + this.storeName + ", and create objects of type : " + objectClass.getCanonicalName() + ", with key :" + wactionKey);
    }
    List<T> results = new ArrayList();
    
    Key key = Key.createKey(this.storeName);
    SortedSet<Key> set = this.kvstore.multiGetKeys(key, null, null);
    Key keyLookingFor = null;
    for (Key k1 : set)
    {
      String wkey = (String)k1.getMinorPath().get(0);
      if (wactionKey.equals(wkey))
      {
        keyLookingFor = k1;
        break;
      }
    }
    if (keyLookingFor != null)
    {
      byte[] valueBytes = this.kvstore.get(keyLookingFor).getValue().getValue();
      
      T waction = (Waction)deSerizlizeObj(valueBytes);
      results.add(waction);
    }
    return results;
  }
  
  public WactionStore getWactionStore()
  {
    return null;
  }
  
  public void setWactionStore(WactionStore ws) {}
}
