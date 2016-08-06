package com.bloom.wactionstore;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;

import com.bloom.wactionstore.exceptions.WActionStoreException;

public final class WActionStores
{
  private static final Class<WActionStores> thisClass = WActionStores.class;
  private static final Logger logger = Logger.getLogger(thisClass);
  private static final String CLASS_NAME = thisClass.getSimpleName();
  private static final Map<String, WActionStoreManager> instanceCache = new HashMap(1);
  private static final AtomicBoolean started = new AtomicBoolean(false);
  
  public static void startup()
  {
    synchronized (started)
    {
      if (started.get()) {
        return;
      }
      logger.info("Starting default storage provider");
      started.set(true);
      WActionStoreManager defaultInstance = getInstance("elasticsearch", null);
      String[] wActionStoreNames = defaultInstance.getNames();
      for (int i = 0; i < wActionStoreNames.length; i++)
      {
        String wActionStoreName = wActionStoreNames[i];
        logger.info(String.format("Discovered WActionStore '%s'", new Object[] { wActionStoreName }));
      }
      logger.info(String.format("Default storage provider, '%s', started", new Object[] { defaultInstance.getInstanceName() }));
    }
  }
  
  public static void shutdown()
  {
    synchronized (started)
    {
      for (WActionStoreManager manager : instanceCache.values()) {
        manager.shutdown();
      }
      instanceCache.clear();
      started.set(false);
    }
  }
  
  public static synchronized com.bloom.wactionstore.elasticsearch.WActionStoreManager getAnyElasticsearchInstance()
  {
    for (Map.Entry<String, WActionStoreManager> entry : instanceCache.entrySet()) {
      if ((entry.getValue() instanceof com.bloom.wactionstore.elasticsearch.WActionStoreManager)) {
        return (com.bloom.wactionstore.elasticsearch.WActionStoreManager)entry.getValue();
      }
    }
    return null;
  }
  
  public static synchronized List<com.bloom.wactionstore.elasticsearch.WActionStoreManager> getAllElasticsearchInstances()
  {
    List<com.bloom.wactionstore.elasticsearch.WActionStoreManager> result = new ArrayList();
    for (Map.Entry<String, WActionStoreManager> entry : instanceCache.entrySet()) {
      if ((entry.getValue() instanceof com.bloom.wactionstore.elasticsearch.WActionStoreManager)) {
        result.add((com.bloom.wactionstore.elasticsearch.WActionStoreManager)entry.getValue());
      }
    }
    return result;
  }
  
  public static synchronized WActionStoreManager getInstance(String instanceProviderName, Map<String, Object> instanceProperties)
  {
    String providerClassName = getProviderClassName(instanceProviderName);
    WActionStoreManager instance;
    try
    {
      Class providerClass = Class.forName(providerClassName);
      String instanceName = getInstanceName(instanceProviderName, instanceProperties);
      instance = (WActionStoreManager)instanceCache.get(instanceName);
      if (instance == null)
      {
        WActionStoreManager newInstance = (WActionStoreManager)providerClass.getConstructor(new Class[] { String.class, Map.class, String.class }).newInstance(new Object[] { instanceProviderName, instanceProperties, instanceName });
        instanceCache.put(instanceName, newInstance);
        instance = newInstance;
      }
    }
    catch (ClassNotFoundException|InstantiationException|IllegalAccessException|NoSuchMethodException|InvocationTargetException exception)
    {
      throw new WActionStoreException(String.format("Unable to instantiate instance of '%s' for provider '%s'", new Object[] { providerClassName, instanceProviderName }), exception);
    }
    return instance;
  }
  
  private static String getInstanceName(String instanceProviderName, Map<String, Object> instanceProperties)
  {
    if ((instanceProviderName == null) || (instanceProviderName.isEmpty())) {
      throw new IllegalArgumentException("Invalid argument");
    }
    Class<? extends WActionStoreManager> providerClass = getProviderClass(instanceProviderName);
    String instanceName = null;
    if (providerClass != null) {
      try
      {
        Method getInstanceName = providerClass.getMethod("getInstanceName", new Class[] { Map.class });
        instanceName = (String)getInstanceName.invoke(null, new Object[] { instanceProperties });
      }
      catch (NoSuchMethodException|IllegalAccessException|InvocationTargetException exception)
      {
        logger.error(String.format("Provider '%s' does not implement 'public static String getInstanceName(String providerName, Map<String, Object> properties)'", new Object[] { instanceProviderName }), exception);
      }
    }
    return instanceName;
  }
  
  public static WActionStoreManager getInstance(Map<String, Object> instanceProperties)
  {
    String instanceProviderName = getWActionStoreProviderName(instanceProperties);
    return getInstance(instanceProviderName, instanceProperties);
  }
  
  private static String getWActionStoreProviderName(Map<String, Object> instanceProperties)
  {
    String result = instanceProperties == null ? null : (String)instanceProperties.get("storageProvider");
    return result != null ? result : "elasticsearch";
  }
  
  private static String getProviderClassName(String instanceProviderName)
  {
    if ((instanceProviderName == null) || (instanceProviderName.isEmpty())) {
      throw new IllegalArgumentException("Invalid argument");
    }
    Class baseClass = WActionStoreManager.class;
    Package basePackage = baseClass.getPackage();
    return basePackage.getName() + '.' + instanceProviderName.toLowerCase() + '.' + baseClass.getSimpleName();
  }
  
  private static Class getProviderClass(String instanceProviderName)
  {
    Class result = null;
    String providerClassName = getProviderClassName(instanceProviderName);
    try
    {
      result = Class.forName(providerClassName);
    }
    catch (ClassNotFoundException exception)
    {
      throw new WActionStoreException(String.format("Unable to instantiate instance of '%s' for provider '%s'", new Object[] { providerClassName, instanceProviderName }), exception);
    }
    return result;
  }
  
  private static final Map<BackgroundTask, String> backgroundTasks = new ConcurrentHashMap(0);
  
  public static void registerBackgroundTask(BackgroundTask backgroundTask)
  {
    String wActionStoreName = backgroundTask.getWActionStoreName();
    backgroundTasks.put(backgroundTask, wActionStoreName);
  }
  
  public static void unregisterBackgroundTask(BackgroundTask backgroundTask)
  {
    backgroundTasks.remove(backgroundTask);
  }
  
  public static void terminateBackgroundTasks(String wActionStoreName)
  {
    for (Map.Entry<BackgroundTask, String> entry : backgroundTasks.entrySet())
    {
      BackgroundTask backgroundTask = (BackgroundTask)entry.getKey();
      String taskWActionStoreName = (String)entry.getValue();
      if (wActionStoreName.equals(taskWActionStoreName)) {
        backgroundTask.terminate();
      }
    }
  }
}
