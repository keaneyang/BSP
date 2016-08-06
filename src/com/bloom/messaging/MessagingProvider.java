package com.bloom.messaging;

import com.bloom.classloading.WALoader;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.uuid.UUID;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

public class MessagingProvider
{
  public static String ZMQ_SYSTEM = "com.bloom.jmqmessaging.ZMQSystem";
  public static String KAFKA_SYSTEM = "com.bloom.kafkamessaging.KafkaSystem";
  private static Logger logger = Logger.getLogger(MessagingProvider.class);
  private static Map<String, MessagingSystem> ourSystems = new HashMap();
  
  public static MessagingSystem getMessagingSystem(String fqcn)
  {
    MessagingSystem ourSystem = (MessagingSystem)ourSystems.get(fqcn);
    if (ourSystem == null) {
      try
      {
        Class clazz = WALoader.get().loadClass(fqcn);
        try
        {
          String serverIpOrName = HazelcastSingleton.getBindingInterface();
          
          String serverDomainName = System.getProperty("com.bloom.config.serverDomainName");
          if ((serverDomainName != null) && (!serverDomainName.isEmpty())) {
            serverIpOrName = serverDomainName;
          }
          Constructor declaredConstructor = clazz.getDeclaredConstructor(new Class[] { UUID.class, String.class });
          UUID nodeId = HazelcastSingleton.getNodeId();
          ourSystem = (MessagingSystem)declaredConstructor.newInstance(new Object[] { nodeId, serverIpOrName });
          
          ourSystems.put(fqcn, ourSystem);
        }
        catch (InstantiationException e)
        {
          logger.error(e.getMessage());
        }
        catch (IllegalAccessException e)
        {
          logger.error(e.getMessage());
        }
        catch (NoSuchMethodException e)
        {
          logger.error(e.getMessage());
        }
        catch (InvocationTargetException e)
        {
          logger.error(e.getMessage());
        }
        catch (ExceptionInInitializerError e)
        {
          logger.error(e.getMessage());
        }
        catch (NoClassDefFoundError e)
        {
          logger.error(e.getMessage());
        }
      }
      catch (ClassNotFoundException e)
      {
        logger.error(e.getMessage());
      }
    }
    return ourSystem;
  }
  
  public static void shutdownAll()
  {
    for (MessagingSystem ms : ourSystems.values()) {
      ms.shutdown();
    }
  }
}
