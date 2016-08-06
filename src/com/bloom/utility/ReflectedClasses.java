package com.bloom.utility;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

import com.bloom.web.RMIWebSocketException;

public class ReflectedClasses
{
  private static ReflectedClasses instance = new ReflectedClasses();
  private Map<String, Map<String, Method>> classMap = new HashMap();
  private Map<String, Object> objectMap = new HashMap();
  private static Logger logger = Logger.getLogger(ReflectedClasses.class);
  
  public static ReflectedClasses getInstance()
  {
    return instance;
  }
  
  public Map<String, Map<String, Method>> getClassMap()
  {
    return this.classMap;
  }
  
  public void setClassMap(Map<String, Map<String, Method>> classMap)
  {
    this.classMap = classMap;
  }
  
  public Map<String, Object> getObjectMap()
  {
    return this.objectMap;
  }
  
  public void setObjectMap(Map<String, Object> objectMap)
  {
    this.objectMap = objectMap;
  }
  
  public void register(Object anyObject)
    throws RMIWebSocketException
  {
    Class<?> anyClass = anyObject.getClass();
    String className = anyClass.getCanonicalName();
    
    this.objectMap.put(className, anyObject);
    Map<String, Method> methodMap = new HashMap();
    this.classMap.put(className, methodMap);
    if (logger.isInfoEnabled()) {
      logger.info("Registering class " + className);
    }
    Method[] allMethods = anyClass.getDeclaredMethods();
    for (Method method : allMethods) {
      if (Modifier.isPublic(method.getModifiers()))
      {
        String methodName = method.getName();
        
        Class<?>[] types = method.getParameterTypes();
        
        String methodKey = methodName + ":" + types.length;
        if (methodMap.containsKey(methodKey)) {
          throw new RMIWebSocketException("Duplicate method with name " + methodName + " and parameter count " + method.getParameterTypes().length + " defined in " + className);
        }
        if (logger.isInfoEnabled()) {
          logger.info("Added method " + methodKey);
        }
        methodMap.put(methodKey, method);
      }
    }
  }
  
  public Map<String, Method> getMethodsForClass(String className)
  {
    return (Map)this.classMap.get(className);
  }
  
  public Object getInstanceForClass(String className)
  {
    return this.objectMap.get(className);
  }
  
  public Method getMethod(String className, String methodKey)
  {
    return (Method)((Map)this.classMap.get(className)).get(methodKey);
  }
}
