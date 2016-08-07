package com.bloom.runtime;

import com.bloom.classloading.WALoader;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.meta.MetaInfo.Window;
import com.bloom.runtime.utils.FieldToObject;
import com.bloom.runtime.utils.NamePolicy;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.UUID;
import com.bloom.runtime.RecordKey;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import org.apache.log4j.Logger;

public abstract class KeyFactory
{
  private static Logger logger = Logger.getLogger(KeyFactory.class);
  
  public abstract RecordKey makeKey(Object paramObject);
  
  public static KeyFactory genKeyFactory(String bundleUri, WALoader wal, String eventTypeClassName, List<String> fieldNames)
    throws Exception
  {
    Class<?> eventType = wal.loadClass(eventTypeClassName);
    ClassPool pool = wal.getBundlePool(bundleUri);
    
    String className = "KeyFactory" + System.nanoTime();
    CtClass cc = pool.makeClass(className);
    CtClass sup = pool.get(KeyFactory.class.getName());
    cc.setSuperclass(sup);
    StringBuilder src = new StringBuilder();
    src.append("public " + RecordKey.class.getName() + " makeKey(Object obj)\n{\n");
    src.append("\t" + eventType.getName() + " tmp = (" + eventType.getName() + ")obj;\n");
    src.append("\tObject[] key = {");
    String sep = "";
    for (String fieldName : fieldNames) {
      try
      {
        Field f = NamePolicy.getField(eventType, fieldName);
        src.append(sep);
        src.append(FieldToObject.genConvert("tmp." + f.getName()));
        sep = ", ";
      }
      catch (NoSuchFieldException|SecurityException e)
      {
        logger.error("Could not obtain field " + fieldName + " from " + eventTypeClassName, e);
        Field[] fs = eventType.getDeclaredFields();
        logger.error("Fields are: " + Arrays.toString(fs));
      }
    }
    src.append("};\n");
    String factory = RecordKey.getObjArrayKeyFactory();
    src.append("\treturn " + factory + "(key);\n");
    src.append("}\n");
    String code = src.toString();
    
    CtMethod m = CtNewMethod.make(code, cc);
    cc.addMethod(m);
    cc.setModifiers(cc.getModifiers() & 0xFBFF);
    cc.setModifiers(1);
    
    wal.addBundleClass(bundleUri, className, cc.toBytecode(), false);
    Class<?> klass = wal.loadClass(className);
    KeyFactory kf = (KeyFactory)klass.newInstance();
    return kf;
  }
  
  public static KeyFactory createKeyFactory(MetaInfo.MetaObject obj, List<String> fields, UUID dataType, BaseServer srv)
    throws Exception
  {
    if (!fields.isEmpty())
    {
      WALoader wal = WALoader.get();
      String bundleUri = wal.createIfNotExistsBundleDefinition(obj.nsName, BundleDefinition.Type.keyFactory, obj.name);
      
      MetaInfo.Type t = srv.getTypeInfo(dataType);
      return genKeyFactory(bundleUri, wal, t.className, fields);
    }
    return null;
  }
  
  public static KeyFactory createKeyFactory(String nsName, String name, List<String> fields, UUID dataType, BaseServer srv)
    throws Exception
  {
    if (!fields.isEmpty())
    {
      WALoader wal = WALoader.get();
      String bundleUri = wal.createIfNotExistsBundleDefinition(nsName, BundleDefinition.Type.keyFactory, name);
      
      MetaInfo.Type t = srv.getTypeInfo(dataType);
      return genKeyFactory(bundleUri, wal, t.className, fields);
    }
    return null;
  }
  
  public static KeyFactory createKeyFactory(MetaInfo.MetaObject obj, List<String> fields, UUID dataType)
    throws Exception
  {
    if (!fields.isEmpty())
    {
      WALoader wal = WALoader.get();
      String bundleUri = wal.createIfNotExistsBundleDefinition(obj.nsName, BundleDefinition.Type.keyFactory, obj.name);
      
      MetaInfo.Type t = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(dataType, WASecurityManager.TOKEN);
      return genKeyFactory(bundleUri, wal, t.className, fields);
    }
    return null;
  }
  
  public static void removeKeyFactory(MetaInfo.MetaObject obj, List<String> fields, UUID dataType, BaseServer srv)
    throws Exception
  {
    if (!fields.isEmpty())
    {
      WALoader wal = WALoader.get();
      wal.removeBundle(obj.nsName, BundleDefinition.Type.keyFactory, obj.name);
    }
  }
  
  public static void removeKeyFactory(MetaInfo.Window windowInfo, BaseServer srv)
    throws Exception
  {
    MetaInfo.Stream streamInfo = srv.getStreamInfo(windowInfo.stream);
    removeKeyFactory(windowInfo, windowInfo.partitioningFields, streamInfo.dataType, srv);
  }
  
  public static void removeKeyFactory(MetaInfo.Stream streamInfo, BaseServer srv)
    throws Exception
  {
    removeKeyFactory(streamInfo, streamInfo.partitioningFields, streamInfo.dataType, srv);
  }
  
  public static KeyFactory createKeyFactory(MetaInfo.Stream streamInfo, BaseServer srv)
    throws Exception
  {
    return createKeyFactory(streamInfo, streamInfo.partitioningFields, streamInfo.dataType, srv);
  }
  
  public static KeyFactory createKeyFactory(MetaInfo.Stream streamInfo)
    throws Exception
  {
    return createKeyFactory(streamInfo, streamInfo.partitioningFields, streamInfo.dataType);
  }
  
  public static KeyFactory createKeyFactory(MetaInfo.Window windowInfo, BaseServer srv)
    throws Exception
  {
    MetaInfo.Stream streamInfo = srv.getStreamInfo(windowInfo.stream);
    return createKeyFactory(windowInfo, windowInfo.partitioningFields, streamInfo.dataType, srv);
  }
}
