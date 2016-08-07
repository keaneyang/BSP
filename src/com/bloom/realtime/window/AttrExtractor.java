package com.bloom.runtime.window;

import com.bloom.classloading.WALoader;
import com.bloom.exception.ServerException;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.BaseServer;
import com.bloom.runtime.Pair;
import com.bloom.runtime.meta.IntervalPolicy;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.IntervalPolicy.AttrBasedPolicy;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.meta.MetaInfo.Window;
import com.bloom.runtime.utils.NamePolicy;
import com.bloom.runtime.containers.WAEvent;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Date;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import org.joda.time.DateTime;

public abstract class AttrExtractor
{
  public abstract Long getAttr(Object paramObject);
  
  private static Pair<AttrExtractor, String> genAttrExtractor(MetaInfo.Window wi, String fieldName, BaseServer srv)
    throws ServerException, ClassNotFoundException, NotFoundException, CannotCompileException, NoSuchFieldException, SecurityException, IOException, InstantiationException, IllegalAccessException, MetaDataRepositoryException
  {
    WALoader wal = WALoader.get();
    String bundleUri = wal.createIfNotExistsBundleDefinition(wi.nsName, BundleDefinition.Type.attrExtractor, wi.name);
    
    MetaInfo.Stream si = srv.getStreamInfo(wi.stream);
    MetaInfo.Type t = srv.getTypeInfo(si.dataType);
    String eventTypeClassName = t.className;
    Class<?> eventType = wal.loadClass(eventTypeClassName);
    ClassPool pool = wal.getBundlePool(bundleUri);
    
    String className = "AttrExtractor" + System.nanoTime();
    CtClass cc = pool.makeClass(className);
    CtClass sup = pool.get(AttrExtractor.class.getName());
    cc.setSuperclass(sup);
    Field field = NamePolicy.getField(eventType, fieldName);
    Class<?> fieldType = field.getType();
    
    StringBuilder src = new StringBuilder();
    src.append("public Long getAttr(Object rec)\n{\n");
    src.append("\t" + fieldType.getName() + " tmp = ((" + eventType.getName() + ")rec)." + field.getName() + ";\n");
    if (DateTime.class.isAssignableFrom(fieldType))
    {
      src.append("\tif(tmp == null) return null;\n");
      src.append("\treturn Long.valueOf(tmp.getMillis());\n");
    }
    else if (Date.class.isAssignableFrom(fieldType))
    {
      src.append("\tif(tmp == null) return null;\n");
      src.append("\treturn Long.valueOf(tmp.getTime());\n");
    }
    else if (Number.class.isAssignableFrom(fieldType))
    {
      src.append("\tif(tmp == null) return null;\n");
      src.append("\treturn Long.valueOf(tmp.longValue());\n");
    }
    else if ((fieldType == Long.TYPE) || (fieldType == Integer.TYPE) || (fieldType == Short.TYPE) || (fieldType == Integer.TYPE))
    {
      src.append("\treturn Long.valueOf((long)tmp);\n");
    }
    else
    {
      throw new RuntimeException("Cannot create a window over " + fieldName + ": " + fieldType.getName() + " field");
    }
    src.append("};\n");
    String code = src.toString();
    
    CtMethod m = CtNewMethod.make(code, cc);
    cc.addMethod(m);
    cc.setModifiers(cc.getModifiers() & 0xFBFF);
    cc.setModifiers(1);
    
    wal.addBundleClass(bundleUri, className, cc.toBytecode(), false);
    Class<?> klass = wal.loadClass(className);
    AttrExtractor ae = (AttrExtractor)klass.newInstance();
    return Pair.make(ae, field.getName());
  }
  
  public static CmpAttrs createAttrComparator(String fieldName, final long interval, final MetaInfo.Window wi, BaseServer srv)
  {
    try
    {
      Pair<AttrExtractor, String> extAndField = genAttrExtractor(wi, fieldName, srv);
      AttrExtractor ae = (AttrExtractor)extAndField.first;
      final String keyFieldName = (String)extAndField.second;
      new CmpAttrs()
      {
        public boolean inRange(WAEvent first, WAEvent last)
        {
          Long firstVal = ae.getAttr(first.data);
          Long lastVal = ae.getAttr(last.data);
          if (lastVal == null) {
            throw new NullPointerException("NULL key (" + keyFieldName + ") in attribute based window [" + wi.getFullName() + "]\n in event " + last.data);
          }
          return firstVal.longValue() + interval > lastVal.longValue();
        }
      };
    }
    catch (Throwable e)
    {
      throw new RuntimeException(e);
    }
  }
  
  public static CmpAttrs createAttrComparator(MetaInfo.Window wi, BaseServer srv)
  {
    IntervalPolicy.AttrBasedPolicy p = wi.windowLen.getAttrPolicy();
    String fieldName = p.getAttrName();
    long interval = p.getAttrValueRange() / 1000L;
    return createAttrComparator(fieldName, interval, wi, srv);
  }
  
  public static void removeAttrExtractor(MetaInfo.Window wi)
  {
    WALoader wal = WALoader.get();
    wal.removeBundle(wi.nsName, BundleDefinition.Type.attrExtractor, wi.name);
  }
}
