package com.bloom.metaRepository;

import com.bloom.anno.PropertyTemplate;
import com.bloom.anno.PropertyTemplateProperty;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.PropertyDef;
import com.bloom.runtime.meta.MetaInfo.PropertyTemplateInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;

public class ReflectionsTest
{
  private static Logger logger = Logger.getLogger(ReflectionsTest.class);
  
  public static void main(String[] args)
  {
    MDCache c = MDCache.getInstance();
    
    Reflections refs = new Reflections(new Object[] { ClasspathHelper.forPackage("com.bloom.proc", new ClassLoader[0]) });
    Set<Class<?>> annotatedClasses = refs.getTypesAnnotatedWith(PropertyTemplate.class);
    Map<String, MetaInfo.PropertyDef> propertyMap = new HashMap();
    MetaInfo.PropertyTemplateInfo pti = null;
    for (Class<?> cc : annotatedClasses)
    {
      Object[] a = cc.getAnnotations();
      PropertyTemplate pt = null;
      for (Object aa : a) {
        if ((aa instanceof PropertyTemplate)) {
          pt = (PropertyTemplate)aa;
        }
      }
      PropertyTemplateProperty[] ptp = pt.properties();
      for (PropertyTemplateProperty val : ptp)
      {
        MetaInfo.PropertyDef def = new MetaInfo.PropertyDef();
        def.construct(val.required(), val.type(), val.defaultValue(), val.label(), val.description());
        propertyMap.put(val.name(), def);
      }
      pti = new MetaInfo.PropertyTemplateInfo();
      pti.construct(pt.name(), pt.type(), propertyMap, pt.inputType().getName(), pt.outputType().getName(), cc.getName(), pt.requiresParser(), pt.requiresFormatter());
      if (logger.isInfoEnabled()) {
        logger.info(pti.name + " ; " + pti.getPropertyMap() + " ; " + pti.className);
      }
      c.put(pti);
    }
  }
}
