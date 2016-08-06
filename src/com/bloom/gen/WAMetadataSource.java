package com.bloom.gen;

import java.util.Map;
import org.eclipse.persistence.internal.jpa.metadata.xml.XMLEntityMappings;
import org.eclipse.persistence.jpa.metadata.XMLMetadataSource;
import org.eclipse.persistence.logging.SessionLog;

public class WAMetadataSource
  extends XMLMetadataSource
{
  String storeName = "default";
  
  public void setStoreName(String storeName)
  {
    this.storeName = storeName;
  }
  
  public XMLEntityMappings getEntityMappings(Map<String, Object> properties, ClassLoader classLoader, SessionLog log)
  {
    if ((this.storeName == null) || (this.storeName.trim().length() == 0)) {
      this.storeName = "default";
    }
    properties.put("eclipselink.metadata-source.xml.file", "eclipselink-orm-" + this.storeName + ".xml");
    properties.put("javax.persistence.validation.factory", null);
    return super.getEntityMappings(properties, classLoader, log);
  }
}
