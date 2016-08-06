package com.bloom.metaRepository;

import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.log4j.Logger;

public class VersionManager
{
  private static VersionManager INSTANCE = new VersionManager();
  private boolean isMapLoaded;
  private static Logger logger = Logger.getLogger(VersionManager.class);
  private IMap<String, List<Integer>> objectVersions;
  
  public static VersionManager getInstance()
  {
    if (!INSTANCE.isMapLoaded) {
      INSTANCE.initMaps();
    }
    return INSTANCE;
  }
  
  private void initMaps()
  {
    if (!this.isMapLoaded)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("Loading IMaps first time from DB");
      }
      HazelcastInstance hz = HazelcastSingleton.get();
      this.objectVersions = hz.getMap("#objectVersions");
      this.isMapLoaded = true;
    }
  }
  
  private String simplePut(String nvUrl, MetaInfo.MetaObject metaObject)
  {
    List<Integer> versions = (List)this.objectVersions.get(nvUrl);
    if (versions == null) {
      versions = new ArrayList();
    }
    if (!versions.contains(Integer.valueOf(metaObject.version)))
    {
      versions.add(Integer.valueOf(metaObject.version));
      this.objectVersions.put(nvUrl, versions);
    }
    return nvUrl + ":" + metaObject.version;
  }
  
  private String incrementVersionAndPut(String nvUrl, MetaInfo.MetaObject obj)
  {
    List<Integer> versions = (List)this.objectVersions.get(nvUrl);
    Integer maxVersion;
    if (!versions.isEmpty()) {
      maxVersion = (Integer)Collections.max(versions);
    } else {
      maxVersion = Integer.valueOf(0);
    }
     maxVersion = Integer.valueOf(maxVersion.intValue() + 1);
    String vUrl = MDConstants.makeURLWithVersion(nvUrl, maxVersion);
    versions.add(maxVersion);
    this.objectVersions.put(nvUrl, versions);
    obj.version = maxVersion.intValue();
    return vUrl;
  }
  
  private void putFirstVersion(String nvUrl, MetaInfo.MetaObject mObject)
    throws MetaDataRepositoryException
  {
    MDConstants.checkNullParams("Either URI or MetaObject was null", new Object[] { nvUrl, mObject });
    List<Integer> vv = new ArrayList();
    vv.add(Integer.valueOf(1));
    this.objectVersions.put(nvUrl, vv);
    mObject.version = 1;
  }
  
  public boolean put(MetaInfo.MetaObject metaObject)
    throws MetaDataRepositoryException
  {
    String nvUrl = MDConstants.makeURLWithoutVersion(metaObject);
    if (logger.isDebugEnabled()) {
      logger.debug("nvUrl : " + nvUrl);
    }
    String vUrl = null;
    if (metaObject.version == 0)
    {
      if (this.objectVersions.containsKey(nvUrl))
      {
        if (metaObject.type.isNotVersionable())
        {
          logger.error(metaObject.type + ":" + metaObject.name + " exists, Cannot create without dropping.");
        }
        else
        {
          vUrl = incrementVersionAndPut(nvUrl, metaObject);
          metaObject.setUri(vUrl);
        }
      }
      else
      {
        vUrl = MDConstants.makeURLWithVersion(nvUrl, Integer.valueOf(1));
        metaObject.setUri(vUrl);
        putFirstVersion(nvUrl, metaObject);
      }
      return metaObject.getUri() != null;
    }
    if (this.objectVersions.containsKey(nvUrl))
    {
      if (metaObject.type.isNotVersionable())
      {
        logger.info(metaObject.type + ":" + metaObject.name + " exists, Cannot create without dropping.");
      }
      else
      {
        vUrl = simplePut(nvUrl, metaObject);
        if (metaObject.getUri() == null) {
          metaObject.setUri(vUrl);
        }
      }
    }
    else
    {
      vUrl = simplePut(nvUrl, metaObject);
      if (metaObject.getUri() == null) {
        metaObject.setUri(vUrl);
      }
    }
    return metaObject.getUri() != null;
  }
  
  public Integer getLatestVersionOfMetaObject(EntityType eType, String namespace, String name)
  {
    String nvUrl = MDConstants.makeUrlWithoutVersion(namespace, eType, name);
    Integer maxVersion = null;
    if (nvUrl != null)
    {
      List<Integer> versionList = (List)this.objectVersions.get(nvUrl);
      if (logger.isDebugEnabled()) {
        logger.debug("version list : " + versionList);
      }
      if (versionList != null) {
        maxVersion = (Integer)Collections.max(versionList);
      }
    }
    return maxVersion;
  }
  
  public List<Integer> getAllVersionsOfMetaObject(EntityType eType, String namespace, String name)
  {
    String nvUrl = MDConstants.makeUrlWithoutVersion(namespace, eType, name);
    List<Integer> versionList = null;
    if (nvUrl != null) {
      versionList = (List)this.objectVersions.get(nvUrl);
    }
    return versionList;
  }
  
  public void removeVersion(EntityType eType, String namespace, String name, Integer version)
  {
    String nvUrl = MDConstants.makeUrlWithoutVersion(namespace, eType, name);
    if (nvUrl != null)
    {
      List<Integer> versionList = (List)this.objectVersions.get(nvUrl);
      if (logger.isDebugEnabled()) {
        logger.debug("version list : " + versionList);
      }
      if (versionList != null)
      {
        if (version != null)
        {
          if (versionList.contains(version)) {
            versionList.remove(version);
          }
        }
        else
        {
          Integer maxVersion = (Integer)Collections.max(versionList);
          versionList.remove(maxVersion);
        }
        if (versionList.isEmpty()) {
          this.objectVersions.remove(nvUrl);
        } else {
          this.objectVersions.put(nvUrl, versionList);
        }
      }
    }
  }
  
  public boolean removeAllVersions(EntityType eType, String namespace, String name)
  {
    String nvUrl = MDConstants.makeUrlWithoutVersion(namespace, eType, name);
    if (nvUrl != null)
    {
      List<Integer> versionList = (List)this.objectVersions.get(nvUrl);
      if (logger.isDebugEnabled()) {
        logger.debug("version list : " + versionList);
      }
      if (versionList != null) {
        this.objectVersions.remove(nvUrl);
      }
    }
    return this.objectVersions.containsKey(nvUrl);
  }
  
  public boolean clear()
  {
    this.objectVersions.clear();
    return this.objectVersions.isEmpty();
  }
}
