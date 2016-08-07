package com.bloom.runtime.meta;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.runtime.components.EntityType;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.AuthToken;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.log4j.Logger;

public class ImplicitApplicationBuilder
{
  private static Logger logger = Logger.getLogger(ImplicitApplicationBuilder.class);
  private Set<MetaInfo.MetaObject> components = new LinkedHashSet();
  private MetaInfo.Namespace namespace;
  private AuthToken authToken;
  private String applicationName;
  
  public ImplicitApplicationBuilder addObject(MetaInfo.MetaObject object)
  {
    getComponents().add(object);
    return this;
  }
  
  public ImplicitApplicationBuilder init(MetaInfo.Namespace namespace, String applicationName, AuthToken authToken)
  {
    this.namespace = namespace;
    this.applicationName = applicationName;
    this.authToken = authToken;
    return this;
  }
  
  public String getApplicationName()
  {
    return this.applicationName;
  }
  
  public MetaInfo.Namespace getNamespace()
  {
    return this.namespace;
  }
  
  public MetaInfo.Flow build()
  {
    if ((this.components.isEmpty()) || (this.namespace == null) || (this.applicationName == null) || (this.authToken == null)) {
      throw new RuntimeException("Build has missing settings.");
    }
    MetaInfo.Flow application = new MetaInfo.Flow();
    application.getMetaInfoStatus().setAdhoc(true).setAnonymous(true);
    application.construct(EntityType.APPLICATION, getApplicationName(), getNamespace(), null, 0, 0L);
    for (MetaInfo.MetaObject metaObject : getComponents())
    {
      metaObject.addReverseIndexObjectDependencies(application.getUuid());
      try
      {
        MetadataRepository.getINSTANCE().updateMetaObject(metaObject, WASecurityManager.TOKEN);
      }
      catch (MetaDataRepositoryException e)
      {
        logger.warn(e.getMessage());
      }
      application.addObject(metaObject.getType(), metaObject.getUuid());
    }
    try
    {
      MetadataRepository.getINSTANCE().putMetaObject(application, getAuthToken());
      application = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(application.getUuid(), getAuthToken());
    }
    catch (MetaDataRepositoryException e)
    {
      logger.warn(e.getMessage());
    }
    return application;
  }
  
  public Set<MetaInfo.MetaObject> getComponents()
  {
    return this.components;
  }
  
  public AuthToken getAuthToken()
  {
    return this.authToken;
  }
  
  public String toString()
  {
    try
    {
      return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
    }
    catch (JsonProcessingException e)
    {
      logger.warn(e.getMessage());
    }
    return null;
  }
}
