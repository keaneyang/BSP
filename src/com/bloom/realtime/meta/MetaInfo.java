package com.bloom.runtime.meta;

import com.bloom.anno.AdapterType;
import com.bloom.anno.NotSet;
import com.bloom.anno.PropertyTemplate;
import com.bloom.appmanager.FlowUtil;
import com.bloom.classloading.WALoader;
import com.bloom.classloading.BundleDefinition.Type;
import com.bloom.exception.SecurityException;
import com.bloom.exceptionhandling.ExceptionType;
import com.bloom.intf.QueryManager;
import com.bloom.intf.QueryManager.QueryProjection;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.metaRepository.actions.ActionableProperties;
import com.bloom.metaRepository.actions.ActionablePropertiesFactory;
import com.bloom.metaRepository.actions.BooleanProperties;
import com.bloom.metaRepository.actions.MetaObjectProperties;
import com.bloom.metaRepository.actions.ObjectProperties;
import com.bloom.metaRepository.actions.TextProperties;
import com.bloom.persistence.WactionStore.FieldFactory;
import com.bloom.runtime.ActionType;
import com.bloom.runtime.DeploymentStrategy;
import com.bloom.runtime.Interval;
import com.bloom.runtime.Pair;
import com.bloom.runtime.Property;
import com.bloom.runtime.compiler.CompilerUtils;
import com.bloom.runtime.compiler.TypeField;
import com.bloom.runtime.compiler.TypeName;
import com.bloom.runtime.compiler.stmts.GracePeriod;
import com.bloom.runtime.compiler.stmts.InputOutputSink;
import com.bloom.runtime.compiler.stmts.MappedStream;
import com.bloom.runtime.compiler.stmts.OutputClause;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.components.MetaObjectPermissionChecker;
import com.bloom.runtime.components.PropertyVariablePermissionChecker;
import com.bloom.runtime.utils.Factory;
import com.bloom.runtime.utils.FieldToObject;
import com.bloom.security.ObjectPermission;
import com.bloom.security.Permissable;
import com.bloom.security.Roleable;
import com.bloom.security.WASecurityManager;
import com.bloom.security.ObjectPermission.PermissionType;
import com.bloom.ser.KryoSingleton;
import com.bloom.utility.GraphUtility;
import com.bloom.utility.Utility;
import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import com.hazelcast.core.HazelcastInstance;
import com.bloom.event.ObjectMapperFactory;
import com.bloom.license.LicenseManager;

import flexjson.JSON;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class MetaInfo
{
  private static Logger logger = Logger.getLogger(MetaInfo.class);
  public static UUID GlobalUUID = new UUID("01e2c3ec-e7fc-0e51-b069-28cfe9165d2d");
  public static final String GlobalSchemaName = "Global";
  public static final String AdminUserName = "admin";
  static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  static ActionablePropertiesFactory actionablePropertiesFactory = new ActionablePropertiesFactory();
  public static Namespace GlobalNamespace = new Namespace();
  private static MDRepository metadataRepository = MetadataRepository.getINSTANCE();
  
  static
  {
    GlobalNamespace.construct("Global", GlobalUUID);
  }
  
  public static MetaObject makeGlobalMetaObject(String name, EntityType t)
  {
    MetaObject o = new MetaObject();
    o.construct(name, GlobalNamespace, t);
    return o;
  }
  
  public static class MetaObjectInfo
    implements Serializable
  {
    private static final long serialVersionUID = -6201744360586692853L;
    public UUID uuid;
    public EntityType type;
    public String name;
    public String nsName;
    
    public MetaObjectInfo() {}
    
    private MetaObjectInfo(MetaInfo.MetaObject o)
    {
      this.uuid = o.uuid;
      this.type = o.type;
      this.name = o.name;
      this.nsName = o.nsName;
    }
  }
  
  public static class MetaObject
    implements Serializable, Cloneable
  {
    private static final long serialVersionUID = -5924386859283245529L;
    public String metaObjectClass = getClass().getName();
    public String name;
    public String uri;
    public UUID uuid;
    public int version;
    public int owner;
    public long ctime;
    public String nsName;
    public UUID namespaceId;
    public EntityType type;
    public String description;
    private Set<UUID> reverseIndexObjectDependencies = new LinkedHashSet();
    private String sourceText;
    private MetaInfoStatus metaInfoStatus = new MetaInfoStatus();
    public Set<UUID> coDependentObjects = new LinkedHashSet(1);
    protected static Map<String, ActionableProperties> actions = new HashMap();
    
    public MetaObject()
    {
      this.name = null;
      this.nsName = null;
      this.namespaceId = null;
      this.type = null;
      this.ctime = 0L;
      this.uuid = null;
    }
    
    public String prettyPrintMap(Map<String, Object> propertyMap)
    {
      String str = "";
      Iterator<Map.Entry<String, Object>> i = propertyMap.entrySet().iterator();
      while (i.hasNext())
      {
        Map.Entry<String, Object> e = (Map.Entry)i.next();
        String key = (String)e.getKey();
        Object value = e.getValue() == null ? "<NOTSET>" : e.getValue();
        if (!i.hasNext())
        {
          if (value != null) {
            if (!key.equals("directory")) {
              str = str + "   " + key + ": " + StringEscapeUtils.escapeJava(value.toString()) + "\n";
            } else {
              str = str + "   " + key + ": " + value + "\n";
            }
          }
        }
        else if (!key.equals("directory")) {
          str = str + "   " + key + ": " + StringEscapeUtils.escapeJava(value.toString()) + ",\n";
        } else {
          str = str + "   " + key + ": " + value + ",\n";
        }
      }
      printDependencyAndStatus();
      return str;
    }
    
    public void construct(String name, MetaInfo.Namespace ns, EntityType objtype)
    {
      construct(name, null, ns, objtype);
    }
    
    public void construct(String name, UUID uuid, MetaInfo.Namespace ns, EntityType objtype)
    {
      construct(name, uuid, ns.name, ns.uuid, objtype);
    }
    
    private void construct(String name, UUID uuid, String nsName, UUID namespaceId, EntityType objtype)
    {
      int dot = name.lastIndexOf('.');
      if ((dot != -1) && (objtype != EntityType.SERVER)) {
        this.name = name.substring(dot + 1);
      } else {
        this.name = name;
      }
      this.nsName = nsName;
      this.namespaceId = namespaceId;
      this.type = objtype;
      this.ctime = System.currentTimeMillis();
      if (uuid == null) {
        this.uuid = new UUID(this.ctime);
      } else {
        this.uuid = uuid;
      }
    }
    
    public String getName()
    {
      return this.name;
    }
    
    public void setName(String name)
    {
      this.name = name;
    }
    
    public UUID getUuid()
    {
      return this.uuid;
    }
    
    public void setUuid(UUID uuid)
    {
      this.uuid = uuid;
    }
    
    public long getCtime()
    {
      return this.ctime;
    }
    
    public void setCtime(long ctime)
    {
      this.ctime = ctime;
    }
    
    public String getNsName()
    {
      return this.nsName;
    }
    
    public void setNsName(String nsName)
    {
      this.nsName = nsName;
    }
    
    public UUID getNamespaceId()
    {
      return this.namespaceId;
    }
    
    public void setNamespaceId(UUID namespaceId)
    {
      this.namespaceId = namespaceId;
    }
    
    public void setType(EntityType type)
    {
      this.type = type;
    }
    
    public EntityType getType()
    {
      return this.type;
    }
    
    @JSON(include=false)
    @JsonIgnore
    public static MetaObject obtainMetaObject(UUID uuid)
    {
      try
      {
        return MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
      }
      catch (MetaDataRepositoryException e)
      {
        MetaInfo.logger.info("Couldn't find the object with UUID " + uuid);
      }
      return null;
    }
    
    @JSON(include=false)
    @JsonIgnore
    public List<UUID> getDependencies()
    {
      return Collections.emptyList();
    }
    
    public Map<UUID, Set<UUID>> inEdges(Graph<UUID, Set<UUID>> graph)
    {
      graph.get(getUuid());
      return graph;
    }
    
    public Graph<UUID, Set<UUID>> exportOrder(@NotNull Graph<UUID, Set<UUID>> graph)
    {
      graph.get(getUuid());
      return graph;
    }
    
    public void addReverseIndexObjectDependencies(UUID uuid)
    {
      this.reverseIndexObjectDependencies.add(uuid);
    }
    
    public String metaToString()
    {
      return this.name + " " + this.type + " " + this.version + " " + this.uuid;
    }
    
    public String toString()
    {
      return metaToString();
    }
    
    public String getUri()
    {
      return this.uri;
    }
    
    public void setUri(String uri)
    {
      this.uri = uri;
    }
    
    public void setDescription(String description)
    {
      this.description = description;
    }
    
    public String getDescription()
    {
      return this.description;
    }
    
    public String describe(AuthToken token)
      throws MetaDataRepositoryException
    {
      return startDescribe();
    }
    
    public String startDescribe()
    {
      String desc = this.type + " " + (this.type.isGlobal() ? this.name : new StringBuilder().append(this.nsName).append(".").append(this.name).toString()) + " CREATED " + MetaInfo.sdf.format(new Date(this.ctime));
      
      desc = desc + "\n";
      if (this.description != null) {
        desc = desc + this.description + "\n";
      }
      return desc;
    }
    
    public void printDependencyAndStatus()
    {
      if (MetaInfo.logger.isDebugEnabled())
      {
        MetaInfo.logger.debug("getReverseIndexObjectDependencies() => \n" + getReverseIndexObjectDependencies());
        MetaInfo.logger.debug("getMetaInfoStatus()=> \n" + getMetaInfoStatus());
        MetaInfo.logger.debug("getSourceText()=> " + getSourceText());
      }
    }
    
    public boolean equals(Object obj)
    {
      if ((obj instanceof MetaObject))
      {
        MetaObject other = (MetaObject)obj;
        return (this.uuid != null) && (other.uuid != null) && (this.uuid.equals(other.uuid));
      }
      return false;
    }
    
    public MetaObject clone()
      throws CloneNotSupportedException
    {
      return (MetaObject)super.clone();
    }
    
    @JsonIgnore
    public String getFullName()
    {
      return this.nsName + "." + this.name;
    }
    
    @JsonIgnore
    public String getFQN()
    {
      return this.nsName + "." + this.type + "." + this.name;
    }
    
    @JsonIgnore
    public String convertNameToFullQualifiedName(String nsName, String fullQualifiedObjectName, EntityType type)
    {
      String objectNamespace = Utility.splitDomain(fullQualifiedObjectName);
      String objectName = Utility.splitName(fullQualifiedObjectName);
      if (objectNamespace == null) {
        objectNamespace = nsName;
      }
      return objectNamespace + "." + type + "." + objectName;
    }
    
    public MetaInfoStatus getMetaInfoStatus()
    {
      return this.metaInfoStatus;
    }
    
    public void setMetaInfoStatus(MetaInfoStatus metaInfoStatus)
    {
      this.metaInfoStatus = metaInfoStatus;
    }
    
    public Set<UUID> getReverseIndexObjectDependencies()
    {
      return this.reverseIndexObjectDependencies;
    }
    
    public String getSourceText()
    {
      return this.sourceText;
    }
    
    public void setSourceText(String sourceText)
    {
      this.sourceText = sourceText;
    }
    
    public int hashCode()
    {
      return this.uuid.hashCode();
    }
    
    @JSON(include=false)
    @JsonIgnore
    public MetaInfo.Flow getCurrentApp()
      throws MetaDataRepositoryException
    {
      return getCurrentApp(this);
    }
    
    @JSON(include=false)
    @JsonIgnore
    public MetaInfo.Flow getCurrentFlow()
      throws MetaDataRepositoryException
    {
      return getCurrentFlow(this);
    }
    
    public static MetaInfo.Flow getCurrentFlow(MetaObject obj)
      throws MetaDataRepositoryException
    {
      if (obj == null) {
        return null;
      }
      if ((obj.type.equals(EntityType.APPLICATION)) || (obj.type.equals(EntityType.FLOW))) {
        return (MetaInfo.Flow)obj;
      }
      if (!obj.type.canBePartOfApp()) {
        return null;
      }
      Set<UUID> parents = obj.getReverseIndexObjectDependencies();
      if (parents != null)
      {
        Iterator<UUID> iter = parents.iterator();
        while (iter.hasNext())
        {
          UUID aUUID = (UUID)iter.next();
          MetaObject parent = MetadataRepository.getINSTANCE().getMetaObjectByUUID(aUUID, WASecurityManager.TOKEN);
          if ((parent != null) && 
            (parent.type.equals(EntityType.FLOW))) {
            return (MetaInfo.Flow)parent;
          }
        }
      }
      return null;
    }
    
    public static MetaInfo.Flow getCurrentApp(MetaObject obj)
      throws MetaDataRepositoryException
    {
      if (obj == null) {
        return null;
      }
      if (obj.type.equals(EntityType.APPLICATION)) {
        return (MetaInfo.Flow)obj;
      }
      if (!obj.type.canBePartOfApp()) {
        return null;
      }
      Set<UUID> parents = obj.getReverseIndexObjectDependencies();
      if (parents != null)
      {
        Iterator<UUID> iter = parents.iterator();
        while (iter.hasNext())
        {
          UUID aUUID = (UUID)iter.next();
          MetaObject parent = MetadataRepository.getINSTANCE().getMetaObjectByUUID(aUUID, WASecurityManager.TOKEN);
          if ((parent != null) && 
            (parent.type == EntityType.APPLICATION)) {
            return (MetaInfo.Flow)parent;
          }
        }
      }
      return null;
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = new JSONObject();
      
      json.put("metaObjectClass", this.metaObjectClass);
      json.put("id", getFullName());
      json.put("name", getName());
      json.put("uri", getUri());
      json.put("uuid", getUuid().getUUIDString());
      json.put("nsName", getNsName());
      json.put("namespaceId", getNamespaceId().getUUIDString());
      json.put("type", getType());
      json.put("description", getDescription());
      json.put("ctime", getCtime());
      
      MetaInfoStatus mis = getMetaInfoStatus();
      JSONObject misJ = new JSONObject();
      
      misJ.put("isAnonymous", mis.isAnonymous());
      misJ.put("isDropped", mis.isDropped());
      misJ.put("isAdhoc", mis.isAdhoc());
      json.put("metaInfoStatus", misJ);
      
      return json;
    }
    
    public String JSONifyString()
    {
      try
      {
        return JSONify().toString();
      }
      catch (JSONException e)
      {
        e.printStackTrace();
      }
      return "{}";
    }
    
    public static ObjectNode convertToJson(Map<String, ActionableProperties> actions)
      throws JsonProcessingException
    {
      ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
      ObjectNode node = jsonMapper.createObjectNode();
      for (Map.Entry<String, ActionableProperties> entry : actions.entrySet()) {
        node.set((String)entry.getKey(), ((ActionableProperties)entry.getValue()).getJsonObject());
      }
      return node;
    }
    
    public static Map buildBaseActions()
    {
      TextProperties textProperties = MetaInfo.actionablePropertiesFactory.createTextProperties();
      textProperties.setIsRequired(true);
      actions.put("name", textProperties);
      actions.put("nsName", textProperties);
      return actions;
    }
    
    public static void clearActionsMap()
    {
      actions.clear();
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
      ObjectNode rootNode = jsonMapper.createObjectNode();
      rootNode.put("id", getFQN());
      rootNode.put("name", this.name);
      rootNode.put("nsName", this.nsName);
      rootNode.put("uuid", this.uuid.toString());
      rootNode.put("type", this.type.toString());
      rootNode.put("ctime", this.ctime);
      
      MetaInfoStatus metaInfoStatus = getMetaInfoStatus();
      ObjectNode metaInfoStatus_JSON = jsonMapper.createObjectNode();
      metaInfoStatus_JSON.put("isValid", metaInfoStatus.isValid());
      metaInfoStatus_JSON.put("isAnonymous", metaInfoStatus.isAnonymous());
      metaInfoStatus_JSON.put("isDropped", metaInfoStatus.isDropped());
      metaInfoStatus_JSON.put("isAdhoc", metaInfoStatus.isAdhoc());
      rootNode.set("metaInfoStatus", metaInfoStatus_JSON);
      
      Set<UUID> reverseDependencies = getReverseIndexObjectDependencies();
      boolean isEditable = true;
      if (reverseDependencies != null)
      {
        for (UUID uuid : reverseDependencies)
        {
          MetaObject metaObject = null;
          try
          {
            metaObject = MetaInfo.metadataRepository.getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
            if ((metaObject != null) && (metaObject.getType().equals(EntityType.APPLICATION)))
            {
              MetaInfo.Flow app = (MetaInfo.Flow)metaObject;
              MetaInfo.StatusInfo statusInfo = FlowUtil.getCurrentStatus(app.getUuid());
              if ((statusInfo != null) && (statusInfo.getStatus() != MetaInfo.StatusInfo.Status.CREATED))
              {
                isEditable = false;
                break;
              }
            }
          }
          catch (MetaDataRepositoryException e)
          {
            MetaInfo.logger.warn(e.getMessage(), e);
          }
          catch (Exception e)
          {
            MetaInfo.logger.error("Problem processing metaobject:" + metaObject, e);
          }
        }
        rootNode.put("isEditable", isEditable);
      }
      return rootNode;
    }
    
    public MetaInfo.MetaObjectInfo makeMetaObjectInfo()
    {
      return new MetaInfo.MetaObjectInfo(this, null);
    }
    
    public static MetaObject deserialize(String json)
      throws JsonParseException, JsonMappingException, IOException
    {
      if (json == null) {
        return null;
      }
      return MetaInfoJsonSerializer.MetaObjectJsonSerializer.deserialize(json);
    }
    
    public void addCoDependentObject(UUID uuid)
    {
      if (uuid != null) {
        this.coDependentObjects.add(uuid);
      }
    }
  }
  
  public static class Namespace
    extends MetaInfo.MetaObject
  {
    private static final long serialVersionUID = 2589468991471181011L;
    
    public void construct(String name, UUID namespaceId)
    {
      MetaInfo.MetaObject.access$300(this, name, namespaceId, "Global", MetaInfo.GlobalUUID, EntityType.NAMESPACE);
    }
    
    public String toString()
    {
      return this.name + " " + "NAMESPACE" + " " + this.uuid;
    }
    
    public String describe(AuthToken token)
    {
      printDependencyAndStatus();
      StringBuffer buffer = new StringBuffer();
      buffer.append(startDescribe() + "CONTAINS OBJECTS (\n");
      Set<UUID> objects = HazelcastSingleton.get().getSet("#" + getName());
      if (objects != null) {
        for (UUID uuid : objects) {
          try
          {
            MetaInfo.MetaObject obj = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, token);
            if (obj != null) {
              buffer.append("\t" + obj.type.toString().toUpperCase() + " " + obj.name.toUpperCase() + ", \n");
            }
          }
          catch (MetaDataRepositoryException e)
          {
            if (MetaInfo.logger.isInfoEnabled()) {
              MetaInfo.logger.info(e.getMessage());
            }
          }
        }
      }
      buffer.append(")\n");
      return buffer.toString();
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      
      return json;
    }
    
    public static Map buildActions()
    {
      return buildBaseActions();
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      return parent;
    }
  }
  
  public static class Type
    extends MetaInfo.MetaObject
  {
    private static final long serialVersionUID = -7638834212660192367L;
    public Boolean generated;
    public String className;
    public UUID extendsType;
    public List<UUID> extendedBy = null;
    public int classId = -1;
    public List<String> keyFields;
    public Map<String, String> fields;
    
    public Type()
    {
      this.generated = Boolean.valueOf(false);
      this.className = null;
      this.extendsType = null;
      this.keyFields = Collections.emptyList();
      this.fields = Collections.emptyMap();
    }
    
    public void construct(String name, MetaInfo.Namespace ns, String className, Map<String, String> fields, List<String> keyFields, boolean generated)
    {
      construct(name, ns, className, null, fields, keyFields, generated);
    }
    
    public void construct(String name, MetaInfo.Namespace ns, String className, UUID extendsType, Map<String, String> fields, List<String> keyFields, boolean generated)
    {
      super.construct(name, ns, EntityType.TYPE);
      this.className = className;
      this.extendsType = extendsType;
      this.keyFields = (keyFields == null ? Collections.emptyList() : keyFields);
      
      this.fields = (fields == null ? Collections.emptyMap() : preprocessFieldNames(fields));
      
      this.generated = Boolean.valueOf(generated);
      if (generated) {
        this.classId = WALoader.get().getClassId(className);
      }
    }
    
    public String toString()
    {
      return metaToString() + " class:" + this.className + " key:" + this.keyFields;
    }
    
    public int getClassId()
    {
      return this.classId;
    }
    
    public synchronized void generateClass()
      throws Exception
    {
      WALoader loader = WALoader.get();
      loader.setClassId(this.className, this.classId);
      if (this.generated.booleanValue())
      {
        AtomicBoolean isSuccesful = new AtomicBoolean(true);
        try
        {
          loader.lockClass(this.className);
          byte[] bytecode = loader.getClassBytes(this.className);
          if (bytecode == null)
          {
            if (MetaInfo.logger.isDebugEnabled()) {
              MetaInfo.logger.debug("Generating class for " + this.className + " fields = " + this.fields);
            }
            if (loader.getTypeBundleDef(this.nsName, this.className) == null) {
              loader.addTypeClass(this.nsName, this.className, this.fields);
            }
          }
        }
        catch (Exception e)
        {
          loader.removeBundle(this.nsName, BundleDefinition.Type.type, this.className);
          isSuccesful.set(false);
        }
        finally
        {
          loader.unlockClass(this.className);
          if (!isSuccesful.get()) {
            throw new Exception("Problem creating type: " + getFullName());
          }
        }
      }
      else
      {
        try
        {
          Class<?> clazz = loader.loadClass(this.className);
          if (!KryoSingleton.get().isClassRegistered(clazz))
          {
            int classId = loader.getClassId(this.className);
            KryoSingleton.get().addClassRegistration(clazz, classId);
          }
        }
        catch (ClassNotFoundException e)
        {
          MetaInfo.logger.error("Could not locate class " + this.className + " for type " + this.nsName + "." + this.name);
        }
      }
    }
    
    public synchronized void removeClass()
    {
      if (this.generated.booleanValue())
      {
        WALoader loader = WALoader.get();
        try
        {
          loader.lockClass(this.className);
          loader.removeTypeClass(this.nsName, this.className);
        }
        catch (Exception e)
        {
          MetaInfo.logger.error("Problem removing class for existing type: " + this.type, e);
        }
        finally
        {
          loader.unlockClass(this.className);
        }
      }
    }
    
    public String describe(AuthToken token)
      throws MetaDataRepositoryException
    {
      String str = startDescribe() + "ATTRIBUTES (\n";
      for (Map.Entry<String, String> attr : this.fields.entrySet())
      {
        String fieldName = (String)attr.getKey();
        boolean isKey = this.keyFields.contains(fieldName);
        str = str + "  " + fieldName + " " + (String)attr.getValue() + (isKey ? " KEY " : "") + "\n";
      }
      str = str + ")\n";
      printDependencyAndStatus();
      if (this.extendsType != null) {
        str = str + "EXTENDS " + MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.extendsType, WASecurityManager.TOKEN).name + "\n";
      }
      if (this.extendedBy != null)
      {
        str = str + "EXTENDED BY [\n";
        boolean first = true;
        for (UUID ex : this.extendedBy)
        {
          if (!first) {
            str = str + ",\n";
          } else {
            first = false;
          }
          str = str + "  " + MetadataRepository.getINSTANCE().getMetaObjectByUUID(ex, WASecurityManager.TOKEN).name;
        }
        str = str + "\n]";
      }
      return str;
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      
      return json;
    }
    
    public static Map buildActions()
    {
      MetaInfo.MetaObject.buildBaseActions();
      ObjectProperties objectProperties_1 = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      objectProperties_1.setIsRequired(true);
      ObjectProperties objectProperties_2 = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      actions.put("fields", objectProperties_1);
      actions.put("keyField", objectProperties_2);
      return actions;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
      parent.put("generated", this.generated);
      ArrayNode fieldArray = jsonMapper.createArrayNode();
      for (Map.Entry<String, String> entry : this.fields.entrySet())
      {
        ObjectNode fieldNode = jsonMapper.createObjectNode();
        fieldNode.put("name", (String)entry.getKey());
        fieldNode.put("type", (String)entry.getValue());
        if (this.keyFields.contains(entry.getKey())) {
          fieldNode.put("isKey", true);
        } else {
          fieldNode.put("isKey", false);
        }
        fieldArray.add(fieldNode);
      }
      parent.set("fields", fieldArray);
      return parent;
    }
    
    public String JSONifyString()
    {
      ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
      jsonMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
      try
      {
        String JSON = jsonMapper.writeValueAsString(this);
        JsonNode actualObj = jsonMapper.readTree(JSON);
        return actualObj.toString();
      }
      catch (JsonProcessingException e)
      {
        if (MetaInfo.logger.isInfoEnabled()) {
          MetaInfo.logger.info(e.getMessage());
        }
      }
      catch (IOException e)
      {
        if (MetaInfo.logger.isInfoEnabled()) {
          MetaInfo.logger.info(e.getMessage());
        }
      }
      return null;
    }
    
    private static Map<String, String> preprocessFieldNames(Map<String, String> fields)
    {
      Map<String, String> ret = Factory.makeLinkedMap();
      for (Map.Entry<String, String> e : fields.entrySet())
      {
        String fieldName = (String)e.getKey();
        String typeName = (String)e.getValue();
        if (CompilerUtils.isJavaKeyword(fieldName)) {
          fieldName = CompilerUtils.capitalize(fieldName);
        }
        ret.put(fieldName, typeName);
      }
      return ret;
    }
  }
  
  public static class Stream
    extends MetaInfo.MetaObject
  {
    private static final long serialVersionUID = 6543159810445090302L;
    public UUID dataType;
    public List<String> partitioningFields;
    public Interval gracePeriodInterval;
    public String gracePeriodField;
    public String pset;
    public MetaInfo.PropertySet propertySet;
    public String avroSchema;
    
    public Stream()
    {
      this.dataType = null;
      this.partitioningFields = null;
      this.gracePeriodInterval = null;
      this.gracePeriodField = null;
      this.propertySet = null;
    }
    
    public void construct(String name, MetaInfo.Namespace ns, UUID dataType, List<String> partitioningFields, GracePeriod gp, String pset)
    {
      construct(name, null, ns, dataType, partitioningFields, gp, pset);
    }
    
    public void construct(String name, UUID streamUUID, MetaInfo.Namespace ns, UUID dataType, List<String> partitioningFields, GracePeriod gp, String pset)
    {
      super.construct(name, streamUUID, ns, EntityType.STREAM);
      this.dataType = dataType;
      this.partitioningFields = (partitioningFields == null ? Collections.emptyList() : partitioningFields);
      this.gracePeriodInterval = (gp == null ? null : gp.sortTimeInterval);
      this.gracePeriodField = (gp == null ? null : gp.fieldName);
      this.pset = pset;
    }
    
    public static Map buildActions()
    {
      MetaInfo.MetaObject.buildBaseActions();
      MetaObjectProperties metaObjectProperties = MetaInfo.actionablePropertiesFactory.createMetaObjectProperties();
      metaObjectProperties.setIsRequired(true);
      ObjectProperties objectProperties = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      BooleanProperties persist = MetaInfo.actionablePropertiesFactory.createBooleanProperties();
      TextProperties propertySet = MetaInfo.actionablePropertiesFactory.createTextProperties();
      actions.put("dataType", metaObjectProperties);
      actions.put("partitioningFields", objectProperties);
      actions.put("persist", persist);
      actions.put("propertySet", propertySet);
      return actions;
    }
    
    public UUID getDataType()
    {
      return this.dataType;
    }
    
    public void setDataType(UUID dataType)
    {
      this.dataType = dataType;
    }
    
    public List<String> getPartitioningFields()
    {
      return this.partitioningFields;
    }
    
    public void setPartitioningFields(List<String> partitioningFields)
    {
      this.partitioningFields = partitioningFields;
    }
    
    public Interval getGracePeriodInterval()
    {
      return this.gracePeriodInterval;
    }
    
    public void setGracePeriodInterval(Interval gracePeriodInterval)
    {
      this.gracePeriodInterval = gracePeriodInterval;
    }
    
    public String getGracePeriodField()
    {
      return this.gracePeriodField;
    }
    
    public void setGracePeriodField(String gracePeriodField)
    {
      this.gracePeriodField = gracePeriodField;
    }
    
    public void setPropertySet(MetaInfo.PropertySet propertySet)
    {
      this.propertySet = propertySet;
    }
    
    public void setPset(String pset)
    {
      this.pset = pset;
    }
    
    public void setAvroSchema(String avroSchema)
    {
      this.avroSchema = avroSchema;
    }
    
    public List<UUID> getDependencies()
    {
      return Collections.singletonList(this.dataType);
    }
    
    public String toString()
    {
      return metaToString() + " dataType:" + this.dataType;
    }
    
    public String describe(AuthToken token)
      throws MetaDataRepositoryException
    {
      String str = startDescribe();
      str = str + "OF TYPE ";
      if (MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.dataType, WASecurityManager.TOKEN) != null) {
        str = str + MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.dataType, WASecurityManager.TOKEN).name;
      }
      str = str + " PARTITION BY " + this.partitioningFields;
      if (this.pset != null) {
        str = str + " PERSIST USING " + this.pset + " WITH PROPERTIES " + this.propertySet;
      }
      printDependencyAndStatus();
      return str;
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      
      return json;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      parent.putPOJO("partitioningFields", this.partitioningFields);
      if (this.dataType != null) {
        try
        {
          MetaInfo.MetaObject typeDef = MetaInfo.metadataRepository.getMetaObjectByUUID(this.dataType, WASecurityManager.TOKEN);
          if (typeDef != null) {
            parent.put("dataType", typeDef.getFQN());
          } else {
            parent.putNull("dataType");
          }
        }
        catch (MetaDataRepositoryException e) {}
      } else {
        parent.putNull("dataType");
      }
      if (this.propertySet != null)
      {
        parent.put("persist", true);
        parent.putPOJO("propertySet", this.propertySet.getFQN());
      }
      else
      {
        parent.put("persist", false);
        parent.putNull("propertySet");
      }
      return parent;
    }
    
    public Map<UUID, Set<UUID>> inEdges(Graph<UUID, Set<UUID>> graph)
    {
      return super.inEdges(graph);
    }
    
    public Graph<UUID, Set<UUID>> exportOrder(@NotNull Graph<UUID, Set<UUID>> graph)
    {
      super.exportOrder(graph);
      MetaInfo.MetaObject obj = obtainMetaObject(this.dataType);
      if (!obj.getMetaInfoStatus().isAnonymous()) {
        ((Set)graph.get(this.dataType)).add(getUuid());
      }
      return graph;
    }
  }
  
  public static class StreamGenerator
    extends MetaInfo.MetaObject
  {
    private static final long serialVersionUID = 245647756917065648L;
    public UUID dataType;
    public String className;
    public Object[] args;
    
    public StreamGenerator()
    {
      this.dataType = null;
      this.className = null;
      this.args = null;
    }
    
    public void construct(String name, MetaInfo.Namespace ns, UUID dataType, String className, Object[] args)
    {
      super.construct(name, ns, EntityType.STREAM_GENERATOR);
      assert (dataType != null);
      this.dataType = dataType;
      this.className = className;
      this.args = ((Object[])args.clone());
    }
    
    public String toString()
    {
      return metaToString() + " class:" + this.className + " args:" + this.args;
    }
    
    public List<UUID> getDependencies()
    {
      return Collections.singletonList(this.dataType);
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      return parent;
    }
  }
  
  public static class Window
    extends MetaInfo.MetaObject
  {
    private static final long serialVersionUID = -5342738459832800581L;
    public UUID stream;
    public boolean jumping;
    public boolean persistent;
    public boolean implicit;
    public List<String> partitioningFields;
    public IntervalPolicy windowLen;
    public IntervalPolicy slidePolicy;
    public Object options;
    
    public Window()
    {
      this.stream = null;
      this.jumping = false;
      this.persistent = false;
      this.implicit = false;
      this.partitioningFields = null;
      this.windowLen = null;
      this.slidePolicy = null;
      this.options = null;
    }
    
    public void construct(String name, MetaInfo.Namespace ns, UUID stream, Pair<IntervalPolicy, IntervalPolicy> windowLen, boolean jumping, boolean persistent, List<String> partitioningFields, boolean implicit, Object options)
    {
      super.construct(name, ns, EntityType.WINDOW);
      this.stream = stream;
      this.jumping = jumping;
      this.persistent = persistent;
      this.partitioningFields = (partitioningFields == null ? Collections.emptyList() : partitioningFields);
      
      this.windowLen = ((IntervalPolicy)windowLen.first);
      this.slidePolicy = ((IntervalPolicy)windowLen.second);
      this.implicit = implicit;
      this.options = options;
    }
    
    public List<UUID> getDependencies()
    {
      return Collections.singletonList(this.stream);
    }
    
    public String toString()
    {
      return metaToString() + (this.jumping ? "jumping" : "sliding") + " stream:" + this.stream + " keep:" + this.windowLen + " partition:" + this.partitioningFields;
    }
    
    public String describe(AuthToken token)
      throws MetaDataRepositoryException
    {
      printDependencyAndStatus();
      return startDescribe() + " ON STREAM " + MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.stream, WASecurityManager.TOKEN).name + (this.jumping ? " JUMPING" : " SLIDING") + " WINDOW CONDITIONS" + this.windowLen + " PARTITIONED BY:" + this.partitioningFields;
    }
    
    @JsonIgnore
    public boolean isLikeCache()
    {
      return (!this.jumping) && (this.windowLen != null) && (this.partitioningFields != null) && (this.windowLen.isCount1()) && (!this.partitioningFields.isEmpty());
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      
      return json;
    }
    
    public Map<UUID, Set<UUID>> inEdges(Graph<UUID, Set<UUID>> graph)
    {
      ((Set)graph.get(this.stream)).add(getUuid());
      return super.inEdges(graph);
    }
    
    public Graph<UUID, Set<UUID>> exportOrder(@NotNull Graph<UUID, Set<UUID>> graph)
    {
      super.exportOrder(graph);
      ((Set)graph.get(this.stream)).add(getUuid());
      return graph;
    }
    
    public static Window deserialize(JsonNode jsonNode)
      throws JsonParseException, JsonMappingException, IOException
    {
      if (jsonNode == null) {
        return null;
      }
      String className = jsonNode.get("metaObjectClass").asText();
      String expect = Window.class.getCanonicalName();
      if (!className.equalsIgnoreCase(expect))
      {
        MetaInfo.logger.warn("wrong node passed. Expecting :" + Window.class.getCanonicalName() + " and got :" + className);
        return null;
      }
      ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
      jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      SimpleModule module = new SimpleModule("IntervalPolicyDeserializer");
      IntervalPolicy.IntervalPolicyDeserializer des = new IntervalPolicy.IntervalPolicyDeserializer();
      module.addDeserializer(IntervalPolicy.class, des);
      jsonMapper.registerModule(module);
      return (Window)jsonMapper.readValue(jsonNode.toString(), Window.class);
    }
    
    public static Map buildActions()
    {
      MetaInfo.MetaObject.buildBaseActions();
      MetaObjectProperties stream_p = MetaInfo.actionablePropertiesFactory.createMetaObjectProperties();
      stream_p.setIsRequired(true);
      BooleanProperties jumping_p = MetaInfo.actionablePropertiesFactory.createBooleanProperties();
      jumping_p.setIsRequired(true);
      ObjectProperties partitionFields_p = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      ObjectProperties windowLen_p = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      windowLen_p.setIsRequired(true);
      actions.put("stream", stream_p);
      actions.put("jumping", jumping_p);
      actions.put("partitioningFields", partitionFields_p);
      actions.put("size", windowLen_p);
      return actions;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
      ObjectNode parent = super.getJsonForClient();
      if (this.stream != null) {
        try
        {
          MetaInfo.MetaObject streamInfo = MetaInfo.metadataRepository.getMetaObjectByUUID(this.stream, WASecurityManager.TOKEN);
          if (streamInfo != null) {
            parent.put("stream", streamInfo.getFQN());
          } else {
            parent.putNull("stream");
          }
        }
        catch (MetaDataRepositoryException e) {}
      } else {
        parent.putNull("stream");
      }
      parent.put("jumping", this.jumping);
      parent.putPOJO("partitioningFields", this.partitioningFields);
      
      Map<String, IntervalPolicy> policyMap = Maps.newHashMap();
      policyMap.put("size", this.windowLen);
      policyMap.put("slidePolicy", this.slidePolicy);
      
      ObjectNode windowPolicy = jsonMapper.createObjectNode();
      if (this.windowLen != null)
      {
        String windowType = Utility.getWindowType(this.windowLen);
        if (StringUtils.isNotBlank(windowType)) {
          windowPolicy.put("type", windowType);
        }
        if (this.windowLen.getCountPolicy() != null) {
          windowPolicy.put("count", this.windowLen.getCountPolicy().getCountInterval());
        }
        if (this.windowLen.getTimePolicy() != null) {
          if (("hybrid".equals(windowType)) && (this.windowLen.getAttrPolicy() == null)) {
            windowPolicy.put("timeout", this.windowLen.getTimePolicy().getTimeInterval());
          } else {
            windowPolicy.put("time", this.windowLen.getTimePolicy().getTimeInterval());
          }
        }
        if (this.windowLen.getAttrPolicy() != null)
        {
          windowPolicy.put("onField", this.windowLen.getAttrPolicy().getAttrName());
          if (("time".equals(windowType)) && (this.windowLen.getTimePolicy() == null)) {
            windowPolicy.put("time", this.windowLen.getAttrPolicy().getAttrValueRange());
          } else {
            windowPolicy.put("timeout", this.windowLen.getAttrPolicy().getAttrValueRange());
          }
        }
        if (this.slidePolicy != null)
        {
          if ((("time".equals(windowType)) || ("hybrid".equals(windowType))) && (this.slidePolicy.getTimePolicy() != null)) {
            windowPolicy.put("outputInterval", this.slidePolicy.getTimePolicy().getTimeInterval());
          }
          if ((("time".equals(windowType)) || ("hybrid".equals(windowType))) && (this.slidePolicy.getTimePolicy() == null) && (this.slidePolicy.getAttrPolicy() != null)) {
            windowPolicy.put("outputInterval", this.slidePolicy.getAttrPolicy().getAttrValueRange());
          }
          if ((("count".equals(windowType)) || ("hybrid".equals(windowType))) && (this.slidePolicy.getCountPolicy() != null)) {
            windowPolicy.put("outputInterval", this.slidePolicy.getCountPolicy().getCountInterval());
          }
        }
        parent.set("size", windowPolicy);
      }
      else
      {
        parent.putNull("size");
      }
      return parent;
    }
  }
  
  public static class CQ
    extends MetaInfo.MetaObject
  {
    private static final long serialVersionUID = 3340037099551644462L;
    public UUID stream;
    public CQExecutionPlan plan;
    public String select;
    public List<String> fieldList;
    
    public CQ()
    {
      this.stream = null;
      this.plan = null;
      this.select = null;
      this.fieldList = null;
    }
    
    public void construct(String name, MetaInfo.Namespace ns, UUID stream, CQExecutionPlan plan, String select, List<String> fieldList)
    {
      super.construct(name, ns, EntityType.CQ);
      this.stream = stream;
      this.plan = plan;
      this.select = select;
      this.fieldList = fieldList;
    }
    
    public UUID getStream()
    {
      return this.stream;
    }
    
    public void setStream(UUID stream)
    {
      this.stream = stream;
    }
    
    public CQExecutionPlan getPlan()
    {
      return this.plan;
    }
    
    public void setPlan(CQExecutionPlan plan)
    {
      this.plan = plan;
    }
    
    public String getSelect()
    {
      return this.select;
    }
    
    public void setSelect(String select)
    {
      this.select = select;
    }
    
    public List<String> getFieldList()
    {
      return this.fieldList;
    }
    
    public void setFieldList(List<String> fieldList)
    {
      this.fieldList = fieldList;
    }
    
    public Map<UUID, Set<UUID>> inEdges(Graph<UUID, Set<UUID>> graph)
    {
      if (this.stream != null)
      {
        graph.get(this.stream);
        ((Set)graph.get(getUuid())).add(this.stream);
      }
      if (this.plan != null) {
        for (UUID dataSource : this.plan.getDataSources()) {
          ((Set)graph.get(dataSource)).add(getUuid());
        }
      }
      return super.inEdges(graph);
    }
    
    public Graph<UUID, Set<UUID>> exportOrder(@NotNull Graph<UUID, Set<UUID>> graph)
    {
      super.exportOrder(graph);
      MetaInfo.MetaObject obj = obtainMetaObject(this.stream);
      if (!obj.getMetaInfoStatus().isGenerated())
      {
        ((Set)graph.get(this.stream)).add(getUuid());
      }
      else
      {
        ((Set)graph.get(getUuid())).add(this.stream);
        if ((obj instanceof MetaInfo.Stream))
        {
          MetaInfo.MetaObject typeObj = obtainMetaObject(((MetaInfo.Stream)obj).getDataType());
          if (typeObj != null) {
            ((Set)graph.get(getUuid())).add(typeObj.getUuid());
          }
        }
      }
      if (this.plan != null) {
        for (UUID dataSource : this.plan.getDataSources()) {
          ((Set)graph.get(dataSource)).add(getUuid());
        }
      }
      return graph;
    }
    
    public List<UUID> getDependencies()
    {
      List<UUID> deps = new ArrayList();
      if (this.stream != null) {
        deps.add(this.stream);
      }
      if (this.plan != null) {
        deps.addAll(this.plan.getDataSources());
      }
      return deps;
    }
    
    public String toString()
    {
      return metaToString() + " outputStream:" + this.stream + " plan:" + this.plan;
    }
    
    public String describe(AuthToken token)
      throws MetaDataRepositoryException
    {
      String str = startDescribe();
      str = str + "INSERT INTO ";
      if ((this.stream != null) && (MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.stream, WASecurityManager.TOKEN) != null)) {
        str = str + MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.stream, WASecurityManager.TOKEN).name + "\n";
      }
      str = str + this.select + "\n";
      printDependencyAndStatus();
      return str;
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      
      return json;
    }
    
    public static Map buildActions()
    {
      MetaInfo.MetaObject.buildBaseActions();
      MetaObjectProperties stream_p = MetaInfo.actionablePropertiesFactory.createMetaObjectProperties();
      stream_p.setIsRequired(true);
      ObjectProperties select_p = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      select_p.setIsRequired(true);
      actions.put("output", stream_p);
      actions.put("select", select_p);
      return actions;
    }
    
    private void extractInputs(ArrayNode dataSourceArray, CQ cq)
      throws MetaDataRepositoryException
    {
      List<CQExecutionPlan.DataSource> dataSourcesList = cq.getPlan().getDataSourceList();
      for (CQExecutionPlan.DataSource dataSource : dataSourcesList)
      {
        MetaInfo.MetaObject metaObject = MetaInfo.metadataRepository.getMetaObjectByUUID(dataSource.getDataSourceID(), WASecurityManager.TOKEN);
        if ((metaObject instanceof MetaInfo.WAStoreView)) {
          metaObject = MetaInfo.metadataRepository.getMetaObjectByUUID(((MetaInfo.WAStoreView)metaObject).getWastoreID(), WASecurityManager.TOKEN);
        }
        if (!metaObject.getMetaInfoStatus().isAnonymous()) {
          dataSourceArray.add(metaObject.getFQN());
        } else if ((metaObject instanceof CQ)) {
          extractInputs(dataSourceArray, (CQ)metaObject);
        } else {
          dataSourceArray.add(metaObject.getFQN());
        }
      }
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
      parent.put("select", this.select);
      try
      {
        ArrayNode dataSourceArray = jsonMapper.createArrayNode();
        extractInputs(dataSourceArray, this);
        parent.set("inputs", dataSourceArray);
        
        MetaInfo.MetaObject streamMetaObject = MetaInfo.metadataRepository.getMetaObjectByUUID(this.stream, WASecurityManager.TOKEN);
        parent.put("output", streamMetaObject.getFQN());
      }
      catch (MetaDataRepositoryException e)
      {
        MetaInfo.logger.error(e.getMessage(), e);
      }
      parent.putPOJO("destinationFields", this.fieldList);
      return parent;
    }
  }
  
  public static class PropertySet
    extends MetaInfo.MetaObject
  {
    private static final long serialVersionUID = 1128712638716283L;
    public Map<String, Object> properties;
    
    public PropertySet()
    {
      this.properties = null;
    }
    
    public void construct(String name, MetaInfo.Namespace ns, Map<String, Object> properties)
    {
      super.construct(name, ns, EntityType.PROPERTYSET);
      this.properties = properties;
    }
    
    public Map<String, Object> getProperties()
    {
      return this.properties;
    }
    
    public void setProperties(Map<String, Object> properties)
    {
      this.properties = properties;
    }
    
    public String toString()
    {
      return metaToString() + " set:" + this.properties;
    }
    
    public String describe(AuthToken token)
    {
      return startDescribe() + "PROPERTIES = " + this.properties;
    }
    
    public static Map buildActions()
    {
      return MetaInfo.MetaObject.buildBaseActions();
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      return parent;
    }
  }
  
  public static class PropertyVariable
    extends MetaInfo.MetaObject
  {
    private static final long serialVersionUID = 1128712638719756L;
    public Map<String, Object> properties;
    
    public PropertyVariable()
    {
      this.properties = null;
    }
    
    public void construct(String name, MetaInfo.Namespace ns, Map<String, Object> properties)
    {
      super.construct(name, ns, EntityType.PROPERTYVARIABLE);
      this.properties = properties;
    }
    
    public Map<String, Object> getProperties()
    {
      return this.properties;
    }
    
    public void setProperties(Map<String, Object> properties)
    {
      this.properties = properties;
    }
    
    public String toString()
    {
      return metaToString() + " set:" + this.properties;
    }
    
    public String describe(AuthToken token)
    {
      Iterator<String> itr = this.properties.keySet().iterator();
      while (itr.hasNext())
      {
        String key = (String)itr.next();
        if (!key.contains("encrypted")) {
          return startDescribe() + "PropertyVariable     : " + key;
        }
      }
      return "Please check if the property variable exists and you have right permissions to describe it";
    }
  }
  
  public static class Source
    extends MetaInfo.MetaObject
    implements MetaObjectPermissionChecker
  {
    private static final long serialVersionUID = 1901374789247928L;
    public String adapterClassName;
    public UUID outputStream;
    public Map<String, Object> properties;
    public Map<String, Object> parserProperties;
    public List<OutputClause> outputClauses = new ArrayList();
    
    public Source()
    {
      this.adapterClassName = null;
      this.outputStream = null;
      this.properties = null;
      this.parserProperties = null;
    }
    
    public void construct(String name, MetaInfo.Namespace ns, String adapterClassName, Map<String, Object> properties, Map<String, Object> parserProperties, UUID outputStream)
    {
      super.construct(name, ns, EntityType.SOURCE);
      this.adapterClassName = adapterClassName;
      this.properties = properties;
      this.parserProperties = parserProperties;
      this.outputStream = outputStream;
    }
    
    public String getAdapterClassName()
    {
      return this.adapterClassName;
    }
    
    public void setAdapterClassName(String adapterClassName)
    {
      this.adapterClassName = adapterClassName;
    }
    
    public UUID getOutputStream()
    {
      return this.outputStream;
    }
    
    public void setOutputStream(UUID outputStream)
    {
      this.outputStream = outputStream;
    }
    
    public Map<String, Object> getProperties()
    {
      return this.properties;
    }
    
    public void setProperties(Map<String, Object> properties)
    {
      this.properties = properties;
    }
    
    public Map<String, Object> getParserProperties()
    {
      return this.parserProperties;
    }
    
    public void setParserProperties(Map<String, Object> parserProperties)
    {
      this.parserProperties = parserProperties;
    }
    
    public String toString()
    {
      return metaToString() + " adapter:" + this.adapterClassName + " props:" + this.properties + " parser:" + this.parserProperties + " output:" + this.outputStream;
    }
    
    public String describe(AuthToken token)
      throws MetaDataRepositoryException
    {
      PropertyTemplate anno = null;
      try
      {
        Class<?> cls = Class.forName(this.adapterClassName);
        anno = (PropertyTemplate)cls.getAnnotation(PropertyTemplate.class);
      }
      catch (ClassNotFoundException e1)
      {
        e1.printStackTrace();
      }
      String str = startDescribe() + "USING " + anno.name() + " WITH PROPERTIES ";
      
      str = str + "(\n";
      str = str + prettyPrintMap(this.properties);
      str = str + ")\n";
      if ((this.parserProperties != null) && (!this.parserProperties.isEmpty()))
      {
        str = str + " WITH PARSER PROPERTIES ";
        str = str + "(\n";
        str = str + prettyPrintMap(this.parserProperties);
        str = str + ")\n";
      }
      if (this.outputClauses != null)
      {
        for (OutputClause outputClause : this.outputClauses) {
          str = str + "OUTPUTS TO STREAM " + outputClause.toString() + "\n";
        }
      }
      else
      {
        str = str + "OUTPUTS TO STREAM ";
        str = str + MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.outputStream, WASecurityManager.TOKEN).name;
      }
      return str;
    }
    
    public List<UUID> getDependencies()
    {
      List<UUID> result = new CopyOnWriteArrayList();
      result.add(this.outputStream);
      result.addAll(this.coDependentObjects);
      return result;
    }
    
    public Map<UUID, Set<UUID>> inEdges(Graph<UUID, Set<UUID>> graph)
    {
      super.inEdges(graph);
      ((Set)graph.get(getUuid())).add(this.outputStream);
      return graph;
    }
    
    public List<OutputClause> getOutputClauses()
    {
      return this.outputClauses;
    }
    
    public void setOutputClauses(List<InputOutputSink> outputClauses)
    {
      List ll = new ArrayList();
      ll.addAll(outputClauses);
      this.outputClauses = ll;
    }
    
    public Graph<UUID, Set<UUID>> exportOrder(@NotNull Graph<UUID, Set<UUID>> graph)
    {
      super.exportOrder(graph);
      MetaInfo.MetaObject obj = obtainMetaObject(this.outputStream);
      if (obj.getMetaInfoStatus().isGenerated()) {
        ((Set)graph.get(getUuid())).add(this.outputStream);
      } else {
        ((Set)graph.get(this.outputStream)).add(getUuid());
      }
      return graph;
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      
      return json;
    }
    
    public boolean checkPermissionForMetaPropertyVariable(AuthToken token)
      throws MetaDataRepositoryException
    {
      String namespace = getNsName();
      Map<String, Object> readerProperties = getProperties();
      Map<String, Object> parserProperties = getParserProperties();
      return PropertyVariablePermissionChecker.propertyVariableAccessChecker(token, namespace, new Map[] { readerProperties, parserProperties });
    }
    
    public static Map buildActions()
    {
      MetaInfo.MetaObject.buildBaseActions();
      MetaObjectProperties stream_p = MetaInfo.actionablePropertiesFactory.createMetaObjectProperties();
      stream_p.setIsRequired(true);
      ObjectProperties properties_p = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      properties_p.setIsRequired(true);
      ObjectProperties parserProperties_p = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      parserProperties_p.setIsRequired(false);
      ObjectProperties outputClausesProperties = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      outputClausesProperties.setIsRequired(true);
      actions.put("outputStream", stream_p);
      actions.put("adapter", properties_p);
      actions.put("parser", parserProperties_p);
      actions.put("outputclause", outputClausesProperties);
      return actions;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
      
      Map propertiesCopy = new HashMap(this.properties);
      StringBuffer tempBuffer = new StringBuffer();
      if (this.adapterClassName != null)
      {
        ObjectNode adapterProps = jsonMapper.createObjectNode();
        String simpleHandlerName = (String)this.properties.get("adapterName");
        tempBuffer.append(MetaInfo.GlobalNamespace.getName()).append(".").append(EntityType.PROPERTYTEMPLATE).append(".").append(simpleHandlerName);
        adapterProps.put("handler", tempBuffer.toString());
        adapterProps.putPOJO("properties", propertiesCopy);
        parent.putPOJO("adapter", adapterProps);
        tempBuffer.setLength(0);
      }
      Map parserPropertiesCopy = new HashMap(this.parserProperties);
      String simpleHandlerNameInParser = (String)parserPropertiesCopy.get("parserName");
      if (simpleHandlerNameInParser != null)
      {
        ObjectNode parserProps = jsonMapper.createObjectNode();
        tempBuffer.append(MetaInfo.GlobalNamespace.getName()).append(".").append(EntityType.PROPERTYTEMPLATE).append(".").append(simpleHandlerNameInParser);
        parserProps.put("handler", tempBuffer.toString());
        parserProps.putPOJO("properties", parserPropertiesCopy);
        parent.putPOJO("parser", parserProps);
        tempBuffer.setLength(0);
      }
      if (this.outputStream != null) {
        try
        {
          MetaInfo.MetaObject streamInfo = MetaInfo.metadataRepository.getMetaObjectByUUID(this.outputStream, WASecurityManager.TOKEN);
          if (streamInfo != null) {
            parent.put("outputStream", streamInfo.getFQN());
          } else {
            parent.putNull("outputStream");
          }
        }
        catch (MetaDataRepositoryException e) {}
      } else {
        parent.putNull("outputStream");
      }
      if (this.outputClauses != null)
      {
        ArrayNode outputClausesJSON = jsonMapper.createArrayNode();
        for (OutputClause outputClause : this.outputClauses)
        {
          ObjectNode outputClauseNode = jsonMapper.createObjectNode();
          if (outputClause.getStreamName() != null) {
            outputClauseNode.put("outputStream", convertNameToFullQualifiedName(this.nsName, outputClause.getStreamName(), EntityType.STREAM));
          }
          if (outputClause.getFilterText() != null) {
            outputClauseNode.put("select", outputClause.getFilterText());
          }
          if (outputClause.getTypeDefinition() != null)
          {
            ArrayNode fieldArray = jsonMapper.createArrayNode();
            for (TypeField typeField : outputClause.getTypeDefinition())
            {
              ObjectNode fieldNode = jsonMapper.createObjectNode();
              fieldNode.put("name", typeField.fieldName);
              fieldNode.put("type", typeField.fieldType.name);
              fieldNode.put("isKey", typeField.isPartOfKey);
              fieldArray.add(fieldNode);
            }
            outputClauseNode.set("fields", fieldArray);
          }
          if (outputClause.getGeneratedStream() != null)
          {
            ObjectNode mappingNode = jsonMapper.createObjectNode();
            Map mappingPropertiesCopy = new HashMap(outputClause.getGeneratedStream().mappingProperties);
            mappingNode.put("streamName", convertNameToFullQualifiedName(this.nsName, outputClause.getGeneratedStream().streamName, EntityType.STREAM));
            mappingNode.putPOJO("mappingProperties", mappingPropertiesCopy);
            outputClauseNode.putPOJO("map", mappingNode);
          }
          outputClausesJSON.add(outputClauseNode);
        }
        parent.set("outputclause", outputClausesJSON);
      }
      return parent;
    }
  }
  
  public static class Target
    extends MetaInfo.MetaObject
    implements MetaObjectPermissionChecker
  {
    private static final long serialVersionUID = 1901374789247928L;
    public String adapterClassName;
    public UUID inputStream;
    public Map<String, Object> properties;
    public Map<String, Object> formatterProperties;
    
    public Target()
    {
      this.adapterClassName = null;
      this.inputStream = null;
      this.properties = null;
      this.formatterProperties = null;
    }
    
    public void construct(String name, MetaInfo.Namespace ns, String adapterClassName, Map<String, Object> properties, Map<String, Object> formatterProperties, UUID inputStream)
    {
      super.construct(name, ns, EntityType.TARGET);
      this.adapterClassName = adapterClassName;
      this.properties = properties;
      this.formatterProperties = formatterProperties;
      this.inputStream = inputStream;
    }
    
    @JsonIgnore
    public boolean isSubscription()
    {
      if (this.properties.get("isSubscription") != null) {
        return true;
      }
      return false;
    }
    
    @JsonIgnore
    public String getChannelName()
    {
      Map<String, Object> temp = new TreeMap(String.CASE_INSENSITIVE_ORDER);
      temp.putAll(this.properties);
      return (String)temp.get("channelName");
    }
    
    public String getAdapterClassName()
    {
      return this.adapterClassName;
    }
    
    public void setAdapterClassName(String adapterClassName)
    {
      this.adapterClassName = adapterClassName;
    }
    
    public UUID getInputStream()
    {
      return this.inputStream;
    }
    
    public void setInputStream(UUID inputStream)
    {
      this.inputStream = inputStream;
    }
    
    public Map<String, Object> getProperties()
    {
      return this.properties;
    }
    
    public void setProperties(Map<String, Object> properties)
    {
      this.properties = properties;
    }
    
    public Map<String, Object> getFormatterProperties()
    {
      return this.formatterProperties;
    }
    
    public void setFormatterProperties(Map<String, Object> formatterProperties)
    {
      this.formatterProperties = formatterProperties;
    }
    
    public String toString()
    {
      return metaToString() + " adapter:" + this.adapterClassName + " props:" + this.properties + " formatter:" + this.formatterProperties + " input:" + this.inputStream;
    }
    
    public String startDescribe()
    {
      String str = super.startDescribe();
      if (isSubscription()) {
        str = str.replaceFirst(EntityType.TARGET.name(), "SUBSCRIPTION");
      }
      return str;
    }
    
    public String describe(AuthToken token)
      throws MetaDataRepositoryException
    {
      PropertyTemplate anno = null;
      try
      {
        Class<?> cls = Class.forName(this.adapterClassName);
        anno = (PropertyTemplate)cls.getAnnotation(PropertyTemplate.class);
      }
      catch (ClassNotFoundException e1)
      {
        e1.printStackTrace();
      }
      String str = startDescribe() + " USING " + anno.name() + " WITH PROPERTIES ";
      
      str = str + "(\n";
      str = str + prettyPrintMap(this.properties);
      str = str + ")\n";
      if ((this.formatterProperties != null) && (!this.formatterProperties.isEmpty()))
      {
        str = str + " WITH FORMATTER PROPERTIES ";
        str = str + "(\n";
        str = str + prettyPrintMap(this.formatterProperties);
        str = str + ")\n";
      }
      str = str + "INPUT FROM STREAM " + MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.inputStream, WASecurityManager.TOKEN).name;
      
      printDependencyAndStatus();
      return str;
    }
    
    public List<UUID> getDependencies()
    {
      return Collections.singletonList(this.inputStream);
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      
      return json;
    }
    
    public static Map buildActions()
    {
      MetaInfo.MetaObject.buildBaseActions();
      MetaObjectProperties stream_p = MetaInfo.actionablePropertiesFactory.createMetaObjectProperties();
      stream_p.setIsRequired(true);
      ObjectProperties properties_p = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      properties_p.setIsRequired(true);
      ObjectProperties formatterProperties_p = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      formatterProperties_p.setIsRequired(false);
      actions.put("inputStream", stream_p);
      actions.put("adapter", properties_p);
      actions.put("formatter", formatterProperties_p);
      return actions;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      StringBuffer tempBuffer = new StringBuffer();
      ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
      if (this.properties != null)
      {
        Map propertiesCopy = new HashMap(this.properties);
        if (this.adapterClassName != null)
        {
          ObjectNode adapterProps = jsonMapper.createObjectNode();
          String simpleHandlerName = (String)propertiesCopy.get("adapterName");
          tempBuffer.append(MetaInfo.GlobalNamespace.getName()).append(".").append(EntityType.PROPERTYTEMPLATE).append(".").append(simpleHandlerName);
          adapterProps.put("handler", tempBuffer.toString());
          adapterProps.putPOJO("properties", propertiesCopy);
          parent.putPOJO("adapter", adapterProps);
          tempBuffer.setLength(0);
        }
      }
      else
      {
        parent.putNull("adapter");
      }
      if (this.formatterProperties != null)
      {
        Map formatterPropertiesCopy = new HashMap(this.formatterProperties);
        String simpleHandlerNameInParser = (String)formatterPropertiesCopy.get("formatterName");
        if (simpleHandlerNameInParser != null)
        {
          ObjectNode parserProps = jsonMapper.createObjectNode();
          tempBuffer.append(MetaInfo.GlobalNamespace.getName()).append(".").append(EntityType.PROPERTYTEMPLATE).append(".").append(simpleHandlerNameInParser);
          parserProps.put("handler", tempBuffer.toString());
          parserProps.putPOJO("properties", formatterPropertiesCopy);
          parent.putPOJO("formatter", parserProps);
        }
      }
      else
      {
        parent.putNull("formatter");
      }
      if (this.inputStream != null) {
        try
        {
          MetaInfo.MetaObject streamInfo = MetaInfo.metadataRepository.getMetaObjectByUUID(this.inputStream, WASecurityManager.TOKEN);
          parent.put("inputStream", streamInfo.getFQN());
        }
        catch (MetaDataRepositoryException e) {}
      } else {
        parent.putNull("outputStream");
      }
      return parent;
    }
    
    public Map<UUID, Set<UUID>> inEdges(Graph<UUID, Set<UUID>> graph)
    {
      ((Set)graph.get(this.inputStream)).add(getUuid());
      return super.inEdges(graph);
    }
    
    public Graph<UUID, Set<UUID>> exportOrder(@NotNull Graph<UUID, Set<UUID>> graph)
    {
      super.exportOrder(graph);
      MetaInfo.MetaObject obj = obtainMetaObject(this.inputStream);
      if (obj.getMetaInfoStatus().isAnonymous()) {
        ((Set)graph.get(getUuid())).add(this.inputStream);
      } else {
        ((Set)graph.get(this.inputStream)).add(getUuid());
      }
      return graph;
    }
    
    public boolean checkPermissionForMetaPropertyVariable(AuthToken token)
      throws MetaDataRepositoryException
    {
      String namespace = getNsName();
      Map<String, Object> readerProperties = getProperties();
      Map<String, Object> formatterProperties = getFormatterProperties();
      return PropertyVariablePermissionChecker.propertyVariableAccessChecker(token, namespace, new Map[] { readerProperties, formatterProperties });
    }
  }
  
  public static class Query
    extends MetaInfo.MetaObject
  {
    private static final long serialVersionUID = -7406372777141475415L;
    private boolean adhocQuery;
    public String queryDefinition;
    public UUID appUUID;
    public UUID streamUUID;
    public UUID cqUUID;
    public Map<String, Long> typeInfo;
    public List<String> queryParameters = new ArrayList();
    public List<Property> bindParameters;
    public List<QueryManager.QueryProjection> projectionFields = new ArrayList();
    
    public Query() {}
    
    public Query(String queryName, MetaInfo.Namespace nameSpace, UUID appUUID, UUID streamUUID, UUID cqUUID, Map<String, Long> typeInfo, String queryDefinition, Boolean isAdhoc)
    {
      super.construct(queryName, nameSpace, EntityType.QUERY);
      this.adhocQuery = isAdhoc.booleanValue();
      this.appUUID = appUUID;
      this.streamUUID = streamUUID;
      this.cqUUID = cqUUID;
      this.queryDefinition = queryDefinition;
      this.typeInfo = typeInfo;
    }
    
    public boolean isAdhocQuery()
    {
      return this.adhocQuery;
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      
      json.put("queryDefinition", this.queryDefinition);
      json.put("appUUID", this.appUUID);
      json.put("streamUUID", this.streamUUID);
      json.put("cqUUID", this.cqUUID);
      json.put("typeInfo", this.typeInfo);
      json.put("queryParameters", this.queryParameters);
      json.put("adhocQuery", this.adhocQuery);
      List<String> projectionCopy = new ArrayList();
      if (this.projectionFields != null) {
        for (QueryManager.QueryProjection qp : this.projectionFields) {
          projectionCopy.add(qp.JSONify().toString());
        }
      }
      json.put("projectionFields", projectionCopy);
      return json;
    }
    
    public String JSONifyString()
    {
      try
      {
        return JSONify().toString();
      }
      catch (JSONException e)
      {
        e.printStackTrace();
      }
      return "{}";
    }
    
    public void setProjectionFields(List<QueryManager.QueryProjection> projectionFields)
    {
      this.projectionFields = projectionFields;
    }
    
    public String describe(AuthToken token)
      throws MetaDataRepositoryException
    {
      String str = startDescribe();
      
      str = str + "Query TQL = " + this.queryDefinition;
      return str;
    }
    
    public static Map buildActions()
    {
      MetaInfo.MetaObject.buildBaseActions();
      TextProperties query_def_properties = MetaInfo.actionablePropertiesFactory.createTextProperties();
      query_def_properties.setIsRequired(true);
      actions.put("queryDefinition", query_def_properties);
      return actions;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      
      ArrayNode projectionArray = ObjectMapperFactory.getInstance().createArrayNode();
      if (this.projectionFields != null) {
        for (QueryManager.QueryProjection qp : this.projectionFields) {
          projectionArray.add(qp.getJsonForClient());
        }
      }
      parent.set("projectionFields", projectionArray);
      parent.put("queryDefinition", this.queryDefinition);
      parent.putPOJO("queryParameters", this.queryParameters);
      
      ObjectNode metaInfoStatus = (ObjectNode)parent.get("metaInfoStatus");
      if ((metaInfoStatus != null) && (!metaInfoStatus.get("isAdhoc").asBoolean())) {
        metaInfoStatus.put("isAnonymous", Boolean.TRUE.toString());
      }
      return parent;
    }
    
    public void setBindParameters(List<Property> bindParameters)
    {
      this.bindParameters = bindParameters;
    }
    
    public List<Property> getBindParameters()
    {
      return this.bindParameters;
    }
    
    public static Query deserialize(JsonNode jsonNode)
      throws JsonParseException, JsonMappingException, IOException
    {
      if (jsonNode == null) {
        return null;
      }
      return MetaInfoJsonSerializer.QueryJsonSerializer.deserialize(jsonNode.toString());
    }
  }
  
  public static class Flow
    extends MetaInfo.MetaObject
  {
    private static final long serialVersionUID = 7905072382713419190L;
    
    public static class Detail
      implements Serializable
    {
      private static final long serialVersionUID = 2589595630815708124L;
      public DeploymentStrategy strategy;
      public UUID deploymentGroup;
      public UUID flow;
      
      public static enum FailOverRule
      {
        AUTO,  MANUAL,  NONE;
        
        private FailOverRule() {}
      }
      
      public FailOverRule failOverRule = FailOverRule.NONE;
      
      public void construct(DeploymentStrategy strategy, UUID dg, UUID flow, FailOverRule failOverRule)
      {
        this.strategy = strategy;
        this.deploymentGroup = dg;
        this.flow = flow;
        this.failOverRule = failOverRule;
      }
      
      public DeploymentStrategy getStrategy()
      {
        return this.strategy;
      }
      
      public void setStrategy(DeploymentStrategy strategy)
      {
        this.strategy = strategy;
      }
      
      public void setFailOverRule(FailOverRule failOverRule)
      {
        this.failOverRule = failOverRule;
      }
      
      public UUID getDeploymentGroup()
      {
        return this.deploymentGroup;
      }
      
      public void setDeploymentGroup(UUID deploymentGroup)
      {
        this.deploymentGroup = deploymentGroup;
      }
      
      public UUID getFlow()
      {
        return this.flow;
      }
      
      public void setFlow(UUID flow)
      {
        this.flow = flow;
      }
    }
    
    public Set<String> importStatements = new LinkedHashSet();
    public Map<EntityType, LinkedHashSet<UUID>> objects;
    public List<Detail> deploymentPlan;
    public int recoveryType;
    public long recoveryPeriod;
    public boolean encrypted = false;
    public MetaInfo.StatusInfo.Status flowStatus;
    public Map<String, Object> ehandlers = Maps.newHashMap();
    
    public Flow()
    {
      this.objects = null;
      this.recoveryType = 0;
      this.recoveryPeriod = 0L;
    }
    
    public void construct(EntityType type, String name, MetaInfo.Namespace ns, Map<EntityType, LinkedHashSet<UUID>> objects, int recoveryType, long recoveryPeriod)
    {
      super.construct(name, ns, type);
      this.objects = new HashMap();
      if (objects != null) {
        this.objects.putAll(objects);
      }
      this.recoveryType = recoveryType;
      this.recoveryPeriod = recoveryPeriod;
      this.flowStatus = MetaInfo.StatusInfo.Status.CREATED;
    }
    
    public boolean isEncrypted()
    {
      return this.encrypted;
    }
    
    public void setEncrypted(boolean encrypted)
    {
      this.encrypted = encrypted;
    }
    
    public void setEhandlers(Map<String, Object> ehandlers)
    {
      this.ehandlers = ehandlers;
    }
    
    public Map<String, Object> getEhandlers()
    {
      return this.ehandlers;
    }
    
    public Map<EntityType, LinkedHashSet<UUID>> getObjects()
    {
      return this.objects;
    }
    
    public void setObjects(Map<EntityType, LinkedHashSet<UUID>> objects)
    {
      this.objects = objects;
    }
    
    public int getRecoveryType()
    {
      return this.recoveryType;
    }
    
    public void setRecoveryType(int recoveryType)
    {
      this.recoveryType = recoveryType;
    }
    
    public long getRecoveryPeriod()
    {
      return this.recoveryPeriod;
    }
    
    public void setRecoveryPeriod(long recoveryPeriod)
    {
      if (recoveryPeriod > 0L) {
        this.recoveryType = 2;
      } else {
        this.recoveryType = 0;
      }
      this.recoveryPeriod = recoveryPeriod;
    }
    
    public List<Detail> getDeploymentPlan()
    {
      return this.deploymentPlan;
    }
    
    public String toString()
    {
      return metaToString() + " contents:" + this.objects;
    }
    
    public Set<String> getImportStatements()
    {
      return this.importStatements;
    }
    
    public void setImportStatements(Set<String> importStatements)
    {
      this.importStatements = importStatements;
    }
    
    public MetaInfo.StatusInfo.Status getFlowStatus()
    {
      return this.flowStatus;
    }
    
    public void setFlowStatus(MetaInfo.StatusInfo.Status flowStatus)
    {
      this.flowStatus = flowStatus;
    }
    
    public String describe(AuthToken token)
      throws MetaDataRepositoryException
    {
      String str = startDescribe();
      if (this.recoveryType == 2)
      {
        str = str + "  RECOVERY ";
        if (this.recoveryPeriod % 3600L == 0L)
        {
          str = str + String.valueOf(this.recoveryPeriod / 3600L);
          str = str + " HOUR INTERVAL\n";
        }
        else if (this.recoveryPeriod % 60L == 0L)
        {
          str = str + String.valueOf(this.recoveryPeriod / 60L);
          str = str + " MINUTE INTERVAL\n";
        }
        else
        {
          str = str + String.valueOf(this.recoveryPeriod);
          str = str + " SECOND INTERVAL\n";
        }
      }
      str = str + "ELEMENTS {\n";
      for (EntityType type : getObjectTypes()) {
        for (UUID uuid : getObjects(type))
        {
          MetaInfo.MetaObject o = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
          str = str + "  " + o.startDescribe();
        }
      }
      str = str + "}\n";
      if (this.deploymentPlan != null)
      {
        str = str + "DEPLOYMENT PLAN {\n";
        for (Detail d : this.deploymentPlan)
        {
          MetaInfo.MetaObject flow = MetadataRepository.getINSTANCE().getMetaObjectByUUID(d.flow, WASecurityManager.TOKEN);
          MetaInfo.MetaObject group = MetadataRepository.getINSTANCE().getMetaObjectByUUID(d.deploymentGroup, WASecurityManager.TOKEN);
          str = str + flow.type + " " + flow.name + " " + d.strategy + " IN " + group.name + "\n";
        }
        str = str + "}\n";
      }
      if (this.type.ordinal() == EntityType.APPLICATION.ordinal()) {
        str = str + "ENCRYPTION: " + this.encrypted + ";\n";
      }
      if ((this.ehandlers != null) && (!this.ehandlers.isEmpty()))
      {
        str = str + "EHANDLERS: {\n";
        for (Map.Entry<String, Object> entry : this.ehandlers.entrySet()) {
          str = str + "  " + (String)entry.getKey() + ": " + entry.getValue() + "\n";
        }
        str = str + "}\n";
      }
      printDependencyAndStatus();
      return str;
    }
    
    public void setDeploymentPlan(List<Detail> deploymentPlan)
    {
      this.deploymentPlan = deploymentPlan;
    }
    
    public Set<UUID> getObjects(EntityType type)
    {
      Set<UUID> l = (Set)this.objects.get(type);
      return l == null ? Collections.emptySet() : l;
    }
    
    @JsonIgnore
    public Collection<EntityType> getObjectTypes()
    {
      return this.objects.keySet();
    }
    
    public void addObject(EntityType type, UUID objid)
    {
      LinkedHashSet<UUID> list = (LinkedHashSet)this.objects.get(type);
      if (list == null)
      {
        list = new LinkedHashSet();
        this.objects.put(type, list);
      }
      list.add(objid);
    }
    
    public List<UUID> getDependencies()
    {
      List<UUID> ret = new ArrayList();
      for (Set<UUID> l : this.objects.values()) {
        ret.addAll(l);
      }
      return ret;
    }
    
    public void setDeepDependencies(Set<UUID> set) {}
    
    public Set<UUID> getDeepDependencies()
      throws MetaDataRepositoryException
    {
      Set<UUID> ret = new HashSet();
      List<Flow> flows = getSubFlows();
      for (Flow flow : flows) {
        ret.addAll(flow.getDependencies());
      }
      for (Set<UUID> l : this.objects.values())
      {
        for (UUID uuid : l)
        {
          MetaInfo.MetaObject obj = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
          if ((obj != null) && (!obj.getMetaInfoStatus().isDropped())) {
            ret.addAll(MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, WASecurityManager.TOKEN).getDependencies());
          }
        }
        ret.addAll(l);
      }
      return ret;
    }
    
    public void setAllObjects(Set<UUID> set) {}
    
    public Set<UUID> getAllObjects()
      throws MetaDataRepositoryException
    {
      Set<UUID> ret = new HashSet();
      List<Flow> flows = getSubFlows();
      for (Flow flow : flows) {
        ret.addAll(flow.getAllObjects());
      }
      for (Set<UUID> l : this.objects.values())
      {
        for (UUID uuid : l) {
          if (MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, WASecurityManager.TOKEN) != null) {
            ret.add(MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, WASecurityManager.TOKEN).getUuid());
          }
        }
        ret.addAll(l);
      }
      return ret;
    }
    
    public Map<UUID, Set<UUID>> inEdges(Graph<UUID, Set<UUID>> graph)
    {
      Map.Entry<EntityType, LinkedHashSet<UUID>> nodes;

      for (Iterator i$ = this.objects.entrySet().iterator(); i$.hasNext();)
      {
        nodes = (Map.Entry)i$.next();
        if ((nodes.getKey() != EntityType.TYPE) && 
          (nodes.getKey() != EntityType.VISUALIZATION) && 
          (nodes.getKey() != EntityType.FLOW)) {
          for (UUID uuid : (LinkedHashSet)nodes.getValue()) {
            try
            {
              MetaInfo.MetaObject appComponent = null;
              if ((appComponent = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, WASecurityManager.TOKEN)) != null) {
                appComponent.inEdges(graph);
              } else if (MetaInfo.logger.isInfoEnabled()) {
                MetaInfo.logger.info(nodes.getKey() + " with uuid " + uuid + ", not found on topological sort.");
              }
            }
            catch (MetaDataRepositoryException e)
            {
              MetaInfo.logger.warn(e);
            }
          }
        }
      }
      return graph;
    }
    
    private Flow findFlow(UUID uuid)
      throws MetaDataRepositoryException
    {
      for (Map.Entry<EntityType, LinkedHashSet<UUID>> ii : this.objects.entrySet()) {
        if (ii.getKey() != EntityType.FLOW) {
          if (((LinkedHashSet)ii.getValue()).contains(uuid)) {
            return this;
          }
        }
      }
      List<Flow> flows = getSubFlows();
      for (Iterator i$ = flows.iterator(); i$.hasNext();)
      {
        flow = (Flow)i$.next();
        for (Map.Entry<EntityType, LinkedHashSet<UUID>> ii : flow.objects.entrySet()) {
          if (ii.getKey() != EntityType.FLOW) {
            if (((LinkedHashSet)ii.getValue()).contains(uuid)) {
              return flow;
            }
          }
        }
      }
      Flow flow;
      return null;
    }
    
    public List<Pair<MetaInfo.MetaObject, Flow>> exportTQL(MetadataRepository metadataRepository, AuthToken authToken)
      throws Exception
    {
      if ((metadataRepository == null) || (authToken == null)) {
        throw new RuntimeException(metadataRepository == null ? "MetadataRepository" : "Authtokennot initialized, cannot export TQL without it");
      }
      Set<UUID> allOfTheObjects = getAllObjects();
      Graph<UUID, Set<UUID>> applicationGraph = new Graph();
      for (UUID metaObjectUUID : allOfTheObjects)
      {
        MetaInfo.MetaObject metaObject = obtainMetaObject(metaObjectUUID);
        if (metaObject.getMetaInfoStatus().isAnonymous())
        {
          if (MetaInfo.logger.isInfoEnabled()) {
            MetaInfo.logger.info("Skipping object " + metaObject.getFullName() + " as they are anonymous");
          }
        }
        else {
          metaObject.exportOrder(applicationGraph);
        }
      }
      if (MetaInfo.logger.isDebugEnabled())
      {
        StringBuffer graphOrdering = new StringBuffer();
        for (Map.Entry<UUID, Set<UUID>> graphEntry : applicationGraph.entrySet())
        {
          MetaInfo.MetaObject mo = metadataRepository.getMetaObjectByUUID((UUID)graphEntry.getKey(), authToken);
          graphOrdering.append("FROM " + mo.getFullName() + " TO ");
          for (UUID outedgeObjectUUID : (Set)graphEntry.getValue())
          {
            MetaInfo.MetaObject outedgeObject = obtainMetaObject(outedgeObjectUUID);
            graphOrdering.append(" " + outedgeObject.getFullName());
          }
          graphOrdering.append("\n");
        }
        MetaInfo.logger.debug("Graph topology is " + graphOrdering.toString());
      }
      List<UUID> orderOfObjects = GraphUtility.topologicalSort(applicationGraph);
      Set<UUID> objectsBelongToCurrentApplication = getAllObjects();
      List<Pair<MetaInfo.MetaObject, Flow>> orderWithFlowInfo = new ArrayList();
      for (UUID metaObjectUUID : orderOfObjects)
      {
        Flow flow = findFlow(metaObjectUUID);
        if (flow == null)
        {
          if (MetaInfo.logger.isInfoEnabled()) {
            MetaInfo.logger.info("Skipping the component as it doesn't belong to this app/flow.");
          }
        }
        else if (objectsBelongToCurrentApplication.contains(metaObjectUUID)) {
          orderWithFlowInfo.add(Pair.make(obtainMetaObject(metaObjectUUID), flow));
        }
      }
      return orderWithFlowInfo;
    }
    
    public List<Pair<MetaInfo.MetaObject, Flow>> showTopology()
      throws Exception
    {
      Graph<UUID, Set<UUID>> graphF = new Graph();
      inEdges(graphF);
      for (Map.Entry<EntityType, LinkedHashSet<UUID>> nodes : this.objects.entrySet()) {
        if (nodes.getKey() == EntityType.FLOW) {
          for (UUID flowUUID : (LinkedHashSet)nodes.getValue())
          {
            Flow flow = null;
            try
            {
              flow = (Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(flowUUID, WASecurityManager.TOKEN);
            }
            catch (MetaDataRepositoryException e)
            {
              MetaInfo.logger.warn(e.getMessage());
            }
            flow.inEdges(graphF);
          }
        }
      }
      List<UUID> metaObjectOrder = GraphUtility.topologicalSort(graphF);
      List<Pair<MetaInfo.MetaObject, Flow>> orderWithFlowInfo = new ArrayList();
      Set<UUID> X = getAllObjects();
      for (UUID uuid : metaObjectOrder)
      {
        Flow flow = findFlow(uuid);
        if (X.contains(uuid))
        {
          new Pair();orderWithFlowInfo.add(Pair.make(MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, WASecurityManager.TOKEN), flow));
        }
      }
      return orderWithFlowInfo;
    }
    
    @JsonIgnore
    public List<Flow> getSubFlows()
      throws MetaDataRepositoryException
    {
      List<Flow> subflows = new ArrayList();
      for (UUID flowid : getObjects(EntityType.FLOW))
      {
        Flow f = (Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(flowid, WASecurityManager.TOKEN);
        if (f != null)
        {
          subflows.add(f);
          subflows.addAll(f.getSubFlows());
        }
      }
      return subflows;
    }
    
    @JSON(include=false)
    @JsonIgnore
    public boolean isDeployed()
    {
      return this.deploymentPlan != null;
    }
    
    public void validate()
      throws MetaDataRepositoryException
    {
      Map<UUID, Flow> map = new HashMap();
      for (UUID id : getDependencies())
      {
        Flow old = (Flow)map.put(id, this);
        assert (old == null);
      }
      List<Flow> subflows = getSubFlows();
      for (Iterator i$ = subflows.iterator(); i$.hasNext();)
      {
        f = (Flow)i$.next();
        for (UUID id : f.getDependencies())
        {
          Flow old = (Flow)map.put(id, f);
          if (old != null)
          {
            MetaInfo.MetaObject o = MetadataRepository.getINSTANCE().getMetaObjectByUUID(id, WASecurityManager.TOKEN);
            if ((o.type != EntityType.TYPE) && (o.type != EntityType.STREAM)) {
              throw new RuntimeException(o + " duplicated in flows :" + old.name + " and " + f.name);
            }
          }
        }
      }
      Flow f;
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      
      JSONObject objectsJ = new JSONObject();
      Map<EntityType, LinkedHashSet<UUID>> objects = getObjects();
      for (EntityType key : objects.keySet())
      {
        LinkedHashSet<UUID> def = (LinkedHashSet)objects.get(key);
        JSONArray uuidArray = new JSONArray();
        for (UUID u : def) {
          uuidArray.put(u.getUUIDString());
        }
        objectsJ.put(key.toString(), uuidArray);
      }
      json.put("objects", objectsJ);
      
      return json;
    }
    
    public static Map buildActions()
    {
      return MetaInfo.MetaObject.buildBaseActions();
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
      ObjectNode parent = super.getJsonForClient();
      String status = "UNKNOWN";
      MetaInfo.StatusInfo statusInfo = null;
      try
      {
        statusInfo = FlowUtil.getCurrentStatus(this.uuid);
      }
      catch (MetaDataRepositoryException e)
      {
        MetaInfo.logger.error("Failed to get status for app " + getName() + " with exception " + e.getMessage());
      }
      if (statusInfo != null) {
        status = statusInfo.getStatus().toString();
      }
      parent.put("flowStatus", status);
      
      ObjectNode obb = jsonMapper.createObjectNode();
      if (this.objects != null)
      {
        for (Map.Entry<EntityType, LinkedHashSet<UUID>> entries : this.objects.entrySet())
        {
          LinkedHashSet<UUID> values = (LinkedHashSet)entries.getValue();
          ArrayNode objectsInFlow = jsonMapper.createArrayNode();
          for (UUID uuid : values) {
            try
            {
              MetaInfo.MetaObject metaObject = MetaInfo.metadataRepository.getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
              if (metaObject != null) {
                objectsInFlow.add(metaObject.getFQN());
              }
            }
            catch (MetaDataRepositoryException e)
            {
              MetaInfo.logger.error(e.getMessage(), e);
            }
          }
          obb.set(((EntityType)entries.getKey()).name(), objectsInFlow);
        }
        parent.set("objects", obb);
      }
      else
      {
        parent.putNull("objects");
      }
      parent.put("recoveryPeriod", this.recoveryPeriod);
      parent.put("recoveryType", this.recoveryType);
      if ((this.ehandlers != null) && (!this.ehandlers.isEmpty()))
      {
        ObjectNode eHandlersNode = jsonMapper.createObjectNode();
        for (ExceptionType exceptionType : ExceptionType.values())
        {
          String exceptionName = exceptionType.name();
          String handlerValue = this.ehandlers.get(exceptionName) != null ? (String)this.ehandlers.get(exceptionName) : null;
          eHandlersNode.put(exceptionName, handlerValue);
        }
        parent.set("eHandlers", eHandlersNode);
      }
      if (this.importStatements != null)
      {
        ArrayNode importStatementsList = jsonMapper.createArrayNode();
        for (String importStatement : this.importStatements) {
          importStatementsList.add(importStatement);
        }
        parent.set("importStatements", importStatementsList);
      }
      parent.put("encrypted", this.encrypted);
      List<Flow> subflows;
      try
      {
        subflows = getSubFlows();
      }
      catch (MetaDataRepositoryException ex)
      {
        subflows = Collections.emptyList();
      }
      ArrayNode deployment_detail_array = jsonMapper.createArrayNode();
      if (this.deploymentPlan != null) {
        for (Detail detail : this.deploymentPlan)
        {
          ObjectNode node = jsonMapper.createObjectNode();
          node.put("strategy", detail.strategy.name());
          node.put("failoverRule", detail.failOverRule.name());
          try
          {
            MetaInfo.DeploymentGroup dg = (MetaInfo.DeploymentGroup)MetaInfo.metadataRepository.getMetaObjectByUUID(detail.deploymentGroup, WASecurityManager.TOKEN);
            node.put("dg", dg.getFQN());
            Flow flow_metaObject = (Flow)MetaInfo.metadataRepository.getMetaObjectByUUID(detail.flow, WASecurityManager.TOKEN);
            node.put("flowName", flow_metaObject.getFQN());
          }
          catch (MetaDataRepositoryException e)
          {
            MetaInfo.logger.error("The deployment detail returned will have some missing info because of: " + e.getMessage());
          }
          deployment_detail_array.add(node);
        }
      }
      for (Flow subflow : subflows)
      {
        boolean subflowMissing = true;
        if (this.deploymentPlan != null)
        {
          List<Detail> temp = new ArrayList();
          for (Detail detail : this.deploymentPlan) {
            if (subflow.getUuid().equals(detail.getFlow())) {
              temp.add(detail);
            }
          }
          subflowMissing = temp.size() == 0;
        }
        if (subflowMissing)
        {
          ObjectNode node = jsonMapper.createObjectNode();
          node.put("strategy", DeploymentStrategy.ON_ALL.name());
          node.put("failoverRule", MetaInfo.Flow.Detail.FailOverRule.NONE.name());
          node.put("dg", "Global.DG.default");
          node.put("flowName", subflow.getFQN());
          deployment_detail_array.add(node);
        }
      }
      parent.set("deploymentInfo", deployment_detail_array);
      
      boolean containsEventTables = false;
      Set<UUID> caches = (Set)this.objects.get(EntityType.CACHE);
      if (caches != null)
      {
        Iterator<UUID> cachesIt = caches.iterator();
        while ((cachesIt.hasNext()) && (!containsEventTables))
        {
          UUID cacheUuid = (UUID)cachesIt.next();
          try
          {
            MetaInfo.Cache c = (MetaInfo.Cache)MetaInfo.metadataRepository.getMetaObjectByUUID(cacheUuid, WASecurityManager.TOKEN);
            containsEventTables |= c.isEventTable();
          }
          catch (MetaDataRepositoryException e)
          {
            throw new RuntimeException(e.getMessage());
          }
        }
      }
      parent.put("isFlowDesignerCompatible", !containsEventTables);
      
      return parent;
    }
  }
  
  public static class PropertyDef
    implements Serializable
  {
    private static final long serialVersionUID = -6231159418970255777L;
    public boolean required;
    public Class<?> type;
    public String defaultValue;
    public String label;
    public String description;
    
    public void construct(boolean required, Class<?> type, String defaultValue, String label, String description)
    {
      this.required = required;
      this.type = type;
      this.defaultValue = defaultValue;
      this.label = label;
      this.description = description;
    }
    
    public boolean isRequired()
    {
      return this.required;
    }
    
    public void setRequired(boolean required)
    {
      this.required = required;
    }
    
    public Class<?> getType()
    {
      return this.type;
    }
    
    public void setType(Class<?> type)
    {
      this.type = type;
    }
    
    public String getDefaultValue()
    {
      return this.defaultValue;
    }
    
    public void setDefaultValue(String defaultValue)
    {
      this.defaultValue = defaultValue;
    }
    
    public String toString()
    {
      return this.type.getCanonicalName() + " default " + this.defaultValue + (this.required ? " required" : " optional");
    }
    
    public boolean equals(PropertyDef that)
    {
      if ((that != null) && (this.required == that.required) && (this.type.toString().equals(that.type.toString())) && (this.defaultValue.equals(that.defaultValue)) && (this.label.equals(that.label)) && (this.description.equals(that.description))) {
        return true;
      }
      return false;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
      ObjectNode parent = jsonMapper.createObjectNode();
      parent.put("required", this.required);
      parent.put("type", this.type.getCanonicalName());
      parent.put("defaultValue", this.defaultValue);
      return parent;
    }
    
    public int hashCode()
    {
      return new HashCodeBuilder().append(this.required).append(this.type.toString()).append(this.defaultValue).toHashCode();
    }
  }
  
  public static class PropertyTemplateInfo
    extends MetaInfo.MetaObject
  {
    private static final long serialVersionUID = 699451660431760677L;
    public AdapterType adapterType;
    public String className;
    public String inputClassName;
    public String outputClassName;
    public Map<String, MetaInfo.PropertyDef> propertyMap;
    private transient Class<?> inputClass = null;
    private transient Class<?> outputClass = null;
    public boolean requiresParser;
    public boolean requiresFormatter;
    
    public PropertyTemplateInfo()
    {
      this.adapterType = AdapterType.notset;
      this.propertyMap = null;
      this.className = null;
      this.inputClassName = null;
      this.outputClassName = null;
      this.requiresParser = false;
      this.requiresFormatter = false;
    }
    
    public void construct(String name, AdapterType adapterType, Map<String, MetaInfo.PropertyDef> propertyMap, String inputClassName, String outputClassName, String className, boolean requiresParser, boolean requiresFormatter)
    {
      super.construct(name, MetaInfo.GlobalNamespace, EntityType.PROPERTYTEMPLATE);
      this.adapterType = adapterType;
      this.propertyMap = propertyMap;
      this.className = className;
      this.inputClassName = inputClassName;
      this.outputClassName = outputClassName;
      this.requiresParser = requiresParser;
      this.requiresFormatter = requiresFormatter;
    }
    
    public AdapterType getAdapterType()
    {
      return this.adapterType;
    }
    
    public void setAdapterType(AdapterType adapterType)
    {
      this.adapterType = adapterType;
    }
    
    public String getClassName()
    {
      return this.className;
    }
    
    public void setClassName(String className)
    {
      this.className = className;
    }
    
    public String getInputClassName()
    {
      return this.inputClassName;
    }
    
    public void setInputClassName(String inputClassName)
    {
      this.inputClassName = inputClassName;
    }
    
    public String getOutputClassName()
    {
      return this.outputClassName;
    }
    
    public void setOutputClassName(String outputClassName)
    {
      this.outputClassName = outputClassName;
    }
    
    public boolean isRequiresParser()
    {
      return this.requiresParser;
    }
    
    public boolean isRequiresFormatter()
    {
      return this.requiresFormatter;
    }
    
    public void setRequiresParser(boolean requiresParser)
    {
      this.requiresParser = requiresParser;
    }
    
    public void setRequiresFormatter(boolean requiresFormatter)
    {
      this.requiresFormatter = requiresFormatter;
    }
    
    public void setPropertyMap(Map<String, MetaInfo.PropertyDef> propertyMap)
    {
      this.propertyMap = propertyMap;
    }
    
    public Map<String, MetaInfo.PropertyDef> getPropertyMap()
    {
      return this.propertyMap;
    }
    
    @JSON(include=false)
    @JsonIgnore
    public Class<?> getInputClass()
    {
      if (this.inputClass == null) {
        try
        {
          this.inputClass = WALoader.get().loadClass(this.inputClassName);
        }
        catch (ClassNotFoundException e)
        {
          MetaInfo.logger.error("Problem loading input class " + this.inputClassName + " for Property Template " + this.name);
        }
      }
      return this.inputClass;
    }
    
    @JSON(include=false)
    @JsonIgnore
    public Class<?> getOutputClass()
    {
      if (this.outputClass == null) {
        try
        {
          this.outputClass = WALoader.get().loadClass(this.outputClassName);
        }
        catch (ClassNotFoundException e)
        {
          MetaInfo.logger.error("Problem loading output class " + this.outputClassName + " for Property Template " + this.name);
        }
      }
      return this.outputClass;
    }
    
    public String toString()
    {
      return metaToString() + " type:" + this.adapterType + " class:" + this.className + " props:" + this.propertyMap + " inputType:" + this.inputClassName + " outputType:" + this.outputClassName;
    }
    
    public String describe(AuthToken token)
    {
      String str = startDescribe() + "FOR " + this.adapterType + " IMPLEMENTED BY " + this.className + "\n";
      if (!getInputClass().equals(NotSet.class)) {
        str = str + "  GENERATES EVENTS OF TYPE " + this.inputClassName + "\n";
      }
      if (!getOutputClass().equals(NotSet.class)) {
        str = str + "  CONSUMES EVENTS OF TYPE " + this.outputClassName + "\n";
      }
      str = str + "  SETTABLE PROPERTIES (\n";
      for (Map.Entry<String, MetaInfo.PropertyDef> entry : this.propertyMap.entrySet())
      {
        String key = (String)entry.getKey();
        MetaInfo.PropertyDef def = (MetaInfo.PropertyDef)entry.getValue();
        Object value = def.defaultValue;
        
        str = str + "   " + key + " (" + def.type + "): default \"";
        if (!key.equals("directory")) {
          str = str + StringEscapeUtils.escapeJava(value.toString());
        } else {
          str = str + value;
        }
        str = str + "\" " + (def.required ? " REQUIRED" : " optional") + "\n";
      }
      str = str + "  )";
      return str;
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      
      json.put("className", getClassName());
      json.put("adapterType", getAdapterType());
      json.put("requiresParser", this.requiresParser);
      
      Map<String, MetaInfo.PropertyDef> propertyMap = getPropertyMap();
      
      JSONObject propertyMapJ = new JSONObject();
      for (String key : propertyMap.keySet())
      {
        MetaInfo.PropertyDef def = (MetaInfo.PropertyDef)propertyMap.get(key);
        String type = def.getType().getSimpleName();
        if (type != null) {
          propertyMapJ.put("type", type);
        } else {
          propertyMapJ.put("type", def.getType().getName());
        }
        propertyMapJ.put("required", def.required);
        propertyMapJ.put("defaultValue", def.getDefaultValue());
      }
      json.put("propertyMap", propertyMapJ);
      
      return json;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
      ObjectNode parent = super.getJsonForClient();
      parent.put("adapterType", this.adapterType.toString());
      parent.put("requiresParser", this.requiresParser);
      parent.put("requiresFormatter", this.requiresFormatter);
      ArrayNode propertyList = jsonMapper.createArrayNode();
      if (this.propertyMap != null)
      {
        for (Map.Entry<String, MetaInfo.PropertyDef> entry : this.propertyMap.entrySet())
        {
          ObjectNode temp = ((MetaInfo.PropertyDef)entry.getValue()).getJsonForClient();
          temp.put("name", (String)entry.getKey());
          propertyList.add(temp);
        }
        parent.set("propertyMap", propertyList);
      }
      else
      {
        parent.putNull("propertyMap");
      }
      return parent;
    }
  }
  
  public static class ShowStream
    implements Serializable
  {
    private static final long serialVersionUID = 4120058926908733541L;
    public boolean show;
    public int line_count;
    public UUID stream_name;
    public UUID session_id;
    public boolean isTungsten;
    
    public void construct(boolean show, int line_count, UUID stream_name, UUID session_id)
    {
      this.show = show;
      this.line_count = line_count;
      this.stream_name = stream_name;
      this.session_id = session_id;
      this.isTungsten = true;
    }
    
    public void construct(boolean show, int line_count, UUID stream_name, boolean isTungsten, UUID session_id)
    {
      this.show = show;
      this.line_count = line_count;
      this.stream_name = stream_name;
      this.session_id = session_id;
      this.isTungsten = isTungsten;
    }
    
    public boolean isShow()
    {
      return this.show;
    }
    
    public void setShow(boolean show)
    {
      this.show = show;
    }
    
    public int getLine_count()
    {
      return this.line_count;
    }
    
    public void setLine_count(int line_count)
    {
      this.line_count = line_count;
    }
    
    public UUID getStream_name()
    {
      return this.stream_name;
    }
    
    public void setStream_name(UUID stream_name)
    {
      this.stream_name = stream_name;
    }
    
    public UUID getSession_id()
    {
      return this.session_id;
    }
    
    public void setSession_id(UUID session_id)
    {
      this.session_id = session_id;
    }
    
    public String toString()
    {
      return " show:" + this.show + " line_count:" + this.line_count + " stream_name: " + this.stream_name;
    }
  }
  
  public static class StatusInfo
    implements Serializable
  {
    private static final long serialVersionUID = -8514361580056565663L;
    
    public static enum Status
      implements Serializable
    {
      UNKNOWN,  CREATED,  DEPLOYED,  CRASH,  RUNNING,  NOT_ENOUGH_SERVERS;
      
      private Status() {}
    }
    
    public List<UUID> SIDs = new ArrayList(1);
    public UUID OID;
    public Status status;
    public Status previousStatus;
    public EntityType type;
    public String name;
    public List<String> exceptionMessages = new LinkedList();
    
    public StatusInfo()
    {
      this.status = Status.UNKNOWN;
      this.previousStatus = Status.UNKNOWN;
    }
    
    public StatusInfo(UUID OID, Status status, EntityType type, String name)
    {
      this(OID, Status.UNKNOWN, status, type, name);
    }
    
    public StatusInfo(UUID OID, Status previousState, Status status, EntityType type, String name)
    {
      this.OID = OID;
      this.previousStatus = previousState;
      this.status = status;
      this.type = type;
      this.name = name;
    }
    
    public void construct(UUID OID, Status status, EntityType type, String name)
    {
      construct(OID, Status.UNKNOWN, status, type, name);
    }
    
    public void construct(UUID OID, Status previousState, Status status, EntityType type, String name)
    {
      this.OID = OID;
      this.previousStatus = previousState;
      this.status = status;
      this.type = type;
      this.name = name;
    }
    
    public List<UUID> getSIDs()
    {
      return this.SIDs;
    }
    
    public void setSIDs(List<UUID> sIDs)
    {
      this.SIDs = sIDs;
    }
    
    public UUID getOID()
    {
      return this.OID;
    }
    
    public void setOID(UUID oID)
    {
      this.OID = oID;
    }
    
    public Status getStatus()
    {
      return this.status;
    }
    
    public void setStatus(Status status)
    {
      this.status = status;
    }
    
    public EntityType getType()
    {
      return this.type;
    }
    
    public void setType(EntityType type)
    {
      this.type = type;
    }
    
    public String getName()
    {
      return this.name;
    }
    
    public void setName(String name)
    {
      this.name = name;
    }
    
    public List<String> getExceptionMessages()
    {
      return this.exceptionMessages;
    }
    
    public void setExceptionMessages(List<String> exceptionMessages)
    {
      this.exceptionMessages.addAll(exceptionMessages);
    }
    
    public void addExceptionMessage(String exceptionMessage)
    {
      this.exceptionMessages.add(exceptionMessage);
    }
    
    public void clearExceptionMessages()
    {
      this.exceptionMessages.clear();
    }
    
    public Status getPreviousStatus()
    {
      return this.previousStatus;
    }
    
    public void setPreviousStatus(Status previousStatus)
    {
      this.previousStatus = previousStatus;
    }
    
    public String toString()
    {
      return this.type + " " + this.name + " " + this.status + " " + this.previousStatus + " " + this.OID;
    }
    
    public String serializeToString()
    {
      return serializeToString(this.OID.toString(), this.name, this.type.name(), this.previousStatus.name(), this.status.name());
    }
    
    public static String serializeToString(String OID, String name, String type, String previousStatus, String status)
    {
      return "{ \"OID\": \"" + OID + "\", \"name\": \"" + name + "\", \"type\": \"" + type + "\", \"previousStatus\": \"" + previousStatus + "\", \"currentStatus\": \"" + status + "\" }";
    }
    
    public static StatusInfo deserializeFromString(String serializedString)
    {
      String[] fiveParts = extractFiveStrings(serializedString);
      if (fiveParts == null) {
        return null;
      }
      try
      {
        UUID OID = new UUID(fiveParts[0]);
        EntityType type = EntityType.forObject(fiveParts[2]);
        Status previousStatus = Status.valueOf(fiveParts[3]);
        Status currentStatus = Status.valueOf(fiveParts[4]);
        
        return new StatusInfo(OID, previousStatus, currentStatus, type, fiveParts[1]);
      }
      catch (Exception e)
      {
        if (MetaInfo.logger.isInfoEnabled()) {
          MetaInfo.logger.info("StatusInfo failed to deserialize " + serializedString, e);
        }
      }
      return null;
    }
    
    public static String[] extractFiveStrings(String serializedString)
    {
      String patternString = "\\{ \"OID\"\\: \"(.*)\", \"name\": \"(.*)\", \"type\"\\: \"(.*)\", \"previousStatus\"\\: \"(.*)\", \"currentStatus\"\\: \"(.*)\" \\}";
      Pattern pattern = Pattern.compile(patternString);
      Matcher matcher = pattern.matcher(serializedString);
      if ((matcher.find()) && (matcher.groupCount() == 5))
      {
        String[] result = new String[5];
        result[0] = matcher.group(1);
        result[1] = matcher.group(2);
        result[2] = matcher.group(3);
        result[3] = matcher.group(4);
        result[4] = matcher.group(5);
        
        return result;
      }
      if (MetaInfo.logger.isInfoEnabled()) {
        MetaInfo.logger.info("StatusInfo failed to match pattern for " + serializedString);
      }
      return null;
    }
  }
  
  public static class Cache
    extends MetaInfo.MetaObject
    implements MetaObjectPermissionChecker
  {
    private static final long serialVersionUID = 1901374789247928L;
    public String adapterClassName;
    public Map<String, Object> reader_properties;
    public Map<String, Object> parser_properties;
    public Map<String, Object> query_properties;
    public Class<?> retType;
    public UUID typename;
    
    public Cache()
    {
      this.adapterClassName = null;
      this.reader_properties = null;
      this.parser_properties = null;
      this.query_properties = null;
      this.typename = null;
      this.retType = null;
    }
    
    public Cache(String name, MetaInfo.Namespace ns, String adapterClassName, Map<String, Object> reader_properties, Map<String, Object> parser_properties, Map<String, Object> query_properties, UUID typename, Class<?> retType)
    {
      super.construct(name, ns, EntityType.CACHE);
      this.adapterClassName = adapterClassName;
      this.reader_properties = reader_properties;
      this.parser_properties = parser_properties;
      this.query_properties = query_properties;
      this.typename = typename;
      this.retType = retType;
    }
    
    public void construct(String name, MetaInfo.Namespace ns, String adapterClassName, Map<String, Object> reader_properties, Map<String, Object> parser_properties, Map<String, Object> query_properties, UUID typename, Class<?> retType)
    {
      super.construct(name, ns, EntityType.CACHE);
      this.adapterClassName = adapterClassName;
      this.reader_properties = reader_properties;
      this.parser_properties = parser_properties;
      this.query_properties = query_properties;
      this.typename = typename;
      this.retType = retType;
    }
    
    public String getAdapterClassName()
    {
      return this.adapterClassName;
    }
    
    public void setAdapterClassName(String adapterClassName)
    {
      this.adapterClassName = adapterClassName;
    }
    
    public Map<String, Object> getReader_properties()
    {
      return this.reader_properties;
    }
    
    public Map<String, Object> getParser_properties()
    {
      return this.parser_properties;
    }
    
    public void setReader_properties(Map<String, Object> reader_properties)
    {
      this.reader_properties = reader_properties;
    }
    
    public Map<String, Object> getQuery_properties()
    {
      return this.query_properties;
    }
    
    public void setQuery_properties(Map<String, Object> query_properties)
    {
      this.query_properties = query_properties;
    }
    
    public Class<?> getRetType()
    {
      return this.retType;
    }
    
    public void setRetType(Class<?> retType)
    {
      this.retType = retType;
    }
    
    public UUID getTypename()
    {
      return this.typename;
    }
    
    public void setTypename(UUID typename)
    {
      this.typename = typename;
    }
    
    public String describe(AuthToken token)
    {
      PropertyTemplate anno = null;
      
      String str = null;
      if (this.adapterClassName == null)
      {
        str = "EVENTTABLE " + (this.type.isGlobal() ? this.name : new StringBuilder().append(this.nsName).append(".").append(this.name).toString()) + " CREATED " + MetaInfo.sdf.format(new Date(this.ctime));
        
        str = str + "\n";
        if (this.description != null) {
          str = str + this.description + "\n";
        }
        str = str + " USING STREAM WITH PROPERTIES";
        str = str + "(\n";
        str = str + prettyPrintMap(this.reader_properties);
        str = str + ") ";
        str = str + "QUERY PROPERTIES ( \n";
        str = str + prettyPrintMap(this.query_properties);
        str = str + ")";
      }
      else
      {
        try
        {
          Class<?> cls = Class.forName(this.adapterClassName);
          anno = (PropertyTemplate)cls.getAnnotation(PropertyTemplate.class);
        }
        catch (ClassNotFoundException e1)
        {
          e1.printStackTrace();
        }
        str = startDescribe() + " USING " + anno.name() + " WITH READER PROPERTIES";
        
        str = str + "(\n";
        str = str + prettyPrintMap(this.reader_properties);
        str = str + " ) \n";
        if (this.parser_properties != null)
        {
          str = str + " WITH PARSER PROPERTIES";
          str = str + "(\n";
          str = str + prettyPrintMap(this.parser_properties);
          str = str + " ) \n";
        }
        str = str + " QUERY PROPERTIES ( \n";
        str = str + prettyPrintMap(this.query_properties);
        str = str + " ) \n";
        
        str = str + " OF TYPE ";
        try
        {
          if (MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.typename, WASecurityManager.TOKEN) != null) {
            str = str + MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.typename, WASecurityManager.TOKEN).name;
          }
        }
        catch (MetaDataRepositoryException e)
        {
          throw new RuntimeException(e.getMessage());
        }
      }
      printDependencyAndStatus();
      return str;
    }
    
    public boolean backedByElasticSearch()
    {
      boolean backedByElasticSearch = false;
      String persistPolicy = (String)getQuery_properties().get("persistPolicy");
      if ((persistPolicy != null) && (persistPolicy.equalsIgnoreCase("TRUE"))) {
        backedByElasticSearch = true;
      }
      return backedByElasticSearch;
    }
    
    public boolean isEventTable()
    {
      if (this.adapterClassName == null) {
        return true;
      }
      return false;
    }
    
    public void setEventTable(boolean EventTable) {}
    
    public List<UUID> getDependencies()
    {
      List<UUID> dependencyList = new ArrayList();
      dependencyList.add(this.typename);
      if (this.adapterClassName == null)
      {
        MetaInfo.Stream stream = null;
        String streamName = (String)this.reader_properties.get("NAME");
        if (streamName != null) {
          try
          {
            if (streamName.indexOf(".") == -1) {
              stream = (MetaInfo.Stream)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.STREAM, this.nsName, streamName, null, WASecurityManager.TOKEN);
            } else {
              stream = (MetaInfo.Stream)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.STREAM, Utility.splitDomain(streamName), Utility.splitName(streamName), null, WASecurityManager.TOKEN);
            }
          }
          catch (SecurityException|MetaDataRepositoryException e)
          {
            MetaInfo.logger.warn(e.getMessage());
          }
        }
        if (stream != null) {
          dependencyList.add(stream.getUuid());
        }
      }
      return dependencyList;
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      
      return json;
    }
    
    public static Map buildActions()
    {
      MetaInfo.MetaObject.buildBaseActions();
      TextProperties adapter_property = MetaInfo.actionablePropertiesFactory.createTextProperties();
      adapter_property.setIsRequired(true);
      ObjectProperties parser_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      parser_property.setIsRequired(false);
      ObjectProperties query_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      query_property.setIsRequired(true);
      MetaObjectProperties type_property = MetaInfo.actionablePropertiesFactory.createMetaObjectProperties();
      type_property.setIsRequired(true);
      actions.put("adapter", adapter_property);
      actions.put("parser", parser_property);
      actions.put("queryProperties", query_property);
      actions.put("typename", type_property);
      return actions;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      
      Map propertiesCopy = new HashMap(this.reader_properties);
      StringBuffer tempBuffer = new StringBuffer();
      ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
      if (this.adapterClassName != null)
      {
        ObjectNode adapterProps = jsonMapper.createObjectNode();
        String simpleHandlerName = this.adapterClassName.split("\\.")[3].split("_")[0];
        tempBuffer.append(MetaInfo.GlobalNamespace.getName()).append(".").append(EntityType.PROPERTYTEMPLATE).append(".").append(simpleHandlerName);
        adapterProps.put("handler", tempBuffer.toString());
        adapterProps.putPOJO("properties", propertiesCopy);
        parent.putPOJO("adapter", adapterProps);
        tempBuffer.setLength(0);
      }
      Map parserPropertiesCopy = new HashMap(this.parser_properties);
      String simpleHandlerNameInParser = (String)parserPropertiesCopy.get("handler");
      if (simpleHandlerNameInParser != null)
      {
        ObjectNode parserProps = jsonMapper.createObjectNode();
        simpleHandlerNameInParser = simpleHandlerNameInParser.split("\\.")[3].split("_")[0];
        tempBuffer.append(MetaInfo.GlobalNamespace.getName()).append(".").append(EntityType.PROPERTYTEMPLATE).append(".").append(simpleHandlerNameInParser);
        parserProps.put("handler", tempBuffer.toString());
        parserProps.putPOJO("properties", parserPropertiesCopy);
        parent.putPOJO("parser", parserProps);
        tempBuffer.setLength(0);
      }
      Map queryPropertiesCopy = new HashMap(this.query_properties);
      parent.putPOJO("queryProperties", queryPropertiesCopy);
      if (this.typename != null) {
        try
        {
          MetaInfo.MetaObject typeInfo = MetaInfo.metadataRepository.getMetaObjectByUUID(this.typename, WASecurityManager.TOKEN);
          if (typeInfo != null) {
            parent.put("typename", typeInfo.getFQN());
          } else {
            parent.putNull("typename");
          }
        }
        catch (MetaDataRepositoryException e) {}
      } else {
        parent.putNull("typename");
      }
      return parent;
    }
    
    public Graph<UUID, Set<UUID>> exportOrder(@NotNull Graph<UUID, Set<UUID>> graph)
    {
      super.exportOrder(graph);
      ((Set)graph.get(this.typename)).add(getUuid());
      return graph;
    }
    
    public boolean checkPermissionForMetaPropertyVariable(AuthToken token)
      throws MetaDataRepositoryException
    {
      String namespace = getNsName();
      Map<String, Object> readerProperties = getReader_properties();
      Map<String, Object> queryProperties = getQuery_properties();
      Map<String, Object> parserProperties = getParser_properties();
      return PropertyVariablePermissionChecker.propertyVariableAccessChecker(token, namespace, new Map[] { readerProperties, queryProperties, parserProperties });
    }
  }
  
  public static class WActionStore
    extends MetaInfo.MetaObject
    implements MetaObjectPermissionChecker
  {
    private static final long serialVersionUID = -8622778110930671241L;
    public UUID contextType;
    public Interval frequency;
    public List<UUID> eventTypes;
    public List<String> eventKeys;
    public Map<String, Object> properties;
    public Type wactionstoretype;
    public int classId = -1;
    
    public WActionStore()
    {
      this.contextType = null;
      this.frequency = null;
      this.eventTypes = null;
      this.eventKeys = null;
      this.properties = null;
      this.type = null;
    }
    
    public void construct(String name, MetaInfo.Namespace ns, UUID contextType, Interval frequency, List<UUID> eventTypes, List<String> eventKeys, Map<String, Object> properties)
    {
      super.construct(name, ns, EntityType.WACTIONSTORE);
      this.contextType = contextType;
      this.frequency = frequency;
      this.eventTypes = eventTypes;
      this.eventKeys = eventKeys;
      this.properties = (properties == null ? Collections.emptyMap() : properties);
      this.wactionstoretype = Type.getType(this.properties, Type.INTERVAL);
    }
    
    public UUID getContextType()
    {
      return this.contextType;
    }
    
    public void setContextType(UUID contextType)
    {
      this.contextType = contextType;
    }
    
    public Interval getFrequency()
    {
      return this.frequency;
    }
    
    public void setFrequency(Interval frequency)
    {
      this.frequency = frequency;
    }
    
    public List<UUID> getEventTypes()
    {
      return this.eventTypes;
    }
    
    public void setEventTypes(List<UUID> eventTypes)
    {
      this.eventTypes = eventTypes;
    }
    
    public List<String> getEventKeys()
    {
      return this.eventKeys;
    }
    
    public void setEventKeys(List<String> eventKeys)
    {
      this.eventKeys = eventKeys;
    }
    
    public Map<String, Object> getProperties()
    {
      return this.properties;
    }
    
    public void setProperties(Map<String, Object> properties)
    {
      this.properties = properties;
      this.wactionstoretype = Type.getType(this.properties, Type.INTERVAL);
    }
    
    public String toString()
    {
      return metaToString() + " context:" + this.contextType + " frequency:" + this.frequency + " event types:" + this.eventTypes + " properties:" + this.properties;
    }
    
    public String describe(AuthToken token)
      throws MetaDataRepositoryException
    {
      String str = startDescribe();
      str = str + "CONTEXT = " + MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.contextType, WASecurityManager.TOKEN).name;
      
      str = str + "\nEVENT TYPE = [ \n";
      for (int i = 0; i < this.eventTypes.size(); i++)
      {
        str = str + MetadataRepository.getINSTANCE().getMetaObjectByUUID((UUID)this.eventTypes.get(i), WASecurityManager.TOKEN).name;
        
        str = str + "(" + (String)this.eventKeys.get(i) + ")";
      }
      str = str + "\n]\n";
      
      boolean showProperties = false;
      switch (this.wactionstoretype.ordinal())
      {
      case 1: 
        str = str + "USING MEMORY\n";
        break;
      case 2: 
        showProperties = true;
        break;
      case 3: 
        showProperties = true;
        str = str + "PERSIST ";
        if (this.frequency == null) {
          str = str + "NONE\n";
        } else if (this.frequency.value == 0L) {
          str = str + "IMMEDIATE\n";
        } else {
          str = str + "EVERY " + this.frequency.toHumanReadable() + '\n';
        }
        break;
      }
      if (showProperties)
      {
        boolean first = true;
        for (Map.Entry<String, Object> entry : this.properties.entrySet())
        {
          String propertyName = (String)entry.getKey();
          if ((!propertyName.toLowerCase().contains("password_encrypted")) && 
          
            (entry.getValue() != null))
          {
            String propertyValue = propertyName.toLowerCase().contains("password") ? "********" : entry.getValue().toString();
            if (!first)
            {
              str = str + ",\n";
            }
            else
            {
              str = str + "USING (\n";
              first = false;
            }
            str = str + "  " + propertyName + ": '" + propertyValue + "'";
          }
        }
        if (!first) {
          str = str + "\n)";
        }
      }
      printDependencyAndStatus();
      return str;
    }
    
    public List<UUID> getDependencies()
    {
      return Collections.singletonList(this.contextType);
    }
    
    public Class<?> getWActionType()
      throws MetaDataRepositoryException
    {
      WALoader loader = WALoader.get();
      MetaInfo.Type contextBeanDef = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.contextType, WASecurityManager.TOKEN);
      
      String fullTableName = this.nsName + "_" + this.name;
      String wactionClassName = "wa.Waction_" + fullTableName;
      Class<?> wactionClass = null;
      try
      {
        wactionClass = loader.loadClass(wactionClassName);
      }
      catch (ClassNotFoundException e1)
      {
        String wbundleUri = WALoader.get().getBundleUri(this.nsName, BundleDefinition.Type.waction, wactionClassName);
        try
        {
          loader.lockBundle(wbundleUri);
          loader.addWactionClass(wactionClassName, contextBeanDef);
          wactionClass = loader.loadClass(wactionClassName);
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
        finally
        {
          loader.unlockBundle(wbundleUri);
        }
      }
      return wactionClass;
    }
    
    List<Pair<String, String>> getColumnsDesc()
    {
      return null;
    }
    
    List<Pair<Integer, List<String>>> getIndexesDesc()
    {
      return null;
    }
    
    public boolean usesInMemoryWActionStore()
    {
      return this.wactionstoretype == Type.IN_MEMORY;
    }
    
    public boolean usesIntervalBasedWActionStore()
    {
      return this.wactionstoretype == Type.INTERVAL;
    }
    
    public boolean usesOldWActionStore()
    {
      return (this.wactionstoretype == Type.INTERVAL) || (this.wactionstoretype == Type.IN_MEMORY);
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      
      return json;
    }
    
    public static Map buildActions()
    {
      MetaInfo.MetaObject.buildBaseActions();
      ObjectProperties context_type_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      context_type_property.setIsRequired(true);
      ObjectProperties frequency_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      ObjectProperties event_type_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      ObjectProperties event_keys_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      ObjectProperties persistence_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      persistence_property.setIsRequired(true);
      actions.put("contextType", context_type_property);
      actions.put("frequency", frequency_property);
      actions.put("eventTypes", event_type_property);
      actions.put("keyField", event_keys_property);
      actions.put("persistence", persistence_property);
      return actions;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
      ObjectNode parent = super.getJsonForClient();
      if (this.contextType != null) {
        try
        {
          MetaInfo.MetaObject ctxType = MetaInfo.metadataRepository.getMetaObjectByUUID(this.contextType, WASecurityManager.TOKEN);
          if (ctxType != null) {
            parent.put("contextType", ctxType.getFQN());
          } else {
            parent.putNull("contextType");
          }
        }
        catch (MetaDataRepositoryException e) {}
      } else {
        parent.putNull("contextType");
      }
      ArrayNode fieldArray = jsonMapper.createArrayNode();
      if (this.eventTypes != null) {
        for (int iter = 0; iter < this.eventTypes.size(); iter++) {
          try
          {
            MetaInfo.Type type = (MetaInfo.Type)MetaInfo.metadataRepository.getMetaObjectByUUID((UUID)this.eventTypes.get(iter), WASecurityManager.TOKEN);
            ObjectNode eventType = jsonMapper.createObjectNode();
            eventType.put("id", type.getUuid().toString());
            eventType.put("typename", type.getFQN());
            eventType.put("keyField", (String)this.eventKeys.get(iter));
            fieldArray.add(eventType);
          }
          catch (MetaDataRepositoryException ex) {}
        }
      }
      parent.putPOJO("eventTypes", fieldArray);
      
      ObjectNode persistenceInfo = jsonMapper.createObjectNode();
      if (this.frequency != null) {
        persistenceInfo.put("frequency", this.frequency.value);
      } else {
        persistenceInfo.putNull("frequency");
      }
      Map<String, Object> processedProperties = Maps.newHashMap();
      if (this.properties != null) {
        for (Map.Entry<String, Object> entry : this.properties.entrySet()) {
          processedProperties.put(entry.getKey(), entry.getValue());
        }
      }
      if (processedProperties.get("storageProvider") == null) {
        if (this.wactionstoretype == Type.STANDARD) {
          processedProperties.put("storageProvider", "elasticsearch");
        } else if ((this.frequency == null) && (this.wactionstoretype == Type.INTERVAL)) {
          processedProperties.put("storageProvider", "inmemory");
        }
      }
      persistenceInfo.putPOJO("properties", processedProperties);
      
      parent.set("persistence", persistenceInfo);
      return parent;
    }
    
    public Graph<UUID, Set<UUID>> exportOrder(@NotNull Graph<UUID, Set<UUID>> graph)
    {
      super.exportOrder(graph);
      ((Set)graph.get(this.contextType)).add(getUuid());
      if (this.eventTypes != null) {
        for (UUID eventType : this.eventTypes) {
          ((Set)graph.get(eventType)).add(getUuid());
        }
      }
      return graph;
    }
    
    public void generateClasses()
      throws Exception
    {
      WALoader waLoader = WALoader.get();
      MetaInfo.Type contextBeanDef = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.contextType, WASecurityManager.TOKEN);
      String wactionContextClassName = contextBeanDef.className + "_wactionContext";
      String fullTableName = this.nsName + "_" + this.name;
      String wactionClassName = "wa.Waction_" + fullTableName;
      this.classId = waLoader.getClassId(wactionContextClassName);
      String cbundleUri = WALoader.get().getBundleUri(this.nsName, Type.context, wactionContextClassName);
      WALoader.get().lockBundle(cbundleUri);
      try
      {
        if ((this.eventTypes == null) || (this.eventTypes.isEmpty())) {
          WALoader.get().addWactionContextClassNoEventType(contextBeanDef);
        } else {
          WALoader.get().addWactionContextClass(contextBeanDef);
        }
      }
      catch (IllegalArgumentException e)
      {
        if (MetaInfo.logger.isDebugEnabled()) {
          MetaInfo.logger.debug("Context Class is already defined elsewhere for " + getFullName(), e);
        }
      }
      catch (Exception e)
      {
        MetaInfo.logger.error("Problem creating waction context class for " + getFullName(), e);
      }
      finally
      {
        WALoader.get().unlockBundle(cbundleUri);
      }
      String wbundleUri = WALoader.get().getBundleUri(this.nsName, BundleDefinition.Type.waction, wactionClassName);
      WALoader.get().lockBundle(wbundleUri);
      try
      {
        WALoader.get().addWactionClass(wactionClassName, contextBeanDef);
        if (MetaInfo.logger.isDebugEnabled()) {
          MetaInfo.logger.debug("Generated WAction class for WAction Store " + this.name + " of type " + contextBeanDef.getFullName());
        }
      }
      catch (IllegalArgumentException e)
      {
        if (MetaInfo.logger.isDebugEnabled()) {
          MetaInfo.logger.debug("WAction Class is already defined elsewhere for " + getFullName(), e);
        }
      }
      catch (Exception e)
      {
        MetaInfo.logger.error("Problem creating waction context class for " + getFullName(), e);
      }
      finally
      {
        WALoader.get().unlockBundle(wbundleUri);
      }
      String bundleUri = WALoader.get().createIfNotExistsBundleDefinition(this.nsName, BundleDefinition.Type.fieldFactory, this.name);
      for (String fname : contextBeanDef.fields.keySet()) {
        try
        {
          genFieldFactory(bundleUri, contextBeanDef.className, fname, this.name + "_" + contextBeanDef.name, contextBeanDef.nsName);
          MetaInfo.logger.info("Generated Field Factory class for context field " + fname + " in WAction Store: " + this.name + " of type: " + contextBeanDef.getFullName());
        }
        catch (Exception e)
        {
          if (MetaInfo.logger.isDebugEnabled()) {
            MetaInfo.logger.debug("Field factory class is already generated for " + fname + " in " + contextBeanDef.getFullName(), e);
          }
        }
      }
    }
    
    private void genFieldFactory(String bundleUri, String eventTypeClassName, String fieldName, String typeName, String typeNamespaceName)
      throws IllegalArgumentException, NotFoundException, CannotCompileException, IOException
    {
      WALoader wal = WALoader.get();
      ClassPool pool = wal.getBundlePool(bundleUri);
      String className = "FieldFactory_" + fieldName + "_" + typeNamespaceName + "_" + typeName;
      CtClass cc = pool.makeClass(className);
      CtClass sup = pool.get(WactionStore.FieldFactory.class.getName());
      cc.setSuperclass(sup);
      
      String code = "public Object getField(Object obj)\n{\n\t" + eventTypeClassName + " tmp = (" + eventTypeClassName + ")obj;\n" + "\treturn " + FieldToObject.genConvert(new StringBuilder().append("tmp.").append(fieldName).toString()) + ";\n" + "}\n";
      
      CtMethod m = CtNewMethod.make(code, cc);
      cc.addMethod(m);
      
      String fieldString = "public String fieldName = \"" + fieldName + '"' + ";";
      CtField ctField = CtField.make(fieldString, cc);
      ctField.setModifiers(1);
      cc.addField(ctField);
      
      String getFieldNameMethod = "public String getFieldName()\n{\n\t return fieldName;\n}\n";
      
      CtMethod cm = CtNewMethod.make(getFieldNameMethod, cc);
      cm.setModifiers(1);
      cc.addMethod(cm);
      cc.setModifiers(cc.getModifiers() & 0xFBFF);
      cc.setModifiers(1);
      wal.addBundleClass(bundleUri, className, cc.toBytecode(), false);
    }
    
    public void removeGeneratedClasses()
      throws MetaDataRepositoryException
    {
      MetaInfo.Type contextBeanDef = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.contextType, WASecurityManager.TOKEN);
      String wactionContextClassName = contextBeanDef.className + "_wactionContext";
      String fullTableName = this.nsName + "_" + this.name;
      String wactionClassName = "wa.Waction_" + fullTableName;
      WALoader.get().removeBundle(this.nsName, Type.fieldFactory, this.name);
      WALoader.get().removeBundle(this.nsName, Type.context, wactionContextClassName);
      WALoader.get().removeBundle(this.nsName, Type.waction, wactionClassName);
    }
    
    public boolean checkPermissionForMetaPropertyVariable(AuthToken token)
      throws MetaDataRepositoryException
    {
      String namespace = getNsName();
      Map<String, Object> readerProperties = getProperties();
      return PropertyVariablePermissionChecker.propertyVariableAccessChecker(token, namespace, new Map[] { readerProperties });
    }
  }
  
  public static class AlertSubscriber
    extends MetaInfo.MetaObject
  {
    private static final long serialVersionUID = -693870604910801142L;
    public String adapterClassName;
    public Map<String, Object> properties;
    
    public void construct(String name, MetaInfo.Namespace ns, String adapterClassName, Map<String, Object> props)
    {
      super.construct(name, ns, EntityType.ALERTSUBSCRIBER);
      this.adapterClassName = adapterClassName;
      this.properties = props;
    }
    
    public String getAdapterClassName()
    {
      return this.adapterClassName;
    }
    
    public void setAdapterClassName(String adapterClassName)
    {
      this.adapterClassName = adapterClassName;
    }
    
    public Map<String, Object> getProperties()
    {
      return this.properties;
    }
    
    public void setProperties(Map<String, Object> properties)
    {
      this.properties = properties;
    }
    
    public String toString()
    {
      return this.type + " " + this.name + " " + this.adapterClassName + " " + this.properties;
    }
    
    public static Map buildActions()
    {
      MetaInfo.MetaObject.buildBaseActions();
      TextProperties adapter_class_property = MetaInfo.actionablePropertiesFactory.createTextProperties();
      adapter_class_property.setIsRequired(true);
      ObjectProperties properties_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      properties_property.setIsRequired(true);
      return actions;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      ObjectMapper objectMapper = ObjectMapperFactory.getInstance();
      parent.put("adapter", this.adapterClassName);
      ObjectNode objectNode = objectMapper.createObjectNode();
      objectNode.putPOJO("properties", this.properties);
      return parent;
    }
  }
  
  public static class ContactMechanism
    implements Serializable
  {
    private static final long serialVersionUID = 7025105184172001669L;
    private boolean isDefault;
    private ContactType type;
    private String data;
    private int index;
    
    public static enum ContactType
    {
      email,  phone,  sms,  web;
      
      private ContactType() {}
    }
    
    public ContactMechanism()
    {
      construct(false, null, null);
    }
    
    public void construct(ContactType type, String data)
    {
      construct(false, type, data);
    }
    
    public void construct(boolean isDefault, ContactType type, String data)
    {
      this.isDefault = isDefault;
      this.type = type;
      this.data = data;
    }
    
    public boolean isDefault()
    {
      return this.isDefault;
    }
    
    public void setDefault(boolean isDefault)
    {
      this.isDefault = isDefault;
    }
    
    public ContactType getType()
    {
      return this.type;
    }
    
    public void setType(ContactType type)
    {
      this.type = type;
    }
    
    public String getData()
    {
      return this.data;
    }
    
    public void setData(String data)
    {
      this.data = data;
    }
    
    public int getIndex()
    {
      return this.index;
    }
    
    public void setIndex(int index)
    {
      this.index = index;
    }
    
    public boolean equals(Object other)
    {
      if (((other instanceof ContactMechanism)) && 
        (((ContactMechanism)other).type == this.type) && (((ContactMechanism)other).data.equals(this.data))) {
        return true;
      }
      return false;
    }
    
    public int hashCode()
    {
      return new HashCodeBuilder().append(this.type.toString()).append(this.data).toHashCode();
    }
    
    public String toString()
    {
      return "type : " + this.type + " value : " + this.data;
    }
  }
  
  public static class User
    extends MetaInfo.MetaObject
    implements Permissable, Roleable
  {
    private static final long serialVersionUID = -8964675659436023782L;
    private String userId;
    private String firstName;
    private String lastName;
    private String mainEmail;
    private String encryptedPassword;
    private String defaultNamespace;
    private String userTimeZone;
    private List<MetaInfo.ContactMechanism> contactMechanisms = new ArrayList();
    private List<ObjectPermission> permissions = new ArrayList();
    private List<UUID> roleUUIDs = new ArrayList();
    private transient List<ObjectPermission> allPermissions;
    private String ldap = null;
    
    public static enum AUTHORIZATION_TYPE
    {
      INTERNAL,  LDAP;
      
      private AUTHORIZATION_TYPE() {}
    }
    
    private AUTHORIZATION_TYPE originType = AUTHORIZATION_TYPE.INTERNAL;
    
    public User()
    {
      this.type = EntityType.USER;
      this.namespaceId = MetaInfo.GlobalNamespace.uuid;
      this.nsName = MetaInfo.GlobalNamespace.name;
      this.userTimeZone = "";
    }
    
    public void construct(String userId)
    {
      construct(userId, null, null, null, null, null, null);
    }
    
    public void construct(String userId, String encryptedPassword)
    {
      construct(userId, encryptedPassword, null, null, null, null, null);
    }
    
    public void construct(String userId, String encryptedPassword, List<MetaInfo.Role> roles)
    {
      construct(userId, encryptedPassword, null, null, null, null, roles);
    }
    
    public void construct(String userId, String encryptedPassword, String mainEmail)
    {
      construct(userId, encryptedPassword, mainEmail, null, null, null, null);
    }
    
    public void construct(String userId, String encryptedPassword, String mainEmail, String firstName, String lastName)
    {
      construct(userId, encryptedPassword, mainEmail, firstName, lastName, null, null);
    }
    
    public void construct(String userId, String encryptedPassword, String mainEmail, String firstName, String lastName, List<MetaInfo.ContactMechanism> contactMechanisms, List<MetaInfo.Role> roles)
    {
      super.construct(userId, MetaInfo.GlobalNamespace, EntityType.USER);
      this.userId = userId;
      this.firstName = firstName;
      this.lastName = lastName;
      this.mainEmail = mainEmail;
      this.userTimeZone = "";
      try
      {
        this.encryptedPassword = WASecurityManager.encrypt(encryptedPassword, this.uuid.toEightBytes());
      }
      catch (UnsupportedEncodingException e)
      {
        MetaInfo.logger.error(e);
      }
      catch (GeneralSecurityException e)
      {
        MetaInfo.logger.error(e);
      }
      if (contactMechanisms != null) {
        this.contactMechanisms.addAll(contactMechanisms);
      }
      if (mainEmail != null)
      {
        MetaInfo.ContactMechanism memail = new MetaInfo.ContactMechanism();
        memail.construct(MetaInfo.ContactMechanism.ContactType.email, mainEmail);
        this.contactMechanisms.add(memail);
      }
      if (roles != null) {
        for (MetaInfo.Role r : roles) {
          this.roleUUIDs.add(r.getUuid());
        }
      }
      this.defaultNamespace = null;
    }
    
    public String getLdap()
    {
      return this.ldap;
    }
    
    public void setLdap(String ldap)
    {
      this.ldap = ldap;
    }
    
    public AUTHORIZATION_TYPE getOriginType()
    {
      return this.originType;
    }
    
    public void setOriginType(AUTHORIZATION_TYPE originType)
    {
      this.originType = originType;
    }
    
    public int hashCode()
    {
      return this.userId.hashCode();
    }
    
    public List<UUID> getRoleUUIDs()
    {
      return this.roleUUIDs;
    }
    
    public void setRoleUUIDs(List<UUID> roleUUIDs)
    {
      this.roleUUIDs = roleUUIDs;
    }
    
    public void setUserId(String userId)
    {
      this.userId = userId;
    }
    
    public void setPermissions(List<ObjectPermission> permissions)
    {
      if (this.permissions == null)
      {
        this.permissions = permissions;
      }
      else
      {
        this.permissions.clear();
        this.permissions.addAll(permissions);
      }
    }
    
    public boolean equals(Object obj)
    {
      if ((obj instanceof User))
      {
        User other = (User)obj;
        return other.userId.equals(this.userId);
      }
      return false;
    }
    
    public String toString()
    {
      return this.userId;
    }
    
    public String getDefaultNamespace()
    {
      return this.defaultNamespace;
    }
    
    public void setDefaultNamespace(String defaultNamespace)
    {
      this.defaultNamespace = defaultNamespace;
    }
    
    public String getUserId()
    {
      return this.userId;
    }
    
    public String getFirstName()
    {
      return this.firstName;
    }
    
    public void setFirstName(String firstName)
    {
      this.firstName = firstName;
    }
    
    public String getUserTimeZone()
    {
      return this.userTimeZone;
    }
    
    public void setUserTimeZone(String tz)
    {
      this.userTimeZone = tz;
    }
    
    public String getLastName()
    {
      return this.lastName;
    }
    
    public void setLastName(String lastName)
    {
      this.lastName = lastName;
    }
    
    public String getMainEmail()
    {
      return this.mainEmail;
    }
    
    public void setMainEmail(String mainEmail)
    {
      this.mainEmail = mainEmail;
      MetaInfo.ContactMechanism memail = new MetaInfo.ContactMechanism();
      memail.construct(MetaInfo.ContactMechanism.ContactType.email, mainEmail);
      if (!this.contactMechanisms.contains(memail)) {
        this.contactMechanisms.add(memail);
      }
    }
    
    public String getEncryptedPassword()
    {
      return this.encryptedPassword;
    }
    
    public void setEncryptedPassword(String encryptedPassword)
    {
      this.encryptedPassword = encryptedPassword;
    }
    
    private void updateContactIndices()
    {
      for (MetaInfo.ContactMechanism contact : this.contactMechanisms) {
        contact.setIndex(this.contactMechanisms.indexOf(contact));
      }
    }
    
    public List<MetaInfo.ContactMechanism> getContactMechanisms()
    {
      return this.contactMechanisms;
    }
    
    public void addContactMechanism(MetaInfo.ContactMechanism.ContactType type, String data)
    {
      MetaInfo.ContactMechanism contact = new MetaInfo.ContactMechanism();
      contact.construct(type, data);
      if (!this.contactMechanisms.contains(contact))
      {
        this.contactMechanisms.add(contact);
        updateContactIndices();
      }
    }
    
    public void removeContactMechanism(int index)
    {
      if (index < this.contactMechanisms.size())
      {
        this.contactMechanisms.remove(index);
        updateContactIndices();
      }
    }
    
    public void updateContactMechanism(int index, MetaInfo.ContactMechanism.ContactType type, String data)
    {
      if (index < this.contactMechanisms.size())
      {
        MetaInfo.ContactMechanism contact = (MetaInfo.ContactMechanism)this.contactMechanisms.get(index);
        contact.setType(type);
        contact.setData(data);
      }
    }
    
    public void setDefaultContactMechanism(int index)
    {
      if (index < this.contactMechanisms.size()) {
        for (MetaInfo.ContactMechanism contact : this.contactMechanisms) {
          contact.setDefault(contact.getIndex() == index);
        }
      }
    }
    
    public void grantPermission(ObjectPermission permission)
      throws SecurityException, MetaDataRepositoryException
    {
      if (this.permissions.contains(permission)) {
        return;
      }
      ObjectPermission reversePermission = new ObjectPermission(permission, ObjectPermission.PermissionType.disallow);
      if (this.permissions.contains(reversePermission)) {
        this.permissions.remove(reversePermission);
      }
      refreshPermissions();
      for (ObjectPermission perm : getAllPermissions()) {
        if (perm.equals(permission)) {
          return;
        }
      }
      this.permissions.add(permission);
    }
    
    public void grantPermissions(List<ObjectPermission> permissionsList)
      throws SecurityException, MetaDataRepositoryException
    {
      for (ObjectPermission perm : permissionsList) {
        grantPermission(perm);
      }
    }
    
    public void resetPermissions(List<ObjectPermission> permissionsList)
      throws SecurityException
    {
      if (permissionsList == null) {
        permissionsList = new ArrayList();
      }
      this.permissions.clear();
      this.permissions.addAll(permissionsList);
    }
    
    public void revokePermission1(ObjectPermission permission)
      throws SecurityException, MetaDataRepositoryException
    {
      if ((permission.getType().equals(ObjectPermission.PermissionType.disallow)) && 
        (this.permissions.contains(permission)))
      {
        this.permissions.remove(permission);
        return;
      }
      ObjectPermission reversePermission = new ObjectPermission(permission, ObjectPermission.PermissionType.disallow);
      if (this.permissions.contains(reversePermission)) {
        return;
      }
      if (this.permissions.contains(permission)) {
        this.permissions.remove(permission);
      }
      refreshPermissions();
      for (ObjectPermission perm : getAllPermissions()) {
        if (perm.implies(permission))
        {
          this.permissions.add(reversePermission);
          return;
        }
      }
      this.permissions.add(reversePermission);
    }
    
    public void revokePermission(ObjectPermission permToBeRevoked)
      throws SecurityException, MetaDataRepositoryException
    {
      UserAndRoleUtility util = new UserAndRoleUtility();
      if (util.isDisallowedPermission(permToBeRevoked)) {
        throw new SecurityException("Revoking a disallowed permission is NOT allowed. Grant to remove it");
      }
      boolean userHasPermission = util.doesListHasImpliedPerm(this.permissions, permToBeRevoked);
      
      boolean userHasPermissionThruRole = util.doesListHasImpliedPerm(getPermissionsThruRoles(), permToBeRevoked);
      if (!userHasPermission)
      {
        String exceptionString = "User has no permission that implies : \"" + permToBeRevoked + "\". Can't revoke a permission that is NOT granted yet.";
        MetaInfo.logger.warn("User has no permission that implies : \"" + permToBeRevoked + "\". Can't revoke a permission that is NOT granted yet.");
        if (userHasPermissionThruRole) {
          exceptionString = exceptionString + "But user has that permission through a role. Revoke permission from Role.";
        }
        throw new SecurityException(exceptionString);
      }
      ObjectPermission revPermToBeRevoked = new ObjectPermission(permToBeRevoked, ObjectPermission.PermissionType.disallow);
      if (this.permissions.contains(revPermToBeRevoked))
      {
        MetaInfo.logger.warn(revPermToBeRevoked + " permission already revoked.");
        return;
      }
      boolean exactPermissionExists = util.doesListHasExactPerm(this.permissions, permToBeRevoked);
      if (exactPermissionExists)
      {
        this.permissions.remove(permToBeRevoked);
        
        List<ObjectPermission> list = util.getDependentPerms(permToBeRevoked, this.permissions);
        for (ObjectPermission perm : list) {
          this.permissions.remove(perm);
        }
      }
      if (util.doesListHasImpliedPerm(this.permissions, permToBeRevoked)) {
        this.permissions.add(revPermToBeRevoked);
      }
      refreshPermissions();
    }
    
    public void revokePermissions(List<ObjectPermission> permissionsList)
      throws SecurityException, MetaDataRepositoryException
    {
      for (ObjectPermission perm : permissionsList) {
        revokePermission(perm);
      }
    }
    
    public void grantRole(MetaInfo.Role role)
    {
      if (this.roleUUIDs.contains(role.getUuid())) {
        return;
      }
      this.roleUUIDs.add(role.getUuid());
    }
    
    public void grantRoles(List<MetaInfo.Role> roles)
    {
      if ((roles == null) || (roles.size() == 0)) {
        return;
      }
      Iterator<MetaInfo.Role> iter = roles.iterator();
      while (iter.hasNext())
      {
        UUID id = ((MetaInfo.Role)iter.next()).getUuid();
        if (!this.roleUUIDs.contains(id)) {
          this.roleUUIDs.add(id);
        }
      }
    }
    
    public void revokeRole(MetaInfo.Role role)
    {
      if (!this.roleUUIDs.contains(role.getUuid())) {
        return;
      }
      this.roleUUIDs.remove(role.getUuid());
    }
    
    public void revokeRoles(List<MetaInfo.Role> roles)
    {
      if ((roles == null) || (roles.size() == 0)) {
        return;
      }
      Iterator<MetaInfo.Role> iter = roles.iterator();
      while (iter.hasNext())
      {
        UUID id = ((MetaInfo.Role)iter.next()).getUuid();
        if (this.roleUUIDs.contains(id)) {
          this.roleUUIDs.remove(id);
        }
      }
    }
    
    public List<MetaInfo.Role> getRoles()
      throws MetaDataRepositoryException
    {
      List<MetaInfo.Role> rs = null;
      if (this.roleUUIDs != null)
      {
        rs = new ArrayList();
        for (UUID id : this.roleUUIDs)
        {
          MetaInfo.Role r = (MetaInfo.Role)MetadataRepository.getINSTANCE().getMetaObjectByUUID(id, WASecurityManager.TOKEN);
          if ((r != null) && (!rs.contains(r))) {
            rs.add(r);
          }
        }
      }
      return rs;
    }
    
    public void setRoles(List<MetaInfo.Role> roles)
    {
      if (roles != null) {
        for (MetaInfo.Role r : roles) {
          if (!this.roleUUIDs.contains(r.getUuid())) {
            this.roleUUIDs.add(r.getUuid());
          }
        }
      }
    }
    
    public void resetRoles(List<MetaInfo.Role> roles)
    {
      if (roles == null)
      {
        this.roleUUIDs.clear();
      }
      else
      {
        this.roleUUIDs.clear();
        setRoles(roles);
      }
    }
    
    List<String> convertToNewSyntax(List<ObjectPermission> permissions)
    {
      List<String> resultSet = new ArrayList();
      if (permissions == null) {
        return resultSet;
      }
      for (ObjectPermission permission : permissions)
      {
        String permissionString = permission.toString();
        String[] splitArray = permissionString.split(":");
        if (splitArray.length >= 4)
        {
          String[] subTypes = splitArray[2].split(",");
          StringBuffer stringBuffer = new StringBuffer();
          for (int i = 0; i < subTypes.length; i++)
          {
            stringBuffer.append(splitArray.length == 4 ? "GRANT " : "REVOKE ");
            if (splitArray[1].equalsIgnoreCase("*")) {
              stringBuffer.append("ALL ");
            } else {
              stringBuffer.append(splitArray[1].toUpperCase() + " ");
            }
            stringBuffer.append("ON ");
            stringBuffer.append(subTypes[i] + " ");
            stringBuffer.append(splitArray[0] + "." + splitArray[3]);
            if (i != subTypes.length - 1) {
              stringBuffer.append(", ");
            }
          }
          resultSet.add(stringBuffer.toString());
        }
      }
      return resultSet;
    }
    
    public List<ObjectPermission> getPermissions()
    {
      return this.permissions;
    }
    
    @JsonIgnore
    public List<ObjectPermission> getPermissionsThruRoles()
      throws MetaDataRepositoryException
    {
      List<ObjectPermission> perms = new ArrayList();
      if ((this.roleUUIDs != null) && (this.roleUUIDs.size() > 0)) {
        for (UUID id : this.roleUUIDs)
        {
          MetaInfo.Role temprole = (MetaInfo.Role)MetadataRepository.getINSTANCE().getMetaObjectByUUID(id, WASecurityManager.TOKEN);
          if (temprole != null) {
            perms.addAll(temprole.getAllPermissions());
          }
        }
      }
      return perms;
    }
    
    @JsonIgnore
    public List<ObjectPermission> getAllPermissions()
    {
      return this.allPermissions;
    }
    
    public void refreshPermissions()
      throws MetaDataRepositoryException
    {
      this.allPermissions = new ArrayList();
      this.allPermissions.addAll(getPermissionsThruRoles());
      this.allPermissions.addAll(this.permissions);
    }
    
    public void setContactMechanisms(List<MetaInfo.ContactMechanism> contactMechanisms)
    {
      if (contactMechanisms != null) {
        this.contactMechanisms = contactMechanisms;
      }
    }
    
    public String describe(AuthToken token)
      throws MetaDataRepositoryException
    {
      String desc = startDescribe() + "USERID " + this.userId + "\n";
      if ((this.firstName != null) && (!this.firstName.isEmpty())) {
        desc = desc + "FIRSTNAME " + this.firstName + "\n";
      }
      if ((this.lastName != null) && (!this.lastName.isEmpty())) {
        desc = desc + "LASTNAME " + this.lastName + "\n";
      }
      if ((this.userTimeZone != null) && (!this.userTimeZone.isEmpty())) {
        desc = desc + "TIMEZONE " + this.userTimeZone + "\n";
      }
      desc = desc + "CONTACT THROUGH " + this.contactMechanisms + "\n";
      desc = desc + "ROLES {";
      boolean notFirst;
      if (this.roleUUIDs != null)
      {
        notFirst = false;
        for (UUID roleUUID : this.roleUUIDs)
        {
          MetaInfo.Role r = (MetaInfo.Role)MetadataRepository.getINSTANCE().getMetaObjectByUUID(roleUUID, WASecurityManager.TOKEN);
          if (r != null)
          {
            if (notFirst) {
              desc = desc + ", ";
            } else {
              notFirst = true;
            }
            desc = desc + r.toString();
          }
        }
      }
      desc = desc + "}\n";
      desc = desc + "PERMISSIONS " + convertToNewSyntax(getPermissions()) + "\n";
      desc = desc + this.originType.toString() + " user.";
      return desc;
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      
      return json;
    }
    
    public static User deserialize(JsonNode jsonNode)
      throws JsonParseException, JsonMappingException, IOException
    {
      if (jsonNode == null) {
        return null;
      }
      return MetaInfoJsonSerializer.UserJsonSerializer.deserialize(jsonNode.toString());
    }
    
    public boolean hasGlobalAdminRole()
    {
      try
      {
        for (MetaInfo.Role role : getRoles()) {
          if (role.isGlobalAdminRole()) {
            return true;
          }
        }
        return false;
      }
      catch (MetaDataRepositoryException e)
      {
        MetaInfo.logger.error("Error looking up user roles", e);
      }
      return false;
    }
    
    public static Map buildActions()
    {
      MetaInfo.MetaObject.buildBaseActions();
      TextProperties userid_property = MetaInfo.actionablePropertiesFactory.createTextProperties();
      TextProperties fname_property = MetaInfo.actionablePropertiesFactory.createTextProperties();
      TextProperties lname_property = MetaInfo.actionablePropertiesFactory.createTextProperties();
      TextProperties mainemail_property = MetaInfo.actionablePropertiesFactory.createTextProperties();
      ObjectProperties contact_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      ObjectProperties permission_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      ObjectProperties role_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      actions.put("firstName", fname_property);
      actions.put("userId", userid_property);
      actions.put("lastName", lname_property);
      actions.put("mainEmail", mainemail_property);
      actions.put("contactMechanisms", contact_property);
      actions.put("permissions", permission_property);
      actions.put("roleUUIDs", role_property);
      return actions;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      parent.put("userId", this.userId);
      parent.put("firstName", this.firstName);
      parent.put("lastName", this.lastName);
      parent.put("mainEmail", this.mainEmail);
      parent.put("defaultNamespace", this.defaultNamespace);
      parent.putPOJO("contactMechanisms", this.contactMechanisms);
      parent.putPOJO("permissions", this.permissions);
      parent.putPOJO("roleUUIDs", this.roleUUIDs);
      return parent;
    }
  }
  
  public static class Role
    extends MetaInfo.MetaObject
    implements Permissable, Roleable
  {
    private static final long serialVersionUID = 1450294322887913505L;
    public String domain;
    public String roleName;
    public List<ObjectPermission> permissions = new ArrayList();
    public List<UUID> roleUUIDs = new ArrayList();
    
    public Role()
    {
      this.uuid = new UUID(System.currentTimeMillis());
      this.type = EntityType.ROLE;
    }
    
    public void construct(MetaInfo.Namespace ns, String roleName)
    {
      construct(ns, roleName, null, null);
    }
    
    public void construct(MetaInfo.Namespace ns, String roleName, List<Role> roles, List<ObjectPermission> permissions)
    {
      super.construct(roleName, ns, EntityType.ROLE);
      this.domain = ns.name;
      this.roleName = roleName;
      if (roles != null) {
        for (Role r : roles) {
          this.roleUUIDs.add(r.getUuid());
        }
      }
      if (permissions != null) {
        this.permissions = permissions;
      }
    }
    
    public UUID getUuid()
    {
      return this.uuid;
    }
    
    public String getDomain()
    {
      return this.domain;
    }
    
    public String getRoleName()
    {
      return this.roleName;
    }
    
    public List<ObjectPermission> getPermissions()
    {
      return this.permissions;
    }
    
    public List<UUID> getRoleUUIDs()
    {
      return this.roleUUIDs;
    }
    
    @JSON(include=false)
    @JsonIgnore
    public String getRole()
    {
      return this.domain + ":" + this.roleName;
    }
    
    public String toString()
    {
      return this.domain + "." + this.roleName;
    }
    
    public boolean equals(Object obj)
    {
      if ((obj instanceof Role))
      {
        Role other = (Role)obj;
        if ((this.domain.equals(other.domain)) && (this.roleName.equals(other.roleName))) {
          return true;
        }
      }
      return false;
    }
    
    public void grantPermission(ObjectPermission permission)
      throws SecurityException, MetaDataRepositoryException
    {
      if (this.permissions.contains(permission)) {
        return;
      }
      ObjectPermission reversePermission = new ObjectPermission(permission, ObjectPermission.PermissionType.disallow);
      if (this.permissions.contains(reversePermission)) {
        this.permissions.remove(reversePermission);
      }
      for (ObjectPermission perm : getAllPermissions()) {
        if (perm.equals(permission)) {
          return;
        }
      }
      this.permissions.add(permission);
    }
    
    public void grantPermissions(List<ObjectPermission> permissionsList)
      throws SecurityException, MetaDataRepositoryException
    {
      Iterator<ObjectPermission> iter = permissionsList.iterator();
      while (iter.hasNext()) {
        grantPermission((ObjectPermission)iter.next());
      }
    }
    
    public void revokePermission1(ObjectPermission permission)
      throws SecurityException, MetaDataRepositoryException
    {
      ObjectPermission reversePermission = new ObjectPermission(permission, ObjectPermission.PermissionType.disallow);
      if (this.permissions.contains(reversePermission)) {
        return;
      }
      if (this.permissions.contains(permission)) {
        this.permissions.remove(permission);
      }
      for (ObjectPermission perm : getAllPermissions()) {
        if (perm.implies(permission))
        {
          this.permissions.add(reversePermission);
          return;
        }
      }
    }
    
    public void revokePermission(ObjectPermission permToBeRevoked)
      throws SecurityException, MetaDataRepositoryException
    {
      UserAndRoleUtility util = new UserAndRoleUtility();
      if (util.isDisallowedPermission(permToBeRevoked)) {
        throw new SecurityException("Revoking a disallowed permission is NOT allowed. Grant to remove it");
      }
      boolean roleHasPermission = util.doesListHasImpliedPerm(this.permissions, permToBeRevoked);
      
      boolean roleHasPermissionThruRole = util.doesListHasImpliedPerm(getPermissionsThruRoles(), permToBeRevoked);
      if (!roleHasPermission)
      {
        String exceptionString = "Role has no permission that implies : \"" + permToBeRevoked + "\". Can't revoke a permission that is NOT granted yet.";
        if (roleHasPermissionThruRole) {
          exceptionString = exceptionString + "But role has that permission through another role. Revoke permission from that Role.";
        }
        throw new SecurityException(exceptionString);
      }
      ObjectPermission revPermToBeRevoked = new ObjectPermission(permToBeRevoked, ObjectPermission.PermissionType.disallow);
      if (this.permissions.contains(revPermToBeRevoked))
      {
        MetaInfo.logger.warn(revPermToBeRevoked + " permission already revoked.");
        return;
      }
      boolean exactPermissionExists = util.doesListHasExactPerm(this.permissions, permToBeRevoked);
      if (exactPermissionExists)
      {
        this.permissions.remove(permToBeRevoked);
        
        List<ObjectPermission> list = util.getDependentPerms(permToBeRevoked, this.permissions);
        for (ObjectPermission perm : list) {
          this.permissions.remove(perm);
        }
      }
      if (util.doesListHasImpliedPerm(this.permissions, permToBeRevoked)) {
        this.permissions.add(revPermToBeRevoked);
      }
    }
    
    public void revokePermissions(List<ObjectPermission> permissionsList)
      throws SecurityException, MetaDataRepositoryException
    {
      Iterator<ObjectPermission> iter = permissionsList.iterator();
      while (iter.hasNext()) {
        revokePermission((ObjectPermission)iter.next());
      }
    }
    
    public void grantRole(Role role)
    {
      if (this.uuid.equals(role.getUuid())) {
        return;
      }
      if (this.roleUUIDs.contains(role.getUuid())) {
        return;
      }
      this.roleUUIDs.add(role.getUuid());
    }
    
    public void grantRoles(List<Role> roles)
    {
      if ((roles == null) || (roles.size() == 0)) {
        return;
      }
      Iterator<Role> iter = roles.iterator();
      while (iter.hasNext())
      {
        UUID id = ((Role)iter.next()).getUuid();
        if (!this.roleUUIDs.contains(id)) {
          this.roleUUIDs.add(id);
        }
      }
    }
    
    public void revokeRole(Role role)
    {
      if (!this.roleUUIDs.contains(role)) {
        return;
      }
      this.roleUUIDs.remove(role);
    }
    
    public void revokeRoles(List<Role> roles)
    {
      if ((roles == null) || (roles.size() == 0)) {
        return;
      }
      Iterator<Role> iter = roles.iterator();
      while (iter.hasNext())
      {
        UUID id = ((Role)iter.next()).getUuid();
        if (this.roleUUIDs.contains(id)) {
          this.roleUUIDs.remove(id);
        }
      }
    }
    
    public List<Role> getRoles()
      throws MetaDataRepositoryException
    {
      List<Role> r1 = null;
      if (this.roleUUIDs != null)
      {
        r1 = new ArrayList();
        Iterator<UUID> iter = this.roleUUIDs.iterator();
        while (iter.hasNext())
        {
          Role r11 = (Role)MetadataRepository.getINSTANCE().getMetaObjectByUUID((UUID)iter.next(), WASecurityManager.TOKEN);
          if (r11 != null) {
            r1.add(r11);
          }
        }
      }
      return r1;
    }
    
    public void setRoles(List<Role> roles)
    {
      if (roles != null) {
        for (Role r : roles) {
          this.roleUUIDs.add(r.getUuid());
        }
      }
    }
    
    @JsonIgnore
    public List<ObjectPermission> getPermissionsThruRoles()
      throws MetaDataRepositoryException
    {
      List<ObjectPermission> perms = new ArrayList();
      if ((this.roleUUIDs != null) && (this.roleUUIDs.size() > 0)) {
        for (UUID id : this.roleUUIDs)
        {
          Role temprole = (Role)MetadataRepository.getINSTANCE().getMetaObjectByUUID(id, WASecurityManager.TOKEN);
          if (temprole != null) {
            perms.addAll(temprole.getAllPermissions());
          }
        }
      }
      return perms;
    }
    
    @JsonIgnore
    public List<ObjectPermission> getAllPermissions()
      throws MetaDataRepositoryException
    {
      List<ObjectPermission> allPermissions = new ArrayList();
      allPermissions.addAll(getPermissionsThruRoles());
      if (this.permissions != null) {
        allPermissions.addAll(this.permissions);
      }
      return allPermissions;
    }
    
    public String describe(AuthToken token)
      throws MetaDataRepositoryException
    {
      String desc = startDescribe();
      desc = desc + "ROLES {";
      boolean notFirst;
      if (this.roleUUIDs != null)
      {
        notFirst = false;
        for (UUID roleUUID : this.roleUUIDs)
        {
          if (notFirst) {
            desc = desc + ", ";
          } else {
            notFirst = true;
          }
          Role r = (Role)MetadataRepository.getINSTANCE().getMetaObjectByUUID(roleUUID, WASecurityManager.TOKEN);
          
          desc = desc + r.toString();
        }
      }
      desc = desc + "}\n";
      desc = desc + "PERMISSIONS " + getAllPermissions() + "\n";
      return desc;
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      
      json.put("roleName", getName());
      
      return json;
    }
    
    @JsonIgnore
    public boolean isGlobalAdminRole()
    {
      return (this.nsName.equalsIgnoreCase("Global")) && (this.name.equals("admin"));
    }
    
    public static Role deserialize(JsonNode jsonNode)
      throws JsonParseException, JsonMappingException, IOException
    {
      if (jsonNode == null) {
        return null;
      }
      return MetaInfoJsonSerializer.RoleJsonSerializer.deserialize(jsonNode.toString());
    }
    
    public static Map buildActions()
    {
      MetaInfo.MetaObject.buildBaseActions();
      TextProperties domain_property = MetaInfo.actionablePropertiesFactory.createTextProperties();
      TextProperties role_name_property = MetaInfo.actionablePropertiesFactory.createTextProperties();
      ObjectProperties permissions_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      ObjectProperties roleids_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
      actions.put("domain", domain_property);
      actions.put("roleName", role_name_property);
      actions.put("permissions", permissions_property);
      actions.put("roleUUIDs", role_name_property);
      return actions;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      ObjectMapper objectMapper = ObjectMapperFactory.getInstance();
      parent.put("domain", this.domain);
      parent.put("roleName", this.roleName);
      
      ArrayNode permissions_array = objectMapper.createArrayNode();
      ArrayNode role_uuids_array = objectMapper.createArrayNode();
      parent.set("permissions", permissions_array);
      parent.set("roleUUIDs", role_uuids_array);
      return parent;
    }
  }
  
  public static class Initializer
    extends MetaInfo.MetaObject
  {
    public String WAClusterName = null;
    public String MetaDataRepositoryLocation = null;
    public String MetaDataRepositoryDBname = null;
    public String MetaDataRepositoryUname = null;
    public String MetaDataRepositoryPass = null;
    public String WAClusterPassword = null;
    public String ProductKey = null;
    public String CompanyName = null;
    public String LicenseKey = null;
    private static final long serialVersionUID = 134312423423234L;
    private static final String initName = "Initializer";
    
    public void construct(String WAClusterName, String WAClusterPassword, String MetaDataRepositoryLocation, String MetaDataRepositoryDBname, String MetaDataRepositoryUname, String MetaDataRepositoryPass, String ProductKey, String CompanyName, String LicenseKey)
    {
      super.construct("Initializer", MetaInfo.GlobalNamespace, EntityType.INITIALIZER);
      this.WAClusterName = WAClusterName;
      this.WAClusterPassword = WAClusterPassword;
      this.MetaDataRepositoryLocation = MetaDataRepositoryLocation;
      this.MetaDataRepositoryDBname = MetaDataRepositoryDBname;
      this.MetaDataRepositoryUname = MetaDataRepositoryUname;
      this.MetaDataRepositoryPass = MetaDataRepositoryPass;
      this.ProductKey = ProductKey;
      this.CompanyName = CompanyName;
      this.LicenseKey = LicenseKey;
    }
    
    public String getWAClusterName()
    {
      return this.WAClusterName;
    }
    
    public void setWAClusterName(String wAClusterName)
    {
      this.WAClusterName = wAClusterName;
    }
    
    public String getMetaDataRepositoryLocation()
    {
      return this.MetaDataRepositoryLocation;
    }
    
    public void setMetaDataRepositoryLocation(String metaDataRepositoryLocation)
    {
      this.MetaDataRepositoryLocation = metaDataRepositoryLocation;
    }
    
    public String getMetaDataRepositoryDBname()
    {
      return this.MetaDataRepositoryDBname;
    }
    
    public void setMetaDataRepositoryDBname(String metaDataRepositoryDBname)
    {
      this.MetaDataRepositoryDBname = metaDataRepositoryDBname;
    }
    
    public String getMetaDataRepositoryUname()
    {
      return this.MetaDataRepositoryUname;
    }
    
    public void setMetaDataRepositoryUname(String metaDataRepositoryUname)
    {
      this.MetaDataRepositoryUname = metaDataRepositoryUname;
    }
    
    public String getMetaDataRepositoryPass()
    {
      return this.MetaDataRepositoryPass;
    }
    
    public void setMetaDataRepositoryPass(String metaDataRepositoryPass)
    {
      this.MetaDataRepositoryPass = metaDataRepositoryPass;
    }
    
    public String getWAClusterPassword()
    {
      return this.WAClusterPassword;
    }
    
    public void setWAClusterPassword(String wAClusterPassword)
    {
      this.WAClusterPassword = wAClusterPassword;
    }
    
    public String getProductKey()
    {
      return this.ProductKey;
    }
    
    public void setProductKey(String productKey)
    {
      this.ProductKey = productKey;
    }
    
    public String getCompanyName()
    {
      return this.CompanyName;
    }
    
    public void setCompanyName(String companyName)
    {
      this.CompanyName = companyName;
    }
    
    public String getLicenseKey()
    {
      return this.LicenseKey;
    }
    
    public void setLicenseKey(String licenseKey)
    {
      this.LicenseKey = licenseKey;
    }
    
    public boolean authenticate(String password)
    {
      if (this.WAClusterPassword.equals(password)) {
        return true;
      }
      return false;
    }
    
    public void currentData()
    {
      if (MetaInfo.logger.isInfoEnabled()) {
        MetaInfo.logger.info("Current initializer Object: Cluster name: " + this.WAClusterName + " , " + "Metadata repository location: " + this.MetaDataRepositoryLocation + " , " + "Metadata repository name: " + this.MetaDataRepositoryDBname + " , " + "Metadata repository user name: " + this.MetaDataRepositoryUname + " , ");
      }
    }
    
    public boolean authenticate(String WAClusterName, String WAClusterPassword)
    {
      boolean b = false;
      if ((this.WAClusterName.equals(WAClusterName)) && (this.WAClusterPassword.equals(WAClusterPassword))) {
        b = true;
      }
      return b;
    }
    
    public String describe(AuthToken token)
    {
      String desc = "ABOUT THIS CLUSTER\n";
      desc = desc + "CLUSTER NAME " + this.WAClusterName + "\n";
      desc = desc + "LICENSED TO " + this.CompanyName + "\n";
      desc = desc + "PRODUCT KEY " + this.ProductKey + "\n";
      desc = desc + "LICENSE KEY " + this.LicenseKey + "\n";
      if (this.MetaDataRepositoryLocation != null)
      {
        desc = desc + "USING DB FOR METADATA\n";
        desc = desc + "  LOCATION " + this.MetaDataRepositoryLocation + "\n";
        desc = desc + "  DBNAME " + this.MetaDataRepositoryDBname + "\n";
        desc = desc + "  USER " + this.MetaDataRepositoryUname;
      }
      return desc;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      parent.put("clusterName", this.WAClusterName);
      parent.put("companyName", this.CompanyName);
      parent.put("productKey", this.ProductKey);
      parent.put("licenseKey", this.LicenseKey);
      LicenseManager lm = LicenseManager.get();
      String expiry = lm.getExpiry(this.WAClusterName, this.ProductKey, this.LicenseKey);
      parent.put("expiry", expiry);
      lm.reset();
      return parent;
    }
  }
  
  public static class Visualization
    extends MetaInfo.MetaObject
  {
    private static final long serialVersionUID = -6868010700688028465L;
    private String visName;
    private String json;
    public String fname;
    
    public Visualization()
    {
      this.uuid = new UUID(System.currentTimeMillis());
      this.type = EntityType.VISUALIZATION;
    }
    
    public void construct(String visName, MetaInfo.Namespace ns, String json, String fname)
    {
      super.construct(visName, ns, EntityType.VISUALIZATION);
      this.visName = visName;
      this.json = json;
      this.fname = fname;
    }
    
    public String getVisName()
    {
      return this.visName;
    }
    
    public void setVisName(String visName)
    {
      this.visName = visName;
    }
    
    public String getJson()
    {
      return this.json;
    }
    
    public void setJson(String json)
    {
      this.json = json;
    }
    
    public String toString()
    {
      return this.json;
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      
      return json;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      return parent;
    }
  }
  
  public static class DeploymentGroup
    extends MetaInfo.MetaObject
  {
    private static final long serialVersionUID = 8369271979074176474L;
    public final List<String> configuredMembers = new ArrayList();
    public long minimumRequiredServers = 0L;
    @JsonIgnore
    public final Map<UUID, Long> groupMembers = Factory.makeLinkedMap();
    
    public void construct(String name)
    {
      super.construct(name, MetaInfo.GlobalNamespace, EntityType.DG);
    }
    
    public void addConfiguredMembers(List<String> servers)
    {
      this.configuredMembers.addAll(servers);
      MetaInfo.logger.info("Adding Configured members " + servers + " to DG " + this.name + ". " + toString());
    }
    
    public void removeConfiguredMember(List<String> servers)
    {
      this.configuredMembers.removeAll(servers);
      MetaInfo.logger.info("Removing Configured members " + servers + " from DG " + this.name + ". " + toString());
    }
    
    public void addMembers(List<UUID> uuids, long serverId)
    {
      for (UUID uuid : uuids) {
        this.groupMembers.put(uuid, Long.valueOf(serverId));
      }
      MetaInfo.logger.info("adding member(s) " + uuids + " to DG " + this.name + ". " + toString());
    }
    
    public void removeMember(UUID memberUUID)
    {
      this.groupMembers.remove(memberUUID);
      MetaInfo.logger.info("Removing member " + memberUUID + " from DG " + this.name + ". " + toString());
    }
    
    public long getMinimumRequiredServers()
    {
      return this.minimumRequiredServers;
    }
    
    public void setMinimumRequiredServers(long minimumRequiredServers)
    {
      this.minimumRequiredServers = minimumRequiredServers;
    }
    
    public boolean isAutomaticDG()
    {
      return this.configuredMembers.isEmpty();
    }
    
    public String toString()
    {
      return this.name + " :Actual: " + this.groupMembers + " :Configured: " + this.configuredMembers + " :MinServers: " + this.minimumRequiredServers;
    }
    
    public String describe(AuthToken token)
      throws MetaDataRepositoryException
    {
      List<String> servers = new ArrayList();
      for (UUID sId : this.groupMembers.keySet())
      {
        MetaInfo.Server s = (MetaInfo.Server)MetadataRepository.getINSTANCE().getMetaObjectByUUID(sId, WASecurityManager.TOKEN);
        
        servers.add(s.name);
      }
      return startDescribe() + "Servers: " + this.configuredMembers + " MIN_SERVERS: " + this.minimumRequiredServers;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      return parent;
    }
  }
  
  public static class Server
    extends MetaInfo.MetaObject
  {
    private static final long serialVersionUID = -4815356066614655011L;
    private static Random rng = new Random();
    private static final String characters = "qwertyuiopasdfghjklzxcvbnm";
    public Map<String, Set<UUID>> currentUUIDs = new ConcurrentHashMap();
    public int numCpus;
    public String macAdd;
    public String version;
    public MetaInfo.Initializer initializer;
    public Set<UUID> deploymentGroupsIDs = new HashSet();
    public long id;
    public boolean isAgent;
    public String webBaseUri;
    
    public Server()
    {
      this.numCpus = 0;
      this.version = null;
      this.initializer = null;
      this.id = 0L;
      this.isAgent = false;
      this.webBaseUri = null;
    }
    
    public void construct(UUID uuid, String name, int numCpus, String version, MetaInfo.Initializer initializer, long id, boolean isAgent)
    {
      super.construct(name, uuid, MetaInfo.GlobalNamespace, EntityType.SERVER);
      this.numCpus = numCpus;
      this.version = version;
      this.initializer = initializer;
      this.id = id;
      this.isAgent = isAgent;
      this.webBaseUri = null;
    }
    
    public String getWebBaseUri()
    {
      return this.webBaseUri;
    }
    
    public void setWebBaseUri(String webBaseUri)
    {
      this.webBaseUri = webBaseUri;
    }
    
    public Map<String, Set<UUID>> getCurrentUUIDs()
    {
      return this.currentUUIDs;
    }
    
    public void setCurrentUUIDs(Map<String, Set<UUID>> currentUUIDs)
    {
      this.currentUUIDs = currentUUIDs;
    }
    
    public int getNumCpus()
    {
      return this.numCpus;
    }
    
    public String getMacAdd()
    {
      return this.macAdd;
    }
    
    public void setMacAdd(String macAdd)
    {
      this.macAdd = macAdd;
    }
    
    public void setNumCpus(int numCpus)
    {
      this.numCpus = numCpus;
    }
    
    public String getVersion()
    {
      return this.version;
    }
    
    public void setVersion(String version)
    {
      this.version = version;
    }
    
    public MetaInfo.Initializer getInitializer()
    {
      return this.initializer;
    }
    
    public void setInitializer(MetaInfo.Initializer initializer)
    {
      this.initializer = initializer;
    }
    
    public Set<UUID> getDeploymentGroupsIDs()
    {
      return this.deploymentGroupsIDs;
    }
    
    public void setDeploymentGroupsIDs(Set<UUID> deploymentGroupsIDs)
    {
      this.deploymentGroupsIDs = deploymentGroupsIDs;
    }
    
    public long getId()
    {
      return this.id;
    }
    
    public void setId(long id)
    {
      this.id = id;
    }
    
    private Set<UUID> getCurrentObjectsInServer(String applicationName)
      throws MetaDataRepositoryException
    {
      Set<UUID> uuids = (Set)this.currentUUIDs.get(applicationName);
      if (uuids == null)
      {
        uuids = Collections.newSetFromMap(new ConcurrentHashMap());
        this.currentUUIDs.put(applicationName, uuids);
      }
      Iterator<UUID> it = uuids.iterator();
      while (it.hasNext())
      {
        UUID uuid = (UUID)it.next();
        MetaInfo.MetaObject obj = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
        if (obj == null) {
          it.remove();
        }
      }
      return uuids;
    }
    
    public void addCurrentObjectsInServer(String applicationName, List<MetaInfo.MetaObjectInfo> added)
      throws MetaDataRepositoryException
    {
      Set<UUID> uuids = getCurrentObjectsInServer(applicationName);
      for (MetaInfo.MetaObjectInfo obj : added) {
        uuids.add(obj.uuid);
      }
    }
    
    public void removeCurrentObjectsInServer(String applicationName, List<MetaInfo.MetaObjectInfo> removed)
      throws MetaDataRepositoryException
    {
      Set<UUID> uuids = getCurrentObjectsInServer(applicationName);
      for (MetaInfo.MetaObjectInfo obj : removed) {
        uuids.remove(obj.uuid);
      }
    }
    
    public Map<String, List<MetaInfo.MetaObject>> getCurrentObjects()
      throws MetaDataRepositoryException
    {     
      String application;
      List<MetaInfo.MetaObject> retM;
      Map<String, List<MetaInfo.MetaObject>> ret = new HashMap();
      for (Iterator i$ = this.currentUUIDs.keySet().iterator(); i$.hasNext();)
      {
        application = (String)i$.next();
        retM = new ArrayList();
        ret.put(application, retM);
        Set<UUID> uuids = (Set)this.currentUUIDs.get(application);
        for (UUID uuid : uuids)
        {
          MetaInfo.MetaObject obj = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
          if (obj != null)
          {
            retM.add(obj);
            if (MetaInfo.logger.isDebugEnabled()) {
              MetaInfo.logger.debug(" Server has meta object " + obj.uri + " as part of " + application);
            }
          }
        }
      }
      
      return ret;
    }
    
    public String toString()
    {
      Map<String, List<MetaInfo.MetaObject>> currObjs = null;
      try
      {
        currObjs = getCurrentObjects();
      }
      catch (MetaDataRepositoryException e)
      {
        MetaInfo.logger.error(e.getMessage());
      }
      return this.uuid + " : " + currObjs + ": total objects : " + currObjs.size();
    }
    
    public static String generateString(int length)
    {
      char[] text = new char[length];
      for (int i = 0; i < length; i++) {
        text[i] = "qwertyuiopasdfghjklzxcvbnm".charAt(rng.nextInt("qwertyuiopasdfghjklzxcvbnm".length()));
      }
      return new String(text);
    }
    
    public String describe(AuthToken token)
    {
      String desc = (this.isAgent ? "AGENT" : "SERVER ") + this.name + " [" + this.uuid + "]\n";
      
      desc = desc + "IN GROUPS " + this.deploymentGroupsIDs + "\n";
      desc = desc + "DEPLOYED OBJECTS [\n";
      Map<String, List<MetaInfo.MetaObject>> objects = null;
      Map.Entry<String, List<MetaInfo.MetaObject>> entry;

      try
      {
        objects = getCurrentObjects();
      }
      catch (MetaDataRepositoryException e)
      {
        MetaInfo.logger.error(e.getMessage());
      }
      for (Iterator i$ = objects.entrySet().iterator(); i$.hasNext();)
      {
        entry = (Map.Entry)i$.next();
        List<MetaInfo.MetaObject> objs = (List)entry.getValue();
        if (objs != null) {
          for (MetaInfo.MetaObject obj : objs) {
            desc = desc + "  (" + (String)entry.getKey() + ") " + obj.startDescribe();
          }
        }
      }
      desc = desc + "]";
      return desc;
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      
      return json;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      
      parent.put("version", this.version);
      
      parent.putPOJO("initializer", this.initializer);
      
      return parent;
    }
  }
  
  @Deprecated
  public static class ExceptionHandler
    extends MetaInfo.MetaObject
  {
    private static final long serialVersionUID = 5142558263978570020L;
    public List<String> exceptions;
    public List<String> components;
    public String cmd;
    public Map<String, Object> properties;
    
    public void construct(String name, MetaInfo.Namespace ns, List<String> exceptions, List<String> components, ActionType cmd, List<Property> props)
    {
      super.construct(name, ns, EntityType.EXCEPTIONHANDLER);
      this.exceptions = exceptions;
      this.components = components;
      this.cmd = cmd.toString();
      this.properties = new HashMap();
      if ((props != null) && (props.size() > 0)) {
        for (Property prop : props) {
          this.properties.put(prop.name, prop.value);
        }
      }
    }
    
    public List<String> getExceptions()
    {
      return this.exceptions;
    }
    
    public void setExceptions(List<String> exceptions)
    {
      this.exceptions = exceptions;
    }
    
    public List<String> getComponents()
    {
      return this.components;
    }
    
    public void setComponents(List<String> components)
    {
      this.components = components;
    }
    
    public String getCmd()
    {
      return this.cmd;
    }
    
    public void setCmd(String cmd)
    {
      this.cmd = cmd;
    }
    
    public Map<String, Object> getProperties()
    {
      return this.properties;
    }
    
    public void setProperties(Map<String, Object> properties)
    {
      this.properties = properties;
    }
    
    public String toString()
    {
      return "ExceptionHandler [exceptions=" + this.exceptions + ", components=" + this.components + ", cmd=" + this.cmd + ", properties=" + this.properties + "]";
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      return parent;
    }
  }
  
  public static class Sorter
    extends MetaInfo.MetaObject
  {
    private static final long serialVersionUID = -8887902378355417094L;
    public Interval sortTimeInterval;
    public List<SorterRule> inOutRules;
    public UUID errorStream;
    
    public static class SorterRule
      implements Serializable
    {
      private static final long serialVersionUID = 3988496773069385192L;
      public UUID inStream;
      public UUID outStream;
      public String inStreamField;
      public UUID inOutType;
      
      public SorterRule(UUID inStream, UUID outStream, String inStreamField, UUID inOutType)
      {
        this.inStream = inStream;
        this.outStream = outStream;
        this.inStreamField = inStreamField;
        this.inOutType = inOutType;
      }
      
      public void construct(UUID inStream, UUID outStream, String inStreamField, UUID inOutType)
      {
        this.inStream = inStream;
        this.outStream = outStream;
        this.inStreamField = inStreamField;
        this.inOutType = inOutType;
      }
      
      public UUID getInStream()
      {
        return this.inStream;
      }
      
      public void setInStream(UUID inStream)
      {
        this.inStream = inStream;
      }
      
      public UUID getOutStream()
      {
        return this.outStream;
      }
      
      public void setOutStream(UUID outStream)
      {
        this.outStream = outStream;
      }
      
      public String getInStreamField()
      {
        return this.inStreamField;
      }
      
      public void setInStreamField(String inStreamField)
      {
        this.inStreamField = inStreamField;
      }
      
      public UUID getInOutType()
      {
        return this.inOutType;
      }
      
      public void setInOutType(UUID inOutType)
      {
        this.inOutType = inOutType;
      }
    }
    
    public Sorter()
    {
      this.sortTimeInterval = null;
      this.inOutRules = null;
      this.errorStream = null;
    }
    
    public void construct(String name, MetaInfo.Namespace ns, Interval sortTimeInterval, List<SorterRule> inOutRules, UUID errorStream)
    {
      super.construct(name, ns, EntityType.SORTER);
      this.sortTimeInterval = sortTimeInterval;
      this.inOutRules = inOutRules;
      this.errorStream = errorStream;
    }
    
    public static Map buildActions()
    {
      return MetaInfo.MetaObject.buildBaseActions();
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      return parent;
    }
    
    public Map<UUID, Set<UUID>> inEdges(Graph<UUID, Set<UUID>> graph)
    {
      if (this.inOutRules != null) {
        for (SorterRule rule : this.inOutRules)
        {
          ((Set)graph.get(rule.inStream)).add(getUuid());
          ((Set)graph.get(rule.outStream)).add(getUuid());
        }
      }
      return super.inEdges(graph);
    }
    
    public Graph<UUID, Set<UUID>> exportOrder(@NotNull Graph<UUID, Set<UUID>> graph)
    {
      if (this.inOutRules != null) {
        for (SorterRule rule : this.inOutRules)
        {
          ((Set)graph.get(rule.inStream)).add(getUuid());
          ((Set)graph.get(rule.outStream)).add(getUuid());
        }
      }
      return super.exportOrder(graph);
    }
  }
  
  public static class WAStoreView
    extends MetaInfo.MetaObject
  {
    private static final long serialVersionUID = 1358685338132027542L;
    public UUID wastoreID;
    public IntervalPolicy viewSize;
    public Boolean isJumping;
    public boolean subscribeToUpdates;
    public byte[] query;
    
    public void construct(String name, MetaInfo.Namespace ns, UUID wastoreID, IntervalPolicy viewSize, Boolean isJumping, boolean subscribeToUpdates)
    {
      super.construct(name, ns, EntityType.WASTOREVIEW);
      this.wastoreID = wastoreID;
      this.viewSize = viewSize;
      this.isJumping = isJumping;
      this.query = null;
      this.subscribeToUpdates = subscribeToUpdates;
      getMetaInfoStatus().setAnonymous(true);
    }
    
    public void setQuery(byte[] query)
    {
      this.query = (query != null ? (byte[])query.clone() : null);
    }
    
    public boolean hasQuery()
    {
      return this.query != null;
    }
    
    public UUID getWastoreID()
    {
      return this.wastoreID;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      return parent;
    }
    
    public Map<UUID, Set<UUID>> inEdges(Graph<UUID, Set<UUID>> graph)
    {
      ((Set)graph.get(this.wastoreID)).add(getUuid());
      return super.inEdges(graph);
    }
  }
  
  public static class Dashboard
    extends MetaInfo.MetaObject
  {
    String title;
    String defaultLandingPage;
    List<String> pages;
    
    public String getTitle()
    {
      return this.title;
    }
    
    public void setTitle(String title)
    {
      this.title = title;
    }
    
    public List<String> getPages()
    {
      return this.pages;
    }
    
    public void setPages(List<String> pages)
    {
      this.pages = pages;
    }
    
    public String getDefaultLandingPage()
    {
      return this.defaultLandingPage;
    }
    
    public void setDefaultLandingPage(String defaultLandingPage)
    {
      this.defaultLandingPage = defaultLandingPage;
    }
    
    public void addPage(String page)
    {
      if (this.pages == null) {
        this.pages = new ArrayList();
      }
      this.pages.add(page);
    }
    
    public void removePage(String page)
    {
      if (this.pages != null) {
        this.pages.remove(page);
      }
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      
      List<String> pages = getPages();
      JSONArray pagesJ = new JSONArray();
      if ((pages != null) && (pages.size() > 0)) {
        for (String u : pages) {
          pagesJ.put(u);
        }
      }
      json.put("title", getTitle());
      json.put("pages", pagesJ);
      json.put("defaultLandingPage", getDefaultLandingPage());
      
      return json;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
      parent.put("title", this.title);
      parent.put("defaultLandingPage", this.defaultLandingPage);
      
      ArrayNode pagesArrayNode = jsonMapper.createArrayNode();
      if ((this.pages != null) && (this.pages.size() > 0)) {
        for (String cup : this.pages) {
          pagesArrayNode.add(cup);
        }
      }
      parent.set("pages", pagesArrayNode);
      return parent;
    }
  }
  
  public static class Page
    extends MetaInfo.MetaObject
  {
    String title;
    String gridJSON;
    List<String> queryVisualizations;
    
    public String getTitle()
    {
      return this.title;
    }
    
    public void setTitle(String title)
    {
      this.title = title;
    }
    
    public List<String> getQueryVisualizations()
    {
      return this.queryVisualizations;
    }
    
    public void setQueryVisualizations(List<String> queryVisualizations)
    {
      this.queryVisualizations = queryVisualizations;
    }
    
    public void addVisualization(String visualization)
    {
      if (this.queryVisualizations == null) {
        this.queryVisualizations = new ArrayList();
      }
      if (visualization != null) {
        this.queryVisualizations.add(visualization);
      }
    }
    
    public void removeVisualization(String visualization)
    {
      if ((visualization != null) && (this.queryVisualizations != null)) {
        this.queryVisualizations.remove(visualization);
      }
    }
    
    public String getGridJSON()
    {
      return this.gridJSON;
    }
    
    public void setGridJSON(String grid)
    {
      this.gridJSON = grid;
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      
      json.put("title", getTitle());
      
      List<String> visualizations = getQueryVisualizations();
      JSONArray componentsJ = new JSONArray();
      if ((visualizations != null) && (visualizations.size() > 0)) {
        for (String u : visualizations) {
          componentsJ.put(u);
        }
      }
      json.put("queryVisualizations", componentsJ);
      json.put("gridJSON", getGridJSON());
      
      return json;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
      parent.put("title", this.title);
      parent.putPOJO("gridJSON", this.gridJSON);
      
      ArrayNode qvNode = jsonMapper.createArrayNode();
      List<String> visualizations = getQueryVisualizations();
      if ((visualizations != null) && (visualizations.size() > 0)) {
        for (String cuv : visualizations) {
          qvNode.add(cuv);
        }
      }
      parent.set("queryVisualizations", qvNode);
      
      return parent;
    }
  }
  
  public static class QueryVisualization
    extends MetaInfo.MetaObject
  {
    String title;
    String query;
    String visualizationType;
    String config;
    
    public QueryVisualization() {}
    
    public QueryVisualization(String name, UUID uuid, String nsName, UUID namespaceId)
    {
      MetaInfo.MetaObject.access$300(this, name, uuid, nsName, namespaceId, EntityType.QUERYVISUALIZATION);
    }
    
    public String getTitle()
    {
      return this.title;
    }
    
    public void setTitle(String title)
    {
      this.title = title;
    }
    
    public String getQuery()
    {
      return this.query;
    }
    
    public void setQuery(String query)
    {
      this.query = query;
    }
    
    public String getVisualizationType()
    {
      return this.visualizationType;
    }
    
    public void setVisualizationType(String visualizationType)
    {
      this.visualizationType = visualizationType;
    }
    
    public String getConfig()
    {
      return this.config;
    }
    
    public void setConfig(String config)
    {
      this.config = config;
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = super.JSONify();
      json.put("title", this.title);
      json.put("query", this.query);
      json.put("visualizationType", this.visualizationType);
      json.put("config", this.config);
      
      return json;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectNode parent = super.getJsonForClient();
      parent.put("title", this.title);
      ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
      if (this.query.startsWith("{")) {
        try
        {
          JsonNode jsonNode = jsonMapper.readTree(this.query);
          parent.putPOJO("query", jsonNode);
        }
        catch (IOException e)
        {
          MetaInfo.logger.error(e.getMessage(), e);
        }
      } else {
        parent.put("query", this.query);
      }
      parent.put("visualizationType", this.visualizationType);
      parent.put("config", this.config);
      
      return parent;
    }
  }
  
  public static ObjectNode getActions(EntityType entityType)
    throws JsonProcessingException
  {
    Map actions_map = new HashMap();
    switch (entityType)
    {
    case APPLICATION: 
      actions_map = Flow.buildActions();
      break;
    case STREAM: 
      actions_map = Stream.buildActions();
      break;
    case WINDOW: 
      actions_map = Window.buildActions();
      break;
    case TYPE: 
      actions_map = Type.buildActions();
      break;
    case CQ: 
      actions_map = CQ.buildActions();
      break;
    case SOURCE: 
      actions_map = Source.buildActions();
      break;
    case TARGET: 
      actions_map = Target.buildActions();
      break;
    case FLOW: 
      actions_map = Flow.buildActions();
      break;
    case PROPERTYSET: 
      actions_map = PropertySet.buildActions();
      break;
    case WACTIONSTORE: 
      actions_map = WActionStore.buildActions();
      break;
    case PROPERTYTEMPLATE: 
      break;
    case CACHE: 
      actions_map = Cache.buildActions();
      break;
    case WI: 
      break;
    case ALERTSUBSCRIBER: 
      break;
    case SERVER: 
      break;
    case USER: 
      actions_map = User.buildActions();
      break;
    case ROLE: 
      actions_map = Role.buildActions();
      break;
    case INITIALIZER: 
      break;
    case DG: 
      break;
    case VISUALIZATION: 
      break;
    case NAMESPACE: 
      actions_map = Namespace.buildActions();
      break;
    case STREAM_GENERATOR: 
      break;
    case SORTER: 
      break;
    case WASTOREVIEW: 
      break;
    case AGENT: 
      break;
    case DASHBOARD: 
      break;
    case PAGE: 
      break;
    case QUERYVISUALIZATION: 
      break;
    case QUERY: 
      actions_map = Query.buildActions();
      break;
    }
    ObjectNode action_node = MetaObject.convertToJson(actions_map);
    MetaObject.clearActionsMap();
    return action_node;
  }
}
