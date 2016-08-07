package com.bloom.runtime.meta;
import com.fasterxml.jackson.core.JsonParser;
import com.bloom.intf.QueryManager.QueryProjection;
import com.bloom.runtime.Property;
import com.bloom.runtime.components.EntityType;
import com.bloom.security.ObjectPermission;
import com.bloom.uuid.UUID;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.bloom.event.ObjectMapperFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetaInfoJsonSerializer
{
  public static ObjectMapper jsonMapper = null;
  
  static
  {
    jsonMapper = ObjectMapperFactory.newInstance();
    jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    jsonMapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
    jsonMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    jsonMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    jsonMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
  }
  
  public static UUID uuidDeserializer(Object obj)
  {
    if (obj == null) {
      return null;
    }
    UUID uuid = null;
    if ((obj instanceof String))
    {
      String uuidstring = (String)obj;
      uuid = new UUID();
      uuid.setUUIDString(uuidstring);
    }
    else if ((obj instanceof Map))
    {
      uuid = new UUID();
      uuid.setUUIDString((String)((Map)obj).get("uuidstring"));
    }
    return uuid;
  }
  
  public static class MetaObjectJsonSerializer
  {
    public static MetaInfo.MetaObject deserialize(String json)
      throws JsonParseException, JsonMappingException, IOException
    {
      if ((json == null) || (json.isEmpty())) {
        return null;
      }
      HashMap<String, Object> results = (HashMap)MetaInfoJsonSerializer.jsonMapper.readValue(json, HashMap.class);
      MetaInfo.MetaObject metaObject = new MetaInfo.MetaObject();
      metaObject.name = ((String)results.get("name"));
      metaObject.uri = ((String)results.get("uri"));
      metaObject.uuid = MetaInfoJsonSerializer.uuidDeserializer(results.get("uuid"));
      metaObject.nsName = ((String)results.get("nsName"));
      metaObject.namespaceId = MetaInfoJsonSerializer.uuidDeserializer(results.get("namespaceId"));
      metaObject.type = EntityType.valueOf((String)results.get("type"));
      metaObject.description = ((String)results.get("description"));
      Map metaInfoStatusmap = (Map)results.get("metaInfoStatus");
      MetaInfoStatus mis = MetaInfoStatus.jsonDeserializer(metaInfoStatusmap);
      metaObject.setMetaInfoStatus(mis);
      
      return metaObject;
    }
    
    public static MetaInfo.MetaObject deserialize(MetaInfo.MetaObject metaObject, String json)
      throws JsonParseException, JsonMappingException, IOException
    {
      MetaInfo.MetaObject temp = deserialize(json);
      metaObject.setName(temp.getName());
      metaObject.setUri(temp.getUri());
      metaObject.setUuid(temp.getUuid());
      metaObject.setNsName(temp.getNsName());
      metaObject.setNamespaceId(temp.getNamespaceId());
      metaObject.setType(temp.getType());
      metaObject.setDescription(temp.getDescription());
      
      return metaObject;
    }
  }
  
  public static class QueryJsonSerializer
  {
    public static MetaInfo.Query deserialize(String json)
      throws JsonParseException, JsonMappingException, IOException
    {
      if ((json == null) || (json.isEmpty())) {
        return null;
      }
      HashMap<String, Object> results = (HashMap)MetaInfoJsonSerializer.jsonMapper.readValue(json, HashMap.class);
      
      MetaInfo.Query metaObject = new MetaInfo.Query();
      metaObject = (MetaInfo.Query)MetaInfoJsonSerializer.MetaObjectJsonSerializer.deserialize(metaObject, json);
      
      metaObject.appUUID = MetaInfoJsonSerializer.uuidDeserializer(results.get("appUUID"));
      metaObject.streamUUID = MetaInfoJsonSerializer.uuidDeserializer(results.get("streamUUID"));
      metaObject.cqUUID = MetaInfoJsonSerializer.uuidDeserializer(results.get("cqUUID"));
      metaObject.queryDefinition = ((String)results.get("queryDefinition"));
      Map typeInfoMap = (Map)results.get("typeInfo");
      if ((typeInfoMap != null) && (typeInfoMap.size() > 0))
      {
        metaObject.typeInfo = new HashMap();
        Set keyset = typeInfoMap.keySet();
        Iterator iter = keyset.iterator();
        while (iter.hasNext())
        {
          String ss = (String)iter.next();
          metaObject.typeInfo.put(ss, Long.valueOf(((Number)typeInfoMap.get(ss)).longValue()));
        }
      }
      List<String> queryParameters = (List)results.get("queryParameters");
      if (queryParameters != null)
      {
        metaObject.queryParameters = new ArrayList();
        metaObject.queryParameters.addAll(queryParameters);
      }
      List<Property> bindParameters = (List)results.get("bindParameters");
      if (bindParameters != null)
      {
        metaObject.bindParameters = new ArrayList();
        metaObject.bindParameters.addAll(bindParameters);
      }
      List qp = (List)results.get("projectionFields");
      metaObject.projectionFields.addAll(QueryManager.QueryProjection.jsonDeserializer(qp));
      return metaObject;
    }
  }
  
  public static class RoleJsonSerializer
  {
    public static MetaInfo.Role deserialize(String json)
      throws JsonParseException, JsonMappingException, IOException
    {
      if ((json == null) || (json.isEmpty())) {
        return null;
      }
      try
      {
        Object ob = MetaInfoJsonSerializer.jsonMapper.readValue(json, MetaInfo.Role.class);
        if (ob != null) {
          return (MetaInfo.Role)ob;
        }
      }
      catch (Exception ex) {}
      HashMap<String, Object> results = (HashMap)MetaInfoJsonSerializer.jsonMapper.readValue(json, HashMap.class);
      
      MetaInfo.Role metaObject = new MetaInfo.Role();
      metaObject = (MetaInfo.Role)MetaInfoJsonSerializer.MetaObjectJsonSerializer.deserialize(metaObject, json);
      
      metaObject.domain = ((String)results.get("domain"));
      metaObject.roleName = ((String)results.get("roleName"));
      
      List<ObjectPermission> permissions = (List)results.get("permissions");
      if (permissions != null) {
        metaObject.permissions.addAll(permissions);
      }
      List<UUID> roleUUIDs = (List)results.get("roleUUIDs");
      if (permissions != null) {
        metaObject.roleUUIDs.addAll(roleUUIDs);
      }
      return metaObject;
    }
  }
  
  public static class UserJsonSerializer
  {
    public static MetaInfo.User deserialize(String json)
      throws JsonParseException, JsonMappingException, IOException
    {
      if ((json == null) || (json.isEmpty())) {
        return null;
      }
      try
      {
        Object ob = MetaInfoJsonSerializer.jsonMapper.readValue(json, MetaInfo.User.class);
        if (ob != null) {
          return (MetaInfo.User)ob;
        }
      }
      catch (Exception ex) {}
      HashMap<String, Object> results = (HashMap)MetaInfoJsonSerializer.jsonMapper.readValue(json, HashMap.class);
      
      MetaInfo.User metaObject = new MetaInfo.User();
      metaObject = (MetaInfo.User)MetaInfoJsonSerializer.MetaObjectJsonSerializer.deserialize(metaObject, json);
      
      metaObject.setUserId((String)results.get("userId"));
      metaObject.setFirstName((String)results.get("firstName"));
      metaObject.setLastName((String)results.get("lastName"));
      metaObject.setMainEmail((String)results.get("mainEmail"));
      metaObject.setEncryptedPassword((String)results.get("encryptedPassword"));
      metaObject.setDefaultNamespace((String)results.get("defaultNamespace"));
      metaObject.setUserTimeZone((String)results.get("userTimeZone"));
      metaObject.setLdap((String)results.get("ldap"));
      
      List<MetaInfo.ContactMechanism> contactMechanisms = (List)results.get("contactMechanisms");
      if (contactMechanisms != null) {
        metaObject.setContactMechanisms(contactMechanisms);
      }
      List<ObjectPermission> permissions = (List)results.get("permissions");
      if (permissions != null) {
        metaObject.setPermissions(permissions);
      }
      List<UUID> roleUUIDs = (List)results.get("roleUUIDs");
      if (permissions != null) {
        metaObject.setRoleUUIDs(roleUUIDs);
      }
      return metaObject;
    }
  }
}
