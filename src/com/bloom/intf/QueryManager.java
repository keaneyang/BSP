package com.bloom.intf;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.select.RSFieldDesc;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Query;
import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.bloom.event.ObjectMapperFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public abstract interface QueryManager
{
  public abstract List<MetaInfo.Query> listAllQueries(AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract List<MetaInfo.Query> listAdhocQueries(AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract List<MetaInfo.Query> listNamedQueries(AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract MetaInfo.Query createAdhocQuery(AuthToken paramAuthToken, String paramString)
    throws Exception;
  
  public abstract MetaInfo.Query createAdhocQueryFromJSON(AuthToken paramAuthToken, String paramString)
    throws Exception;
  
  public abstract MetaInfo.Query createNamedQuery(AuthToken paramAuthToken, String paramString)
    throws Exception;
  
  public abstract MetaInfo.Query createNamedQueryFromJSON(AuthToken paramAuthToken, String paramString)
    throws Exception;
  
  public abstract void deleteAdhocQuery(AuthToken paramAuthToken, UUID paramUUID)
    throws MetaDataRepositoryException;
  
  public abstract void deleteNamedQueryByUUID(AuthToken paramAuthToken, UUID paramUUID)
    throws MetaDataRepositoryException;
  
  public abstract void deleteNamedQueryByName(AuthToken paramAuthToken, String paramString)
    throws MetaDataRepositoryException;
  
  public abstract MetaInfo.Query cloneNamedQueryFromAdhoc(AuthToken paramAuthToken, UUID paramUUID, String paramString)
    throws Exception;
  
  public abstract boolean startAdhocQuery(AuthToken paramAuthToken, UUID paramUUID)
    throws MetaDataRepositoryException;
  
  public abstract boolean startNamedQuery(AuthToken paramAuthToken, UUID paramUUID)
    throws MetaDataRepositoryException;
  
  public abstract boolean stopAdhocQuery(AuthToken paramAuthToken, UUID paramUUID)
    throws MetaDataRepositoryException;
  
  public abstract boolean stopNamedQuery(AuthToken paramAuthToken, UUID paramUUID)
    throws MetaDataRepositoryException;
  
  public abstract UUID createQueryIfNotExists(AuthToken paramAuthToken, String paramString1, String paramString2)
    throws Exception;
  
  public abstract MetaInfo.Query prepareQuery(AuthToken paramAuthToken, UUID paramUUID, Map<String, Object> paramMap)
    throws MetaDataRepositoryException, IllegalArgumentException, Exception;
  
  public abstract MetaInfo.Query createParameterizedQuery(boolean paramBoolean, AuthToken paramAuthToken, String paramString1, String paramString2)
    throws Exception;
  
  public static class QueryParameters
  {
    private Set<String> stringParams = new HashSet();
    private Set<String> otherParams = new HashSet();
    
    public Set<String> allParams()
    {
      Set<String> fullList = new HashSet();
      fullList.addAll(getStringParams());
      fullList.addAll(getOtherParams());
      return fullList;
    }
    
    public int size()
    {
      return allParams().size();
    }
    
    public Set<String> getStringParams()
    {
      return this.stringParams;
    }
    
    public void setStringParams(Set<String> stringParams)
    {
      this.stringParams = stringParams;
    }
    
    public Set<String> getOtherParams()
    {
      return this.otherParams;
    }
    
    public void setOtherParams(Set<String> otherParams)
    {
      this.otherParams = otherParams;
    }
    
    public void addOtherParams(String string)
    {
      this.otherParams.add(string);
    }
  }
  
  public static class QueryProjection
    implements Serializable
  {
    private static final long serialVersionUID = 1838167613576358600L;
    public String name;
    public String type;
    
    public QueryProjection() {}
    
    public QueryProjection(RSFieldDesc field)
    {
      this.name = field.name;
      this.type = (field.type == null ? null : field.type.getName());
    }
    
    public JSONObject JSONify()
      throws JSONException
    {
      JSONObject json = new JSONObject();
      json.put("name", this.name);
      json.put("type", this.type);
      return json;
    }
    
    @JsonIgnore
    public ObjectNode getJsonForClient()
    {
      ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
      ObjectNode rootNode = jsonMapper.createObjectNode();
      rootNode.put("name", this.name);
      rootNode.put("type", this.type);
      return rootNode;
    }
    
    public static List<QueryProjection> jsonDeserializer(List results)
      throws JsonParseException, JsonMappingException, IOException
    {
      if (results == null) {
        return null;
      }
      List<QueryProjection> qps = new ArrayList();
      for (int ik = 0; ik < results.size(); ik++)
      {
        Object obj = results.get(ik);
        if ((obj instanceof Map))
        {
          qps.add(jsonDeserializer((Map)results.get(ik)));
        }
        else if ((obj instanceof String))
        {
          ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
          jsonMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
          Map map = (Map)jsonMapper.readValue((String)obj, Map.class);
          qps.add(jsonDeserializer(map));
        }
      }
      return qps;
    }
    
    public static QueryProjection jsonDeserializer(Map results)
    {
      if (results == null) {
        return null;
      }
      QueryProjection qp = new QueryProjection();
      if (results.get("name") != null) {
        qp.name = ((String)results.get("name"));
      }
      if (results.get("type") != null) {
        qp.type = ((String)results.get("type"));
      }
      return qp;
    }
  }
  
  public static enum TYPE
  {
    ADHOC,  NAMEDQUERY;
    
    private TYPE() {}
  }
}
