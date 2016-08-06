package com.bloom.rest;

import com.bloom.drop.DropMetaObject;
import com.bloom.drop.DropMetaObject.DropDashboard;
import com.bloom.drop.DropMetaObject.DropRule;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Dashboard;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Namespace;
import com.bloom.runtime.meta.MetaInfo.Page;
import com.bloom.runtime.meta.MetaInfo.QueryVisualization;
import com.bloom.utility.Utility;
import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;

import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class MetadataRepositoryServlet
  extends BloomServlet
{
  private static Logger logger = Logger.getLogger(MetadataRepositoryServlet.class);
  private static final long serialVersionUID = 1L;
  private static MDRepository repo = MetadataRepository.getINSTANCE();
  
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException
  {
    try
    {
      int responseStatus = 200;
      String responseData = null;
      
      Map<String, String> params = getParams(request);
      response.setBufferSize(1048576);
      String type = (String)params.get("type");
      String uuid = (String)params.get("uuid");
      String namespace = (String)params.get("nsName");
      String name = (String)params.get("name");
      
      String ver = (String)params.get("version");
      Integer version = null;
      if (ver != null) {
        version = Integer.valueOf(Integer.parseInt((String)params.get("version")));
      }
      if (uuid == null) {
        if (type == null)
        {
          responseStatus = 400;
          responseData = new JSONObject().put("error", "Must specify either type or uuid").toString();
        }
        else if (type.equals("metatypes"))
        {
          responseData = getMetaTypes();
        }
      }
      AuthToken token = getToken(request);
      if (token == null)
      {
        responseStatus = 401;
        responseData = new JSONObject().put("error", "Must provide login token with request").toString();
      }
      if ((responseData == null) && (uuid != null)) {
        try
        {
          UUID uuidobj = new UUID(uuid);
          
          MetaInfo.MetaObject o = repo.getMetaObjectByUUID(uuidobj, token);
          if (o != null)
          {
            responseData = o.JSONifyString();
          }
          else
          {
            responseStatus = 404;
            responseData = new JSONObject().put("error", "Object with specified UUID not found").toString();
          }
        }
        catch (StringIndexOutOfBoundsException e)
        {
          responseStatus = 400;
          responseData = new JSONObject().put("error", "UUID format incorrect").toString();
        }
        catch (MetaDataRepositoryException e)
        {
          responseStatus = 500;
          responseData = new JSONObject().put("error", e.getMessage()).toString();
        }
      }
      EntityType etype = getEntityTypeByPath(type);
      if ((responseData == null) && (etype == EntityType.UNKNOWN))
      {
        responseStatus = 400;
        responseData = new JSONObject().put("error", "Unknown type").toString();
      }
      if ((responseData == null) && (namespace != null) && (name != null)) {
        try
        {
          MetaInfo.MetaObject o = repo.getMetaObjectByName(etype, namespace, name, version, token);
          if (o != null)
          {
            responseData = o.JSONifyString();
          }
          else
          {
            responseStatus = 404;
            responseData = new JSONObject().put("error", "Object with specified type, namespace, and name not found").toString();
          }
        }
        catch (MetaDataRepositoryException e)
        {
          responseStatus = 500;
          responseData = new JSONObject().put("error", e.getMessage()).toString();
        }
      }
      if (responseData == null)
      {
        List<MetaInfo.MetaObject> objects = new ArrayList();
        try
        {
          Set<MetaInfo.MetaObject> objs = null;
          if (namespace != null) {
            objs = repo.getByEntityTypeInNameSpace(etype, namespace, token);
          } else {
            objs = repo.getByEntityType(etype, token);
          }
          if (objs != null)
          {
            if ((etype == EntityType.FLOW) || (etype == EntityType.APPLICATION))
            {
              List<MetaInfo.Flow> flowObjs = new ArrayList();
              for (MetaInfo.MetaObject o : objs) {
                flowObjs.add((MetaInfo.Flow)o);
              }
              Utility.removeDroppedFlow(flowObjs);
              objects.addAll(flowObjs);
            }
            else
            {
              objects.addAll(objs);
            }
            JSONArray objectsJ = new JSONArray();
            for (MetaInfo.MetaObject o : objects) {
              objectsJ.put(o.JSONify());
            }
            responseData = objectsJ.toString();
          }
          else
          {
            responseStatus = 404;
            responseData = new JSONObject().put("error", "No objects found").toString();
          }
        }
        catch (MetaDataRepositoryException e)
        {
          responseStatus = 500;
          responseData = new JSONObject().put("error", e.getMessage()).toString();
        }
      }
      if (responseStatus == 200)
      {
        String hashtext = null;
        try
        {
          MessageDigest m = MessageDigest.getInstance("MD5");
          m.reset();
          m.update(responseData.getBytes());
          byte[] digest = m.digest();
          BigInteger bigInt = new BigInteger(1, digest);
          hashtext = bigInt.toString(16);
          while (hashtext.length() < 32) {
            hashtext = "0" + hashtext;
          }
          response.addHeader("ETag", hashtext);
        }
        catch (NoSuchAlgorithmException e)
        {
          e.printStackTrace();
        }
        String ETag = request.getHeader("If-None-Match");
        if ((hashtext != null) && (hashtext.equals(ETag))) {
          responseStatus = 304;
        }
      }
      response.setStatus(responseStatus);
      response.setContentType("application/json");
      response.setCharacterEncoding("UTF-8");
      byte[] bytes = responseData.getBytes();
      response.setContentLength(bytes.length);
      try
      {
        response.getOutputStream().write(bytes);
      }
      catch (Exception e)
      {
        String requestString = type + ":" + namespace + "." + name + "." + ver + ":" + uuid;
        logger.error("Problem writing response " + responseData + " for request " + requestString, e);
        throw new RuntimeException("Problem writing response " + responseData + " for request " + requestString);
      }
    }
    catch (Exception e)
    {
      logger.error("Problem processing metadata request", e);
    }
  }
  
  private String getMetaTypes()
  {
    List<EntityType> entities = new ArrayList();
    entities.addAll(Arrays.asList(EntityType.values()));
    
    JSONArray entitiesJ = new JSONArray();
    for (EntityType entity : entities)
    {
      JSONObject entityJ = new JSONObject();
      try
      {
        entityJ.put("name", entity.name());
      }
      catch (JSONException e)
      {
        e.printStackTrace();
      }
      entitiesJ.put(entityJ);
    }
    return entitiesJ.toString();
  }
  
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException
  {
    try
    {
      int responseStatus = 200;
      String responseData = null;
      
      Map<String, String> params = getParams(request);
      
      String type = (String)params.get("type");
      String uuid = (String)params.get("uuid");
      String namespace = (String)params.get("nsName");
      String name = (String)params.get("name");
      if (uuid != null)
      {
        responseStatus = 400;
        responseData = new JSONObject().put("error", "Cannot post to individual objects. To update objects, use PUT. To add new object, post to collection").toString();
      }
      if (responseData == null) {
        if (namespace == null)
        {
          responseStatus = 400;
          responseData = new JSONObject().put("error", "Must specify namespace").toString();
        }
        else if (name == null)
        {
          responseStatus = 400;
          responseData = new JSONObject().put("error", "Must specify name").toString();
        }
        else if (type == null)
        {
          responseStatus = 400;
          responseData = new JSONObject().put("error", "Must specify type").toString();
        }
      }
      if (responseData == null)
      {
        EntityType etype = getEntityTypeByPath(type);
        
        AuthToken token = getToken(request);
        if (token == null)
        {
          response.setStatus(401);
          return;
        }
        MetaInfo.MetaObject o = null;
        try
        {
          o = repo.getMetaObjectByName(etype, namespace, name, null, token);
        }
        catch (MetaDataRepositoryException e)
        {
          e.printStackTrace();
        }
        if (o != null)
        {
          responseStatus = 400;
          responseData = new JSONObject().put("error", "Object by that namespace and name combination already exists").toString();
        }
        else
        {
          try
          {
            switch (etype)
            {
            case DASHBOARD: 
              o = updateDashboard(namespace, name, request, token);
              break;
            case PAGE: 
              o = updatePage(namespace, name, request, token);
              break;
            case QUERYVISUALIZATION: 
              o = updateQueryVisualization(namespace, name, request, token);
            }
          }
          catch (MetaDataRepositoryException e)
          {
            e.printStackTrace();
          }
          if (o != null) {
            responseData = o.JSONifyString();
          }
        }
      }
      if (responseData == null)
      {
        responseStatus = 405;
        responseData = new JSONObject().put("error", "Method Unsupported").toString();
      }
      response.setStatus(responseStatus);
      response.setContentType("application/json");
      response.setCharacterEncoding("UTF-8");
      response.getWriter().write(responseData);
    }
    catch (JSONException e)
    {
      e.printStackTrace();
    }
  }
  
  protected void doPut(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException
  {
    try
    {
      int responseStatus = 200;
      String responseData = null;
      
      Map<String, String> params = getParams(request);
      
      String type = (String)params.get("type");
      String uuid = (String)params.get("uuid");
      String namespace = (String)params.get("nsName");
      String name = (String)params.get("name");
      if (uuid == null) {
        if (namespace == null)
        {
          responseStatus = 400;
          responseData = new JSONObject().put("error", "Must specify namespace").toString();
        }
        else if (name == null)
        {
          responseStatus = 400;
          responseData = new JSONObject().put("error", "Must specify name").toString();
        }
        else if (type == null)
        {
          responseStatus = 400;
          responseData = new JSONObject().put("error", "Must specify type").toString();
        }
      }
      if (responseData == null)
      {
        AuthToken token = getToken(request);
        if (token == null)
        {
          response.setStatus(401);
          return;
        }
        EntityType etype = EntityType.UNKNOWN;
        if (uuid != null) {
          try
          {
            MetaInfo.MetaObject o = repo.getMetaObjectByUUID(new UUID(uuid), token);
            etype = o.getType();
            name = o.getName();
            namespace = o.getNsName();
          }
          catch (MetaDataRepositoryException e)
          {
            e.printStackTrace();
          }
        } else {
          etype = getEntityTypeByPath(type);
        }
        try
        {
          MetaInfo.MetaObject o = null;
          switch (etype)
          {
          case DASHBOARD: 
            o = updateDashboard(namespace, name, request, token);
            break;
          case PAGE: 
            o = updatePage(namespace, name, request, token);
            break;
          case QUERYVISUALIZATION: 
            o = updateQueryVisualization(namespace, name, request, token);
          }
          if (o != null) {
            responseData = o.JSONifyString();
          }
        }
        catch (MetaDataRepositoryException e)
        {
          e.printStackTrace();
        }
      }
      if (responseData == null)
      {
        responseStatus = 405;
        responseData = new JSONObject().put("error", "Object not found or unsupported").toString();
      }
      response.setStatus(responseStatus);
      response.setContentType("application/json");
      response.setCharacterEncoding("UTF-8");
      response.getWriter().write(responseData);
    }
    catch (JSONException e)
    {
      e.printStackTrace();
    }
  }
  
  protected void doDelete(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException
  {
    try
    {
      int responseStatus = 200;
      String responseData = null;
      
      Map<String, String> params = getParams(request);
      
      String type = (String)params.get("type");
      String uuid = (String)params.get("uuid");
      String namespace = (String)params.get("nsName");
      String name = (String)params.get("name");
      if (uuid == null) {
        if (namespace == null)
        {
          responseStatus = 400;
          responseData = new JSONObject().put("error", "Must specify namespace").toString();
        }
        else if (name == null)
        {
          responseStatus = 400;
          responseData = new JSONObject().put("error", "Must specify name").toString();
        }
        else if (type == null)
        {
          responseStatus = 400;
          responseData = new JSONObject().put("error", "Must specify type").toString();
        }
      }
      if (responseData == null)
      {
        AuthToken token = getToken(request);
        if (token == null)
        {
          response.setStatus(401);
          return;
        }
        EntityType etype = EntityType.UNKNOWN;
        MetaInfo.MetaObject o;
        if (uuid != null) {
          try
          {
            UUID u = new UUID(uuid);
            o = repo.getMetaObjectByUUID(u, token);
            if (o != null) {
              etype = o.getType();
            }
          }
          catch (MetaDataRepositoryException e)
          {
            e.printStackTrace();
          }
        } else {
          etype = getEntityTypeByPath(type);
        }
        try
        {
          switch (etype)
          {
          case DASHBOARD: 
            if (uuid == null)
            {
              o = repo.getMetaObjectByName(etype, namespace, name, null, token);
              if (o != null)
              {
                DropMetaObject.DropDashboard.drop(null, o, DropMetaObject.DropRule.NONE, token);
                responseData = new JSONObject().toString();
              }
              else
              {
                responseData = new JSONObject().put("error", "Not Found").toString();
              }
            }
            else
            {
              UUID u = new UUID(uuid);
              o = repo.getMetaObjectByUUID(u, token);
              if (o != null)
              {
                DropMetaObject.DropDashboard.drop(null, o, DropMetaObject.DropRule.NONE, token);
                responseData = new JSONObject().toString();
              }
              else
              {
                responseData = new JSONObject().put("error", "Not Found").toString();
              }
            }
            break;
          case PAGE: 
          case QUERYVISUALIZATION: 
            if (uuid == null)
            {
              o = repo.getMetaObjectByName(etype, namespace, name, null, token);
              if (o != null)
              {
                repo.removeMetaObjectByName(etype, namespace, name, null, token);
                responseData = new JSONObject().toString();
              }
              else
              {
                responseData = new JSONObject().put("error", "Not Found").toString();
              }
            }
            else
            {
              UUID u = new UUID(uuid);
              MetaInfo.MetaObject o = repo.getMetaObjectByUUID(u, token);
              if (o != null)
              {
                repo.removeMetaObjectByUUID(u, token);
                responseData = new JSONObject().toString();
              }
              else
              {
                responseData = new JSONObject().put("error", "Not Found").toString();
              }
            }
            break;
          }
        }
        catch (MetaDataRepositoryException e)
        {
          e.printStackTrace();
        }
      }
      if (responseData == null)
      {
        responseStatus = 404;
        responseData = new JSONObject().put("error", "Object not found or unsupported").toString();
      }
      response.setStatus(responseStatus);
      response.setContentType("application/json");
      response.setCharacterEncoding("UTF-8");
      response.getWriter().write(responseData);
    }
    catch (JSONException e)
    {
      e.printStackTrace();
    }
  }
  
  private MetaInfo.Dashboard updateDashboard(String namespace, String name, HttpServletRequest request, AuthToken token)
    throws IndexOutOfBoundsException, MetaDataRepositoryException
  {
    MetaInfo.Dashboard d = (MetaInfo.Dashboard)repo.getMetaObjectByName(EntityType.DASHBOARD, namespace, name, null, token);
    boolean isNew = false;
    if (d == null)
    {
      d = createDashboard(namespace, name, token);
      isNew = true;
    }
    String title = null;
    String defaultpage = null;
    JSONArray pages = null;
    
    String model = request.getParameter("model");
    if (model != null) {
      try
      {
        JSONObject obj = new JSONObject(model);
        if (obj.has("title")) {
          title = obj.getString("title");
        }
        if (obj.has("defaultLandingPage")) {
          defaultpage = obj.getString("defaultLandingPage");
        }
        if (obj.has("pages")) {
          pages = obj.getJSONArray("pages");
        }
      }
      catch (JSONException e)
      {
        e.printStackTrace();
      }
    }
    if (title != null) {
      d.setTitle(title);
    }
    if (defaultpage != null) {
      d.setDefaultLandingPage(defaultpage);
    }
    if (pages != null) {
      try
      {
        List<String> allPages = new ArrayList();
        for (int i = 0; i < pages.length(); i++) {
          allPages.add(pages.get(i).toString());
        }
        d.setPages(allPages);
      }
      catch (JSONException e)
      {
        e.printStackTrace();
      }
    }
    if (isNew) {
      repo.putMetaObject(d, token);
    } else {
      repo.updateMetaObject(d, token);
    }
    return d;
  }
  
  private MetaInfo.Page updatePage(String namespace, String name, HttpServletRequest request, AuthToken token)
    throws IndexOutOfBoundsException, MetaDataRepositoryException
  {
    MetaInfo.Page p = (MetaInfo.Page)repo.getMetaObjectByName(EntityType.PAGE, namespace, name, null, token);
    boolean isNew = false;
    if (p == null)
    {
      p = createPage(namespace, name, token);
      isNew = true;
    }
    String title = null;
    JSONArray visualizations = null;
    String gridJSON = null;
    
    String model = request.getParameter("model");
    if (model != null) {
      try
      {
        JSONObject obj = new JSONObject(model);
        if (obj.has("title")) {
          title = obj.getString("title");
        }
        if (obj.has("queryVisualizations")) {
          visualizations = obj.getJSONArray("queryVisualizations");
        }
        if (obj.has("gridJSON")) {
          gridJSON = obj.getString("gridJSON");
        }
      }
      catch (JSONException e)
      {
        e.printStackTrace();
      }
    }
    if (title != null) {
      p.setTitle(title);
    }
    if (visualizations != null) {
      try
      {
        List<String> allVisualizations = new ArrayList();
        for (int i = 0; i < visualizations.length(); i++) {
          allVisualizations.add(visualizations.get(i).toString());
        }
        p.setQueryVisualizations(allVisualizations);
      }
      catch (JSONException e)
      {
        e.printStackTrace();
      }
    }
    if (gridJSON != null) {
      p.setGridJSON(gridJSON);
    }
    if (isNew) {
      repo.putMetaObject(p, token);
    } else {
      repo.updateMetaObject(p, token);
    }
    return p;
  }
  
  private MetaInfo.QueryVisualization updateQueryVisualization(String namespace, String name, HttpServletRequest request, AuthToken token)
    throws IndexOutOfBoundsException, MetaDataRepositoryException
  {
    MetaInfo.QueryVisualization uic = (MetaInfo.QueryVisualization)repo.getMetaObjectByName(EntityType.QUERYVISUALIZATION, namespace, name, null, token);
    boolean isNew = false;
    if (uic == null)
    {
      uic = createQueryVisualization(namespace, name, token);
      isNew = true;
    }
    String title = null;
    String query = null;
    String visualizationType = null;
    String config = null;
    
    String model = request.getParameter("model");
    if (model != null) {
      try
      {
        JSONObject obj = new JSONObject(model);
        if (obj.has("title")) {
          title = obj.getString("title");
        }
        if (obj.has("query")) {
          query = obj.getString("query");
        }
        if (obj.has("visualizationType")) {
          visualizationType = obj.getString("visualizationType");
        }
        if (obj.has("config")) {
          config = obj.getString("config");
        }
      }
      catch (JSONException e)
      {
        e.printStackTrace();
      }
    }
    if (title != null) {
      uic.setTitle(title);
    }
    if (query != null) {
      uic.setQuery(query);
    }
    if (visualizationType != null) {
      uic.setVisualizationType(visualizationType);
    }
    if (config != null) {
      uic.setConfig(config);
    }
    if (isNew) {
      repo.putMetaObject(uic, token);
    } else {
      repo.updateMetaObject(uic, token);
    }
    return uic;
  }
  
  private MetaInfo.Dashboard createDashboard(String namespace, String name, AuthToken token)
    throws MetaDataRepositoryException
  {
    MetaInfo.Dashboard d = new MetaInfo.Dashboard();
    MetaInfo.Namespace ns = (MetaInfo.Namespace)repo.getMetaObjectByName(EntityType.NAMESPACE, "Global", namespace, null, token);
    
    d.construct(name, ns, EntityType.DASHBOARD);
    return d;
  }
  
  private MetaInfo.Page createPage(String namespace, String name, AuthToken token)
    throws MetaDataRepositoryException
  {
    MetaInfo.Page p = new MetaInfo.Page();
    MetaInfo.Namespace ns = (MetaInfo.Namespace)repo.getMetaObjectByName(EntityType.NAMESPACE, "Global", namespace, null, token);
    
    p.construct(name, ns, EntityType.PAGE);
    return p;
  }
  
  private MetaInfo.QueryVisualization createQueryVisualization(String namespace, String name, AuthToken token)
    throws MetaDataRepositoryException
  {
    MetaInfo.QueryVisualization uic = new MetaInfo.QueryVisualization();
    MetaInfo.Namespace ns = (MetaInfo.Namespace)repo.getMetaObjectByName(EntityType.NAMESPACE, "Global", namespace, null, token);
    
    uic.construct(name, ns, EntityType.QUERYVISUALIZATION);
    return uic;
  }
  
  private Map<String, String> getParams(HttpServletRequest request)
  {
    String path = request.getPathInfo();
    Map<String, String> params = new HashMap();
    
    String type = null;
    String uuid = null;
    String namespace = null;
    String name = null;
    if (path != null)
    {
      String[] parts = path.split("/");
      if (parts.length > 1) {
        type = parts[1];
      }
      if (parts.length > 2) {
        if (parts[2].indexOf('.') > 1)
        {
          String[] fullname = parts[2].split("\\.");
          namespace = fullname[0];
          name = fullname[1];
        }
        else
        {
          uuid = parts[2];
        }
      }
    }
    if (type == null) {
      type = request.getParameter("type");
    }
    if (uuid == null) {
      uuid = request.getParameter("uuid");
    }
    if (namespace == null) {
      namespace = request.getParameter("nsName");
    }
    if (name == null) {
      name = request.getParameter("name");
    }
    if ((uuid == null) && (namespace == null) && (name == null))
    {
      String model = request.getParameter("model");
      if (model != null) {
        try
        {
          JSONObject obj = new JSONObject(model);
          if (obj.has("type")) {
            type = obj.getString("type");
          }
          if (obj.has("uuid")) {
            uuid = obj.getString("uuid");
          }
          if (obj.has("nsName")) {
            namespace = obj.getString("nsName");
          }
          if (obj.has("name")) {
            name = obj.getString("name");
          }
        }
        catch (JSONException e)
        {
          e.printStackTrace();
        }
      }
    }
    String version = request.getParameter("version");
    params.put("type", type);
    params.put("uuid", uuid);
    params.put("nsName", namespace);
    params.put("name", name);
    params.put("version", version);
    
    return params;
  }
  
  private EntityType getEntityTypeByPath(String stype)
  {
    EntityType type = EntityType.UNKNOWN;
    switch (stype)
    {
    case "template": 
    case "templates": 
      type = EntityType.PROPERTYTEMPLATE;
      break;
    case "role": 
    case "roles": 
      type = EntityType.ROLE;
      break;
    case "server": 
    case "servers": 
      type = EntityType.SERVER;
      break;
    case "app": 
    case "apps": 
      type = EntityType.APPLICATION;
      break;
    case "namespace": 
    case "namespaces": 
      type = EntityType.NAMESPACE;
      break;
    case "flow": 
    case "flows": 
      type = EntityType.FLOW;
      break;
    case "source": 
    case "sources": 
      type = EntityType.SOURCE;
      break;
    case "cache": 
    case "caches": 
      type = EntityType.CACHE;
      break;
    case "window": 
    case "windows": 
      type = EntityType.WINDOW;
      break;
    case "stream": 
    case "streams": 
      type = EntityType.STREAM;
      break;
    case "cq": 
    case "cqs": 
      type = EntityType.CQ;
      break;
    case "target": 
    case "targets": 
      type = EntityType.TARGET;
      break;
    case "type": 
    case "types": 
      type = EntityType.TYPE;
      break;
    case "wactionstore": 
    case "wactionstores": 
      type = EntityType.WACTIONSTORE;
      break;
    case "dashboard": 
    case "dashboards": 
      type = EntityType.DASHBOARD;
      break;
    case "pages": 
    case "page": 
      type = EntityType.PAGE;
      break;
    case "queryvisualization": 
    case "queryvisualizations": 
      type = EntityType.QUERYVISUALIZATION;
      break;
    case "query": 
    case "queries": 
      type = EntityType.QUERY;
    }
    return type;
  }
}
