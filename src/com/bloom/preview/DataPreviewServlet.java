package com.bloom.preview;

import com.bloom.exception.SecurityException;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.runtime.Property;
import com.bloom.runtime.QueryValidator;
import com.bloom.runtime.Server;
import com.bloom.runtime.compiler.TypeField;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.PropertyDef;
import com.bloom.runtime.meta.MetaInfo.PropertyTemplateInfo;
import com.bloom.runtime.meta.MetaInfo.User;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.AuthToken;
import com.bloom.proc.events.WAEvent;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class DataPreviewServlet
  extends HttpServlet
{
  private static Logger logger = Logger.getLogger(DataPreviewServlet.class);
  DataPreview dataPreview;
  SampleDataFiles sampleDataFiles;
  QueryValidator queryValidator;
  MDRepository mdRepository;
  
  public DataPreviewServlet()
  {
    this.queryValidator = new QueryValidator(Server.server);
    
    this.mdRepository = MetadataRepository.getINSTANCE();
  }
  
  public static enum ResultWordsForGetRequest
  {
    path,  readertype,  events,  fileinfo,  recommendation,  doctype,  delimiters,  header,  proptemplate,  parserproperties;
    
    private ResultWordsForGetRequest() {}
  }
  
  public static enum ResultWordsForPostRequest
  {
    model,  token,  columns,  directory,  hadoopurl,  wildcard,  sourcename,  streamname,  appname,  namespace,  cachename,  cacheproperties,  loaddata,  SELECT,  FROM;
    
    private ResultWordsForPostRequest() {}
  }
  
  public static enum PropertyDefFields
  {
    Required,  Type,  DefaultValue;
    
    private PropertyDefFields() {}
  }
  
  public static enum DataTypesForCq
  {
    String("TO_STRING"),  Integer("TO_INT"),  Long("TO_LONG"),  Date("TO_DATE"),  Float("TO_FLOAT"),  Double("TO_DOUBLE"),  Boolean("TO_BOOLEAN"),  Short("TO_SHORT");
    
    String function = null;
    
    private DataTypesForCq(String function)
    {
      this.function = function;
    }
    
    public String getFunction()
    {
      return this.function;
    }
  }
  
  public class SimpleMetaObjectInfo
  {
    EntityType entityType;
    String name;
    
    public SimpleMetaObjectInfo(EntityType entityType, String name)
    {
      this.entityType = entityType;
      this.name = name;
    }
  }
  
  public void init()
    throws ServletException
  {
    super.init();
    this.dataPreview = new DataPreviewImpl();
    this.sampleDataFiles = new SampleDataFiles();
  }
  
  public class DataTypePosition
  {
    int position;
    String type;
    String name;
    
    public DataTypePosition(int position, String fieldType, String name)
    {
      this.position = position;
      this.type = fieldType;
      this.name = name;
    }
    
    public String toString()
    {
      return this.position + ":" + this.type + ":" + this.name;
    }
  }
  
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException
  {
    int responseStatus = 200;
    String responseData = null;
    String error = null;
    try
    {
      String fqPathToFile = request.getParameter(ResultWordsForGetRequest.path.name());
      if (fqPathToFile == null) {
        throw new NullPointerException("Path to Data File is NULL");
      }
      if (fqPathToFile.equalsIgnoreCase("samples"))
      {
        if (!this.sampleDataFiles.samplesListCreated) {
          this.sampleDataFiles.init(DataPreviewConstants.HOME);
        }
        List<DataFile> samples = this.sampleDataFiles.getSamples();
        
        JSONObject jsonObject = new JSONObject();
        JSONArray jArray = new JSONArray();
        for (Iterator<DataFile> dataFileIterator = samples.iterator(); dataFileIterator.hasNext();) {
          jArray.put(((DataFile)dataFileIterator.next()).toJson());
        }
        jsonObject.put("samples", jArray);
        responseData = jsonObject.toString();
      }
      else
      {
        String readerType = request.getParameter(ResultWordsForGetRequest.readertype.name());
        
        String docType = request.getParameter(ResultWordsForGetRequest.doctype.name());
        
        String[] parserProperties = request.getQueryString().split("parserProperties=");
        if (logger.isDebugEnabled()) {
          logger.debug("Get Request Params\nFully Qualified Path To File: " + fqPathToFile + "\n Reader Type: " + readerType + "\n" + "Doc Type: " + docType + "\n Parser Properties Length: " + parserProperties.length);
        }
        ReaderType fType = ReaderType.valueOf(readerType);
        
        Map<String, Object> parserPropertyMap = null;
        ParserType pType;
        if ((docType == null) || (docType.isEmpty())) {
          pType = null;
        } else {
          pType = ParserType.valueOf(docType);
        }
        if ((parserProperties.length >= 2) && (parserProperties[1] != null))
        {
          String[] splitByComma = parserProperties[1].split("\\|");
          if (splitByComma.length > 0) {
            for (String ss : splitByComma)
            {
              String[] splitByColon = ss.split(":");
              if (parserPropertyMap == null) {
                parserPropertyMap = new HashMap();
              }
              if (splitByColon.length > 1) {
                parserPropertyMap.put(splitByColon[0], URLDecoder.decode(splitByColon[1], "UTF-8"));
              } else {
                parserPropertyMap.put(splitByColon[0], null);
              }
            }
          }
        }
        if (logger.isDebugEnabled()) {
          if (parserPropertyMap != null) {
            logger.debug("Parser Properties: " + parserPropertyMap.entrySet());
          }
        }
        JSONObject jsonObject = new JSONObject();
        if ((parserPropertyMap != null) && (!parserPropertyMap.isEmpty()))
        {
          parserPropertyMap.put(DataPreviewImpl.MODULE_NAME, pType.getForClassName());
          List<WAEvent> resultOfPreview = this.dataPreview.doPreview(fType, fqPathToFile, parserPropertyMap);
          JSONArray jArray2 = new JSONArray();
          for (WAEvent waEvent : resultOfPreview) {
            jArray2.put(waEvent.getJSONObject());
          }
          jsonObject.put(ResultWordsForGetRequest.events.name(), jArray2);
          responseData = jsonObject.toString();
          if (logger.isDebugEnabled()) {
            logger.debug("Response Data: " + responseData);
          }
        }
        else
        {
          List<WAEvent> rawData = this.dataPreview.getRawData(fqPathToFile, fType);
          JSONArray jArray = new JSONArray();
          for (WAEvent waEvent : rawData) {
            jArray.put(waEvent.getJSONObject());
          }
          jsonObject.put(ResultWordsForGetRequest.events.name(), jArray);
          if (logger.isDebugEnabled()) {
            logger.debug("Raw events: " + jsonObject.get(ResultWordsForGetRequest.events.name()));
          }
          DataFile fileInfo = this.dataPreview.getFileInfo(fqPathToFile, fType);
          jsonObject.put(ResultWordsForGetRequest.fileinfo.name(), fileInfo.toJson());
          if (logger.isDebugEnabled()) {
            logger.debug("File Info: " + jsonObject.get(ResultWordsForGetRequest.fileinfo.name()));
          }
          List<Map<String, Object>> resultOfAnanlysis = this.dataPreview.analyze(fType, fqPathToFile, pType);
          
          JSONObject jObject = new JSONObject();
          String recommendedDocType;
          if (pType == null)
          {
             recommendedDocType = (String)((Map)resultOfAnanlysis.get(0)).get("docType");
            jsonObject.put(ResultWordsForGetRequest.doctype.name(), recommendedDocType);
            if (logger.isDebugEnabled()) {
              logger.debug("Recommending parser: " + jsonObject.get(ResultWordsForGetRequest.doctype.name()));
            }
          }
          else
          {
            recommendedDocType = pType.name();
            for (int itr = 0; itr < resultOfAnanlysis.size(); itr++)
            {
              JSONObject docJSon = new JSONObject();
              for (Map.Entry<String, Object> entrySet : ((Map)resultOfAnanlysis.get(itr)).entrySet()) {
                docJSon.put((String)entrySet.getKey(), entrySet.getValue());
              }
              jObject.put("" + itr + "", docJSon);
            }
            jsonObject.put(ResultWordsForGetRequest.recommendation.name(), jObject);
            if (logger.isDebugEnabled()) {
              logger.debug("Recommending delimiters: " + jsonObject.get(ResultWordsForGetRequest.recommendation.name()));
            }
          }
          MetaInfo.PropertyTemplateInfo propertyTemplateInfo = this.dataPreview.getPropertyTemplate(ParserType.valueOf(recommendedDocType).getParserName());
          JSONObject pJsonObject = new JSONObject();
          for (Map.Entry<String, MetaInfo.PropertyDef> entry : propertyTemplateInfo.getPropertyMap().entrySet())
          {
            MetaInfo.PropertyDef propertyDef = (MetaInfo.PropertyDef)entry.getValue();
            JSONObject pDefJsonObject = new JSONObject();
            pDefJsonObject.put(PropertyDefFields.Required.name(), propertyDef.isRequired());
            pDefJsonObject.put(PropertyDefFields.DefaultValue.name(), propertyDef.getDefaultValue());
            pDefJsonObject.put(PropertyDefFields.Type.name(), propertyDef.getType().getSimpleName());
            pJsonObject.put((String)entry.getKey(), pDefJsonObject);
          }
          jsonObject.put(ResultWordsForGetRequest.proptemplate.name(), pJsonObject);
          responseData = jsonObject.toString();
        }
      }
    }
    catch (NullPointerException|MetaDataRepositoryException|JSONException|IOException e)
    {
      logger.error("Problem performing request ", e);
      responseStatus = 400;
      responseData = e.getMessage();
    }
    catch (Exception e)
    {
      logger.error("Problem performing request ", e);
      responseStatus = 500;
      responseData = e.getMessage();
    }
    response.setStatus(responseStatus);
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    try
    {
      if (responseData != null) {
        response.getWriter().write(responseData);
      } else {
        response.getWriter().write(error);
      }
    }
    catch (IOException e)
    {
      logger.error("Problem sending response", e);
    }
  }
  
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException
  {
    int responseStatus = 200;
    String responseData = null;
    String error = null;
    AuthToken authToken = null;
    
    String requestAsString = request.getParameter(ResultWordsForPostRequest.model.name());
    try
    {
      JSONObject requestAsJson = new JSONObject(requestAsString);
      
      String token = null;
      if (requestAsJson.has(ResultWordsForPostRequest.token.name())) {
        token = requestAsJson.getString("token");
      }
      if (token == null) {
        throw new SecurityException("Yiikkkeeeessss..Authentication Token cannot be NULL");
      }
      String pathToFile = null;
      if (requestAsJson.has(ResultWordsForGetRequest.path.name())) {
        pathToFile = requestAsJson.getString(ResultWordsForGetRequest.path.name());
      } else {
        throw new NullPointerException("Path to data file cannot be NULL");
      }
      String columns = null;
      if (requestAsJson.has(ResultWordsForPostRequest.columns.name())) {
        columns = requestAsJson.getString(ResultWordsForPostRequest.columns.name());
      } else {
        throw new NullPointerException("Column data can't be NULL");
      }
      String parserType = null;
      if (requestAsJson.has(ResultWordsForGetRequest.doctype.name())) {
        parserType = requestAsJson.getString(ResultWordsForGetRequest.doctype.name());
      } else {
        throw new NullPointerException("Document Type can't be NULL");
      }
      String readerType = null;
      if (requestAsJson.has(ResultWordsForGetRequest.readertype.name())) {
        readerType = requestAsJson.getString(ResultWordsForGetRequest.readertype.name());
      } else {
        throw new NullPointerException("Reader Type can't be NULL");
      }
      String parserString = null;
      if (requestAsJson.has(ResultWordsForGetRequest.parserproperties.name())) {
        parserString = requestAsJson.getString(ResultWordsForGetRequest.parserproperties.name());
      } else {
        throw new NullPointerException("Parser Properties can't be NULL");
      }
      String namespaceForCurrentOperation = null;
      if (requestAsJson.has(ResultWordsForPostRequest.namespace.name())) {
        namespaceForCurrentOperation = requestAsJson.getString(ResultWordsForPostRequest.namespace.name());
      }
      String appName = null;
      if (requestAsJson.has(ResultWordsForPostRequest.appname.name())) {
        appName = requestAsJson.getString(ResultWordsForPostRequest.appname.name());
      }
      String cacheName = null;
      if (requestAsJson.has(ResultWordsForPostRequest.cachename.name())) {
        cacheName = requestAsJson.getString(ResultWordsForPostRequest.cachename.name());
      }
      String cacheOptionsString = null;
      if (requestAsJson.has(ResultWordsForPostRequest.cacheproperties.name())) {
        cacheOptionsString = requestAsJson.getString(ResultWordsForPostRequest.cacheproperties.name());
      }
      boolean loadCache = false;
      if (requestAsJson.has(ResultWordsForPostRequest.loaddata.name())) {
        loadCache = requestAsJson.getBoolean(ResultWordsForPostRequest.loaddata.name());
      }
      String sourceName = null;
      if (requestAsJson.has(ResultWordsForPostRequest.sourcename.name())) {
        sourceName = requestAsJson.getString(ResultWordsForPostRequest.sourcename.name());
      }
      makeSureSourceOrCacheNameisSet(cacheName, sourceName);
      
      List<DataTypePosition> dataPositions = new ArrayList();
      
      String typeName = null;
      
      List<Map<String, String>> readerProperties = null;
      
      List<Map<String, String>> parserProperties = null;
      if (logger.isDebugEnabled()) {
        logger.debug("POST Reuqest Params\n Token: " + token + "\n Path To File: " + pathToFile + "\n App Name: " + appName + " Columns: " + columns + "\n Doc Type: " + parserType + "\n Reader Type: " + readerType + "\n Parser Props: " + parserString + "\n Cache Name: " + cacheName + "\n Cache Options: " + cacheOptionsString + "\n Source Name: " + sourceName);
      }
      if (token != null) {
        authToken = new AuthToken(token);
      }
      setupQueryValidator(authToken);
      if (namespaceForCurrentOperation == null)
      {
        namespaceForCurrentOperation = getCurrentNamespace(authToken);
      }
      else
      {
        MetaInfo.MetaObject namespace = this.mdRepository.getMetaObjectByName(EntityType.NAMESPACE, "Global", namespaceForCurrentOperation, null, authToken);
        if ((namespace == null) || (namespace.getMetaInfoStatus().isDropped())) {
          this.queryValidator.CreateNameSpaceStatement(authToken, namespaceForCurrentOperation, Boolean.valueOf(false));
        }
      }
      if (namespaceForCurrentOperation == null) {
        throw new NullPointerException("Current User does not have a default Namespace");
      }
      setCurrentNamespace(namespaceForCurrentOperation, authToken);
      
      MetaInfo.MetaObject app = this.mdRepository.getMetaObjectByName(EntityType.APPLICATION, namespaceForCurrentOperation, appName, null, authToken);
      if ((app == null) || (app.getMetaInfoStatus().isDropped()))
      {
        this.queryValidator.CreateAppStatement(authToken, appName, Boolean.valueOf(false), null, null, null, null, null, null);
        app = this.mdRepository.getMetaObjectByName(EntityType.APPLICATION, namespaceForCurrentOperation, appName, null, authToken);
      }
      this.queryValidator.setCurrentApp(app.getFullName(), authToken);
      if (columns == null) {
        throw new NullPointerException("Columns sent from the browser are null, therefore we can't create a Type definition.");
      }
      ReaderType reader = null;
      ParserType parser = null;
      if (readerType != null) {
        reader = ReaderType.valueOf(readerType);
      } else {
        throw new NullPointerException("Reader Type not specified");
      }
      if (parserType != null) {
        parser = ParserType.valueOf(parserType);
      } else {
        throw new NullPointerException("Parser Type not sepcified");
      }
      if (pathToFile != null)
      {
        readerProperties = new ArrayList();
        putPathInReaderProperties(readerProperties, reader, pathToFile);
      }
      if (parserString != null)
      {
        parserProperties = new ArrayList();
        createParserProperties(parserProperties, parserString);
      }
      addToReaderPropertiesList(readerProperties, reader.getDefaultReaderPropertiesAsString());
      if (sourceName != null)
      {
        typeName = sourceName.concat("_Type");
        createAndStoreType(columns, dataPositions, namespaceForCurrentOperation, typeName, authToken, false);
        
        String destinationstreamName = sourceName.concat("_TransformedStream");
        
        String implicitSouceStreamName = sourceName.concat("_Stream");
        if ((authToken != null) && (sourceName != null) && (readerProperties != null) && (!readerProperties.isEmpty()) && (parserProperties != null) && (!parserProperties.isEmpty()) && (implicitSouceStreamName != null))
        {
          createAndStoreSource(authToken, sourceName, Boolean.valueOf(false), reader.name(), readerProperties, parser.getParserName(), parserProperties, implicitSouceStreamName, namespaceForCurrentOperation);
        }
        else
        {
          if (logger.isDebugEnabled()) {
            logger.debug("Unexpected NULL value[authToken: " + authToken + ", sourceName: " + sourceName + ", readerProperties: " + readerProperties + ", parserProperties: " + parserProperties + " streamName: " + implicitSouceStreamName + "]");
          }
          throw new NullPointerException("Unexpected NULL value[authToken: " + authToken + ", sourceName: " + sourceName + ", readerProperties: " + readerProperties + ", parserProperties: " + parserProperties + " streamName: " + implicitSouceStreamName + "]");
        }
        String implicitCqName = sourceName.concat("_CQ");
        
        createAndStoreCq(authToken, namespaceForCurrentOperation, implicitCqName, Boolean.valueOf(false), implicitSouceStreamName, destinationstreamName, new String[0], dataPositions);
      }
      if ((cacheName != null) && (cacheOptionsString != null))
      {
        MetaInfo.MetaObject cache = this.mdRepository.getMetaObjectByName(EntityType.CACHE, namespaceForCurrentOperation, cacheName, null, authToken);
        if ((cache != null) && (!cache.getMetaInfoStatus().isDropped())) {
          throw new MetaDataRepositoryException("The cache name " + cacheName + " already exists in namespace " + namespaceForCurrentOperation);
        }
        typeName = cacheName.concat("_Type");
        createAndStoreType(columns, dataPositions, namespaceForCurrentOperation, typeName, authToken, true);
        if ((authToken != null) && (cacheName != null) && (readerProperties != null) && (!readerProperties.isEmpty()) && (parserProperties != null) && (!parserProperties.isEmpty())) {
          if (((cacheOptionsString != null ? 1 : 0) & (typeName != null ? 1 : 0)) != 0)
          {
            createAndStoreCache(authToken, appName, cacheName, Boolean.valueOf(false), reader.name(), readerProperties, parser.getParserName(), parserProperties, cacheOptionsString, typeName, loadCache, namespaceForCurrentOperation);
            break label1656;
          }
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Unexpected NULL value[authToken: " + authToken + ", cacheName: " + cacheName + ", readerProperties: " + readerProperties + ", parserProperties: " + parserProperties + " cacheOptionsString: " + cacheOptionsString + ", Type Name: " + typeName + "]");
        }
        throw new NullPointerException("Unexpected NULL value[authToken: " + authToken + ", cacheName: " + cacheName + ", readerProperties: " + readerProperties + ", parserProperties: " + parserProperties + " cacheOptionsString: " + cacheOptionsString + ", Type Name: " + typeName + "]");
      }
      label1656:
      this.queryValidator.setCurrentApp(null, authToken);
      responseData = "Success";
    }
    catch (SecurityException|NullPointerException|MetaDataRepositoryException|JSONException e)
    {
      logger.error("Problem saving results of source preview from request: " + requestAsString, e);
      if ((e instanceof SecurityException)) {
        responseStatus = 401;
      } else {
        responseStatus = 400;
      }
      responseData = e.getMessage();
    }
    catch (Exception e)
    {
      logger.error("Problem saving results of source preview from request: " + requestAsString, e);
      responseStatus = 400;
      responseData = e.getMessage();
    }
    this.queryValidator.removeContext(authToken);
    response.setStatus(responseStatus);
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    try
    {
      if (responseData != null) {
        response.getWriter().write(responseData);
      } else {
        response.getWriter().write(error);
      }
    }
    catch (IOException e)
    {
      logger.error(e.getMessage());
    }
  }
  
  private void makeSureSourceOrCacheNameisSet(String cacheName, String sourceName)
    throws NullPointerException
  {
    if ((cacheName == null) && (sourceName == null)) {
      throw new NullPointerException("Both the Source name and Cache name are NULL, please save the configuration either as a Real-time Source or as a Cache");
    }
  }
  
  private List<Map<String, String>> makeCacheProperties(String cacheOptionsString)
    throws JSONException
  {
    JSONObject json = new JSONObject(cacheOptionsString);
    Iterator iter = json.keys();
    List<Map<String, String>> properties = new ArrayList();
    while (iter.hasNext())
    {
      Map<String, String> property = new HashMap();
      String key = (String)iter.next();
      property.put(key, json.getString(key));
      properties.add(property);
    }
    return properties;
  }
  
  private void createAndStoreCache(AuthToken token, String appName, String cacheName, Boolean r, String reader_type, List<Map<String, String>> reader_props, String parser_type, List<Map<String, String>> parser_props, String cacheOptionsString, String typename, boolean loadCache, String namespaceForCurrentOperation)
    throws MetaDataRepositoryException, JSONException
  {
    MetaInfo.MetaObject cache = this.mdRepository.getMetaObjectByName(EntityType.CACHE, namespaceForCurrentOperation, cacheName, null, token);
    if (cache == null)
    {
      List<Map<String, String>> cacheOptions = makeCacheProperties(cacheOptionsString);
      
      List<Property> readerPropList = new ArrayList();
      for (Map<String, String> map : reader_props)
      {
        Map.Entry<String, String> singleEntry = (Map.Entry)map.entrySet().iterator().next();
        readerPropList.add(new Property((String)singleEntry.getKey(), singleEntry.getValue()));
      }
      List<Property> parserPropList = new ArrayList();
      for (Map<String, String> map : parser_props)
      {
        Map.Entry<String, String> singleEntry = (Map.Entry)map.entrySet().iterator().next();
        parserPropList.add(new Property((String)singleEntry.getKey(), singleEntry.getValue()));
      }
      List<Property> cachePropList = new ArrayList();
      for (Map<String, String> map : cacheOptions)
      {
        Map.Entry<String, String> singleEntry = (Map.Entry)map.entrySet().iterator().next();
        cachePropList.add(new Property((String)singleEntry.getKey(), singleEntry.getValue()));
      }
      this.queryValidator.CreateCacheStatement_New(token, cacheName, r, reader_type, readerPropList, parser_type, parserPropList, cachePropList, typename);
      if (loadCache)
      {
        Map<String, String> appDescription = new HashMap();
        appDescription.put("flow", appName);
        appDescription.put("strategy", "any");
        this.queryValidator.CreateDeployFlowStatement(token, EntityType.APPLICATION.name(), appDescription, null);
      }
    }
    else
    {
      throw new MetaDataRepositoryException("The cache name " + cacheName + " already exists in namespace " + namespaceForCurrentOperation);
    }
  }
  
  public void addToReaderPropertiesList(List<Map<String, String>> readerPropertiesList, Map<String, String> readerProperties)
  {
    for (Map.Entry<String, String> entry : readerProperties.entrySet())
    {
      Map<String, String> property = new HashMap();
      property.put(entry.getKey(), entry.getValue());
      readerPropertiesList.add(property);
    }
  }
  
  private String getCurrentNamespace(AuthToken authToken)
    throws SecurityException, MetaDataRepositoryException
  {
    MetaInfo.User user = WASecurityManager.get().getAuthenticatedUser(authToken);
    if (user != null) {
      return user.getDefaultNamespace();
    }
    return null;
  }
  
  private void setCurrentNamespace(String namespaceForCurrentOperation, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    this.queryValidator.CreateUseSchemaStmt(authToken, namespaceForCurrentOperation);
  }
  
  private void createAndStoreCq(AuthToken token, String namespaceForCurrentOperation, String cq_name, Boolean doReplace, String implicitSouceStreamName, String destinationstreamName, String[] field_name_list, List<DataTypePosition> dPosition)
    throws Exception
  {
    MetaInfo.MetaObject cq = this.mdRepository.getMetaObjectByName(EntityType.CQ, namespaceForCurrentOperation, cq_name, null, token);
    if ((cq == null) || (cq.getMetaInfoStatus().isDropped()))
    {
      String selectText = "";
      selectText = selectText + ResultWordsForPostRequest.SELECT.name();
      selectText = selectText + " ";
      int size = dPosition.size();
      for (DataTypePosition dPos : dPosition)
      {
        switch (dPos.type)
        {
        case "String": 
          selectText = selectText + DataTypesForCq.String.getFunction();
          break;
        case "Integer": 
          selectText = selectText + DataTypesForCq.Integer.getFunction();
          break;
        case "Long": 
          selectText = selectText + DataTypesForCq.Long.getFunction();
          break;
        case "DateTime": 
          selectText = selectText + DataTypesForCq.Date.getFunction();
          break;
        case "Float": 
          selectText = selectText + DataTypesForCq.Float.getFunction();
          break;
        case "Double": 
          selectText = selectText + DataTypesForCq.Double.getFunction();
          break;
        case "Boolean": 
          selectText = selectText + DataTypesForCq.Boolean.getFunction();
          break;
        case "Short": 
          selectText = selectText + DataTypesForCq.Short.getFunction();
        }
        selectText = selectText + "(data[" + dPos.position + "]) as " + dPos.name;
        if (size > 1) {
          selectText = selectText + ",\n  ";
        } else {
          selectText = selectText + "\n";
        }
        size--;
      }
      selectText = selectText + ResultWordsForPostRequest.FROM.name();
      selectText = selectText + " ";
      selectText = selectText + implicitSouceStreamName;
      selectText = selectText + ";";
      try
      {
        this.queryValidator.CreateCqStatement(token, cq_name, doReplace, destinationstreamName, field_name_list, selectText);
      }
      catch (Throwable t)
      {
        logger.error("Problem creating CQ from: " + selectText, t);
        throw t;
      }
    }
    else
    {
      throw new MetaDataRepositoryException("CQ with name: " + cq_name + " already exists");
    }
  }
  
  private void createAndStoreSource(AuthToken authToken, String sourceName, Boolean b, String readerName, List<Map<String, String>> readerProperties, String parserName, List<Map<String, String>> parserProperties, String implicitStreamName, String namespaceForCurrentOperation)
    throws MetaDataRepositoryException
  {
    MetaInfo.MetaObject stream = this.mdRepository.getMetaObjectByName(EntityType.STREAM, namespaceForCurrentOperation, implicitStreamName, null, authToken);
    if ((stream == null) || (stream.getMetaInfoStatus().isDropped()))
    {
      MetaInfo.MetaObject source = this.mdRepository.getMetaObjectByName(EntityType.SOURCE, namespaceForCurrentOperation, sourceName, null, authToken);
      if ((source == null) || (source.getMetaInfoStatus().isDropped()))
      {
        List<Property> readerPropList = new ArrayList();
        for (Map<String, String> map : readerProperties)
        {
          Map.Entry<String, String> singleEntry = (Map.Entry)map.entrySet().iterator().next();
          readerPropList.add(new Property((String)singleEntry.getKey(), singleEntry.getValue()));
        }
        List<Property> parserPropList = new ArrayList();
        for (Map<String, String> map : parserProperties)
        {
          Map.Entry<String, String> singleEntry = (Map.Entry)map.entrySet().iterator().next();
          parserPropList.add(new Property((String)singleEntry.getKey(), singleEntry.getValue()));
        }
        this.queryValidator.CreateSourceStatement_New(authToken, sourceName, b, readerName, readerPropList, parserName, parserPropList, implicitStreamName);
      }
      else
      {
        throw new MetaDataRepositoryException("The source name " + sourceName + " already exists in namespace " + namespaceForCurrentOperation);
      }
    }
    else
    {
      throw new MetaDataRepositoryException("The source name " + sourceName + " already exists in namespace " + namespaceForCurrentOperation);
    }
  }
  
  private void createParserProperties(List<Map<String, String>> parserProperties, String parserString)
    throws JSONException
  {
    JSONObject jParser = new JSONObject(parserString);
    Iterator allKeys = jParser.keys();
    while (allKeys.hasNext())
    {
      Map<String, String> property = new HashMap();
      String key = (String)allKeys.next();
      property.put(key, jParser.getString(key));
      parserProperties.add(property);
    }
  }
  
  public void putPathInReaderProperties(List<Map<String, String>> readerProperties, ReaderType reader, String pathToFile)
    throws Exception
  {
    int pos = pathToFile.lastIndexOf("/");
    String dir = pathToFile.substring(0, pos + 1);
    String fileName = pathToFile.substring(pos + 1, pathToFile.length());
    if (fileName == null) {
      throw new Exception("File Name is NULL");
    }
    if (dir == null) {
      throw new Exception("Directory is NULL");
    }
    Map<String, String> property = null;
    property = new HashMap();
    switch (reader)
    {
    case FileReader: 
      property.put(ResultWordsForPostRequest.directory.name(), dir);
      break;
    case HDFSReader: 
      property.put(ResultWordsForPostRequest.hadoopurl.name(), dir);
      break;
    default: 
      throw new Exception("Unexpected Reader Type, allowed: " + Arrays.asList(ReaderType.values()) + ", sent " + reader);
    }
    readerProperties.add(property);
    property = new HashMap();
    property.put(ResultWordsForPostRequest.wildcard.name(), fileName);
    readerProperties.add(property);
  }
  
  public AuthToken setupQueryValidator(AuthToken authToken)
    throws Exception
  {
    this.queryValidator.createNewContext(authToken);
    this.queryValidator.setUpdateMode(true);
    return authToken;
  }
  
  private void createAndStoreType(String columns, List<DataTypePosition> dataPositions, String namespaceForCurrentOperation, String typeName, AuthToken authToken, boolean doStore)
    throws Exception
  {
    MetaInfo.MetaObject typeExists = this.mdRepository.getMetaObjectByName(EntityType.TYPE, namespaceForCurrentOperation, typeName, null, authToken);
    if ((typeExists == null) || (typeExists.getMetaInfoStatus().isDropped()))
    {
      JSONArray jsonArray = new JSONArray(columns);
      int size = jsonArray.length();
      List<Map<String, String>> typeDef = new ArrayList();
      
      int position = 0;
      while (position < size)
      {
        JSONObject colDef = (JSONObject)jsonArray.get(position);
        Map<String, String> fieldDef = new HashMap();
        boolean isDiscard = colDef.getBoolean("discard");
        if (!isDiscard)
        {
          String fieldName = colDef.getString("name");
          String fieldType = colDef.getString("type");
          DataTypePosition dPos = new DataTypePosition(position, fieldType, fieldName);
          dataPositions.add(dPos);
          fieldDef.put(fieldName, fieldType);
          typeDef.add(fieldDef);
        }
        position++;
      }
      TypeField[] typeDefArray = (TypeField[])typeDef.toArray(new TypeField[typeDef.size()]);
      if (logger.isDebugEnabled()) {
        logger.debug("Storing Type\n AuthToken: " + authToken + "\n TypeName: " + typeName + "\n Type Definition: " + typeDefArray);
      }
      if (doStore) {
        this.queryValidator.CreateTypeStatement_New(authToken, typeName, Boolean.valueOf(false), typeDefArray);
      }
    }
    else
    {
      throw new MetaDataRepositoryException("Type with name " + typeName + " exists");
    }
  }
}
