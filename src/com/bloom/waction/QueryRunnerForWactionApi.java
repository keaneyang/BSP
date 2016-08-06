package com.bloom.waction;

import com.bloom.distribution.WAQueue;
import com.bloom.distribution.WAQueue.Listener;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.runtime.QueryValidator;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Query;
import com.bloom.runtime.meta.MetaInfo.User;
import com.bloom.runtime.utils.RuntimeUtils;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.AuthToken;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.ITaskEvent;

import java.util.Arrays;
import java.util.Map;
import org.apache.log4j.Logger;

public class QueryRunnerForWactionApi
{
  private static Logger logger = Logger.getLogger(QueryRunnerForWactionApi.class);
  private String dataStore;
  private String[] fields;
  private Map<String, Object> filters;
  private AuthToken token;
  private QueryValidator queryValidator;
  public volatile boolean gotResults = false;
  public ITaskEvent TE;
  MetaInfo.Query adhocQuery;
  WactionApiRestCompiler compiler;
  private String keyField;
  private String[] implicitFields = { "$timestamp", "$id" };
  private boolean getEventList = false;
  private String queryString;
  private Map<String, Object> queryParams;
  
  public void setAllFields(String dataStore, String[] fields, Map<String, Object> filters, AuthToken token, String keyField, boolean getEventList, QueryValidator queryValidator)
  {
    this.dataStore = dataStore;
    this.fields = ((String[])fields.clone());
    this.filters = filters;
    this.token = token;
    this.keyField = keyField;
    this.getEventList = getEventList;
    this.queryValidator = queryValidator;
    this.compiler = new WactionApiRestCompiler();
  }
  
  public void setQueryString(String queryString, Map<String, Object> queryParams, QueryValidator qVal, AuthToken token)
  {
    this.token = token;
    this.queryParams = queryParams;
    this.queryString = queryString;
    this.queryValidator = qVal;
  }
  
  public ITaskEvent call()
    throws Exception
  {
    this.queryValidator.createNewContext(this.token);
    this.queryValidator.setUpdateMode(true);
    String selectString;
    if (this.queryString != null)
    {
      selectString = this.queryString;
    }
    else
    {
      if ((this.filters != null) && (this.filters.containsKey("singlewactions")) && (((String)this.filters.get("singlewactions")).equalsIgnoreCase("false"))) {
        selectString = this.compiler.createQueryToGetLatestPerKey(this.dataStore, this.fields, this.filters, this.keyField, this.implicitFields, this.getEventList);
      } else {
        selectString = this.compiler.createSelectString(this.dataStore, this.fields, this.filters, this.keyField, this.implicitFields, this.getEventList);
      }
    }
    if (selectString != null)
    {
      if (!selectString.endsWith(";")) {
        selectString = selectString.concat(";");
      }
      if (logger.isInfoEnabled())
      {
        logger.info("Generated CQ: " + selectString);
        logger.info("Token for adhoc: " + this.token);
      }
      if (this.queryParams != null)
      {
        String userName = WASecurityManager.getAutheticatedUserName(this.token);
        MetaInfo.User currentUser = WASecurityManager.get().getUser(userName);
        String queryName = RuntimeUtils.genRandomName("queryName");
        
        String fQQueryName = currentUser.getDefaultNamespace().concat(".").concat(queryName);
        MetaInfo.Query intermediateQuery = this.queryValidator.createParameterizedQuery(false, this.token, selectString, fQQueryName);
        this.adhocQuery = this.queryValidator.prepareQuery(this.token, intermediateQuery.getUuid(), this.queryParams);
        if (logger.isInfoEnabled())
        {
          logger.info("Creating Paramaetrized Query with paramters: " + this.queryParams + " and Query Name: " + fQQueryName);
          logger.info("Generated query after compiling parameters: " + this.adhocQuery.queryDefinition);
        }
      }
      else
      {
        this.adhocQuery = this.queryValidator.createAdhocQuery(this.token, selectString);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("CREATED Adhoc" + MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.adhocQuery.getUuid(), this.token));
      }
      WAQueue queue = WAQueue.getQueue("consoleQueue" + this.token);
      WAQueue.Listener listener = new WAQueue.Listener()
      {
        public void onItem(Object item)
        {
          QueryRunnerForWactionApi.this.TE = ((ITaskEvent)item);
          QueryRunnerForWactionApi.this.gotResults = true;
        }
      };
      queue.subscribe(listener);
      
      this.queryValidator.startAdhocQuery(this.token, this.adhocQuery.getUuid());
      while (!this.gotResults) {
        Thread.sleep(10L);
      }
      this.gotResults = false;
      
      queue.unsubscribe(listener);
      if (logger.isDebugEnabled())
      {
        logger.debug("In Query Runner for WAction API, batch size: " + this.TE.batch().size());
        logger.debug("In Query Runner for WAction API, batch data: " + this.TE.batch());
      }
      return this.TE;
    }
    logger.error("Unable to produce a valid Query using: \nName of Store: " + this.dataStore + "\nFields: " + Arrays.asList(this.fields) + "\nFilters: " + this.filters + "\nToken: " + this.token);
    
    throw new NullPointerException("Unable to produce a valid Query using: \nName of Store: " + this.dataStore + "\nFields: " + Arrays.asList(this.fields) + "\nFilters: " + this.filters + "\nToken: " + this.token);
  }
  
  public void cleanUp()
    throws MetaDataRepositoryException
  {
    this.queryValidator.stopAdhocQuery(this.token, this.adhocQuery.getUuid());
    this.queryValidator.deleteAdhocQuery(this.token, this.adhocQuery.getUuid());
    clearAllFields();
  }
  
  private void clearAllFields()
  {
    this.dataStore = null;
    this.fields = null;
    this.filters = null;
    this.token = null;
    this.queryString = null;
    this.queryValidator = null;
  }
}
