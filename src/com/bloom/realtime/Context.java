package com.bloom.runtime;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hazelcast.core.Endpoint;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.MessageListener;
import com.bloom.appmanager.AppManagerRequestClient;
import com.bloom.appmanager.ChangeApplicationStateResponse;
import com.bloom.appmanager.FlowUtil;
import com.bloom.classloading.WALoader;
import com.bloom.distribution.WAQueue;
import com.bloom.distribution.WAQueue.Listener;
import com.bloom.drop.DropMetaObject;
import com.bloom.drop.DropMetaObject.DropApplication;
import com.bloom.drop.DropMetaObject.DropCQ;
import com.bloom.drop.DropMetaObject.DropCache;
import com.bloom.drop.DropMetaObject.DropDG;
import com.bloom.drop.DropMetaObject.DropDashboard;
import com.bloom.drop.DropMetaObject.DropFlow;
import com.bloom.drop.DropMetaObject.DropNamespace;
import com.bloom.drop.DropMetaObject.DropPropertySet;
import com.bloom.drop.DropMetaObject.DropPropertyVariable;
import com.bloom.drop.DropMetaObject.DropRole;
import com.bloom.drop.DropMetaObject.DropRule;
import com.bloom.drop.DropMetaObject.DropSource;
import com.bloom.drop.DropMetaObject.DropStream;
import com.bloom.drop.DropMetaObject.DropTarget;
import com.bloom.drop.DropMetaObject.DropType;
import com.bloom.drop.DropMetaObject.DropUser;
import com.bloom.drop.DropMetaObject.DropVisualization;
import com.bloom.drop.DropMetaObject.DropWactionStore;
import com.bloom.drop.DropMetaObject.DropWindow;
import com.bloom.event.ObjectMapperFactory;
import com.bloom.exception.CompilationException;
import com.bloom.exception.FatalException;
import com.bloom.exception.SecurityException;
import com.bloom.exception.Warning;
import com.bloom.intf.QueryManager.QueryProjection;
import com.bloom.intf.QueryManager.TYPE;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.MDConstants;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.metaRepository.PermissionUtility;
import com.bloom.metaRepository.RemoteCall;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.Compiler.ExecutionCallback;
import com.bloom.runtime.compiler.CompilerUtils;
import com.bloom.runtime.compiler.Imports;
import com.bloom.runtime.compiler.ObjectName;
import com.bloom.runtime.compiler.select.ParamDesc;
import com.bloom.runtime.compiler.select.RSFieldDesc;
import com.bloom.runtime.compiler.stmts.GracePeriod;
import com.bloom.runtime.compiler.stmts.GrantPermissionToStmt;
import com.bloom.runtime.compiler.stmts.GrantRoleToStmt;
import com.bloom.runtime.compiler.stmts.RecoveryDescription;
import com.bloom.runtime.compiler.stmts.RevokePermissionFromStmt;
import com.bloom.runtime.compiler.stmts.RevokeRoleFromStmt;
import com.bloom.runtime.compiler.stmts.Stmt;
import com.bloom.runtime.compiler.stmts.UserProperty;
import com.bloom.runtime.components.CQTask;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.components.FlowComponent;
import com.bloom.runtime.components.MetaObjectPermissionChecker;
import com.bloom.runtime.containers.TaskEvent;
import com.bloom.runtime.exceptions.ErrorCode.Error;
import com.bloom.runtime.meta.CQExecutionPlan;
import com.bloom.runtime.meta.CQExecutionPlan.CQExecutionPlanFactory;
import com.bloom.runtime.meta.CQExecutionPlan.DataSource;
import com.bloom.runtime.meta.IntervalPolicy;
import com.bloom.runtime.meta.IntervalPolicy.AttrBasedPolicy;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.CQ;
import com.bloom.runtime.meta.MetaInfo.Cache;
import com.bloom.runtime.meta.MetaInfo.ContactMechanism;
import com.bloom.runtime.meta.MetaInfo.ContactMechanism.ContactType;
import com.bloom.runtime.meta.MetaInfo.Dashboard;
import com.bloom.runtime.meta.MetaInfo.DeploymentGroup;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.meta.MetaInfo.Flow.Detail;
import com.bloom.runtime.meta.MetaInfo.Flow.Detail.FailOverRule;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.MetaObjectInfo;
import com.bloom.runtime.meta.MetaInfo.Namespace;
import com.bloom.runtime.meta.MetaInfo.Page;
import com.bloom.runtime.meta.MetaInfo.PropertySet;
import com.bloom.runtime.meta.MetaInfo.PropertyTemplateInfo;
import com.bloom.runtime.meta.MetaInfo.PropertyVariable;
import com.bloom.runtime.meta.MetaInfo.Query;
import com.bloom.runtime.meta.MetaInfo.QueryVisualization;
import com.bloom.runtime.meta.MetaInfo.Role;
import com.bloom.runtime.meta.MetaInfo.Server;
import com.bloom.runtime.meta.MetaInfo.ShowStream;
import com.bloom.runtime.meta.MetaInfo.Sorter;
import com.bloom.runtime.meta.MetaInfo.Sorter.SorterRule;
import com.bloom.runtime.meta.MetaInfo.Source;
import com.bloom.runtime.meta.MetaInfo.StatusInfo;
import com.bloom.runtime.meta.MetaInfo.StatusInfo.Status;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.meta.MetaInfo.Target;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.meta.MetaInfo.User;
import com.bloom.runtime.meta.MetaInfo.User.AUTHORIZATION_TYPE;
import com.bloom.runtime.meta.MetaInfo.Visualization;
import com.bloom.runtime.meta.MetaInfo.WAStoreView;
import com.bloom.runtime.meta.MetaInfo.WActionStore;
import com.bloom.runtime.meta.MetaInfo.Window;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.ProtectedNamespaces;
import com.bloom.runtime.monitor.MonitorModel;
import com.bloom.security.ObjectPermission;
import com.bloom.security.ObjectPermission.Action;
import com.bloom.security.ObjectPermission.ObjectType;
import com.bloom.security.WASecurityManager;
import com.bloom.tungsten.Tungsten;
import com.bloom.utility.Utility;
import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;

public class Context
{
  private static Logger logger = Logger.getLogger(Context.class);
  private boolean readOnly;
  private final MDRepository metadataRepository;
  private Imports imports;
  private MetaInfo.Namespace curNamespace;
  private AuthToken sessionID;
  private final WASecurityManager security_manager;
  private MetaInfo.Flow curApp;
  private MetaInfo.Flow curFlow;
  private MetaInfo.User curUser;
  private ITopic<TaskEvent> remoteCmdOutput;
  private String lastRegisteredListenerId;
  public static final String PACKAGE_NAME = "wa";
  
  protected Context()
  {
    this.metadataRepository = null;
    this.security_manager = null;
  }
  
  public static Context createContextAndLocalServerPersist()
  {
    return createContextAndLocalServer(true);
  }
  
  public static Context createContextAndLocalServer()
  {
    return createContextAndLocalServer(false);
  }
  
  public static Context createContextAndLocalServer(boolean persist)
  {
    System.setProperty("com.bloom.config.persist", persist + "");
    System.setProperty("com.bloom.config.adminPassword", "pass");
    try
    {
      Server.main(new String[0]);
      Server srv = Server.server;
      WASecurityManager sm = WASecurityManager.get();
      AuthToken sessionID = sm.authenticate("admin", "pass");
      return srv.createContext(sessionID);
    }
    catch (Exception e)
    {
      logger.error(e, e);
      e.printStackTrace();
    }
    return null;
  }
  
  public static AuthToken createLocalServer()
  {
    System.setProperty("com.bloom.config.persist", "false");
    System.setProperty("com.bloom.config.adminPassword", "pass");
    String cNameAndPwd = new Long(System.nanoTime()).toString();
    System.setProperty("com.bloom.config.clusterName", cNameAndPwd);
    System.setProperty("com.bloom.config.password", cNameAndPwd);
    System.setProperty("com.bloom.config.company_name", "wa");
    System.setProperty("com.bloom.config.license_key", "9F0C62AF5-D1DFAFBD0-C36A9B938-EDB01E714-37445A");
    try
    {
      Server.main(new String[0]);
      WASecurityManager sm = WASecurityManager.get();
      return sm.authenticate("admin", "pass");
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    return null;
  }
  
  public static Context createContext(AuthToken sid)
  {
    return new Context(sid);
  }
  
  public Context(AuthToken sid)
  {
    this.metadataRepository = MetadataRepository.getINSTANCE();
    this.curNamespace = MetaInfo.GlobalNamespace;
    this.imports = new Imports();
    this.sessionID = sid;
    this.security_manager = WASecurityManager.get();
    this.readOnly = false;
  }
  
  private void removeMetaObject(UUID id)
    throws MetaDataRepositoryException
  {
    this.metadataRepository.removeMetaObjectByUUID(id, this.sessionID);
  }
  
  public void updateMetaObject(MetaInfo.MetaObject obj)
    throws MetaDataRepositoryException
  {
    this.metadataRepository.updateMetaObject(obj, this.sessionID);
  }
  
  private void putMetaObject(MetaInfo.MetaObject obj)
    throws MetaDataRepositoryException
  {
    this.metadataRepository.putMetaObject(obj, this.sessionID);
  }
  
  private MetaInfo.MetaObject getMetaObject(EntityType type, String ns, String name)
    throws MetaDataRepositoryException
  {
    return this.metadataRepository.getMetaObjectByName(type, ns, name, null, this.sessionID);
  }
  
  private MetaInfo.MetaObject getMetaObject(UUID id)
    throws MetaDataRepositoryException
  {
    return this.metadataRepository.getMetaObjectByUUID(id, this.sessionID);
  }
  
  public void resetAppScope()
  {
    this.curFlow = null;
    this.curApp = null;
  }
  
  public void resetNamespace()
  {
    resetAppScope();
    this.curNamespace = MetaInfo.GlobalNamespace;
  }
  
  public AuthToken getAuthToken()
  {
    return this.sessionID;
  }
  
  public boolean setReadOnly(boolean rdonly)
  {
    boolean prevValue = this.readOnly;
    this.readOnly = rdonly;
    return prevValue;
  }
  
  public boolean isReadOnly()
  {
    return this.readOnly;
  }
  
  public static void shutdown(String who)
  {
    try
    {
      Server.shutDown(who);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  
  public static void shutdown()
  {
    shutdown("context");
  }
  
  public void setAdHocQueryCallback(MessageListener<TaskEvent> listener)
  {
    if (this.remoteCmdOutput == null) {
      this.remoteCmdOutput = HazelcastSingleton.get().getTopic("session_" + this.sessionID);
    }
    if (this.lastRegisteredListenerId != null) {
      this.remoteCmdOutput.removeMessageListener(this.lastRegisteredListenerId);
    }
    if (listener != null) {
      this.lastRegisteredListenerId = this.remoteCmdOutput.addMessageListener(listener);
    }
  }
  
  private void cleanUp(EntityType entityType, String namespaceName, String objectName)
    throws SecurityException, MetaDataRepositoryException
  {
    MetaInfo.MetaObject o = getMetaObject(entityType, getNamespaceName(namespaceName), objectName);
    if (o != null) {
      removeMetaObject(o.getUuid());
    }
    if ((o != null) && (o.getType() == EntityType.APPLICATION))
    {
      List<UUID> deps = o.getDependencies();
      for (UUID dep : deps)
      {
        MetaInfo.MetaObject obj = getObject(dep);
        if ((obj instanceof MetaInfo.WAStoreView))
        {
          MetaInfo.WAStoreView wastoreview = (MetaInfo.WAStoreView)obj;
          MetaInfo.WActionStore wastore = (MetaInfo.WActionStore)getMetaObject(wastoreview.wastoreID);
          try
          {
            wastore.getReverseIndexObjectDependencies().remove(dep);
            updateMetaObject(wastore);
          }
          catch (Exception e) {}
          removeMetaObject(dep);
        }
      }
    }
  }
  
  public MetaInfo.Query buildAdHocQuery(String appName, String streamName, String cqName, String queryName, CQExecutionPlan.CQExecutionPlanFactory planFactory, String selectText, String sourceText, boolean isAdhoc, List<Property> queryParams, String namespaceName)
    throws SecurityException, MetaDataRepositoryException
  {
    MetaInfo.Namespace ns = getNamespace(namespaceName);
    this.curApp = null;
    this.curFlow = null;
    UUID queryUUID = new UUID(System.currentTimeMillis());
    MetaInfo.Flow app = null;
    MetaInfo.Stream stream = null;
    MetaInfo.CQ cq = null;
    MetaInfo.Query query = null;
    MetaInfo.Flow curApp = getCurApp();
    try
    {
      app = new MetaInfo.Flow();
      app.addReverseIndexObjectDependencies(queryUUID);
      app.construct(EntityType.APPLICATION, appName, ns, null, 0, 0L);
      
      app.getMetaInfoStatus().setAnonymous(true);
      if (isAdhoc) {
        app.getMetaInfoStatus().setAdhoc(true);
      }
      setCurApp(app);
      MetaInfoStatus statusOfObjects = isAdhoc ? new MetaInfoStatus().setAnonymous(true).setAdhoc(true) : null;
      
      stream = putStream(false, makeObjectName(namespaceName, streamName), UUID.nilUUID(), null, null, null, null, statusOfObjects);
      
      CQExecutionPlan plan = planFactory.createPlan();
      cq = putCQ(false, makeObjectName(namespaceName, cqName), stream.getUuid(), plan, selectText, Collections.emptyList(), statusOfObjects);
      
      endBlock(app);
      if (!selectText.trim().endsWith(";")) {
        selectText = selectText.trim() + ";";
      }
      query = new MetaInfo.Query(queryName, ns, app.getUuid(), stream.getUuid(), cq.getUuid(), plan.makeFieldsMap(), selectText, Boolean.valueOf(isAdhoc));
      
      query.setBindParameters(queryParams);
      for (ParamDesc d : cq.plan.paramsDesc) {
        query.queryParameters.add(d.paramName);
      }
      for (RSFieldDesc d : plan.resultSetDesc) {
        query.projectionFields.add(new QueryManager.QueryProjection(d));
      }
      query.setUuid(queryUUID);
      if (isAdhoc) {
        query.getMetaInfoStatus().setAdhoc(true);
      }
      putQuery(query);
      return query;
    }
    catch (Exception e)
    {
      if (app != null)
      {
        endBlock(app);
        cleanUp(EntityType.APPLICATION, namespaceName, appName);
      }
      if (stream != null) {
        cleanUp(EntityType.STREAM, namespaceName, stream.getName());
      }
      if (cq != null) {
        cleanUp(EntityType.CQ, namespaceName, cq.getName());
      }
      if (query != null) {
        cleanUp(EntityType.QUERY, namespaceName, queryName);
      }
      throw new RuntimeException(e);
    }
    finally
    {
      curApp = null;
      this.curFlow = null;
    }
  }
  
  public MetaInfo.MetaObject duplicateMetaObject(String newName, MetaInfo.MetaObject obj)
    throws CloneNotSupportedException, MetaDataRepositoryException
  {
    checkDropForObjectClone(obj);
    MetaInfo.MetaObject newobj = obj.clone();
    newobj.setUuid(UUID.genCurTimeUUID());
    newobj.setName(newName);
    newobj.getReverseIndexObjectDependencies().clear();
    if ((newobj instanceof MetaInfo.WAStoreView))
    {
      MetaInfo.WAStoreView wastoreview = (MetaInfo.WAStoreView)newobj;
      MetaInfo.MetaObject wactionStore = getMetaObject(wastoreview.wastoreID);
      wactionStore.addReverseIndexObjectDependencies(wastoreview.getUuid());
      updateMetaObject(wactionStore);
    }
    String nvUrl = MDConstants.makeURLWithoutVersion(newobj);
    String vUrl = MDConstants.makeURLWithVersion(nvUrl, Integer.valueOf(newobj.version));
    newobj.setUri(vUrl);
    return newobj;
  }
  
  public MetaInfo.MetaObject checkDropForObjectClone(MetaInfo.MetaObject obj)
  {
    if ((obj instanceof MetaInfo.WAStoreView))
    {
      MetaInfo.MetaObject wactionStore = null;
      try
      {
        wactionStore = this.metadataRepository.getMetaObjectByUUID(((MetaInfo.WAStoreView)obj).wastoreID, WASecurityManager.TOKEN);
        if ((wactionStore != null) && (wactionStore.getMetaInfoStatus().isDropped()))
        {
          MetaInfo.MetaObject newWactionStoreMetaObject = this.metadataRepository.getMetaObjectByName(wactionStore.getType(), wactionStore.getNsName(), wactionStore.getName(), null, WASecurityManager.TOKEN);
          if ((newWactionStoreMetaObject != null) && (!newWactionStoreMetaObject.getMetaInfoStatus().isDropped())) {
            ((MetaInfo.WAStoreView)obj).wastoreID = newWactionStoreMetaObject.getUuid();
          }
        }
        else
        {
          return null;
        }
      }
      catch (MetaDataRepositoryException e)
      {
        if (logger.isInfoEnabled()) {
          logger.info(wactionStore == null ? "WAStoreview" : "Wactionstore object is null");
        }
      }
    }
    else if (obj.getMetaInfoStatus().isDropped())
    {
      try
      {
        MetaInfo.MetaObject updatedObj = this.metadataRepository.getMetaObjectByName(obj.getType(), obj.getNsName(), obj.getName(), null, WASecurityManager.TOKEN);
        if ((updatedObj != null) && (!updatedObj.getMetaInfoStatus().isDropped())) {
          obj = updatedObj;
        }
        return updatedObj;
      }
      catch (MetaDataRepositoryException e)
      {
        if (logger.isInfoEnabled()) {
          logger.info("Metaobject " + obj.getFullName() + " not found");
        }
      }
    }
    else
    {
      return null;
    }
    try
    {
      this.metadataRepository.updateMetaObject(obj, WASecurityManager.TOKEN);
    }
    catch (MetaDataRepositoryException e)
    {
      if (logger.isInfoEnabled()) {
        logger.info("Metaobject " + obj.getFullName() + " is null.");
      }
    }
    return obj;
  }
  
  public MetaInfo.Query prepareQuery(MetaInfo.Query query, List<Property> queryParams)
    throws SecurityException, MetaDataRepositoryException
  {
    MetaInfo.Flow app = (MetaInfo.Flow)getMetaObject(query.appUUID);
    final MetaInfo.CQ cq = (MetaInfo.CQ)getMetaObject(query.cqUUID);
    MetaInfo.Stream stream = (MetaInfo.Stream)getMetaObject(query.streamUUID);
    validatePermissions(cq);
    
    final List<UUID> deps = app.getDependencies();
    if (!deps.remove(cq.getUuid())) {
      throw new RuntimeException("query " + app.name + " does not have cq " + cq.name);
    }
    if (!deps.remove(stream.getUuid())) {
      throw new RuntimeException("query " + app.name + " does not have stream " + stream.name);
    }
    final String uniqueID = "_" + System.nanoTime();
    String appName = app.getName() + uniqueID;
    String streamName = stream.getName() + uniqueID;
    String cqName = cq.getName() + uniqueID;
    String queryName = query.getName() + uniqueID;
    CQExecutionPlan.CQExecutionPlanFactory planFactory = new CQExecutionPlan.CQExecutionPlanFactory()
    {
      public CQExecutionPlan createPlan()
        throws MetaDataRepositoryException, CloneNotSupportedException
      {
        boolean isCQupdated = false;
        List<CQExecutionPlan.DataSource> dss = cq.getPlan().dataSources;
        for (int i = 0; i < dss.size(); i++)
        {
          MetaInfo.MetaObject subtaskObj = Context.this.metadataRepository.getMetaObjectByUUID(((CQExecutionPlan.DataSource)dss.get(i)).getDataSourceID(), WASecurityManager.TOKEN);
          MetaInfo.MetaObject obj = Context.this.checkDropForObjectClone(subtaskObj);
          if (obj != null)
          {
            ((CQExecutionPlan.DataSource)dss.get(i)).replaceDataSourceID(obj.getUuid());
            isCQupdated = true;
          }
        }
        if (isCQupdated) {
          Context.this.metadataRepository.updateMetaObject(cq, WASecurityManager.TOKEN);
        }
        CQExecutionPlan plan = cq.getPlan().clone();
        List<Pair<MetaInfo.MetaObject, MetaInfo.MetaObject>> oldnew = new ArrayList();
        for (UUID id : deps)
        {
          MetaInfo.MetaObject obj = Context.this.getObject(id);
          if (obj != null)
          {
            String newName = obj.getName() + uniqueID;
            MetaInfo.MetaObject newobj = Context.this.duplicateMetaObject(newName, obj);
            newobj.getMetaInfoStatus().setAdhoc(true);
            Context.this.put(newobj);
            CQExecutionPlan.DataSource ds = plan.findDataSource(id);
            if (ds != null) {
              ds.replaceDataSourceID(newobj.getUuid());
            }
            oldnew.add(Pair.make(obj, newobj));
          }
        }
        for (Pair<MetaInfo.MetaObject, MetaInfo.MetaObject> p : oldnew) {
          if ((p.second instanceof MetaInfo.CQ))
          {
            MetaInfo.CQ newcq = (MetaInfo.CQ)p.second;
            newcq.setPlan(newcq.getPlan().clone());
            CQExecutionPlan plan2 = newcq.getPlan();
            for (Pair<MetaInfo.MetaObject, MetaInfo.MetaObject> p2 : oldnew)
            {
              CQExecutionPlan.DataSource ds = plan2.findDataSource(((MetaInfo.MetaObject)p2.first).uuid);
              if (ds != null) {
                ds.replaceDataSourceID(((MetaInfo.MetaObject)p2.second).uuid);
              }
            }
            Context.this.updateMetaObject(newcq);
          }
        }
        return plan;
      }
    };
    String selectText = cq.getSelect();
    String sourceText = query.queryDefinition;
    boolean isAdhoc = true;
    return buildAdHocQuery(appName, streamName, cqName, queryName, planFactory, selectText, sourceText, true, queryParams, query.getNsName());
  }
  
  public void validatePermissions(MetaInfo.CQ cq)
    throws MetaDataRepositoryException
  {
    for (UUID fromUUID : cq.getPlan().getDataSources())
    {
      MetaInfo.MetaObject mo = getMetaObject(fromUUID);
      if ((mo instanceof MetaInfo.WAStoreView)) {
        mo = getMetaObject(((MetaInfo.WAStoreView)mo).wastoreID);
      }
      if (mo != null) {
        PermissionUtility.checkPermission(mo, ObjectPermission.Action.select, this.sessionID, true);
      }
    }
  }
  
  public void startAdHocQuery(MetaInfo.Query queryMetaObject)
    throws MetaDataRepositoryException
  {
    if (queryMetaObject == null) {
      throw new RuntimeException("Query metaobject is not found");
    }
    MetaInfo.Flow app = (MetaInfo.Flow)getMetaObject(queryMetaObject.appUUID);
    UUID cquuid = (UUID)app.getObjects(EntityType.CQ).iterator().next();
    MetaInfo.CQ cq = (MetaInfo.CQ)getObject(cquuid);
    UUID deploymentGroup = null;
    for (UUID ds : cq.plan.getDataSources()) {
      if ((deploymentGroup = Utility.getDeploymentGroupFromMetaObject(ds, WASecurityManager.TOKEN)) != null) {
        break;
      }
    }
    List<MetaInfo.Flow.Detail> deploymentPlan = new ArrayList();
    MetaInfo.DeploymentGroup dg;
    if (deploymentGroup == null)
    {
      MetaInfo.DeploymentGroup dg = (MetaInfo.DeploymentGroup)getMetaObject(EntityType.DG, "Global", "default");
      if (dg == null) {
        throw new FatalException("Invoking adhoc on not deployed object.");
      }
    }
    else
    {
      dg = (MetaInfo.DeploymentGroup)getObject(deploymentGroup);
    }
    assert (dg != null);
    MetaInfo.Flow.Detail detail = new MetaInfo.Flow.Detail();
    detail.construct(DeploymentStrategy.ON_ONE, dg.uuid, app.uuid, MetaInfo.Flow.Detail.FailOverRule.NONE);
    deploymentPlan.add(detail);
    app.setDeploymentPlan(deploymentPlan);
    putFlow(app);
    
    List<Property> params = queryMetaObject.getBindParameters();
    
    changeFlowState(ActionType.START_ADHOC, app, params);
  }
  
  public void stopAndDropAdHocQuery(MetaInfo.Query query)
    throws MetaDataRepositoryException
  {
    MetaInfo.Flow app = (MetaInfo.Flow)getMetaObject(query.appUUID);
    if (app != null)
    {
      changeFlowState(ActionType.STOP_ADHOC, app);
      for (UUID dep : app.getDependencies())
      {
        MetaInfo.MetaObject obj = getMetaObject(dep);
        if ((obj instanceof MetaInfo.WAStoreView))
        {
          MetaInfo.WAStoreView wastoreview = (MetaInfo.WAStoreView)obj;
          MetaInfo.WActionStore wastore = (MetaInfo.WActionStore)getMetaObject(wastoreview.wastoreID);
          try
          {
            wastore.getReverseIndexObjectDependencies().remove(dep);
            updateMetaObject(wastore);
          }
          catch (Exception e) {}
        }
        removeMetaObject(dep);
      }
      removeMetaObject(app.uuid);
      removeMetaObject(query.uuid);
    }
  }
  
  public void stopAdHocQuery(MetaInfo.Query queryMetaObject)
  {
    try
    {
      MetaInfo.Flow app = (MetaInfo.Flow)getObject(queryMetaObject.appUUID);
      if (app != null) {
        changeFlowState(ActionType.STOP_ADHOC, app);
      }
    }
    catch (MetaDataRepositoryException e)
    {
      logger.warn(e.getMessage());
    }
  }
  
  public MetaInfo.MetaObject getFlow(String name, EntityType type)
    throws MetaDataRepositoryException
  {
    MetaInfo.MetaObject result = get(addSchemaPrefix(null, name), type);
    return result;
  }
  
  public String[] splitNamespaceAndName(String name, EntityType type)
  {
    String[] namespaceAndName = new String[2];
    if (name.indexOf('.') == -1)
    {
      if (EntityType.isGlobal(type)) {
        namespaceAndName[0] = "Global";
      } else {
        namespaceAndName[0] = this.curNamespace.name;
      }
      namespaceAndName[1] = name;
    }
    else
    {
      namespaceAndName[0] = name.split("\\.")[0];
      namespaceAndName[1] = name.split("\\.")[1];
    }
    return namespaceAndName;
  }
  
  public <T extends MetaInfo.MetaObject> T get(String name, EntityType type)
    throws MetaDataRepositoryException
  {
    String[] namespaceAndName = splitNamespaceAndName(name, type);
    MetaInfo.MetaObject resultObject = getMetaObject(type, namespaceAndName[0], namespaceAndName[1]);
    if (resultObject == null) {
      return null;
    }
    if (resultObject.getMetaInfoStatus().isDropped()) {
      return null;
    }
    return resultObject;
  }
  
  public void updateFlow(MetaInfo.Flow flow)
    throws MetaDataRepositoryException
  {
    putMetaObject(flow);
  }
  
  public void updateWAStoreView(MetaInfo.WAStoreView view)
    throws MetaDataRepositoryException
  {
    putMetaObject(view);
  }
  
  private boolean currentlyInScope()
  {
    return (this.curApp != null) || (this.curFlow != null);
  }
  
  public MetaInfo.MetaObject put(MetaInfo.MetaObject obj)
    throws MetaDataRepositoryException
  {
    long opStartTime = 0L;long opEndTime = 0L;
    if ((obj instanceof MetaObjectPermissionChecker)) {
      if (!((MetaObjectPermissionChecker)obj).checkPermissionForMetaPropertyVariable(this.sessionID)) {
        logger.error("Problem accessing PropVariable. See if you have right permissions or check if the prop variable exists");
      }
    }
    if (this.readOnly) {
      return null;
    }
    if (logger.isInfoEnabled()) {
      opStartTime = System.currentTimeMillis();
    }
    if ((currentlyInScope()) && 
      (!correctScopeForObject(obj.getType()))) {
      throw new RuntimeException("Cannot put " + obj.type + " while inside of application/flow.");
    }
    if ((this.curApp != null) || (this.curFlow != null))
    {
      MetaInfo.Flow f = this.curFlow != null ? this.curFlow : this.curApp;
      if (this.curApp != null) {
        obj.addReverseIndexObjectDependencies(this.curApp.uuid);
      }
      if (this.curFlow != null) {
        obj.addReverseIndexObjectDependencies(this.curFlow.uuid);
      }
    }
    MetaInfo.MetaObject got = getMetaObject(obj.getUuid());
    putMetaObject(obj);
    MetaInfo.MetaObject old = getMetaObject(obj.getUuid());
    MetaInfo.StatusInfo status = new MetaInfo.StatusInfo();
    status.construct(obj.uuid, MetaInfo.StatusInfo.Status.CREATED, obj.type, obj.name);
    
    this.metadataRepository.putStatusInfo(status, this.sessionID);
    if ((this.curApp != null) || (this.curFlow != null))
    {
      MetaInfo.Flow f = this.curFlow != null ? this.curFlow : this.curApp;
      
      f.addObject(obj.type, obj.uuid);
      updateFlow(f);
    }
    if (logger.isInfoEnabled())
    {
      opEndTime = System.currentTimeMillis();
      logger.info("context put for put of " + obj.getFullName() + " took " + (opEndTime - opStartTime) / 1000L + " seconds");
    }
    return old;
  }
  
  private boolean correctScopeForObject(EntityType entityType)
  {
    if ((this.curFlow != null) && (!entityType.canBePartOfFlow())) {
      return false;
    }
    return (this.curFlow != null) || (this.curApp == null) || (entityType.canBePartOfApp());
  }
  
  private void removeObject(UUID uuid)
    throws MetaDataRepositoryException
  {
    MetaInfo.MetaObject metaObjectToDrop = getObject(uuid);
    dropObject(metaObjectToDrop.name, metaObjectToDrop.type, DropMetaObject.DropRule.NONE);
  }
  
  public MetaInfo.MetaObject removeObject(MetaInfo.MetaObject obj)
    throws MetaDataRepositoryException
  {
    if (this.readOnly) {
      return null;
    }
    removeMetaObject(obj.getUuid());
    return null;
  }
  
  private MetaInfo.MetaObject putStream(MetaInfo.Stream obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  private MetaInfo.MetaObject putCache(MetaInfo.Cache obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  private MetaInfo.MetaObject putWindow(MetaInfo.Window obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  private MetaInfo.MetaObject putNamespace(MetaInfo.Namespace obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  public MetaInfo.MetaObject putType(MetaInfo.Type obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  private MetaInfo.MetaObject putCQ(MetaInfo.CQ obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  private MetaInfo.MetaObject putPropertySet(MetaInfo.PropertySet obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  private MetaInfo.MetaObject putPropertyVariable(MetaInfo.PropertyVariable obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  private MetaInfo.MetaObject putTarget(MetaInfo.Target obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  private MetaInfo.MetaObject putSorter(MetaInfo.Sorter obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  private MetaInfo.MetaObject putWAStoreView(MetaInfo.WAStoreView obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  private MetaInfo.MetaObject putSource(MetaInfo.Source obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  private MetaInfo.MetaObject putFlow(MetaInfo.Flow obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  private MetaInfo.MetaObject putDeploymentGroup(MetaInfo.DeploymentGroup obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  private MetaInfo.MetaObject putWActionStore(MetaInfo.WActionStore obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  private MetaInfo.MetaObject putWActionStoreView(MetaInfo.WAStoreView obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  private MetaInfo.MetaObject putUser(MetaInfo.User obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  private MetaInfo.MetaObject putRole(MetaInfo.Role obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  private MetaInfo.MetaObject putVisualization(MetaInfo.Visualization obj)
    throws MetaDataRepositoryException
  {
    return put(obj);
  }
  
  public void putTmpObject(MetaInfo.MetaObject obj)
    throws MetaDataRepositoryException
  {
    assert (obj.name != null);
    put(obj);
  }
  
  public MetaInfo.MetaObject getObject(UUID uuid)
    throws MetaDataRepositoryException
  {
    MetaInfo.MetaObject result = getMetaObject(uuid);
    if ((result != null) && (result.getMetaInfoStatus() != null) && (result.getMetaInfoStatus().isDropped())) {
      return null;
    }
    return result;
  }
  
  private MetaInfo.Stream getStream(String name)
    throws MetaDataRepositoryException
  {
    return (MetaInfo.Stream)get(name, EntityType.STREAM);
  }
  
  private MetaInfo.MetaObject getDataSource(String name)
    throws MetaDataRepositoryException
  {
    MetaInfo.MetaObject ret = get(name, EntityType.STREAM);
    if (ret == null) {
      ret = get(name, EntityType.WINDOW);
    }
    if (ret == null) {
      ret = get(name, EntityType.CACHE);
    }
    if (ret == null) {
      ret = get(name, EntityType.WACTIONSTORE);
    }
    return ret;
  }
  
  public MetaInfo.Type getType(String name)
    throws MetaDataRepositoryException
  {
    return (MetaInfo.Type)get(name, EntityType.TYPE);
  }
  
  private MetaInfo.Cache getCache(String name)
    throws MetaDataRepositoryException
  {
    return (MetaInfo.Cache)get(name, EntityType.CACHE);
  }
  
  private MetaInfo.CQ getCQ(String name)
    throws MetaDataRepositoryException
  {
    return (MetaInfo.CQ)get(name, EntityType.CQ);
  }
  
  private MetaInfo.PropertySet getPropertySet(String name)
    throws MetaDataRepositoryException
  {
    return (MetaInfo.PropertySet)get(name, EntityType.PROPERTYSET);
  }
  
  private MetaInfo.PropertyVariable getPropertyVariable(String name)
    throws MetaDataRepositoryException
  {
    return (MetaInfo.PropertyVariable)get(name, EntityType.PROPERTYVARIABLE);
  }
  
  private MetaInfo.Source getSource(String name)
    throws MetaDataRepositoryException
  {
    return (MetaInfo.Source)get(name, EntityType.SOURCE);
  }
  
  private MetaInfo.Target getTarget(String name)
    throws MetaDataRepositoryException
  {
    return (MetaInfo.Target)get(name, EntityType.TARGET);
  }
  
  private MetaInfo.Sorter getSorter(String name)
    throws MetaDataRepositoryException
  {
    return (MetaInfo.Sorter)get(name, EntityType.SORTER);
  }
  
  public MetaInfo.DeploymentGroup getDeploymentGroup(String name)
    throws MetaDataRepositoryException
  {
    return (MetaInfo.DeploymentGroup)get(name, EntityType.DG);
  }
  
  private MetaInfo.User getUser(String name)
    throws MetaDataRepositoryException
  {
    return (MetaInfo.User)get(name, EntityType.USER);
  }
  
  private MetaInfo.Role getRole(String name)
    throws MetaDataRepositoryException
  {
    return (MetaInfo.Role)get(name, EntityType.ROLE);
  }
  
  public MetaInfo.PropertyTemplateInfo getAdapterProperties(String name)
    throws MetaDataRepositoryException
  {
    return (MetaInfo.PropertyTemplateInfo)get(name, EntityType.PROPERTYTEMPLATE);
  }
  
  public Class<?> loadClass(String name)
    throws ClassNotFoundException
  {
    ClassLoader classLoader = WALoader.get();
    return classLoader.loadClass(name);
  }
  
  public Class<?> getClassWithoutReplacingPrimitives(String name)
    throws ClassNotFoundException
  {
    Class<?> c = CompilerUtils.getPrimitiveClassByName(name);
    if (c == null) {
      return getClass(name);
    }
    return c;
  }
  
  public Class<?> getClass(String name)
    throws ClassNotFoundException
  {
    try
    {
      Class<?> c = CompilerUtils.getClassNameShortcut(name);
      if (c == null) {}
      return loadClass(name);
    }
    catch (ClassNotFoundException e)
    {
      Class<?> c = this.imports.findClass(name);
      if (c == null) {
        throw e;
      }
      return c;
    }
  }
  
  public MetaInfo.Namespace putNamespace(boolean doReplace, String name)
    throws MetaDataRepositoryException
  {
    MetaInfo.Namespace nameSpace = getNamespace(name);
    if ((nameSpace != null) && 
      (!nameSpace.getMetaInfoStatus().isDropped()))
    {
      if (!doReplace) {
        throw new CompilationException("Namespace " + name + " already exists");
      }
      throw new FatalException("Cannot create or replace namespace, drop the namespace first.");
    }
    MetaInfo.Namespace newschema = new MetaInfo.Namespace();
    newschema.construct(name, new UUID(System.currentTimeMillis()));
    MetaInfo.Namespace namespace = (MetaInfo.Namespace)putNamespace(newschema);
    addAppSpecificRoles(namespace);
    addAppRolesToGlobalRoles(namespace);
    return newschema;
  }
  
  private void addAppSpecificRoles(MetaInfo.Namespace ns)
    throws MetaDataRepositoryException
  {
    if (ns.name.equals("Global")) {
      return;
    }
    try
    {
      List<ObjectPermission> permissions1 = new ArrayList();
      permissions1.add(new ObjectPermission(ns.name + ":*:*:*"));
      permissions1.add(new ObjectPermission("Global:*:namespace:" + ns.name));
      MetaInfo.Role admin = new MetaInfo.Role();
      admin.construct(ns, "admin", null, permissions1);
      putMetaObject(admin);
      
      List<ObjectPermission> permissions2 = null;
      permissions2 = new ArrayList();
      permissions2.add(new ObjectPermission(ns.name + ":create,update,read,select,start,stop:*:*"));
      permissions2.add(new ObjectPermission("Global:read,select:namespace:" + ns.name));
      MetaInfo.Role dev = new MetaInfo.Role();
      dev.construct(ns, "dev", null, permissions2);
      putMetaObject(dev);
      
      List<ObjectPermission> permissions3 = null;
      permissions3 = new ArrayList();
      permissions3.add(new ObjectPermission(ns.name + ":read,select:*:*"));
      permissions3.add(new ObjectPermission("Global:read,select:namespace:" + ns.name));
      MetaInfo.Role eu = new MetaInfo.Role();
      eu.construct(ns, "enduser", null, permissions3);
      putMetaObject(eu);
    }
    catch (SecurityException se)
    {
      logger.error("error in creating roles for namespace " + ns.name);
    }
  }
  
  private void addAppRolesToGlobalRoles(MetaInfo.Namespace namespace)
    throws MetaDataRepositoryException
  {
    if (logger.isInfoEnabled()) {
      logger.info("added app specific roles ");
    }
    String uid = WASecurityManager.getAutheticatedUserName(this.sessionID);
    MetaInfo.User u = (MetaInfo.User)getMetaObject(EntityType.USER, "Global", uid);
    MetaInfo.Role r1 = (MetaInfo.Role)getMetaObject(EntityType.ROLE, namespace.getName(), "admin");
    MetaInfo.Role r2 = (MetaInfo.Role)getMetaObject(EntityType.ROLE, namespace.getName(), "dev");
    MetaInfo.Role r3 = (MetaInfo.Role)getMetaObject(EntityType.ROLE, namespace.getName(), "enduser");
    if (!namespace.name.equals("admin"))
    {
      if (!u.getUserId().equalsIgnoreCase("admin"))
      {
        u.grantRole(r1);
        updateMetaObject(u);
      }
      MetaInfo.Role adr = (MetaInfo.Role)getMetaObject(EntityType.ROLE, "Global", "appadmin");
      adr.grantRole(r1);
      updateMetaObject(adr);
      
      MetaInfo.Role adv = (MetaInfo.Role)getMetaObject(EntityType.ROLE, "Global", "appdev");
      adv.grantRole(r2);
      updateMetaObject(adv);
      
      MetaInfo.Role aur = (MetaInfo.Role)getMetaObject(EntityType.ROLE, "Global", "appuser");
      aur.grantRole(r3);
      updateMetaObject(aur);
    }
  }
  
  public MetaInfo.Cache putCache(boolean doReplace, ObjectName objectName, String adapterClassName, Map<String, Object> reader_propertySet, Map<String, Object> parser_propertySet, Map<String, Object> query_propertySet, UUID typename, Class<?> ret)
    throws MetaDataRepositoryException
  {
    UUID oldUUID = null;
    String name = addSchemaPrefix(objectName);
    MetaInfo.MetaObject dataSource = getDataSource(name);
    if (dataSource != null) {
      if (this.recompileMode)
      {
        removeObject(dataSource);
        oldUUID = dataSource.uuid;
      }
      else
      {
        if (!doReplace) {
          throw new CompilationException(dataSource.type + " with name <" + name + "> already exists");
        }
        dropObject(dataSource.name, dataSource.type, DropMetaObject.DropRule.NONE);
      }
    }
    MetaInfo.Cache newcache = new MetaInfo.Cache();
    newcache.construct(name, getNamespace(objectName.getNamespace()), adapterClassName, reader_propertySet, parser_propertySet, query_propertySet, typename, ret);
    if ((this.recompileMode) && 
      (oldUUID != null)) {
      newcache.uuid = oldUUID;
    }
    putCache(newcache);
    newcache = (MetaInfo.Cache)getDataSource(name);
    return newcache;
  }
  
  public MetaInfo.Type putType(boolean doReplace, ObjectName objectName, String extendsType, Map<String, String> fields, List<String> keyFields, boolean isAnonymous)
    throws MetaDataRepositoryException
  {
    return putType(doReplace, objectName, extendsType, fields, keyFields, new MetaInfoStatus().setAnonymous(isAnonymous));
  }
  
  public MetaInfo.Type putType(boolean doReplace, ObjectName objectName, String extendsType, Map<String, String> fields, List<String> keyFields, MetaInfoStatus statusOfObjects)
    throws MetaDataRepositoryException
  {
    UUID oldUUID = null;
    String name = addSchemaPrefix(objectName);
    MetaInfo.Type type = getType(name);
    MetaInfo.Flow saveCurrentFlow = null;
    MetaInfo.Flow saveCurrentApp = null;
    if (type != null) {
      if (this.recompileMode)
      {
        if (type != null)
        {
          oldUUID = type.uuid;
          removeObject(type);
        }
      }
      else if (!type.getMetaInfoStatus().isDropped())
      {
        if (!doReplace) {
          throw new CompilationException("Type " + name + " already exists");
        }
        saveCurrentFlow = getCurFlow();
        saveCurrentApp = getCurApp();
        
        MetaInfo.Flow aflow = type.getCurrentFlow();
        MetaInfo.Flow anapp = type.getCurrentApp();
        if ((anapp != null) && (!anapp.equals(aflow))) {
          setCurFlow(aflow);
        }
        setCurApp(anapp);
        
        DropMetaObject.DropType.drop(this, type, DropMetaObject.DropRule.NONE, this.sessionID);
      }
    }
    WALoader loader = WALoader.get();
    
    String className = "wa." + name + "_1_0";
    if (loader.isExistingClass(className)) {
      if ((type == null) && (loader.isGeneratedClass(className))) {
        loader.removeTypeClass(getNamespaceName(objectName.getNamespace()), className);
      } else if ((loader.isGeneratedClass(className)) && (doReplace)) {
        loader.removeTypeClass(getNamespaceName(objectName.getNamespace()), className);
      } else {
        throw new CompilationException("Cannot create type " + type + " another class " + className + " already exists");
      }
    }
    MetaInfo.Namespace ns = getNamespace(objectName.getNamespace());
    UUID extendsTypeUUID = null;
    MetaInfo.Type et = null;
    if (extendsType != null)
    {
      et = getType(addSchemaPrefix(null, extendsType));
      if (et != null) {
        extendsTypeUUID = et.uuid;
      }
    }
    type = new MetaInfo.Type();
    type.construct(name, ns, className, extendsTypeUUID, fields, keyFields, true);
    if ((this.recompileMode) && 
      (oldUUID != null)) {
      type.uuid = oldUUID;
    }
    if (statusOfObjects != null) {
      type.setMetaInfoStatus(statusOfObjects);
    }
    putType(type);
    type = getType(name);
    try
    {
      type.generateClass();
    }
    catch (Exception e)
    {
      removeMetaObject(type.getUuid());
      throw new RuntimeException(e.getMessage());
    }
    if (et != null)
    {
      if (et.extendedBy == null) {
        et.extendedBy = new ArrayList();
      }
      et.extendedBy.add(type.uuid);
      putType(et);
    }
    if (saveCurrentFlow != null) {
      setCurFlow(saveCurrentFlow);
    }
    if (saveCurrentApp != null) {
      setCurApp(saveCurrentApp);
    }
    return type;
  }
  
  public Class<?> getTypeClass(UUID uuid)
    throws MetaDataRepositoryException
  {
    MetaInfo.MetaObject type = getObject(uuid);
    assert ((type instanceof MetaInfo.Type));
    MetaInfo.Type t = (MetaInfo.Type)type;
    try
    {
      return WALoader.get().loadClass(t.className);
    }
    catch (ClassNotFoundException e)
    {
      throw new CompilationException("Internal error: cannot load type <" + t.name + ">", e);
    }
  }
  
  public MetaInfo.Type getTypeInfo(UUID uuid)
    throws MetaDataRepositoryException
  {
    MetaInfo.MetaObject type = getObject(uuid);
    assert ((type instanceof MetaInfo.Type));
    return (MetaInfo.Type)type;
  }
  
  public MetaInfo.Stream putStream(boolean doReplace, ObjectName objectName, UUID type, List<String> partitioning_fields, GracePeriod gp, String pset, UUID oldUUIDparam, MetaInfoStatus statusOfObjects)
    throws MetaDataRepositoryException
  {
    UUID oldUUID = null;
    String name = addSchemaPrefix(objectName);
    MetaInfo.MetaObject dataSource = getDataSource(name);
    if (dataSource != null) {
      if (this.recompileMode)
      {
        removeObject(dataSource);
        oldUUID = dataSource.uuid;
      }
      else
      {
        if (!doReplace) {
          throw new CompilationException(dataSource.type + " with name <" + name + "> already exists");
        }
        dropObject(dataSource.name, dataSource.type, DropMetaObject.DropRule.NONE);
      }
    }
    MetaInfo.Namespace ns = getNamespace(objectName.getNamespace());
    
    MetaInfo.Stream newstream = new MetaInfo.Stream();
    newstream.construct(name, ns, type, partitioning_fields, gp, pset);
    if (this.recompileMode)
    {
      if (oldUUID != null) {
        newstream.uuid = oldUUID;
      }
      if (oldUUIDparam != null) {
        newstream.uuid = oldUUIDparam;
      }
    }
    if (statusOfObjects != null) {
      newstream.setMetaInfoStatus(statusOfObjects);
    }
    putStream(newstream);
    newstream = (MetaInfo.Stream)getObject(newstream.uuid);
    return newstream;
  }
  
  public MetaInfo.Window putWindow(boolean doReplace, ObjectName objectName, UUID stream, List<String> partitioningFields, Pair<IntervalPolicy, IntervalPolicy> windowPolicy, boolean jumping, boolean persistent, boolean implicit, Object options)
    throws MetaDataRepositoryException
  {
    UUID oldUUID = null;
    String name = addSchemaPrefix(objectName);
    MetaInfo.MetaObject dataSource = getDataSource(name);
    if (dataSource != null) {
      if (this.recompileMode)
      {
        removeObject(dataSource);
        oldUUID = dataSource.uuid;
      }
      else
      {
        if (!doReplace) {
          throw new CompilationException(dataSource.type + " with name <" + name + "> already exists");
        }
        if (dataSource.type != EntityType.WINDOW) {
          throw new CompilationException("Window have the same name with the stream");
        }
        dropObject(dataSource.name, dataSource.type, DropMetaObject.DropRule.NONE);
      }
    }
    IntervalPolicy windowLen = (IntervalPolicy)windowPolicy.first;
    if ((jumping) && (windowPolicy.second != null)) {
      throw new CompilationException("Jumping window cannot have a Slide policy.");
    }
    if (windowLen.isAttrBased())
    {
      String on_field = windowLen.getAttrPolicy().getAttrName();
      MetaInfo.Stream stream_metainfo = (MetaInfo.Stream)getObject(stream);
      
      MetaInfo.Type type_metainfo = (MetaInfo.Type)getObject(stream_metainfo.dataType);
      boolean matched = false;
      for (Map.Entry<String, String> f : type_metainfo.fields.entrySet()) {
        if (((String)f.getKey()).equalsIgnoreCase(on_field))
        {
          matched = true;
          break;
        }
      }
      if (!matched) {
        throw new CompilationException("stream <" + stream_metainfo.name + "> has no such field <" + on_field + "> ");
      }
    }
    MetaInfo.Window newwindow = new MetaInfo.Window();
    MetaInfo.Namespace ns = getNamespace(Utility.splitDomain(name));
    if (ns == null) {
      throw new CompilationException("Cannot find the namespace to put windows: " + Utility.splitDomain(name));
    }
    newwindow.construct(Utility.splitName(name), ns, stream, windowPolicy, jumping, persistent, partitioningFields, implicit, options);
    if ((this.recompileMode) && 
      (oldUUID != null)) {
      newwindow.uuid = oldUUID;
    }
    putWindow(newwindow);
    newwindow = (MetaInfo.Window)getDataSource(name);
    return newwindow;
  }
  
  public MetaInfo.CQ putCQ(boolean doReplace, ObjectName objectName, UUID outputStream, CQExecutionPlan plan, String select, List<String> fieldList, MetaInfoStatus statusOfObjects)
    throws MetaDataRepositoryException
  {
    String name = addSchemaPrefix(objectName);
    UUID oldUUID = null;
    MetaInfo.CQ cq = getCQ(name);
    if (cq != null) {
      if (this.recompileMode)
      {
        if (cq != null)
        {
          oldUUID = cq.uuid;
          removeObject(cq);
        }
      }
      else if (!cq.getMetaInfoStatus().isDropped())
      {
        if (!doReplace) {
          throw new CompilationException("Continuous query " + name + " already exists");
        }
        DropMetaObject.DropCQ.drop(this, cq, DropMetaObject.DropRule.NONE, this.sessionID);
      }
    }
    MetaInfo.CQ newcq = new MetaInfo.CQ();
    newcq.construct(name, getNamespace(objectName.getNamespace()), outputStream, plan, select, fieldList);
    if ((this.recompileMode) && 
      (oldUUID != null)) {
      newcq.uuid = oldUUID;
    }
    if (statusOfObjects != null) {
      newcq.setMetaInfoStatus(statusOfObjects);
    }
    putCQ(newcq);
    newcq = getCQ(name);
    return newcq;
  }
  
  public MetaInfo.PropertySet putPropertySet(boolean doReplace, String propsName, Map<String, Object> propertySet)
    throws MetaDataRepositoryException
  {
    String name = addSchemaPrefix(null, propsName);
    MetaInfo.PropertySet props = getPropertySet(name);
    if (props != null)
    {
      if (!doReplace) {
        throw new CompilationException("Property Set " + name + " already exists");
      }
      removeObject(props.uuid);
    }
    MetaInfo.PropertySet newprops = new MetaInfo.PropertySet();
    newprops.construct(name, getNamespace(null), propertySet);
    
    putPropertySet(newprops);
    return newprops;
  }
  
  public MetaInfo.PropertyVariable putPropertyVariable(boolean doReplace, String paramName, Map<String, Object> propertySet)
    throws MetaDataRepositoryException
  {
    String name = addSchemaPrefix(null, paramName);
    MetaInfo.PropertyVariable propertyVariable = getPropertyVariable(name);
    if (propertyVariable != null)
    {
      if (!doReplace) {
        throw new CompilationException("PropertyVariable " + name + " already exists");
      }
      removeObject(propertyVariable.uuid);
    }
    MetaInfo.PropertyVariable newPropertyVariable = new MetaInfo.PropertyVariable();
    newPropertyVariable.construct(name, getNamespace(null), propertySet);
    putPropertyVariable(newPropertyVariable);
    return newPropertyVariable;
  }
  
  public MetaInfo.Source putSource(boolean doReplace, ObjectName objectName, String adapterClassName, Map<String, Object> propertySet, Map<String, Object> parserPropertySet, UUID outputStream, UUID oldUUID)
    throws MetaDataRepositoryException
  {
    String name = addSchemaPrefix(objectName);
    MetaInfo.Source src = getSource(name);
    if ((src != null) && 
      (!src.getMetaInfoStatus().isDropped())) {
      if (!doReplace) {
        throw new CompilationException("Source " + name + " already exists");
      }
    }
    MetaInfo.Source newsrc = new MetaInfo.Source();
    newsrc.construct(name, getNamespace(objectName.getNamespace()), adapterClassName, propertySet, parserPropertySet, outputStream);
    if ((this.recompileMode) && 
      (oldUUID != null)) {
      newsrc.uuid = oldUUID;
    }
    putSource(newsrc);
    newsrc = getSource(name);
    return newsrc;
  }
  
  public MetaInfo.Target putTarget(boolean doReplace, ObjectName objectName, String adapterClassName, Map<String, Object> propertySet, Map<String, Object> formatterPropertySet, UUID inputStream, UUID oldUUID)
    throws MetaDataRepositoryException
  {
    String name = addSchemaPrefix(objectName);
    MetaInfo.Target target = getTarget(name);
    if ((target != null) && 
      (!target.getMetaInfoStatus().isDropped())) {
      if (!doReplace) {
        throw new CompilationException("Target " + name + " already exists");
      }
    }
    MetaInfo.Target newtarget = new MetaInfo.Target();
    newtarget.construct(name, getNamespace(objectName.getNamespace()), adapterClassName, propertySet, formatterPropertySet, inputStream);
    if ((this.recompileMode) && 
      (oldUUID != null)) {
      newtarget.uuid = oldUUID;
    }
    putTarget(newtarget);
    newtarget = getTarget(name);
    
    return newtarget;
  }
  
  public MetaInfo.Flow putFlow(EntityType type, boolean doReplace, ObjectName objectName, boolean encrypted, RecoveryDescription recov, Map<String, Object> eh, Map<EntityType, LinkedHashSet<UUID>> objects, Set<String> importStatements)
    throws MetaDataRepositoryException
  {
    assert ((type == EntityType.FLOW) || (type == EntityType.APPLICATION));
    UUID oldUUID = null;
    int oldRecovType = 0;
    long oldRecovPeriod = 0L;
    
    String name = addSchemaPrefix(objectName);
    MetaInfo.Flow flow = (MetaInfo.Flow)getFlow(name, type);
    if (flow != null) {
      if (this.recompileMode)
      {
        if (flow != null)
        {
          oldUUID = flow.uuid;
          oldRecovType = flow.recoveryType;
          oldRecovPeriod = flow.recoveryPeriod;
          removeObject(flow);
        }
      }
      else if (!flow.getMetaInfoStatus().isDropped())
      {
        if (!doReplace) {
          throw new CompilationException(type + " " + flow.name + " already exists");
        }
        DropMetaObject.DropApplication.drop(this, flow, DropMetaObject.DropRule.CASCADE, this.sessionID);
        flow = (MetaInfo.Flow)getFlow(name, type);
      }
    }
    boolean startBlock = objects == null;
    if (startBlock) {
      if (type == EntityType.APPLICATION)
      {
        if (this.curApp != null) {
          throw new CompilationException("cannot create APPLICATION <" + name + "> inside of another APPLICATION <" + this.curApp.name + ">, use command 'END APPLICATION <" + this.curApp.name + ">' to close the application scope.");
        }
        if (this.curFlow != null) {
          throw new CompilationException("cannot create APPLICATION <" + name + "> inside of FLOW <" + this.curFlow.name + ">, use command 'END FLOW <" + this.curFlow.name + ">' to close the flow scope.");
        }
      }
      else
      {
        if (this.curApp == null) {
          throw new CompilationException("cannot create FLOW <" + name + "> outside of APPLICATION");
        }
        if (this.curFlow != null) {
          throw new CompilationException("cannot create FLOW <" + name + "> inside of another FLOW <" + this.curFlow.name + ">");
        }
      }
    }
    MetaInfo.Namespace ns = getNamespace(objectName.getNamespace());
    
    int recoveryType = recov != null ? recov.type : 0;
    long recoveryPeriod = recov != null ? recov.interval : 0L;
    MetaInfo.Flow newflow = new MetaInfo.Flow();
    newflow.construct(type, name, ns, objects, recoveryType, recoveryPeriod);
    if (type.equals(EntityType.APPLICATION))
    {
      newflow.setEncrypted(encrypted);
      newflow.setEhandlers(eh);
      newflow.setImportStatements(importStatements);
    }
    if ((this.recompileMode) && 
      (oldUUID != null))
    {
      newflow.uuid = oldUUID;
      newflow.recoveryType = oldRecovType;
      newflow.recoveryPeriod = oldRecovPeriod;
    }
    MetaInfo.Flow flower = (MetaInfo.Flow)putFlow(newflow);
    if (startBlock) {
      if (type == EntityType.APPLICATION)
      {
        if (this.recompileMode) {
          this.curApp = flower;
        } else if ((flow != null) && (!flow.getMetaInfoStatus().isDropped())) {
          this.curApp = flow;
        } else {
          this.curApp = flower;
        }
      }
      else if (this.recompileMode) {
        this.curFlow = flower;
      } else if ((flow != null) && (!flow.getMetaInfoStatus().isDropped())) {
        this.curFlow = flow;
      } else {
        this.curFlow = flower;
      }
    }
    return flower;
  }
  
  public MetaInfo.DeploymentGroup putDeploymentGroup(String deploymentGroupName, List<String> deploymentGroup, Long minServers)
    throws MetaDataRepositoryException
  {
    MetaInfo.DeploymentGroup dGroup = getDeploymentGroup(deploymentGroupName);
    if (dGroup != null) {
      throw new CompilationException("Deployment Group " + deploymentGroupName + " already exists");
    }
    MetaInfo.DeploymentGroup newDGroup = new MetaInfo.DeploymentGroup();
    newDGroup.construct(deploymentGroupName);
    newDGroup.addConfiguredMembers(deploymentGroup);
    newDGroup.setMinimumRequiredServers(minServers.longValue());
    if (logger.isInfoEnabled()) {
      logger.info("Create new DG : " + newDGroup.toString());
    }
    putDeploymentGroup(newDGroup);
    return newDGroup;
  }
  
  private MetaInfo.MetaObject getBaseServer(String baseServerName)
    throws MetaDataRepositoryException
  {
    MetaInfo.MetaObject server = this.metadataRepository.getServer(baseServerName, this.sessionID);
    if (server == null)
    {
      MetaInfo.MetaObject agent = getMetaObject(EntityType.AGENT, "Global", baseServerName);
      if (agent == null) {
        throw new CompilationException("Server or Agent " + baseServerName + " does not exist");
      }
      return agent;
    }
    return server;
  }
  
  public void alterDeploymentGroup(boolean add, String deploymentGroupName, List<String> deploymentGroup)
    throws MetaDataRepositoryException
  {
    if ((deploymentGroup == null) || (deploymentGroup == null)) {
      throw new CompilationException("Null server name or deployment group entered.");
    }
    MetaInfo.DeploymentGroup dGroup = getDeploymentGroup(deploymentGroupName);
    if (dGroup == null) {
      throw new CompilationException("Deployment Group " + deploymentGroupName + " does not exists");
    }
    if (add) {
      dGroup.addConfiguredMembers(deploymentGroup);
    } else {
      dGroup.removeConfiguredMember(deploymentGroup);
    }
    this.metadataRepository.updateMetaObject(dGroup, this.sessionID);
    if (logger.isInfoEnabled()) {
      logger.info("Alter DG : " + dGroup.toString());
    }
  }
  
  public Imports getImports()
  {
    return this.imports;
  }
  
  public String getNamespaceName(@Nullable String optionalNamespace)
  {
    return getNamespace(optionalNamespace).getName();
  }
  
  @Nonnull
  public String getCurNamespaceName()
  {
    return getNamespace(null).getName();
  }
  
  @Nonnull
  public MetaInfo.Namespace getCurNamespace()
  {
    return getNamespace(null);
  }
  
  public MetaInfo.Namespace getNamespace(@Nullable String optionalNamespace)
  {
    MetaInfo.Namespace namespaceObject = null;
    if (optionalNamespace == null) {
      return this.curNamespace;
    }
    if (Utility.checkIfFullName(optionalNamespace)) {
      optionalNamespace = Utility.splitDomain(optionalNamespace);
    }
    try
    {
      namespaceObject = (MetaInfo.Namespace)get(optionalNamespace, EntityType.NAMESPACE);
    }
    catch (MetaDataRepositoryException e)
    {
      if (logger.isInfoEnabled()) {
        logger.info("Looking for namespace: " + optionalNamespace + ", namespace doesn't exist.");
      }
    }
    if (namespaceObject == null) {
      return null;
    }
    return namespaceObject;
  }
  
  public ObjectName makeObjectName(String name)
  {
    if (Utility.checkIfFullName(name)) {
      return ObjectName.makeObjectName(Utility.splitDomain(name), Utility.splitName(name));
    }
    return ObjectName.makeObjectName(getCurNamespaceName(), Utility.splitName(name));
  }
  
  public ObjectName makeObjectName(String namespacename, String objname)
  {
    return makeObjectName(addSchemaPrefix(namespacename, objname));
  }
  
  public String addSchemaPrefix(@Nullable String optionalNamespaceName, @Nonnull String name)
  {
    return Utility.convertToFullQualifiedName(getNamespace(optionalNamespaceName), name);
  }
  
  public String addSchemaPrefix(ObjectName object)
  {
    return Utility.convertToFullQualifiedName(getNamespace(object.getNamespace()), object.getName());
  }
  
  public void recompileQuery(String queryName)
    throws Exception
  {
    String fullQualifiedName = Utility.convertToFullQualifiedName(getNamespace(null), queryName);
    MetaInfo.Query queryMetaObject = (MetaInfo.Query)getMetaObject(EntityType.QUERY, Utility.splitDomain(fullQualifiedName), Utility.splitName(fullQualifiedName));
    if (queryMetaObject == null) {
      throw new RuntimeException("No such query exist.");
    }
    if (queryMetaObject.getMetaInfoStatus().isValid()) {
      throw new RuntimeException("Query " + queryMetaObject.getFullName() + " is already valid, no need to recompile.");
    }
    UUID queryId = queryMetaObject.getUuid();
    String selectQuery = queryMetaObject.queryDefinition;
    String namespaceName = queryMetaObject.getNsName();
    dropObject(queryMetaObject.getFullName(), EntityType.QUERY, DropMetaObject.DropRule.NONE);
    try
    {
      final AtomicBoolean isCompilationSucessful = new AtomicBoolean(true);
      Compiler.compile("CREATE NAMEDQUERY " + queryMetaObject.name + " " + queryMetaObject.queryDefinition, this, new Compiler.ExecutionCallback()
      {
        public void execute(Stmt stmt, Compiler compiler)
          throws MetaDataRepositoryException
        {
          try
          {
            compiler.compileStmt(stmt);
          }
          catch (Warning e)
          {
            isCompilationSucessful.set(false);
          }
        }
      });
      if (!isCompilationSucessful.get()) {
        throw new RuntimeException("Problem in recompilation");
      }
    }
    catch (Exception e)
    {
      MetaInfo.Query Q = new MetaInfo.Query(queryName, getNamespace(namespaceName), null, null, null, null, selectQuery, Boolean.valueOf(false));
      Q.setUuid(queryId);
      Q.getMetaInfoStatus().setValid(false);
      putQuery(Q);
      throw new RuntimeException("Recompilation failed");
    }
  }
  
  public void useNamespace(String nsName)
    throws MetaDataRepositoryException
  {
    MetaInfo.Namespace ns = getNamespace(nsName);
    if (ns == null) {
      throw new CompilationException("Namespace " + nsName + " does not exist");
    }
    this.curNamespace = ns;
  }
  
  public boolean recompileMode = false;
  
  public void alterAppOrFlow(EntityType type, String name, boolean recompile)
    throws MetaDataRepositoryException
  {
    MetaInfo.Flow f = getFlowInCurSchema(name, type);
    if (f == null) {
      throw new CompilationException(type + " " + name + " does not exist in current namespace " + this.curNamespace.name);
    }
    PermissionUtility.checkPermission(f, ObjectPermission.Action.update, this.sessionID, true);
    if (f.isDeployed()) {
      throw new CompilationException(type + " " + f.getFullName() + " is deployed, cannot alter. (Undeploy first) ");
    }
    if (recompile)
    {
      this.curApp = null;
      this.curFlow = null;
      this.recompileMode = true;
      try
      {
        recompile(f);
      }
      catch (Exception e)
      {
        throw e;
      }
      this.recompileMode = false;
      return;
    }
    if (type == EntityType.APPLICATION)
    {
      MetaInfo.Namespace ns = getNamespace(f.nsName);
      if (ns == null) {
        throw new CompilationException("There is no namespace " + f.nsName + " for application " + name);
      }
      this.curApp = f;
      this.curFlow = null;
      this.curNamespace = ns;
    }
    else
    {
      if (this.curApp != null)
      {
        Set<UUID> flowsInApp = this.curApp.getObjects(EntityType.FLOW);
        if ((flowsInApp == null) || (!flowsInApp.contains(f.uuid))) {
          throw new CompilationException("FLOW " + name + " does not exist");
        }
      }
      this.curFlow = f;
    }
  }
  
  Compiler.ExecutionCallback cb = new Compiler.ExecutionCallback()
  {
    public void execute(Stmt stmt, Compiler compiler)
      throws MetaDataRepositoryException
    {
      compiler.compileStmt(stmt);
    }
  };
  
  private void executeTQL(String tqlText)
    throws Exception
  {
    Compiler.compile(tqlText, this, this.cb);
  }
  
  private void recompileObject(MetaInfo.MetaObject metaObject, Map<String, String> recompileMessagesBookkeeping)
    throws Exception
  {
    if (metaObject.getSourceText().toUpperCase().startsWith("CREATE OR REPLACE")) {
      try
      {
        executeTQL(metaObject.getSourceText() + ";");
      }
      catch (Throwable e)
      {
        recompileMessagesBookkeeping.put(metaObject.getFullName(), e.getMessage());
        this.isAppValidAfterRecompile = false;
        if (logger.isDebugEnabled())
        {
          logger.debug("-> FAILURE \n");
          if ((e instanceof NullPointerException))
          {
            logger.debug("Internal error:" + e + "\n");
          }
          else
          {
            String msg = e.getLocalizedMessage();
            logger.debug(msg + "\n");
          }
        }
        throw new RuntimeException("Error in recompile");
      }
    } else {
      try
      {
        executeTQL("CREATE OR REPLACE " + metaObject.getSourceText().substring(6) + ";");
      }
      catch (Throwable e)
      {
        recompileMessagesBookkeeping.put(metaObject.getFullName(), e.getMessage());
        this.isAppValidAfterRecompile = false;
        if (logger.isDebugEnabled())
        {
          logger.debug("-> FAILURE \n");
          if ((e instanceof NullPointerException))
          {
            logger.debug("Internal error:" + e + "\n");
          }
          else
          {
            String msg = e.getLocalizedMessage();
            logger.debug(msg + "\n");
          }
        }
        throw new RuntimeException("Error in recompile");
      }
    }
  }
  
  void createOrEndApplicationOrFlow(MetaInfo.Flow flow, boolean start)
    throws Exception
  {
    if (start) {
      try
      {
        executeTQL("CREATE OR REPLACE " + flow.type + " " + flow.name + ";");
      }
      catch (Throwable e)
      {
        this.isAppValidAfterRecompile = false;
        if (logger.isDebugEnabled())
        {
          logger.debug("-> FAILURE \n");
          if ((e instanceof NullPointerException))
          {
            logger.debug("Internal error:" + e + "\n");
          }
          else
          {
            String msg = e.getLocalizedMessage();
            logger.debug(msg + "\n");
          }
        }
        throw new RuntimeException("Error in recompile");
      }
    } else {
      try
      {
        executeTQL("END " + flow.type + " " + flow.name + ";");
      }
      catch (Throwable e)
      {
        this.isAppValidAfterRecompile = false;
        if (logger.isDebugEnabled())
        {
          logger.debug("-> FAILURE \n");
          if ((e instanceof NullPointerException))
          {
            logger.debug("Internal error:" + e + "\n");
          }
          else
          {
            String msg = e.getLocalizedMessage();
            logger.debug(msg + "\n");
          }
        }
        throw new RuntimeException("Error in recompile");
      }
    }
  }
  
  void recompileVisualization(MetaInfo.Visualization metaObject)
    throws Exception
  {
    executeTQL("CREATE VISUALIZATION \"" + metaObject.fname + "\";");
  }
  
  void recompileSubscription(MetaInfo.Target metaObject)
    throws Exception
  {
    String str = "";
    for (Map.Entry<String, Object> entry : metaObject.properties.entrySet()) {
      str = str + (String)entry.getKey() + ":" + entry.getValue() + ",";
    }
    str = str.substring(0, str.length() - 1);
    MetaInfo.Stream stream = (MetaInfo.Stream)MetadataRepository.getINSTANCE().getMetaObjectByUUID(metaObject.getInputStream(), WASecurityManager.TOKEN);
    
    String sr = "CREATE SUBSCRIPTION " + metaObject.getName() + " USING " + metaObject.adapterClassName + "(" + str + ") INPUT FROM " + stream.name + ";";
    executeTQL(sr);
  }
  
  private void addToFlow(MetaInfo.MetaObject obj)
    throws MetaDataRepositoryException
  {
    updateReverseIndex(obj);
    if ((this.curApp != null) || (this.curFlow != null))
    {
      MetaInfo.Flow f = this.curFlow != null ? this.curFlow : this.curApp;
      
      f.addObject(obj.type, obj.uuid);
      updateFlow(f);
    }
  }
  
  private void updateReverseIndex(MetaInfo.MetaObject metaObject)
    throws MetaDataRepositoryException
  {
    MetaInfo.Flow app = this.curApp;
    if (app != null) {
      metaObject.addReverseIndexObjectDependencies(app.uuid);
    }
    MetaInfo.Flow flow = this.curFlow;
    if (flow != null) {
      metaObject.addReverseIndexObjectDependencies(flow.uuid);
    }
    updateMetaObject(metaObject);
  }
  
  private void recompileSpecificEntityType(MetaInfo.Flow flowMetaObject, EntityType type, Map<String, String> recompileMessagesBookkeeping)
    throws MetaDataRepositoryException
  {
    Set<UUID> typeMetaObjectUUIDs = (Set)flowMetaObject.objects.get(type);
    if (typeMetaObjectUUIDs != null)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("----> RECOMPILE " + type);
      }
      for (UUID metaObjectUUID : typeMetaObjectUUIDs)
      {
        MetaInfo.MetaObject metaObject = getObject(metaObjectUUID);
        if (metaObject == null)
        {
          if (logger.isDebugEnabled()) {
            logger.debug("No Such " + type + " UUID exists " + metaObjectUUID);
          }
        }
        else if (metaObject.getMetaInfoStatus().isValid())
        {
          if (logger.isDebugEnabled()) {
            logger.debug("---> added to flow " + metaObject.name);
          }
          addToFlow(metaObject);
        }
        else if ((metaObject != null) && (metaObject.type == EntityType.TYPE))
        {
          if ((((MetaInfo.Type)metaObject).generated.booleanValue()) && (metaObject.getSourceText() != null)) {
            try
            {
              recompileObject(metaObject, recompileMessagesBookkeeping);
            }
            catch (Exception e)
            {
              if (logger.isDebugEnabled()) {
                logger.debug("---> added old object to flow " + metaObject.name);
              }
              addToFlow(metaObject);
              if (this.curFlow != null) {
                this.flowsFailed.add(this.curFlow.name);
              }
            }
          } else if (!((MetaInfo.Type)metaObject).generated.booleanValue()) {
            addToFlow(metaObject);
          }
        }
        else if ((metaObject != null) && (metaObject.type == EntityType.VISUALIZATION))
        {
          try
          {
            recompileVisualization((MetaInfo.Visualization)metaObject);
          }
          catch (Exception e)
          {
            if (logger.isDebugEnabled()) {
              logger.debug("---> added old object to flow " + metaObject.name);
            }
            addToFlow(metaObject);
            if (this.curFlow != null) {
              this.flowsFailed.add(this.curFlow.name);
            }
          }
        }
        else if ((metaObject != null) && (metaObject.type == EntityType.TARGET) && (metaObject.getSourceText() == null) && (((MetaInfo.Target)metaObject).isSubscription()))
        {
          try
          {
            recompileSubscription((MetaInfo.Target)metaObject);
          }
          catch (Exception e)
          {
            recompileMessagesBookkeeping.put(metaObject.getFullName(), e.getMessage());
            if (logger.isDebugEnabled()) {
              logger.debug("---> added old object to flow " + metaObject.name);
            }
            addToFlow(metaObject);
            if (this.curFlow != null) {
              this.flowsFailed.add(this.curFlow.name);
            }
          }
        }
        else if ((metaObject != null) && (metaObject.getSourceText() != null))
        {
          try
          {
            recompileObject(metaObject, recompileMessagesBookkeeping);
          }
          catch (Exception e)
          {
            if (logger.isDebugEnabled()) {
              logger.debug("---> added old object to flow " + metaObject.name);
            }
            addToFlow(metaObject);
            if (this.curFlow != null) {
              this.flowsFailed.add(this.curFlow.name);
            }
          }
        }
        else if (logger.isDebugEnabled())
        {
          logger.debug("Object's source text is null: " + type + " " + metaObject.name);
        }
      }
    }
  }
  
  private void alterOrEndFlow(MetaInfo.Flow flowOrApp, boolean start)
    throws Exception
  {
    if (start) {
      executeTQL("ALTER " + flowOrApp.type + " " + flowOrApp.name + ";");
    } else {
      executeTQL("END " + flowOrApp.type + " " + flowOrApp.name + ";");
    }
  }
  
  boolean isAppValidAfterRecompile = true;
  List<String> flowsFailed = new ArrayList();
  
  private void recompile(MetaInfo.Flow appMetaObject)
    throws MetaDataRepositoryException
  {
    Map<String, String> recompileMessagesBookkeeping = new LinkedHashMap();
    this.isAppValidAfterRecompile = true;
    this.flowsFailed.clear();
    if (appMetaObject.getMetaInfoStatus().isValid()) {
      return;
    }
    List<MetaInfo.Flow> openFlows = new ArrayList();
    try
    {
      createOrEndApplicationOrFlow(appMetaObject, true);
    }
    catch (Exception e) {}
    if (appMetaObject.objects.get(EntityType.FLOW) != null) {
      for (UUID flowUUID : (LinkedHashSet)appMetaObject.objects.get(EntityType.FLOW))
      {
        MetaInfo.Flow flowMetaObject = (MetaInfo.Flow)getObject(flowUUID);
        if (flowMetaObject != null) {
          if (flowMetaObject.getMetaInfoStatus().isValid())
          {
            addToFlow(flowMetaObject);
          }
          else
          {
            openFlows.add(flowMetaObject);
            try
            {
              createOrEndApplicationOrFlow(flowMetaObject, true);
            }
            catch (Exception e) {}
            try
            {
              createOrEndApplicationOrFlow(flowMetaObject, false);
            }
            catch (Exception e) {}
          }
        }
      }
    }
    try
    {
      createOrEndApplicationOrFlow(appMetaObject, false);
    }
    catch (Exception e) {}
    EntityType entityType;
    for (entityType : EntityType.orderOfRecompile)
    {
      try
      {
        alterOrEndFlow(appMetaObject, true);
      }
      catch (Exception e) {}
      recompileSpecificEntityType(appMetaObject, entityType, recompileMessagesBookkeeping);
      try
      {
        alterOrEndFlow(appMetaObject, false);
      }
      catch (Exception e) {}
      if (!openFlows.isEmpty()) {
        for (MetaInfo.Flow flowMetaObject : openFlows)
        {
          try
          {
            alterOrEndFlow(appMetaObject, true);
          }
          catch (Exception e) {}
          try
          {
            alterOrEndFlow(flowMetaObject, true);
          }
          catch (Exception e) {}
          recompileSpecificEntityType(flowMetaObject, entityType, recompileMessagesBookkeeping);
          try
          {
            alterOrEndFlow(flowMetaObject, false);
          }
          catch (Exception e) {}
          try
          {
            alterOrEndFlow(appMetaObject, false);
          }
          catch (Exception e) {}
        }
      }
    }
    if (!this.isAppValidAfterRecompile)
    {
      MetaInfo.Flow newAppObj = (MetaInfo.Flow)get(appMetaObject.nsName + "." + appMetaObject.name, EntityType.APPLICATION);
      newAppObj.getMetaInfoStatus().setValid(false);
      updateMetaObject(newAppObj);
      for (String flow : this.flowsFailed)
      {
        MetaInfo.Flow newFlowObj = (MetaInfo.Flow)get(appMetaObject.nsName + "." + flow, EntityType.FLOW);
        newFlowObj.getMetaInfoStatus().setValid(false);
        updateMetaObject(newFlowObj);
      }
      StringBuffer buffer = new StringBuffer();
      for (Map.Entry<String, String> entry : recompileMessagesBookkeeping.entrySet())
      {
        String key = (String)entry.getKey();
        String value = (String)entry.getValue();
        
        buffer.append("For: " + key + " \nIssue Occurred: " + value + "\n");
      }
      throw new Warning(buffer.toString());
    }
  }
  
  public MetaInfo.Type getTypeInCurSchema(String name)
    throws MetaDataRepositoryException
  {
    return getType(addSchemaPrefix(null, name));
  }
  
  public MetaInfo.Stream getStreamInCurSchema(String name)
    throws MetaDataRepositoryException
  {
    return getStream(addSchemaPrefix(null, name));
  }
  
  public MetaInfo.MetaObject getDataSourceInCurSchema(String name)
    throws MetaDataRepositoryException
  {
    return getDataSource(addSchemaPrefix(null, name));
  }
  
  public MetaInfo.PropertySet getPropertySetInCurSchema(String name)
    throws MetaDataRepositoryException
  {
    return getPropertySet(addSchemaPrefix(null, name));
  }
  
  public MetaInfo.Source getSourceInCurSchema(String name)
    throws MetaDataRepositoryException
  {
    return getSource(addSchemaPrefix(null, name));
  }
  
  public MetaInfo.Target getTargetInCurSchema(String name)
    throws MetaDataRepositoryException
  {
    return getTarget(addSchemaPrefix(null, name));
  }
  
  public MetaInfo.CQ getCQInCurSchema(String name)
    throws MetaDataRepositoryException
  {
    return getCQ(addSchemaPrefix(null, name));
  }
  
  public MetaInfo.Flow getFlowInCurSchema(String name, EntityType type)
    throws MetaDataRepositoryException
  {
    String flowName = addSchemaPrefix(null, name);
    assert ((type == EntityType.FLOW) || (type == EntityType.APPLICATION));
    return (MetaInfo.Flow)get(flowName, type);
  }
  
  public void showStreamStmt(MetaInfo.ShowStream show_stream)
    throws MetaDataRepositoryException
  {
    if (this.readOnly) {
      return;
    }
    this.metadataRepository.putShowStream(show_stream, WASecurityManager.TOKEN);
  }
  
  public AuthToken getSessionID()
  {
    return this.sessionID;
  }
  
  public MetaInfo.WActionStore putWActionStore(boolean doReplace, ObjectName objectName, UUID typeid, Interval howOften, List<UUID> eventTypes, List<String> eventKeys, Map<String, Object> props)
    throws MetaDataRepositoryException
  {
    UUID oldUUID = null;
    String name = addSchemaPrefix(objectName);
    MetaInfo.MetaObject dataSource = getDataSource(name);
    if (dataSource != null) {
      if (this.recompileMode)
      {
        removeObject(dataSource);
        oldUUID = dataSource.uuid;
      }
      else
      {
        if (!doReplace) {
          throw new CompilationException(dataSource.type + " with name <" + name + "> already exists");
        }
        dropObject(dataSource.name, dataSource.type, DropMetaObject.DropRule.NONE);
      }
    }
    MetaInfo.WActionStore newwas = new MetaInfo.WActionStore();
    newwas.construct(name, getNamespace(objectName.getNamespace()), typeid, howOften, eventTypes, eventKeys, props);
    try
    {
      if (this.recompileMode) {
        ((MetaInfo.WActionStore)dataSource).removeGeneratedClasses();
      }
      newwas.generateClasses();
    }
    catch (Exception e)
    {
      logger.error("Problem generating classes: ", e);
    }
    if ((this.recompileMode) && 
      (oldUUID != null)) {
      newwas.uuid = oldUUID;
    }
    putWActionStore(newwas);
    newwas = (MetaInfo.WActionStore)getDataSource(name);
    return newwas;
  }
  
  public MetaInfo.User putUser(String uname, String password, UserProperty userProperty)
    throws Exception
  {
    MetaInfo.User newuser = new MetaInfo.User();
    newuser.construct(uname, password);
    MetaInfo.Namespace usernamespace = null;
    List<String> role_name = userProperty.lroles;
    String namespace = userProperty.defaultnamespace;
    List<Property> attributes = userProperty.properties;
    if ((userProperty.ldap != null) && (!userProperty.ldap.isEmpty()))
    {
      newuser.setOriginType(MetaInfo.User.AUTHORIZATION_TYPE.LDAP);
      newuser.setLdap(userProperty.ldap);
    }
    List<String> lrole = new ArrayList();
    if (role_name != null) {
      for (String string : role_name)
      {
        MetaInfo.Role r = this.security_manager.getRole(string.split(":")[0], string.split(":")[1]);
        if (r == null) {
          throw new CompilationException("ROLE " + string + " does not exist");
        }
        lrole.add(r.getRole());
      }
    }
    String user_namespace_name = namespace == null ? uname : namespace;
    usernamespace = getNamespace(user_namespace_name);
    if (usernamespace == null) {
      usernamespace = putNamespace(false, user_namespace_name);
    }
    newuser.setDefaultNamespace(user_namespace_name);
    if ((attributes != null) && (attributes.size() > 0)) {
      for (Property attr : attributes) {
        setUserAttributes(newuser, attr.name, attr.value.toString());
      }
    }
    newuser.setOriginType(userProperty.originType);
    this.security_manager.addUser(newuser, this.sessionID);
    lrole.add(user_namespace_name + ":admin");
    this.security_manager.grantUserRoles(newuser.getUserId(), lrole, this.sessionID);
    
    grantDefaultUserPermissions(newuser);
    
    return newuser;
  }
  
  private void grantDefaultUserPermissions(MetaInfo.User user)
    throws Exception
  {
    List<ObjectPermission> defaultUserPermissions = new ArrayList();
    
    Set<String> domains = new HashSet();
    Set<ObjectPermission.Action> actions = new HashSet();
    Set<ObjectPermission.ObjectType> objectTypes = new HashSet();
    Set<String> names = new HashSet();
    
    domains.add("Global");
    actions.add(ObjectPermission.Action.read);
    actions.add(ObjectPermission.Action.update);
    objectTypes.add(ObjectPermission.ObjectType.user);
    names.add(user.getUserId());
    ObjectPermission readSelfPerm = new ObjectPermission(domains, actions, objectTypes, names);
    
    defaultUserPermissions.add(readSelfPerm);
    MetaInfo.Role uRole = new MetaInfo.Role();
    MetaInfo.Namespace uNamespace = getNamespace(user.getName());
    uRole.construct(uNamespace, "useradmin", null, defaultUserPermissions);
    this.security_manager.addRole(uRole, this.sessionID);
    
    List<String> rlist = new ArrayList();
    rlist.add(uRole.getRole());
    MetaInfo.Role rSys = this.security_manager.getRole("Global", "systemuser");
    rlist.add(rSys.getRole());
    MetaInfo.Role rUi = this.security_manager.getRole("Global", "uiuser");
    rlist.add(rUi.getRole());
    this.security_manager.grantUserRoles(user.getUserId(), rlist, this.sessionID);
  }
  
  private void setUserAttributes(MetaInfo.User u, String what, String value)
    throws SecurityException, UnsupportedEncodingException, GeneralSecurityException
  {
    if (value == null) {
      return;
    }
    List<MetaInfo.ContactMechanism> contactMechanismList = u.getContactMechanisms();
    switch (what.toUpperCase())
    {
    case "EMAIL": 
      for (MetaInfo.ContactMechanism cm : contactMechanismList) {
        if (cm.getType() == MetaInfo.ContactMechanism.ContactType.email)
        {
          u.updateContactMechanism(cm.getIndex(), MetaInfo.ContactMechanism.ContactType.email, value);
          break;
        }
      }
      u.addContactMechanism(MetaInfo.ContactMechanism.ContactType.email, value);
      break;
    case "SMS": 
      for (MetaInfo.ContactMechanism cm : contactMechanismList) {
        if (cm.getType() == MetaInfo.ContactMechanism.ContactType.sms)
        {
          u.updateContactMechanism(cm.getIndex(), MetaInfo.ContactMechanism.ContactType.sms, value);
          break;
        }
      }
      u.addContactMechanism(MetaInfo.ContactMechanism.ContactType.sms, value);
      break;
    case "PHONE": 
      for (MetaInfo.ContactMechanism cm : contactMechanismList) {
        if (cm.getType() == MetaInfo.ContactMechanism.ContactType.phone)
        {
          u.updateContactMechanism(cm.getIndex(), MetaInfo.ContactMechanism.ContactType.phone, value);
          break;
        }
      }
      u.addContactMechanism(MetaInfo.ContactMechanism.ContactType.phone, value);
      break;
    case "PASSWORD": 
      u.setEncryptedPassword(WASecurityManager.encrypt(value, u.uuid.toEightBytes()));
      break;
    case "FIRSTNAME": 
      u.setFirstName(value);
      break;
    case "LASTNAME": 
      u.setLastName(value);
      break;
    case "MAINEMAIL": 
      u.setMainEmail(value);
      break;
    case "TIMEZONE": 
      TimeZone tz = TimeZone.getTimeZone(value);
      if (tz.getID().equals(value)) {
        u.setUserTimeZone(value);
      } else {
        throw new UnsupportedEncodingException("Unknown timezone - " + value);
      }
      break;
    }
    logger.warn("Cannot set unknown user attribute: " + what);
  }
  
  public MetaInfo.Visualization putVisualization(String objectName, String fname)
    throws MetaDataRepositoryException
  {
    UUID oldUUID = null;
    
    File file = new File(fname);
    Scanner scanner = null;
    try
    {
      scanner = new Scanner(file);
    }
    catch (FileNotFoundException e)
    {
      e.printStackTrace();
    }
    String json = "";
    if (scanner != null)
    {
      while (scanner.hasNextLine()) {
        json = json + scanner.nextLine();
      }
      scanner.close();
    }
    else
    {
      throw new RuntimeException("File creator is failing.");
    }
    String visualizationName = addSchemaPrefix(null, objectName);
    
    MetaInfo.Visualization visualizationMetaObject = (MetaInfo.Visualization)get(visualizationName, EntityType.VISUALIZATION);
    if (visualizationMetaObject != null) {
      if (this.recompileMode)
      {
        if (visualizationMetaObject != null)
        {
          oldUUID = visualizationMetaObject.uuid;
          removeObject(visualizationMetaObject);
        }
      }
      else {
        DropMetaObject.DropVisualization.drop(this, visualizationMetaObject, DropMetaObject.DropRule.NONE, this.sessionID);
      }
    }
    MetaInfo.Visualization newvisualization = new MetaInfo.Visualization();
    newvisualization.construct(visualizationName, getNamespace(null), json, fname);
    if ((this.recompileMode) && 
      (oldUUID != null)) {
      newvisualization.uuid = oldUUID;
    }
    putVisualization(newvisualization);
    newvisualization = (MetaInfo.Visualization)getObject(newvisualization.uuid);
    
    return newvisualization;
  }
  
  public void UpdateUserInfoStmt(String username, Map<String, Object> props_toupdate)
    throws Exception
  {
    if ((props_toupdate == null) || (props_toupdate.isEmpty())) {
      return;
    }
    MetaInfo.User u = (MetaInfo.User)get(username, EntityType.USER);
    if (u == null) {
      throw new RuntimeException("User " + username + " is not found.");
    }
    for (String key : props_toupdate.keySet()) {
      if (!key.equalsIgnoreCase("oldpassword")) {
        if ((key.equalsIgnoreCase("password")) || (key.equalsIgnoreCase("newpassword")))
        {
          String providedOldPassword = (String)props_toupdate.get("oldpassword");
          if (providedOldPassword == null) {
            throw new Exception("Must provide 'oldpassword'");
          }
          String userOldPasswordEncrypted = u.getEncryptedPassword();
          String providedOldPasswordEncrypted = WASecurityManager.encrypt(providedOldPassword, u.uuid.toEightBytes());
          if (!userOldPasswordEncrypted.equals(providedOldPasswordEncrypted)) {
            throw new Exception("Old password is incorrect");
          }
          String value = (String)props_toupdate.get(key);
          setUserAttributes(u, "password", value);
        }
        else
        {
          String value = (String)props_toupdate.get(key);
          setUserAttributes(u, key, value);
        }
      }
    }
    this.security_manager.updateUser(u, this.sessionID);
    if ((HazelcastSingleton.isClientMember()) && (Tungsten.currUserMetaInfo.getName().equals(u.getName())))
    {
      TimeZone jtz = TimeZone.getTimeZone(u.getUserTimeZone());
      Tungsten.userTimeZone = u.getUserTimeZone().equals("") ? null : DateTimeZone.forTimeZone(jtz);
    }
  }
  
  public MetaInfo.Role putRole(String rname)
    throws Exception
  {
    String[] part = rname.split(":");
    MetaInfo.Namespace ns = getNamespace(part[0]);
    if (ns == null) {
      throw new SecurityException("No permission to create role " + part[0] + "." + part[1]);
    }
    MetaInfo.Role newRole = new MetaInfo.Role();
    newRole.construct(ns, part[1]);
    this.security_manager.addRole(newRole, this.sessionID);
    return newRole;
  }
  
  private String objectFullName(EntityType objType, String objectName)
  {
    String name;
    String name;
    if (objType == EntityType.DG)
    {
      name = objectName;
    }
    else
    {
      String name;
      if ((objType != EntityType.NAMESPACE) && (objectName.split("\\.").length == 1)) {
        name = addSchemaPrefix(null, objectName);
      } else {
        name = objectName;
      }
    }
    return name;
  }
  
  private MetaInfo.MetaObject doesObjectExists(EntityType objType, String objectName)
    throws MetaDataRepositoryException
  {
    MetaInfo.MetaObject obj = get(objectName, objType);
    if (obj == null) {
      return null;
    }
    return obj;
  }
  
  private MetaInfo.MetaObject getUserOrRole(String objName, EntityType objType)
  {
    MetaInfo.MetaObject metaObject = null;
    if (objType == EntityType.USER) {
      try
      {
        metaObject = this.security_manager.getUser(objName);
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    } else if (objType == EntityType.ROLE) {
      try
      {
        metaObject = this.security_manager.getRole(objName);
      }
      catch (Exception e) {}
    }
    return metaObject;
  }
  
  public List<String> dropObject(String objName, EntityType objType, DropMetaObject.DropRule dropRule)
    throws MetaDataRepositoryException
  {
    List<String> resultString = new ArrayList();
    MetaInfo.Namespace currentNamespace = getNamespace(null);
    String fullObjectName = objectFullName(objType, objName);
    MetaInfo.MetaObject objectToDrop = null;
    if (this.metadataRepository == null)
    {
      resultString.add("MetaData Cache is not initialized!");
      return resultString;
    }
    if (currentNamespace == null)
    {
      resultString.add("Current namespace is not set!");
      return resultString;
    }
    if ((objType == EntityType.ROLE) || (objType == EntityType.USER)) {
      objectToDrop = getUserOrRole(objName, objType);
    }
    if ((objType != EntityType.ROLE) && (objType != EntityType.USER) && ((objectToDrop = doesObjectExists(objType, fullObjectName)) == null)) {
      throw new Warning(objType + " " + objName + " does not exist");
    }
    switch (objType)
    {
    case ALERTSUBSCRIBER: 
      break;
    case APPLICATION: 
      resultString = DropMetaObject.DropApplication.drop(this, objectToDrop, dropRule, this.sessionID);
      break;
    case CACHE: 
      resultString = DropMetaObject.DropCache.drop(this, objectToDrop, dropRule, this.sessionID);
      break;
    case CQ: 
      resultString = DropMetaObject.DropCQ.drop(this, objectToDrop, dropRule, this.sessionID);
      break;
    case DG: 
      resultString = DropMetaObject.DropDG.drop(this, objectToDrop, dropRule, this.sessionID);
      break;
    case FLOW: 
      resultString = DropMetaObject.DropFlow.drop(this, objectToDrop, dropRule, this.sessionID);
      break;
    case INITIALIZER: 
      break;
    case NAMESPACE: 
      if (getNamespace(null).name.equalsIgnoreCase(splitName(objName))) {
        throw new FatalException("Namespace cannot be dropped while in use; Use different namespace to drop it.");
      }
      if (ProtectedNamespaces.getEnum(splitName(objName)) != null) {
        throw new FatalException("Namespace " + objName + " cannot be dropped.");
      }
      resultString = DropMetaObject.DropNamespace.drop(this, objectToDrop, dropRule, this.sessionID);
      this.curFlow = null;
      this.curApp = null;
      break;
    case PROPERTYSET: 
      resultString = DropMetaObject.DropPropertySet.drop(this, objectToDrop, dropRule, this.sessionID);
      break;
    case PROPERTYVARIABLE: 
      resultString = DropMetaObject.DropPropertyVariable.drop(this, objectToDrop, dropRule, this.sessionID);
      break;
    case PROPERTYTEMPLATE: 
      break;
    case ROLE: 
      resultString = DropMetaObject.DropRole.drop(this, objectToDrop, dropRule, this.sessionID);
      break;
    case SERVER: 
      break;
    case SOURCE: 
      resultString = DropMetaObject.DropSource.drop(this, objectToDrop, dropRule, this.sessionID);
      break;
    case STREAM: 
      resultString = DropMetaObject.DropStream.drop(this, objectToDrop, dropRule, this.sessionID);
      break;
    case STREAM_GENERATOR: 
      break;
    case TARGET: 
      resultString = DropMetaObject.DropTarget.drop(this, objectToDrop, dropRule, this.sessionID);
      break;
    case TYPE: 
      resultString = DropMetaObject.DropType.drop(this, objectToDrop, dropRule, this.sessionID);
      break;
    case USER: 
      resultString = DropMetaObject.DropUser.drop(this, objectToDrop, dropRule, this.sessionID);
      break;
    case VISUALIZATION: 
      resultString = DropMetaObject.DropVisualization.drop(this, objectToDrop, dropRule, this.sessionID);
      break;
    case WACTIONSTORE: 
      resultString = DropMetaObject.DropWactionStore.drop(this, objectToDrop, dropRule, this.sessionID);
      break;
    case WINDOW: 
      resultString = DropMetaObject.DropWindow.drop(this, objectToDrop, dropRule, this.sessionID);
      break;
    case DASHBOARD: 
      resultString = DropMetaObject.DropDashboard.drop(this, objectToDrop, dropRule, this.sessionID);
      break;
    case UNKNOWN: 
      resultString.add("Cannot drop " + objectToDrop.getType() + " " + objectToDrop.getFullName() + ".\n");
      return resultString;
    case QUERY: 
      if (!(objectToDrop instanceof MetaInfo.Query)) {
        throw new Warning("Wrong type to drop, trying to drop Query, but actually " + objectToDrop.type);
      }
      if (((MetaInfo.Query)objectToDrop).isAdhocQuery()) {
        throw new Warning("Query is adhoc query not NamedQuery.");
      }
      deleteQuery((MetaInfo.Query)objectToDrop, this.sessionID);
      resultString.add(objectToDrop.type + " " + objectToDrop.name + " dropped successfully\n");
      break;
    default: 
      resultString.add("Cannot drop " + objectToDrop.getType() + " " + objectToDrop.getFullName() + ".\n");
      return resultString;
    }
    return resultString;
  }
  
  private String splitName(String name)
  {
    String[] splitName = name.split("\\.");
    if (splitName.length == 2) {
      return splitName[1];
    }
    return name;
  }
  
  public void GrantRoleToStmt(GrantRoleToStmt stmt)
  {
    MetaInfo.User toUser = null;
    MetaInfo.Role toRole = null;
    try
    {
      if (stmt.towhat == EntityType.USER) {
        toUser = this.security_manager.getUser(stmt.name);
      }
      if (stmt.towhat == EntityType.ROLE) {
        toRole = this.security_manager.getRole(stmt.name.split(":")[0], stmt.name.split(":")[1]);
      }
      List<String> rlist = new ArrayList();
      for (String rolename : stmt.rolename)
      {
        MetaInfo.Role r = this.security_manager.getRole(rolename.split(":")[0], rolename.split(":")[1]);
        rlist.add(r.getRole());
      }
      if (toUser != null) {
        this.security_manager.grantUserRoles(toUser.getUserId(), rlist, this.sessionID);
      } else {
        this.security_manager.grantRoleRoles(toRole.getRole(), rlist, this.sessionID);
      }
    }
    catch (Exception e)
    {
      throw new FatalException(ErrorCode.Error.GRANTROLEFAILURE.getDescription());
    }
  }
  
  public void GrantPermissionToStmt(GrantPermissionToStmt stmt, List<String> permissions)
  {
    MetaInfo.User toUser = null;
    MetaInfo.Role toRole = null;
    try
    {
      if (stmt.towhat == EntityType.USER)
      {
        toUser = this.security_manager.getUser(stmt.name);
        if (toUser == null) {
          throw new RuntimeException("Invalid user name: " + stmt.name);
        }
      }
      if (stmt.towhat == EntityType.ROLE)
      {
        toRole = this.security_manager.getRole(stmt.name.split(":")[0], stmt.name.split(":")[1]);
        if (toRole == null) {
          throw new RuntimeException("Invalid role name: " + stmt.name);
        }
      }
      for (String perm : permissions)
      {
        ObjectPermission op = new ObjectPermission(perm);
        if (toUser != null) {
          this.security_manager.grantUserPermission(toUser.getUserId(), op, this.sessionID);
        } else {
          this.security_manager.grantRolePermission(toRole.getRole(), op, this.sessionID);
        }
      }
    }
    catch (Exception e)
    {
      throw new RuntimeException("Error granting permission to " + stmt.towhat + " : " + stmt.name + ". Message : " + e.getMessage());
    }
  }
  
  public void RevokePermissionFromStmt(RevokePermissionFromStmt stmt, List<String> permissions)
  {
    MetaInfo.User toUser = null;
    MetaInfo.Role toRole = null;
    try
    {
      if (stmt.towhat == EntityType.USER)
      {
        toUser = this.security_manager.getUser(stmt.name);
        if (toUser == null) {
          throw new RuntimeException("Invalid user name: " + stmt.name);
        }
      }
      if (stmt.towhat == EntityType.ROLE)
      {
        toRole = this.security_manager.getRole(stmt.name.split(":")[0], stmt.name.split(":")[1]);
        if (toRole == null) {
          throw new RuntimeException("Invalid role name: " + stmt.name);
        }
      }
      for (String perm : permissions)
      {
        ObjectPermission op = new ObjectPermission(perm);
        if (toUser != null) {
          this.security_manager.revokeUserPermission(toUser.getUserId(), op, this.sessionID);
        } else {
          this.security_manager.revokeRolePermission(toRole.getRole(), op, this.sessionID);
        }
      }
    }
    catch (Exception e)
    {
      throw new RuntimeException("Error granting permission to " + stmt.towhat + " : " + stmt.name + ". Message : " + e.getMessage());
    }
  }
  
  public void RevokeRoleFromStmt(RevokeRoleFromStmt stmt)
  {
    MetaInfo.User toUser = null;
    MetaInfo.Role toRole = null;
    try
    {
      if (stmt.towhat == EntityType.USER) {
        toUser = this.security_manager.getUser(stmt.name);
      }
      if (stmt.towhat == EntityType.ROLE) {
        toRole = this.security_manager.getRole(stmt.name.split(":")[0], stmt.name.split(":")[1]);
      }
      List<String> rlist = new ArrayList();
      for (String rolename : stmt.rolename)
      {
        MetaInfo.Role r = this.security_manager.getRole(rolename.split(":")[0], rolename.split(":")[1]);
        rlist.add(r.getRole());
      }
      if (toUser != null) {
        this.security_manager.revokeUserRoles(toUser.getUserId(), rlist, this.sessionID);
      } else {
        this.security_manager.revokeRoleRoles(toRole.getRole(), rlist, this.sessionID);
      }
    }
    catch (Exception e)
    {
      throw new RuntimeException("Error revoking role: " + stmt.rolename + ". Message : " + e.getMessage());
    }
  }
  
  public synchronized AuthToken Connect(String uname, String password, String clusterName, String host)
    throws MetaDataRepositoryException
  {
    if ((clusterName != null) && (!clusterName.isEmpty()))
    {
      HazelcastInstance in = HazelcastSingleton.initIfPopulated(clusterName, host);
      if (in == null) {
        throw new MetaDataRepositoryException("Cluster " + clusterName + " not found");
      }
      boolean isCleared = MetadataRepository.getINSTANCE().clear(false);
      if (!isCleared) {
        logger.warn("Failed to clear the MDR when changing clusters.");
      }
      MonitorModel.resetDbConnection();
    }
    AuthToken session_id;
    try
    {
      String clientId = HazelcastSingleton.get().getLocalEndpoint().getUuid();
      session_id = this.security_manager.authenticate(uname, password, clientId, "Tungsten");
    }
    catch (Exception ex)
    {
      throw new MetaDataRepositoryException(ex.getLocalizedMessage());
    }
    if (session_id != null)
    {
      System.out.println("Successfully connected as " + uname);
      if (this.sessionID != null) {
        this.security_manager.logout(this.sessionID);
      }
      Tungsten.nodeIDToAuthToken.put(HazelcastSingleton.getNodeId(), session_id);
      Tungsten.checkAndCleanupAdhoc(Boolean.valueOf(false));
      Tungsten.setSessionQueue(WAQueue.getQueue("consoleQueue" + session_id));
      Tungsten.setQueuelistener(new WAQueue.Listener()
      {
        public void onItem(Object item)
        {
          if (Tungsten.isAdhocRunning.get() == true) {
            Tungsten.prettyPrintEvent(item);
          }
        }
      });
      try
      {
        Tungsten.getSessionQueue().subscribeForTungsten(Tungsten.getQueuelistener());
      }
      catch (Exception e)
      {
        logger.error(e.getMessage(), e);
      }
      Tungsten.session_id = session_id;
      Tungsten.currUserMetaInfo = (MetaInfo.User)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.USER, "Global", uname, null, session_id);
      
      TimeZone jtz = TimeZone.getTimeZone(Tungsten.currUserMetaInfo.getUserTimeZone());
      Tungsten.userTimeZone = Tungsten.currUserMetaInfo.getUserTimeZone().equals("") ? null : DateTimeZone.forTimeZone(jtz);
      this.sessionID = session_id;
      this.curUser = getUser(uname);
      String curUsrNamespace = this.curUser.getDefaultNamespace();
      useNamespace(curUsrNamespace);
      ConsoleReader.clearHistory();
      return session_id;
    }
    System.out.println("Could not log in as as " + uname);
    
    return null;
  }
  
  public String getCurUser()
  {
    if (this.curUser != null) {
      return this.curUser.name;
    }
    return null;
  }
  
  public String endBlock(MetaInfo.Flow flow)
  {
    if (flow.type == EntityType.FLOW)
    {
      if (this.curFlow == null) {
        return "END FLOW <" + flow.name + "> block has no matching CREATE FLOW <" + flow.name + ">";
      }
      if (!this.curFlow.name.equalsIgnoreCase(flow.name)) {
        return "END FLOW <" + flow.name + "> block does not match previous CREATE FLOW <" + this.curFlow.name + ">";
      }
      this.curFlow = null;
    }
    else
    {
      if (this.curApp == null) {
        return "END APPLICATION <" + flow.name + "> block has no matching CREATE APPLICATION <" + flow.name + ">";
      }
      if (!this.curApp.name.equalsIgnoreCase(flow.name)) {
        return "END APPLICATION <" + flow.name + "> block does not match previous CREATE APPLICATION <" + this.curApp.name + ">";
      }
      if (this.curFlow != null) {
        return "there is no END FLOW <" + this.curFlow.name + "> statement before END APPLICATION <" + flow.name + ">";
      }
      this.curApp = null;
    }
    return null;
  }
  
  public static class Result
    implements Serializable
  {
    private static final long serialVersionUID = 4217752353922101919L;
    public UUID serverID;
    public Collection<MetaInfo.MetaObjectInfo> processedObjects;
    public Collection<String> serverDGs;
    public Map<UUID, Boolean> runningStatus;
    public MetaInfo.StatusInfo.Status flowStatus;
    
    private static String id2name(UUID id)
      throws MetaDataRepositoryException
    {
      MetaInfo.MetaObject o = MetadataRepository.getINSTANCE().getMetaObjectByUUID(id, WASecurityManager.TOKEN);
      if ((o instanceof MetaInfo.Server))
      {
        MetaInfo.Server s = (MetaInfo.Server)o;
        if ((s.name != null) && (!s.name.isEmpty())) {
          return s.name;
        }
        return "srv_" + s.id;
      }
      return id.toString();
    }
    
    public String toString()
    {
      StringBuilder sb = new StringBuilder();
      try
      {
        sb.append("\nON " + id2name(this.serverID) + " IN " + this.serverDGs + "\n[\n");
      }
      catch (MetaDataRepositoryException e)
      {
        Context.logger.error(e.getMessage());
      }
      for (MetaInfo.MetaObjectInfo o : this.processedObjects) {
        sb.append("\t(" + o.nsName + "." + o.name + " " + o.type + "),\n");
      }
      sb.append("]\n");
      return sb.toString();
    }
  }
  
  public static class SetStmtRemoteCall
    implements RemoteCall<Context.Result>
  {
    private static final long serialVersionUID = -1130287312659963213L;
    String paramname;
    Object paramvalue;
    
    public SetStmtRemoteCall() {}
    
    public SetStmtRemoteCall(String paramname, Object paramvalue)
    {
      this.paramname = paramname;
      this.paramvalue = paramvalue;
    }
    
    public Context.Result call()
      throws Exception
    {
      Server srv = Server.server;
      srv.changeOptions(this.paramname, this.paramvalue);
      return null;
    }
  }
  
  public void changeCQInputOutput(String cqname, int mode, int action, AuthToken clToken)
    throws MetaDataRepositoryException
  {
    MetaInfo.CQ cqMeta = null;
    
    String[] sNames = splitNamespaceAndName(cqname, EntityType.CQ);
    try
    {
      cqMeta = (MetaInfo.CQ)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.CQ, sNames[0], sNames[1], Integer.valueOf(-1), getAuthToken());
      if (cqMeta == null) {
        return;
      }
      PermissionUtility.checkPermission(cqMeta, ObjectPermission.Action.update, getAuthToken(), true);
    }
    catch (Exception e)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to find object or no permissions for " + cqname);
      }
      throw new MetaDataRepositoryException("Unable to find object or no permissions for " + cqname);
    }
    execute(new DumpStmtRemoteCall(cqMeta.getUuid(), mode, action, clToken));
  }
  
  public static class DumpStmtRemoteCall
    implements RemoteCall<Context.Result>
  {
    private static final long serialVersionUID = -1130287312659963214L;
    UUID cqUid;
    int mode;
    int action;
    AuthToken clientToken;
    
    public DumpStmtRemoteCall() {}
    
    public DumpStmtRemoteCall(UUID cquuid, int dumpmode, int dumpaction, AuthToken cltoken)
    {
      this.cqUid = cquuid;
      this.mode = dumpmode;
      this.action = dumpaction;
      this.clientToken = cltoken;
    }
    
    public Context.Result call()
      throws Exception
    {
      FlowComponent fc = Server.getServer().getOpenObject(this.cqUid);
      if (fc == null) {
        return null;
      }
      CQTask cqfc = (CQTask)fc;
      
      int traceFlags = 0;
      switch (this.mode)
      {
      case 1: 
        if (this.action == 1) {
          traceFlags |= 0x1;
        } else {
          traceFlags &= 0xFFFFFFFE;
        }
        break;
      case 2: 
        if (this.action == 1) {
          traceFlags |= 0x2;
        } else {
          traceFlags &= 0xFFFFFFFD;
        }
        break;
      case 3: 
        if (this.action == 1) {
          traceFlags |= 0x3;
        } else {
          traceFlags &= 0xFFFFFFFC;
        }
        break;
      case 4: 
        MetaInfo.CQ cqMeta = (MetaInfo.CQ)MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.cqUid, WASecurityManager.TOKEN);
        if (cqMeta != null)
        {
          List<String> sourceClasses = cqMeta.getPlan().sourcecode;
          if (sourceClasses != null) {
            for (String temp : sourceClasses) {
              System.out.println(temp);
            }
          }
        }
        return null;
      }
      TraceOptions traceOptions = new TraceOptions(traceFlags, (String)null, (String)null);
      
      WAQueue tracequeue = WAQueue.getQueue("consoleQueue" + this.clientToken);
      cqfc.setTraceOptions(traceOptions, tracequeue);
      return null;
    }
  }
  
  public static class ChangeFlowState
    implements RemoteCall<Context.Result>
  {
    private static final long serialVersionUID = -8137681093645820789L;
    private final ActionType what;
    private final MetaInfo.Flow flow;
    private final List<UUID> servers;
    private final UUID clientSessionID;
    private final List<Property> params;
    private final Long epochNumber;
    
    public ChangeFlowState()
    {
      this(null, null, null, null, null, null);
    }
    
    public ChangeFlowState(ActionType what, MetaInfo.Flow flow, List<UUID> servers, UUID clientSessionID, Long epochNumber)
    {
      this(what, flow, servers, clientSessionID, null, epochNumber);
    }
    
    public ChangeFlowState(ActionType what, MetaInfo.Flow flow, List<UUID> servers, UUID clientSessionID, List<Property> params, Long epochNumber)
    {
      this.what = what;
      this.flow = flow;
      this.servers = servers;
      this.clientSessionID = clientSessionID;
      this.params = params;
      this.epochNumber = epochNumber;
    }
    
    public Context.Result call()
      throws Exception
    {
      if (HazelcastSingleton.isClientMember()) {
        return null;
      }
      Server srv = Server.server;
      if (srv != null)
      {
        Context.Result r = new Context.Result();
        if (Context.logger.isInfoEnabled()) {
          Context.logger.info("Got request " + this.what.toString() + " for flow " + this.flow.uuid + " with servers " + this.servers + " EpochNumber " + this.epochNumber);
        }
        srv.changeFlowState(this.what, this.flow, this.servers, this.clientSessionID, this.params, this.epochNumber);
        r.processedObjects = srv.getDeployedObjects(this.flow.getUuid());
        r.serverID = srv.getServerID();
        r.serverDGs = srv.getDeploymentGroups();
        if (this.what == ActionType.STATUS)
        {
          r.runningStatus = srv.whatIsRunning(this.flow.getUuid());
          r.flowStatus = srv.getFlowStatus(this.flow);
        }
        return r;
      }
      throw new RuntimeException("non-lite member has no running server");
    }
    
    public ActionType getAction()
    {
      return this.what;
    }
  }
  
  public Collection<Result> execute(Callable<Result> action)
  {
    if (this.readOnly) {
      return Collections.emptyList();
    }
    HazelcastInstance hz = HazelcastSingleton.get();
    
    Set<Member> srvs = DistributedExecutionManager.getAllServers(hz);
    return DistributedExecutionManager.exec(hz, action, srvs);
  }
  
  public Collection<Result> executeOnOne(Callable<Result> action)
  {
    if (this.readOnly) {
      return Collections.emptyList();
    }
    HazelcastInstance hz = HazelcastSingleton.get();
    
    Set<Member> srvs = DistributedExecutionManager.getFirstServer(hz);
    return DistributedExecutionManager.exec(hz, action, srvs);
  }
  
  public Collection<Result> SetStmtRemoteCall(String paramname, Object paramvalue)
  {
    return execute(new SetStmtRemoteCall(paramname, paramvalue));
  }
  
  public Collection<Result> changeFlowState(ActionType what, MetaInfo.Flow flow)
  {
    return execute(new ChangeFlowState(what, flow, null, this.sessionID, null, null));
  }
  
  public Collection<Result> remoteCallOnObject(ActionType what, MetaInfo.MetaObject object, Object... loadDetails)
  {
    return executeOnOne(new RemoteCallOnObject(what, object, this.sessionID, loadDetails));
  }
  
  public static class RemoteCallOnObject
    implements RemoteCall<Context.Result>
  {
    private static final long serialVersionUID = -8137681093645820789L;
    private final ActionType what;
    private final MetaInfo.MetaObject obj;
    private final UUID clientSessionID;
    private final Object[] loadDetails;
    
    public RemoteCallOnObject()
    {
      this(null, null, null, new Object[] { null, null, null });
    }
    
    public RemoteCallOnObject(ActionType what, MetaInfo.MetaObject obj, AuthToken sessionID, Object... loadDetails)
    {
      this.loadDetails = loadDetails;
      this.what = what;
      this.obj = obj;
      this.clientSessionID = sessionID;
    }
    
    public Context.Result call()
      throws Exception
    {
      if (HazelcastSingleton.isClientMember()) {
        return null;
      }
      Server srv = Server.server;
      if (srv != null)
      {
        Context.Result r = new Context.Result();
        if (Context.logger.isInfoEnabled()) {
          Context.logger.info("Got request " + this.what.toString() + " for flow " + this.obj.uuid);
        }
        srv.remoteCallOnObject(this.what, this.obj, this.loadDetails, this.clientSessionID);
        return r;
      }
      throw new RuntimeException("non-lite member has no running server");
    }
  }
  
  public Collection<Result> changeFlowState(ActionType what, MetaInfo.Flow flow, List<Property> params)
  {
    return execute(new ChangeFlowState(what, flow, null, this.sessionID, params, null));
  }
  
  public ChangeApplicationStateResponse changeApplicationState(ActionType what, MetaInfo.Flow flow, Map<String, Object> params)
    throws Exception
  {
    AppManagerRequestClient appManagerRequestHandler = new AppManagerRequestClient(this.sessionID);
    ChangeApplicationStateResponse response = appManagerRequestHandler.sendRequest(what, flow, params);
    appManagerRequestHandler.close();
    return response;
  }
  
  public static class ShutDown
    implements RemoteCall<Object>
  {
    private static final long serialVersionUID = -4760044703938736365L;
    
    public Object call()
      throws Exception
    {
      System.exit(888);
      return null;
    }
  }
  
  public Collection<Object> executeShutdown(String where)
  {
    if (this.readOnly) {
      return Collections.emptyList();
    }
    HazelcastInstance hz = HazelcastSingleton.get();
    Set<Member> srvs;
    Set<Member> srvs;
    if (where.equalsIgnoreCase("ALL"))
    {
      srvs = DistributedExecutionManager.getAllServers(hz);
    }
    else
    {
      srvs = DistributedExecutionManager.getServerByAddress(hz, where);
      if (srvs.isEmpty()) {
        throw new RuntimeException("cannot find server " + where);
      }
    }
    return DistributedExecutionManager.exec(hz, new ShutDown(), srvs);
  }
  
  public String getStatus(String name, EntityType type)
    throws Exception
  {
    MetaInfo.Flow app = getFlowInCurSchema(name, type);
    if (app == null) {
      throw new RuntimeException("cannot find " + type + " " + name);
    }
    MetaInfo.StatusInfo.Status si = FlowUtil.getCurrentStatusFromAppManager(app.getUuid());
    if (si != null) {
      return si.name();
    }
    return "UNKNOWN";
  }
  
  public Set<String> getErrors(String name, EntityType type, AuthToken token)
    throws Exception
  {
    MetaInfo.Flow app = getFlowInCurSchema(name, type);
    if (app == null) {
      throw new RuntimeException("cannot find " + type + " " + name);
    }
    Set<String> errors = FlowUtil.getCurrentErrorsFromAppManager(app.getUuid(), token);
    return errors;
  }
  
  public void setCurApp(MetaInfo.Flow flow)
  {
    this.curApp = flow;
  }
  
  public void setCurFlow(MetaInfo.Flow flow)
  {
    this.curFlow = flow;
  }
  
  public MetaInfo.Flow getCurApp()
  {
    return this.curApp;
  }
  
  public MetaInfo.Flow getCurFlow()
  {
    return this.curFlow;
  }
  
  public MetaInfo.Sorter putSorter(boolean doReplace, ObjectName objectName, Interval sortTimeInterval, List<MetaInfo.Sorter.SorterRule> inOutRules, UUID errorStream)
    throws MetaDataRepositoryException
  {
    String name = addSchemaPrefix(objectName);
    MetaInfo.Sorter sorter = getSorter(name);
    if (sorter != null)
    {
      if (!doReplace) {
        throw new CompilationException("Stream sorter " + name + " already exists");
      }
      removeObject(sorter.uuid);
    }
    MetaInfo.Sorter newsorter = new MetaInfo.Sorter();
    newsorter.construct(name, getNamespace(objectName.getNamespace()), sortTimeInterval, inOutRules, errorStream);
    
    putSorter(newsorter);
    return newsorter;
  }
  
  public MetaInfo.WAStoreView putWAStoreView(String waStoreName, String namespaceName, UUID wastoreID, IntervalPolicy windowLen, Boolean isJumping, boolean subscribeToUpdates, boolean isAdhoc)
    throws MetaDataRepositoryException
  {
    String name = addSchemaPrefix(namespaceName, waStoreName);
    MetaInfo.WAStoreView view = new MetaInfo.WAStoreView();
    view.construct(name, getNamespace(namespaceName), wastoreID, windowLen, isJumping, subscribeToUpdates);
    
    view.getMetaInfoStatus().setAdhoc(isAdhoc);
    putWAStoreView(view);
    view = (MetaInfo.WAStoreView)getMetaObject(view.getUuid());
    MetaInfo.MetaObject wactionStore = getMetaObject(wastoreID);
    wactionStore.addReverseIndexObjectDependencies(view.getUuid());
    updateMetaObject(wactionStore);
    return view;
  }
  
  public List<MetaInfo.Query> listQueries(QueryManager.TYPE[] entityTypes, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    if (entityTypes == null) {
      return null;
    }
    List<MetaInfo.Query> resultSet = new ArrayList();
    Set<MetaInfo.Query> metaDataQueryResult = this.metadataRepository.getByEntityType(EntityType.QUERY, authToken);
    QueryManager.TYPE entityType;
    for (entityType : entityTypes) {
      for (MetaInfo.Query query : metaDataQueryResult)
      {
        if ((query.isAdhocQuery()) && (entityType == QueryManager.TYPE.ADHOC)) {
          resultSet.add(query);
        }
        if ((!query.isAdhocQuery()) && (entityType == QueryManager.TYPE.NAMEDQUERY)) {
          resultSet.add(query);
        }
      }
    }
    return resultSet;
  }
  
  public void putQuery(MetaInfo.Query query)
  {
    try
    {
      putMetaObject(query);
    }
    catch (MetaDataRepositoryException e)
    {
      logger.warn(e.getMessage());
    }
  }
  
  public void deleteQuery(MetaInfo.Query query, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    if (query.appUUID != null)
    {
      MetaInfo.Flow applicationMetaObject = (MetaInfo.Flow)getMetaObject(query.appUUID);
      if (applicationMetaObject == null) {
        throw new RuntimeException("Application MetaObject not found");
      }
      List<UUID> deps = applicationMetaObject.getDependencies();
      
      removeMetaObject(applicationMetaObject.uuid);
      for (UUID dep : deps)
      {
        MetaInfo.MetaObject obj = getObject(dep);
        if ((obj instanceof MetaInfo.WAStoreView))
        {
          MetaInfo.WAStoreView wastoreview = (MetaInfo.WAStoreView)obj;
          MetaInfo.WActionStore wastore = (MetaInfo.WActionStore)getMetaObject(wastoreview.wastoreID);
          try
          {
            wastore.getReverseIndexObjectDependencies().remove(dep);
            updateMetaObject(wastore);
          }
          catch (Exception e) {}
        }
        removeMetaObject(dep);
      }
    }
    removeMetaObject(query.uuid);
  }
  
  public UUID createDashboard(String json, String toNamespace)
    throws MetaDataRepositoryException
  {
    VisualizationArtifacts visualizationArtifacts = null;
    try
    {
      visualizationArtifacts = (VisualizationArtifacts)new ObjectMapper().readValue(json, VisualizationArtifacts.class);
    }
    catch (IOException e)
    {
      List<JsonNode> qvObjects = new ArrayList();
      List<JsonNode> pageObjects = new ArrayList();
      visualizationArtifacts = new VisualizationArtifacts();
      ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
      jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      JsonNode listNode = null;
      try
      {
        listNode = jsonMapper.readTree(json);
      }
      catch (IOException e1)
      {
        e1.printStackTrace();
        return null;
      }
      Iterator<JsonNode> it = listNode.elements();
      while (it.hasNext())
      {
        JsonNode node = (JsonNode)it.next();
        if ((node instanceof ArrayNode)) {
          for (JsonNode jsonNode : node) {
            try
            {
              extractInfo(visualizationArtifacts, qvObjects, pageObjects, jsonMapper, jsonNode);
            }
            catch (ClassNotFoundException|IOException e1)
            {
              logger.warn(e1.getMessage());
              return null;
            }
          }
        }
        if ((node instanceof ObjectNode)) {
          try
          {
            extractInfo(visualizationArtifacts, qvObjects, pageObjects, jsonMapper, node);
          }
          catch (ClassNotFoundException|IOException e1)
          {
            logger.warn(e1.getMessage());
            return null;
          }
        }
      }
      try
      {
        List<MetaInfo.MetaObject> convertedDashboardObjects = ServerUpgradeUtility.DashboardConverter.convert(qvObjects, pageObjects);
        for (MetaInfo.MetaObject mo : convertedDashboardObjects)
        {
          if (mo.getType() == EntityType.QUERYVISUALIZATION) {
            visualizationArtifacts.addQueryVisualization((MetaInfo.QueryVisualization)mo);
          }
          if (mo.getType() == EntityType.PAGE) {
            visualizationArtifacts.addPages((MetaInfo.Page)mo);
          }
        }
      }
      catch (IOException e1)
      {
        logger.warn(e1.getMessage());
        return null;
      }
    }
    if ((visualizationArtifacts == null) || (visualizationArtifacts.getDashboard() == null) || (visualizationArtifacts.getPages().contains(null)) || (visualizationArtifacts.getQueryVisualizations().contains(null))) {
      throw new RuntimeException("Failed to create Dashboard object, there are missing objects.");
    }
    if (this.metadataRepository.getMetaObjectByName(EntityType.DASHBOARD, toNamespace == null ? getNamespaceName(null) : toNamespace, visualizationArtifacts.getDashboard().getName(), null, WASecurityManager.TOKEN) != null) {
      throw new CompilationException("Dashboard already exists, cannot import the dashboard");
    }
    modifyAndValidate(visualizationArtifacts, toNamespace);
    for (MetaInfo.Query queryMetaObject : visualizationArtifacts.getParametrizedQuery()) {
      if (this.metadataRepository.getMetaObjectByName(EntityType.QUERY, getNamespaceName(null), queryMetaObject.getName(), null, WASecurityManager.TOKEN) == null) {
        try
        {
          Compiler.compile("CREATE NAMEDQUERY " + queryMetaObject.name + " " + ServerUpgradeUtility.convertNewQueries(queryMetaObject.queryDefinition), this, new Compiler.ExecutionCallback()
          {
            public void execute(Stmt stmt, Compiler compiler)
              throws MetaDataRepositoryException
            {
              try
              {
                compiler.compileStmt(stmt);
              }
              catch (Warning e) {}
            }
          });
        }
        catch (Exception e)
        {
          logger.warn(e.getMessage());
        }
      }
    }
    for (MetaInfo.QueryVisualization qv : visualizationArtifacts.getQueryVisualizations()) {
      put(qv);
    }
    for (MetaInfo.Page page : visualizationArtifacts.getPages()) {
      put(page);
    }
    MetaInfo.Dashboard dashboard = visualizationArtifacts.getDashboard();
    put(dashboard);
    return dashboard.getUuid();
  }
  
  public void extractInfo(VisualizationArtifacts visualizationArtifacts, List<JsonNode> qvObjects, List<JsonNode> pageObjects, ObjectMapper jsonMapper, JsonNode moNode)
    throws IOException, ClassNotFoundException
  {
    String className = moNode.get("metaObjectClass").asText();
    if ((className.equalsIgnoreCase("com.bloom.runtime.MetaInfo$Dashboard")) || (className.equalsIgnoreCase("com.bloom.runtime.meta.MetaInfo$Dashboard")))
    {
      String moNodeText = moNode.toString();
      
      Class<?> moClass = Class.forName("com.bloom.runtime.meta.MetaInfo$Dashboard");
      Object ob = jsonMapper.readValue(moNodeText, moClass);
      visualizationArtifacts.setDashboard((MetaInfo.Dashboard)ob);
    }
    if ((className.equalsIgnoreCase("com.bloom.runtime.MetaInfo$Query")) || (className.equalsIgnoreCase("com.bloom.runtime.meta.MetaInfo$Query")))
    {
      MetaInfo.Query query = MetaInfo.Query.deserialize(moNode);
      visualizationArtifacts.addParametrizedQuery(query);
    }
    if ((className.equalsIgnoreCase("com.bloom.runtime.MetaInfo$QueryVisualization")) || (className.equalsIgnoreCase("com.bloom.runtime.meta.MetaInfo$QueryVisualization"))) {
      qvObjects.add(moNode);
    }
    if ((className.equalsIgnoreCase("com.bloom.runtime.MetaInfo$Page")) || (className.equalsIgnoreCase("com.bloom.runtime.meta.MetaInfo$Page"))) {
      pageObjects.add(moNode);
    }
  }
  
  private void modifyAndValidate(VisualizationArtifacts visualizationArtifacts, String toNamespace)
    throws MetaDataRepositoryException
  {
    List<String> namespaceDependentPages = new ArrayList();
    List<String> namespaceDependentQueryVisualisations = new ArrayList();
    List<String> namespaceDependentQueries = new ArrayList();
    
    MetaInfo.Namespace namespace = getNamespace(toNamespace);
    
    MetaInfo.Dashboard dashboard = visualizationArtifacts.getDashboard();
    dashboard.setNsName(namespace.getName());
    dashboard.setNamespaceId(namespace.getUuid());
    dashboard.setUri(replaceNamespaceOfURI(dashboard.getUri(), namespace));
    if (dashboard.getPages() != null) {
      for (int i = 0; i < dashboard.getPages().size(); i++)
      {
        String pageName = (String)dashboard.getPages().get(i);
        if (pageName.indexOf(".") == -1) {
          namespaceDependentPages.add(pageName);
        }
      }
    }
    dashboard.setUuid(UUID.genCurTimeUUID());
    for (MetaInfo.Page page : visualizationArtifacts.getPages())
    {
      if (page.getQueryVisualizations() != null) {
        for (String qv : page.getQueryVisualizations()) {
          if (qv.indexOf(".") == -1) {
            namespaceDependentQueryVisualisations.add(qv);
          }
        }
      }
      if (namespaceDependentPages.contains(page.getName()))
      {
        page.setNsName(namespace.getName());
        page.setNamespaceId(namespace.getUuid());
        page.setUri(replaceNamespaceOfURI(page.getUri(), namespace));
        page.setUuid(UUID.genCurTimeUUID());
      }
    }
    for (MetaInfo.QueryVisualization queryVisualization : visualizationArtifacts.getQueryVisualizations())
    {
      if ((queryVisualization.getQuery() != null) && (queryVisualization.getQuery().indexOf(".") == -1)) {
        namespaceDependentQueries.add(queryVisualization.getQuery());
      }
      if (namespaceDependentQueryVisualisations.contains(queryVisualization.getName()))
      {
        queryVisualization.setNsName(namespace.getName());
        queryVisualization.setQuery(Utility.convertToFullQualifiedName(namespace, Utility.splitName(queryVisualization.getQuery())));
        queryVisualization.setNamespaceId(namespace.getUuid());
        queryVisualization.setUri(replaceNamespaceOfURI(queryVisualization.getUri(), namespace));
        queryVisualization.setUuid(UUID.genCurTimeUUID());
      }
    }
    for (MetaInfo.Query query : visualizationArtifacts.getParametrizedQuery()) {
      if (namespaceDependentQueries.contains(query.getName()))
      {
        query.setNsName(namespace.getName());
        query.setNamespaceId(namespace.getUuid());
        query.setUri(replaceNamespaceOfURI(query.getUri(), namespace));
      }
    }
  }
  
  private String replaceNamespaceOfURI(String URI, MetaInfo.Namespace namespace)
  {
    String[] URI_SPLIT = URI.split(":");
    URI_SPLIT[0] = namespace.getName().toUpperCase();
    String URI_UPDATED = Arrays.toString(URI_SPLIT).replace(", ", ":").replaceAll("[\\[\\]]", "");
    return URI_UPDATED;
  }
}
