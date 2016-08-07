package com.bloom.runtime;

import com.bloom.drop.DropMetaObject.DropRule;
import com.bloom.exception.FatalException;
import com.bloom.exception.SecurityException;
import com.bloom.exception.Warning;
import com.bloom.exceptionhandling.ExceptionType;
import com.bloom.intf.DashboardOperations;
import com.bloom.intf.QueryManager;
import com.bloom.intf.QueryManager.QueryParameters;
import com.bloom.intf.QueryManager.TYPE;
import com.bloom.kafkamessaging.StreamPersistencePolicy;
import com.bloom.logging.UserCommandLogger;
import com.bloom.metaRepository.MDConstants;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.metaRepository.PermissionUtility;
import com.bloom.runtime.compiler.AST;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.Lexer;
import com.bloom.runtime.compiler.TypeDefOrName;
import com.bloom.runtime.compiler.TypeField;
import com.bloom.runtime.compiler.TypeName;
import com.bloom.runtime.compiler.Compiler.ExecutionCallback;
import com.bloom.runtime.compiler.select.ParamDesc;
import com.bloom.runtime.compiler.stmts.ConnectStmt;
import com.bloom.runtime.compiler.stmts.CreateAdHocSelectStmt;
import com.bloom.runtime.compiler.stmts.CreateDashboardStatement;
import com.bloom.runtime.compiler.stmts.CreatePropertySetStmt;
import com.bloom.runtime.compiler.stmts.CreateUserStmt;
import com.bloom.runtime.compiler.stmts.DeploymentRule;
import com.bloom.runtime.compiler.stmts.EventType;
import com.bloom.runtime.compiler.stmts.ExceptionHandler;
import com.bloom.runtime.compiler.stmts.MappedStream;
import com.bloom.runtime.compiler.stmts.OutputClause;
import com.bloom.runtime.compiler.stmts.RecoveryDescription;
import com.bloom.runtime.compiler.stmts.Select;
import com.bloom.runtime.compiler.stmts.Stmt;
import com.bloom.runtime.compiler.stmts.UserProperty;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.components.Flow;
import com.bloom.runtime.meta.CQExecutionPlan;
import com.bloom.runtime.meta.IntervalPolicy;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.MetaInfo.CQ;
import com.bloom.runtime.meta.MetaInfo.Dashboard;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Namespace;
import com.bloom.runtime.meta.MetaInfo.Page;
import com.bloom.runtime.meta.MetaInfo.Query;
import com.bloom.runtime.meta.MetaInfo.QueryVisualization;
import com.bloom.runtime.meta.MetaInfo.User;
import com.bloom.runtime.utils.Factory;
import com.bloom.runtime.utils.ObjectReference;
import com.bloom.security.ObjectPermission;
import com.bloom.security.WASecurityManager;
import com.bloom.security.ObjectPermission.Action;
import com.bloom.security.ObjectPermission.ObjectType;
import com.bloom.security.ObjectPermission.PermissionType;
import com.bloom.utility.CQUtility;
import com.bloom.utility.Utility;
import com.bloom.utility.WindowUtility;
import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;
import com.bloom.web.api.ClientCreateOperation;
import com.bloom.web.api.ClientOperations;
import com.bloom.web.api.ClientOperations.CRUD;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.InvalidPropertiesFormatException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.sourceforge.czt.java_cup.runtime.Symbol;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;

public class QueryValidator
  implements QueryManager, DashboardOperations
{
  private static Logger logger = Logger.getLogger(QueryValidator.class);
  private final Server srv;
  private final Map<AuthToken, Context> contexts;
  private boolean updateMode;
  private MetadataRepository metadataRepository = MetadataRepository.getINSTANCE();
  private ClientOperations clientOperations = ClientOperations.getInstance();
  
  public QueryValidator(Server srv)
  {
    this.srv = srv;
    this.contexts = Factory.makeMap();
    this.updateMode = false;
  }
  
  public Context getContext(AuthToken token)
  {
    Context context = (Context)this.contexts.get(token);
    if (context == null) {
      throw new RuntimeException("Could not find context for invalid authorization token: " + token);
    }
    return context;
  }
  
  public void createNewContext(AuthToken token)
    throws MetaDataRepositoryException
  {
    if (this.contexts.containsKey(token)) {
      return;
    }
    if (logger.isInfoEnabled()) {
      logger.info("createNewContext -- ");
    }
    Context ctx = this.srv.createContext(token);
    String userName = WASecurityManager.getAutheticatedUserName(token);
    if (userName == null)
    {
      ctx.useNamespace("Global");
    }
    else
    {
      MetaInfo.User user = WASecurityManager.get().getUser(userName);
      ctx.useNamespace(user.getDefaultNamespace());
    }
    addContext(token, ctx);
  }
  
  public void setCurrentApp(String name, AuthToken token)
    throws MetaDataRepositoryException
  {
    if (logger.isInfoEnabled()) {
      logger.info("setCurrentApp : " + name);
    }
    Context context = getContext(token);
    if (name == null)
    {
      context.setCurApp(null);
    }
    else
    {
      assert (this.metadataRepository != null);
      String[] namespaceAndName = name.split("\\.");
      if ((namespaceAndName[0] != null) && (namespaceAndName[1] != null))
      {
        MetaInfo.Flow f = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByName(EntityType.APPLICATION, namespaceAndName[0], namespaceAndName[1], null, token);
        context.setCurApp(f);
      }
    }
  }
  
  public void setCurrentFlow(String name, AuthToken token)
    throws MetaDataRepositoryException
  {
    if (logger.isInfoEnabled()) {
      logger.info("setCurrentFlow : " + name);
    }
    Context c = getContext(token);
    if (name == null)
    {
      c.setCurFlow(null);
    }
    else
    {
      assert (this.metadataRepository != null);
      String[] namespaceAndName = name.split("\\.");
      if ((namespaceAndName[0] != null) && (namespaceAndName[1] != null))
      {
        MetaInfo.Flow f = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByName(EntityType.FLOW, namespaceAndName[0], namespaceAndName[1], null, token);
        c.setCurFlow(f);
      }
    }
  }
  
  public void addContext(AuthToken token, Context ctx)
  {
    this.contexts.put(token, ctx);
  }
  
  public void removeContext(AuthToken token)
  {
    if (logger.isInfoEnabled()) {
      logger.info("removeContext : ");
    }
    this.contexts.remove(token);
  }
  
  public void setUpdateMode(boolean mode)
  {
    this.updateMode = mode;
  }
  
  private void compile(AuthToken token, Stmt stmt)
    throws MetaDataRepositoryException
  {
    compile(token, stmt, this.updateMode);
  }
  
  private void compile(final AuthToken token, Stmt stmt, boolean updateMode)
    throws MetaDataRepositoryException
  {
    Context context = getContext(token);
    boolean oldVal = context.setReadOnly(!updateMode);
    try
    {
      Compiler.compile(stmt, context, new Compiler.ExecutionCallback()
      {
        public void execute(Stmt stmt, Compiler compiler)
          throws Exception
        {
          compiler.compileStmt(stmt);
          if ((!(stmt instanceof CreatePropertySetStmt)) && (!(stmt instanceof CreateUserStmt)) && (!(stmt instanceof ConnectStmt))) {
            QueryValidator.this.storeCommand(token, QueryValidator.this.getUserId(token), (stmt.sourceText == null) || (stmt.sourceText.isEmpty()) ? stmt.toString() : stmt.sourceText);
          }
        }
      });
    }
    catch (MetaDataRepositoryException e)
    {
      throw e;
    }
    catch (Throwable e)
    {
      throw new RuntimeException(e);
    }
    finally
    {
      context.setReadOnly(oldVal);
    }
  }
  
  public String compileText(AuthToken token, String text)
    throws Exception
  {
    String returnString = compileText(token, text, false);
    return returnString;
  }
  
  public String compileText(final AuthToken token, String text, boolean readOnly)
    throws Exception
  {
    Context context = getContext(token);
    boolean oldVal = context.setReadOnly(readOnly);
    final StringBuilder builder = new StringBuilder();
    try
    {
      Compiler.compile(text, context, new Compiler.ExecutionCallback()
      {
        public void execute(Stmt stmt, Compiler compiler)
          throws MetaDataRepositoryException
        {
          try
          {
            compiler.compileStmt(stmt);
            QueryValidator.this.storeStmt(token, QueryValidator.this.getUserId(token), stmt);
          }
          catch (Warning e)
          {
            builder.append(e.getLocalizedMessage());
            builder.append("\n");
          }
        }
      });
    }
    catch (Exception e)
    {
      setCurrentApp(null, token);
      setCurrentFlow(null, token);
      throw e;
    }
    finally
    {
      context.setReadOnly(oldVal);
    }
    return builder.toString();
  }
  
  private String getUserId(AuthToken token)
  {
    Context context = (Context)this.contexts.get(token);
    String uid = null;
    if (context != null) {
      uid = context.getCurUser();
    }
    if (uid == null) {
      uid = WASecurityManager.getAutheticatedUserName(token);
    }
    return uid;
  }
  
  public void storeStmt(AuthToken token, String userid, Stmt stmt)
  {
    if ((!(stmt instanceof CreatePropertySetStmt)) && (!(stmt instanceof CreateUserStmt)) && (!(stmt instanceof ConnectStmt))) {
      storeCommand(token, userid, (stmt.sourceText == null) || (stmt.sourceText.isEmpty()) ? stmt.toString() : stmt.sourceText);
    }
  }
  
  public void storeCommand(AuthToken token, String userid, String text)
  {
    try
    {
      UserCommandLogger.logCmd(userid, token.getUUIDString(), text);
    }
    catch (Exception ex)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("error storing command : " + text);
      }
    }
  }
  
  public void testCompatibility(AuthToken token, String userid, String text)
    throws Exception
  {
    logger.warn("testing compatibility : " + text);
    String json = null;
    if (text.split(" ").length > 2) {
      json = ServerUpgradeUtility.getJsonString(text.split(" ")[2]);
    }
    ServerUpgradeUtility.testCompatibility(json);
  }
  
  private static Pair<EntityType, String> makeEntity(String entityType, String entityName)
  {
    EntityType e = EntityType.forObject(entityType);
    Pair<EntityType, String> p = Pair.make(e, entityName);
    return p;
  }
  
  public void CreateAlterStmt(AuthToken token, String entityType, String entityName)
    throws MetaDataRepositoryException
  {
    compile(token, AST.CreateAlterAppOrFlowStmt(EntityType.forObject(entityType), entityName, false));
  }
  
  public void CreateAlterRecompileStmt(AuthToken token, String entityType, String entityName)
    throws MetaDataRepositoryException
  {
    if (logger.isInfoEnabled()) {
      logger.info("CreateAlterRecompileStmt (token " + token + ", entityType " + entityType + ", entityName " + entityName + ")");
    }
    compile(token, AST.CreateAlterAppOrFlowStmt(EntityType.forObject(entityType), entityName, true));
  }
  
  public static List<Property> makePropList(Map<String, String>[] props)
  {
    if (props == null) {
      return null;
    }
    List<Property> plist = new ArrayList();
    for (Map<String, String> prop : props)
    {
      assert (prop.size() == 1);
      if (prop != null) {
        for (Map.Entry<String, String> e : prop.entrySet())
        {
          Property p = new Property((String)e.getKey(), e.getValue());
          plist.add(p);
        }
      }
    }
    return plist;
  }
  
  public String GetFlowStatus(AuthToken token, String appOrFlowName, String eType)
    throws Exception
  {
    Context context = getContext(token);
    String ret = context.getStatus(appOrFlowName, EntityType.forObject(eType));
    return ret;
  }
  
  public Set<String> GetFlowErrors(AuthToken token, String appOrFlowName, String eType)
    throws Exception
  {
    Context context = getContext(token);
    Set<String> ret = context.getErrors(appOrFlowName, EntityType.forObject(eType), token);
    return ret;
  }
  
  public String getApplicationTQL(String nsName, String appName, boolean deepTraverse, AuthToken token)
    throws MetaDataRepositoryException
  {
    MetadataRepository mr = MetadataRepository.getINSTANCE();
    return mr.getApplicationTQL(nsName, appName, deepTraverse, token);
  }
  
  public void CreateStartFlowStatement(AuthToken token, String appOrFlowName, String eType)
    throws MetaDataRepositoryException
  {
    compile(token, AST.CreateStartStmt(appOrFlowName, EntityType.forObject(eType), null));
  }
  
  public void CreateStopFlowStatement(AuthToken token, String appOrFlowName, String eType)
    throws MetaDataRepositoryException
  {
    compile(token, AST.CreateStopStmt(appOrFlowName, EntityType.forObject(eType)));
  }
  
  public void CreateResumeFlowStatement(AuthToken token, String appOrFlowName, String eType)
    throws MetaDataRepositoryException
  {
    compile(token, AST.CreateResumeStmt(appOrFlowName, EntityType.forObject(eType)));
  }
  
  public void CreateUndeployFlowStatement(AuthToken token, String appOrFlowName, String eType)
    throws MetaDataRepositoryException
  {
    compile(token, AST.CreateUndeployStmt(appOrFlowName, EntityType.forObject(eType)));
  }
  
  private DeploymentRule makeRule(Map<String, String> desc, boolean canBeInDefaultDG)
  {
    String flowName = null;
    DeploymentStrategy st = DeploymentStrategy.ON_ALL;
    String deploymentGroup = canBeInDefaultDG ? "default" : null;
    for (Map.Entry<String, String> e : desc.entrySet())
    {
      String key = (String)e.getKey();
      String value = (String)e.getValue();
      if (key.equalsIgnoreCase("flow")) {
        flowName = value;
      } else if (key.equalsIgnoreCase("strategy"))
      {
        if (value.equalsIgnoreCase("all")) {
          st = DeploymentStrategy.ON_ALL;
        } else if (value.equalsIgnoreCase("any")) {
          st = DeploymentStrategy.ON_ONE;
        } else {
          throw new RuntimeException("unkown deployment strategy <" + value + ">");
        }
      }
      else if (key.equalsIgnoreCase("group")) {
        deploymentGroup = value;
      } else {
        throw new RuntimeException("unknown field <" + key + "> in flow deployment rule");
      }
    }
    if ((flowName == null) || (deploymentGroup == null)) {
      throw new RuntimeException("invalid deployment rule <" + desc + ">");
    }
    return AST.CreateDeployRule(st, flowName, deploymentGroup);
  }
  
  public void CreateAlterStatement(AuthToken token, String entityType, String entityName)
    throws MetaDataRepositoryException
  {
    compile(token, AST.CreateAlterAppOrFlowStmt(EntityType.forObject(entityType), entityName, false));
  }
  
  public void CreateAlterRecompileStatement(AuthToken token, String entityType, String entityName)
    throws MetaDataRepositoryException
  {
    if (logger.isInfoEnabled()) {
      logger.info("CreateAlterRecompileStatement (token " + token + ", entityType " + entityType + ", entityName " + entityName + ")");
    }
    compile(token, AST.CreateAlterAppOrFlowStmt(EntityType.forObject(entityType), entityName, true));
  }
  
  public void CreateDeployFlowStatement(AuthToken token, String eType, Map<String, String> appDesc, Map<String, String>[] detailsDesc)
    throws MetaDataRepositoryException
  {
    DeploymentRule appRule = makeRule(appDesc, true);
    List<DeploymentRule> details = new ArrayList();
    if (detailsDesc != null) {
      for (Map<String, String> d : detailsDesc)
      {
        DeploymentRule flowSubRule = makeRule(d, false);
        details.add(flowSubRule);
      }
    }
    compile(token, AST.CreateDeployStmt(EntityType.forObject(eType), appRule, details, null), true);
  }
  
  public void CreateStreamStatement(AuthToken requestToken, String stream_name, Boolean doReplace, String[] partition_fields, String typeName, StreamPersistencePolicy spp)
    throws MetaDataRepositoryException
  {
    String streamNameWithoutDomain = Utility.splitName(stream_name);
    String str = Utility.createStreamStatementText(streamNameWithoutDomain, doReplace, typeName, partition_fields);
    if (logger.isInfoEnabled()) {
      logger.info("CreateStreamStatement : " + streamNameWithoutDomain + " " + doReplace + " " + typeName);
    }
    Stmt stmt = AST.CreateStreamStatement(streamNameWithoutDomain, doReplace, Arrays.asList(partition_fields), new TypeDefOrName(typeName, null), null, spp);
    
    stmt.sourceText = str;
    compile(requestToken, stmt);
  }
  
  private static final Pattern typePat = Pattern.compile("(\\[+)?(L)?([\\w\\.\\_\\$]+)(\\s+[Kk][Ee][Yy])?");
  
  public static List<TypeField> makeFieldList(Map<String, String>[] defs)
  {
    List<TypeField> flist = new ArrayList();
    for (Map<String, String> def : defs)
    {
      assert (def.size() == 1);
      for (Map.Entry<String, String> e : def.entrySet())
      {
        Matcher m = typePat.matcher((CharSequence)e.getValue());
        if (m.matches())
        {
          String name = m.group(3);
          int ndims = 0;
          if (m.group(1) != null)
          {
            String dims = m.group(1);
            int k = dims.indexOf('[');
            while (k != -1)
            {
              ndims++;
              k = dims.indexOf('[', k + 1);
            }
          }
          boolean isKey = m.group(4) != null;
          TypeName tn = new TypeName(name, ndims);
          TypeField tf = new TypeField((String)e.getKey(), tn, isKey);
          flist.add(tf);
        }
        else
        {
          throw new RuntimeException("invalid type name " + (String)e.getValue());
        }
      }
    }
    return flist;
  }
  
  public void CreateStreamStatementWithTypeDef(AuthToken token, String streamName, Boolean doReplace, String[] partition_fields, Map<String, String>[] fields, String pset, boolean doPersist)
    throws MetaDataRepositoryException
  {
    compile(token, AST.CreateStreamStatement(streamName, doReplace, Arrays.asList(partition_fields), new TypeDefOrName(null, makeFieldList(fields)), null, null));
  }
  
  public void CreateTypeStatement(AuthToken token, String type_name, Boolean doReplace, Map<String, String>[] fields)
    throws MetaDataRepositoryException
  {
    String typeNameWithoutDomain = Utility.splitName(type_name);
    List<TypeField> typeFields = makeFieldList(fields);
    String str = Utility.createTypeStatementText(typeNameWithoutDomain, doReplace, typeFields);
    if (logger.isInfoEnabled()) {
      logger.info("CreateTypeStatement_New " + typeNameWithoutDomain + " " + doReplace + " ");
    }
    TypeDefOrName tdon = new TypeDefOrName(null, typeFields);
    Stmt stmt = AST.CreateTypeStatement(typeNameWithoutDomain, doReplace, tdon);
    stmt.sourceText = str;
    compile(token, stmt);
  }
  
  public void CreateTypeStatement_New(AuthToken token, String type_name, Boolean doReplace, TypeField[] fields)
    throws MetaDataRepositoryException
  {
    String typeNameWithoutDomain = Utility.splitName(type_name);
    String str = Utility.createTypeStatementText(typeNameWithoutDomain, doReplace, Arrays.asList(fields));
    if (logger.isInfoEnabled()) {
      logger.info("CreateTypeStatement_New " + typeNameWithoutDomain + " " + doReplace + " ");
    }
    TypeDefOrName tdon = new TypeDefOrName(null, Arrays.asList(fields));
    Stmt stmt = AST.CreateTypeStatement(typeNameWithoutDomain, doReplace, tdon);
    stmt.sourceText = str;
    compile(token, stmt);
  }
  
  public void CreateAppStatement(AuthToken token, String n, Boolean r, Map<String, String>[] entities, Boolean encrypt, RecoveryDescription recov, ExceptionHandler eh, String[] importStatements, List<DeploymentRule> deploymentRules)
    throws MetaDataRepositoryException
  {
    if (logger.isDebugEnabled()) {
      logger.debug("AuthToken: " + token);
    }
    if (logger.isInfoEnabled()) {
      logger.info("CreateAppStatement " + n + " " + r + " " + entities);
    }
    List<Pair<EntityType, String>> l = null;
    if (entities != null)
    {
      l = new ArrayList();
      for (Map<String, String> ent : entities)
      {
        assert (ent.size() == 1);
        for (Map.Entry<String, String> e : ent.entrySet()) {
          l.add(makeEntity((String)e.getKey(), (String)e.getValue()));
        }
      }
    }
    compile(token, AST.CreateFlowStatement(n, r, EntityType.APPLICATION, l, encrypt, recov, eh));
  }
  
  public void CreateNameSpaceStatement(AuthToken token, String n, Boolean r)
    throws MetaDataRepositoryException
  {
    if (logger.isInfoEnabled()) {
      logger.info("---> " + n + " " + r);
    }
    String name = Utility.splitName(n);
    if (logger.isInfoEnabled()) {
      logger.info("---> Using name: " + name);
    }
    compile(token, AST.CreateNamespaceStatement(name, r));
  }
  
  public void CreateVisualizationStatement(AuthToken token, String objectName, String n)
    throws MetaDataRepositoryException
  {
    compile(token, AST.CreateVisualization(objectName, n));
  }
  
  public void CreateCacheStatement(AuthToken token, String n, Boolean r, String reader_type, Map<String, String>[] reader_props, String parser_type, Map<String, String>[] parser_props, Map<String, String>[] query_props, String typename)
    throws MetaDataRepositoryException
  {
    String cacheNameWithoutDomain = Utility.splitName(n);
    String str = Utility.createCacheStatementText(cacheNameWithoutDomain, r, reader_type, makePropList(reader_props), parser_type, makePropList(parser_props), makePropList(query_props), typename);
    
    Stmt stmt = AST.CreateCacheStatement(cacheNameWithoutDomain, r, AST.CreateAdapterDesc(reader_type, makePropList(reader_props)), parser_type != null ? AST.CreateAdapterDesc(parser_type, makePropList(parser_props)) : null, makePropList(query_props), typename);
    
    stmt.sourceText = str;
    compile(token, stmt);
  }
  
  public void CreateCacheStatement_New(AuthToken token, String n, Boolean r, String reader_type, List<Property> reader_props, String parser_type, List<Property> parser_props, List<Property> query_props, String typename)
    throws MetaDataRepositoryException
  {
    String cacheNameWithoutDomain = Utility.splitName(n);
    String str = Utility.createCacheStatementText(cacheNameWithoutDomain, r, reader_type, reader_props, parser_type, parser_props, query_props, typename);
    
    Stmt stmt = AST.CreateCacheStatement(cacheNameWithoutDomain, r, AST.CreateAdapterDesc(reader_type, reader_props), parser_type != null ? AST.CreateAdapterDesc(parser_type, parser_props) : null, query_props, typename);
    
    stmt.sourceText = str;
    compile(token, stmt);
  }
  
  public void CreateDeploymentGroupStatement(AuthToken token, String groupname, String[] deploymentGroup, long minServers)
    throws MetaDataRepositoryException
  {
    compile(token, AST.CreateDeploymentGroupStatement(groupname, Arrays.asList(deploymentGroup), minServers));
  }
  
  public void CreateFlowStatement(AuthToken token, String n, Boolean r, Map<String, String>[] entities)
    throws MetaDataRepositoryException
  {
    List<Pair<EntityType, String>> l = null;
    if (entities != null)
    {
      l = new ArrayList();
      for (Map<String, String> ent : entities)
      {
        assert (ent.size() == 1);
        for (Map.Entry<String, String> e : ent.entrySet()) {
          l.add(makeEntity((String)e.getKey(), (String)e.getValue()));
        }
      }
    }
    compile(token, AST.CreateFlowStatement(n, r, EntityType.FLOW, l, Boolean.valueOf(false), null, null));
  }
  
  public void CreatePropertySet(AuthToken token, String n, Boolean r, Map<String, String>[] props)
    throws MetaDataRepositoryException
  {
    compile(token, AST.CreatePropertySet(n, r, makePropList(props)));
  }
  
  public void CreateRoleStatement(AuthToken token, String name)
    throws MetaDataRepositoryException
  {
    compile(token, AST.CreateRoleStatement(name));
  }
  
  public void CreateSourceStatement(AuthToken token, String n, Boolean r, String adapType, Map<String, String>[] adapProps, String parserType, Map<String, String>[] parserProps, String instream)
    throws MetaDataRepositoryException
  {
    String sourceNameWithoutDomain = Utility.splitName(n);
    if (logger.isInfoEnabled()) {
      logger.info("CreateSourceStatement_New (token " + token + ", entityName " + sourceNameWithoutDomain + ", replace " + r + " )");
    }
    String str;
    if ((parserType != null) && (parserProps != null)) {
      str = Utility.createSourceStatementText(sourceNameWithoutDomain, r, adapType, makePropList(adapProps), parserType, makePropList(parserProps), instream, null);
    } else {
      str = Utility.createSourceStatementText(sourceNameWithoutDomain, r, adapType, makePropList(adapProps), null, null, instream, null);
    }
    ArrayList<OutputClause> sinks = new ArrayList();
    sinks.add(AST.newOutputClause(instream, null, null, null, null, null));
    if ((parserType != null) && (parserProps != null))
    {
      Stmt stmt = AST.CreateSourceStatement(sourceNameWithoutDomain, r, AST.CreateAdapterDesc(adapType, makePropList(adapProps)), AST.CreateAdapterDesc(parserType, makePropList(parserProps)), sinks);
      
      stmt.sourceText = str;
      compile(token, stmt);
    }
    else
    {
      Stmt stmt = AST.CreateSourceStatement(sourceNameWithoutDomain, r, AST.CreateAdapterDesc(adapType, makePropList(adapProps)), null, sinks);
      
      stmt.sourceText = str;
      compile(token, stmt);
    }
  }
  
  private static String SOURCE_SIDE_FILTERING_VALIDATION_ERROR = "Creating Source Failed, %s cannot be %s";
  
  public void CreateFilteredSourceStatement_New(AuthToken authToken, String sourceName, Boolean doReplace, String adapterType, List<Property> adapterProperty, String parserType, List<Property> parserProperty, List<OutputClause> outputClauses)
    throws MetaDataRepositoryException
  {
    Validate.notNull(authToken, SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Authentication Token", null });
    Validate.notNull(sourceName, SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Source name", null });
    Validate.notNull(doReplace, SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Replace flag", null });
    Validate.notNull(adapterType, SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Adapter name", null });
    Validate.notNull(adapterProperty, SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Adapter properties", null });
    Validate.notNull(outputClauses, SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Output clause", null });
    
    Validate.notEmpty(sourceName, SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Source name", "empty" });
    Validate.notEmpty(adapterType, SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Adapter name", "empty" });
    Validate.notEmpty(adapterProperty, SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Adapter properties", "empty" });
    Validate.notEmpty(outputClauses, SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Output clause", "empty" });
    
    OutputClause outputClause = (OutputClause)outputClauses.get(0);
    
    List<TypeField> typeFields = outputClause.getTypeDefinition();
    MappedStream mappedStreams = outputClause.getGeneratedStream();
    String outputStream = outputClause.getStreamName();
    String selectText = outputClause.getFilterText();
    if (mappedStreams != null) {
      Validate.isTrue(typeFields == null, SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Type definition", "not null" });
    }
    if (typeFields != null)
    {
      Validate.notEmpty(typeFields, SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Type definition", "empty" });
      Validate.isTrue(mappedStreams == null, SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Map clause", "not null" });
    }
    String sourceNameWithoutDomain = Utility.splitName(sourceName);
    if (logger.isInfoEnabled()) {
      logger.info("CreateFilteredSourceStatement_New (token " + authToken + ", entityName " + sourceNameWithoutDomain + ", replace " + doReplace + " )");
    }
    String str = Utility.createSourceStatementText(sourceName, doReplace, adapterType, adapterProperty, parserType, parserProperty, outputClauses) + ";";
    try
    {
      compileText(authToken, str);
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw new MetaDataRepositoryException(e.getMessage(), e);
    }
  }
  
  public void CreateSourceStatement_New(AuthToken token, String n, Boolean r, String adapType, List<Property> adapProps, String parserType, List<Property> parserProps, String instream)
    throws MetaDataRepositoryException
  {
    String sourceNameWithoutDomain = Utility.splitName(n);
    if (logger.isInfoEnabled()) {
      logger.info("CreateSourceStatement_New (token " + token + ", entityName " + sourceNameWithoutDomain + ", replace " + r + " )");
    }
    String str;
    String str;
    if ((parserType != null) && (parserProps != null)) {
      str = Utility.createSourceStatementText(sourceNameWithoutDomain, r, adapType, adapProps, parserType, parserProps, instream, null);
    } else {
      str = Utility.createSourceStatementText(sourceNameWithoutDomain, r, adapType, adapProps, null, null, instream, null);
    }
    ArrayList<OutputClause> sinks = new ArrayList();
    sinks.add(AST.newOutputClause(instream, null, null, null, null, null));
    if ((parserType != null) && (parserProps != null))
    {
      Stmt stmt = AST.CreateSourceStatement(sourceNameWithoutDomain, r, AST.CreateAdapterDesc(adapType, adapProps), AST.CreateAdapterDesc(parserType, parserProps), sinks);
      
      stmt.sourceText = str;
      compile(token, stmt);
    }
    else
    {
      Stmt stmt = AST.CreateSourceStatement(sourceNameWithoutDomain, r, AST.CreateAdapterDesc(adapType, adapProps), null, sinks);
      
      stmt.sourceText = str;
      compile(token, stmt);
    }
  }
  
  public void CreateTargetStatement(AuthToken token, String n, Boolean r, String adapType, Map<String, String>[] adapProps, String formatterType, Map<String, String>[] formatterProps, String stream)
    throws MetaDataRepositoryException
  {
    String targetNameWithoutDomain = Utility.splitName(n);
    String str = Utility.createTargetStatementText(targetNameWithoutDomain, r, adapType, makePropList(adapProps), formatterType, makePropList(formatterProps), stream);
    Stmt stmt = null;
    if ((formatterType != null) && (formatterProps != null)) {
      stmt = AST.CreateTargetStatement(targetNameWithoutDomain, r, AST.CreateAdapterDesc(adapType, makePropList(adapProps)), AST.CreateAdapterDesc(formatterType, makePropList(formatterProps)), stream, null);
    } else {
      stmt = AST.CreateTargetStatement(targetNameWithoutDomain, r, AST.CreateAdapterDesc(adapType, makePropList(adapProps)), null, stream, null);
    }
    stmt.sourceText = str;
    compile(token, stmt);
  }
  
  public void CreateUserStatement(AuthToken token, String userid, String password, String[] role_name, String namespace, Map<String, String>[] props)
    throws MetaDataRepositoryException
  {
    UserProperty userProperty = new UserProperty(Arrays.asList(role_name), namespace, makePropList(props));
    
    compile(token, AST.CreateUserStatement(userid, password, userProperty, null));
  }
  
  public void CreateUseSchemaStmt(AuthToken token, String schemaName)
    throws MetaDataRepositoryException
  {
    if (logger.isInfoEnabled()) {
      logger.info("Use NameSpace " + schemaName);
    }
    String name = Utility.splitName(schemaName);
    if (logger.isInfoEnabled()) {
      logger.info("Actually using NameSpace: " + name);
    }
    compile(token, AST.CreateUseNamespaceStmt(name));
  }
  
  private List<EventType> createEventTypes(Map<String, List<String>> ets)
  {
    List<EventType> ret = new ArrayList();
    for (Map.Entry<String, List<String>> et : ets.entrySet())
    {
      String typeName = (String)et.getKey();
      List<String> keyFields = (List)et.getValue();
      EventType evt = new EventType(typeName, keyFields);
      if (keyFields.isEmpty()) {
        throw new RuntimeException("Event field is missing key attribute, so cannot create a wactionstore");
      }
      ret.add(evt);
    }
    return ret;
  }
  
  public void CreateWASWithTypeNameStatement(AuthToken token, String n, Boolean r, String typeName, Map<String, List<String>> ets, String howLong, Map<String, String>[] props)
    throws MetaDataRepositoryException
  {
    String wasNameWithoutDomain = Utility.splitName(n);
    if (logger.isInfoEnabled()) {
      logger.info("CreateWASWithTypeNameStatement " + wasNameWithoutDomain);
    }
    Interval interval = null;
    if (StringUtils.isNotBlank(howLong)) {
      interval = new Interval(Long.parseLong(howLong));
    }
    String str = Utility.createWASStatementText(wasNameWithoutDomain, r, typeName, ets, howLong, new WactionStorePersistencePolicy(interval, makePropList(props)));
    WactionStorePersistencePolicy wactionStorePersistencePolicy;
    if (howLong == null) {
      wactionStorePersistencePolicy = new WactionStorePersistencePolicy(interval, makePropList(props));
    } else {
      wactionStorePersistencePolicy = new WactionStorePersistencePolicy(interval, makePropList(props));
    }
    Stmt stmt = AST.CreateWASStatement(wasNameWithoutDomain, r, new TypeDefOrName(typeName, null), createEventTypes(ets), wactionStorePersistencePolicy);
    
    stmt.sourceText = str;
    compile(token, stmt);
  }
  
  public void CreateWASWithTypeDefStatement(AuthToken token, String n, Boolean r, Map<String, String>[] typeDef, Map<String, List<String>> ets, String howLong, Map<String, String>[] props)
    throws MetaDataRepositoryException
  {
    if (logger.isInfoEnabled()) {
      logger.info("CreateWASStatementWithTypeDef" + n);
    }
    compile(token, AST.CreateWASStatement(n, r, new TypeDefOrName(null, makeFieldList(typeDef)), createEventTypes(ets), new WactionStorePersistencePolicy(Utility.parseInterval(howLong), makePropList(props))));
  }
  
  public void CreateWindowStatement(AuthToken token, String window_name, Boolean doReplace, String stream_name, Map<String, Object> window_len, boolean isJumping, String[] partition_fields, boolean isPersistent, Map<String, Object> slidePolicy)
    throws MetaDataRepositoryException
  {
    WindowUtility.validateIntervalPolicies(isJumping, window_len, slidePolicy);
    
    String windowNameWithoutDomain = Utility.splitName(window_name);
    String str = Utility.createWindowStatementText(windowNameWithoutDomain, doReplace, stream_name, window_len, isJumping, partition_fields, isPersistent);
    
    Stmt stmt = AST.CreateWindowStatement(windowNameWithoutDomain, doReplace, stream_name, Pair.make(makeIntervalPolicy(window_len), makeIntervalPolicy(slidePolicy)), isJumping, Arrays.asList(partition_fields));
    
    stmt.sourceText = str;
    compile(token, stmt);
  }
  
  private static IntervalPolicy makeIntervalPolicy(Map<String, Object> desc)
  {
    Object countVal = desc.get("count");
    Object timeVal = desc.get("time");
    
    Integer countSize = null;
    Interval timeLimit = null;
    Long timeoutVal = null;
    if (countVal != null) {
      if ((countVal instanceof String)) {
        countSize = Integer.valueOf((String)countVal);
      } else if ((countVal instanceof Integer)) {
        countSize = (Integer)countVal;
      } else if ((countVal instanceof Long)) {
        countSize = Integer.valueOf(((Long)countVal).intValue());
      } else {
        throw new RuntimeException("invalid count interval");
      }
    }
    if (timeVal != null)
    {
      timeVal = desc.get("time");
      if ((timeVal instanceof String)) {
        timeLimit = new Interval(Long.parseLong((String)timeVal));
      } else if ((timeVal instanceof Long)) {
        timeLimit = new Interval(((Long)timeVal).longValue());
      } else {
        throw new RuntimeException("invalid time interval");
      }
    }
    Object timeout = desc.get("range");
    String range = null;
    if (timeout != null) {
      if ((timeout instanceof String)) {
        timeoutVal = Long.valueOf((String)timeout);
      } else if ((timeout instanceof Long)) {
        timeoutVal = (Long)timeout;
      } else {
        throw new RuntimeException("invalid RANGE attribute");
      }
    }
    Object onField = desc.get("on");
    String field = null;
    if (onField != null) {
      if ((onField instanceof String)) {
        field = (String)onField;
      } else {
        throw new RuntimeException("invalid ON attribute");
      }
    }
    if ((timeoutVal != null) && (field != null) && (!field.isEmpty()) && (timeLimit != null)) {
      return IntervalPolicy.createTimeAttrPolicy(timeLimit, field, timeoutVal.longValue());
    }
    if ((countSize != null) && (timeoutVal != null) && ((field == null) || (field.isEmpty()))) {
      return IntervalPolicy.createTimeCountPolicy(new Interval(timeoutVal.longValue()), countSize.intValue());
    }
    if ((countSize != null) && (timeLimit != null)) {
      return IntervalPolicy.createTimeCountPolicy(timeLimit, countSize.intValue());
    }
    if ((field != null) && (!field.isEmpty()) && (timeLimit != null)) {
      return IntervalPolicy.createAttrPolicy(field, timeLimit.value);
    }
    if (countSize != null) {
      return IntervalPolicy.createCountPolicy(countSize.intValue());
    }
    if (timeLimit != null) {
      return IntervalPolicy.createTimePolicy(timeLimit);
    }
    return null;
  }
  
  public void ImportClassDeclaration(AuthToken token, String pkgname, boolean isStatic)
    throws MetaDataRepositoryException
  {
    compile(token, AST.ImportClassDeclaration(pkgname, isStatic));
  }
  
  public void ImportPackageDeclaration(AuthToken token, String pkgname, boolean isStatic)
    throws MetaDataRepositoryException
  {
    compile(token, AST.ImportPackageDeclaration(pkgname, isStatic));
  }
  
  public void CreateCqStatement(AuthToken token, final String cq_name, final Boolean doReplace, final String dest_stream_name, final String[] field_name_list, final String selectText)
    throws Exception
  {
    Context context = getContext(token);
    final String tqlText = selectText + (selectText.endsWith(";") ? "" : ";");
    boolean oldVal = context.setReadOnly(!this.updateMode);
    try
    {
      Compiler.compile(tqlText, context, new Compiler.ExecutionCallback()
      {
        public void execute(Stmt stmt, Compiler compiler)
          throws Exception
        {
          if (!(stmt instanceof CreateAdHocSelectStmt)) {
            throw new RuntimeException("invalid select statement");
          }
          Select select = ((CreateAdHocSelectStmt)stmt).select;
          String cqNameWithoutDomain = Utility.splitName(cq_name);
          Stmt cqStmt = AST.CreateCqStatement(cqNameWithoutDomain, doReplace, dest_stream_name, Arrays.asList(field_name_list), select, selectText);
          
          cqStmt.sourceText = Utility.createCQStatementText(cqNameWithoutDomain, doReplace, dest_stream_name, field_name_list, tqlText);
          
          compiler.compileStmt(cqStmt);
        }
      });
    }
    finally
    {
      context.setReadOnly(oldVal);
    }
  }
  
  public void DropStatement(AuthToken token, String entityType, String entityName, boolean doCascade, boolean doForce)
    throws MetaDataRepositoryException
  {
    if (logger.isInfoEnabled()) {
      logger.info("DropStatement (token " + token + ", entityType " + entityType + ", entityName " + entityName + ", entityName " + doCascade + " , doForce " + doForce + ")");
    }
    EntityType what = EntityType.forObject(entityType);
    DropMetaObject.DropRule rule = DropMetaObject.DropRule.NONE;
    if (what == EntityType.UNKNOWN) {
      throw new RuntimeException("unknown object type <" + entityType + ">");
    }
    if ((doCascade) && (doForce)) {
      rule = DropMetaObject.DropRule.ALL;
    }
    if (doCascade) {
      rule = DropMetaObject.DropRule.CASCADE;
    }
    if (doForce) {
      rule = DropMetaObject.DropRule.FORCE;
    }
    compile(token, AST.DropStatement(what, entityName, rule));
  }
  
  public String CreateShowStreamStatement(AuthToken token, String streamName, int lineCount)
    throws MetaDataRepositoryException
  {
    compile(token, AST.CreateShowStmt(streamName, lineCount, false));
    
    return "consoleQueue" + token.toString();
  }
  
  public List<MetaInfo.MetaObject> getAllObjectsByEntityType(String[] eTypes, AuthToken token)
    throws MetaDataRepositoryException
  {
    if ((eTypes instanceof String[]))
    {
      List<MetaInfo.MetaObject> mObjects = new ArrayList();
      for (String eType : eTypes)
      {
        Set<MetaInfo.MetaObject> metaObjectSet = this.metadataRepository.getByEntityType(EntityType.forObject(eType), token);
        if (metaObjectSet != null) {
          mObjects.addAll(metaObjectSet);
        }
      }
    }
    else
    {
      throw new RuntimeException("Expected String Array, Passed : " + eTypes.getClass().toString());
    }
    List<MetaInfo.MetaObject> mObjects;
    if (mObjects != null)
    {
      Set<? extends MetaInfo.MetaObject> result = Utility.removeInternalApplications(new HashSet(mObjects));
      if (result != null) {
        return new ArrayList(result);
      }
    }
    return null;
  }
  
  private MetaInfo.Query compileQuery(final AuthToken token, String text)
    throws Exception
  {
    final ObjectReference<MetaInfo.Query> type = new ObjectReference();
    if (logger.isDebugEnabled()) {
      logger.debug("Compiling text: " + text);
    }
    Context context = getContext(token);
    boolean oldVal = context.setReadOnly(!this.updateMode);
    try
    {
      Compiler.compile(text, context, new Compiler.ExecutionCallback()
      {
        public void execute(Stmt stmt, Compiler compiler)
          throws MetaDataRepositoryException
        {
          if (QueryValidator.logger.isDebugEnabled()) {
            QueryValidator.logger.debug("Compiling stmt" + stmt.toString());
          }
          if ((stmt instanceof CreateAdHocSelectStmt))
          {
            CreateAdHocSelectStmt adhoc = (CreateAdHocSelectStmt)stmt;
            MetaInfo.Query q = compiler.compileCreateAdHocSelect(adhoc);
            type.set(q);
            if ((!(stmt instanceof CreatePropertySetStmt)) && (!(stmt instanceof CreateUserStmt)) && (!(stmt instanceof ConnectStmt))) {
              QueryValidator.this.storeCommand(token, QueryValidator.this.getUserId(token), stmt.sourceText);
            }
          }
        }
      });
    }
    finally
    {
      context.setReadOnly(oldVal);
    }
    return (MetaInfo.Query)type.get();
  }
  
  public MetaInfo.Query compileAdHocText(AuthToken token, String text)
    throws Exception
  {
    return compileQuery(token, text);
  }
  
  public MetaInfo.Query createNamedQuery(AuthToken token, String queryDefinition)
    throws Exception
  {
    return compileQuery(token, queryDefinition);
  }
  
  public MetaInfo.Query createAdhocQuery(AuthToken token, String queryDefinition)
    throws Exception
  {
    return compileQuery(token, queryDefinition);
  }
  
  public List<MetaInfo.Query> listAllQueries(AuthToken authToken)
    throws MetaDataRepositoryException
  {
    Context context = getContext(authToken);
    return context.listQueries(new QueryManager.TYPE[] { QueryManager.TYPE.ADHOC, QueryManager.TYPE.NAMEDQUERY }, authToken);
  }
  
  public List<MetaInfo.Query> listAdhocQueries(AuthToken authToken)
    throws MetaDataRepositoryException
  {
    Context context = getContext(authToken);
    return context.listQueries(new QueryManager.TYPE[] { QueryManager.TYPE.ADHOC }, authToken);
  }
  
  public List<MetaInfo.Query> listNamedQueries(AuthToken authToken)
    throws MetaDataRepositoryException
  {
    Context context = getContext(authToken);
    return context.listQueries(new QueryManager.TYPE[] { QueryManager.TYPE.NAMEDQUERY }, authToken);
  }
  
  public MetaInfo.Query createNamedQuery(AuthToken token, String queryDefinition, String alias)
    throws Exception
  {
    return createNamedQuery(token, "CREATE NAMEDQUERY " + alias + " " + queryDefinition);
  }
  
  public MetaInfo.Query createParameterizedQuery(boolean doReplace, AuthToken authToken, String query, String alias)
    throws Exception
  {
    if (query == null) {
      throw new IllegalArgumentException("Invalid query.");
    }
    Context context = getContext(authToken);
    if (alias.indexOf(".") == -1) {
      throw new RuntimeException("Wrong query name to create.");
    }
    String nsname = Utility.splitDomain(alias);
    String name = Utility.splitName(alias);
    MetaInfo.Namespace ns = null;
    if (nsname == null)
    {
      ns = context.getCurNamespace();
    }
    else
    {
      List<MetaInfo.Namespace> nss = this.metadataRepository.getAllNamespaces(authToken);
      for (MetaInfo.Namespace ans : nss) {
        if (ans.name.equalsIgnoreCase(nsname))
        {
          ns = ans;
          break;
        }
      }
      if (ns == null) {
        throw new RuntimeException("Namespace " + nsname + " for query " + name + " does not exist");
      }
    }
    MetaInfo.Query queryMetaObject = (MetaInfo.Query)this.metadataRepository.getMetaObjectByName(EntityType.QUERY, nsname, name, null, authToken);
    if ((queryMetaObject != null) && (!doReplace)) {
      throw new RuntimeException("Query already exists, user create or replace");
    }
    if ((queryMetaObject != null) && (doReplace)) {
      deleteNamedQueryByUUID(authToken, queryMetaObject.getUuid());
    }
    MetaInfo.Query queryInstance = null;
    try
    {
      queryInstance = createNamedQuery(authToken, query, alias);
    }
    catch (Exception e)
    {
      queryInstance = new MetaInfo.Query(name, ns, null, null, null, null, query, Boolean.valueOf(false));
      queryInstance.getMetaInfoStatus().setValid(false);
      this.metadataRepository.putMetaObject(queryInstance, authToken);
      throw new RuntimeException("Cannot compile the query, query " + nsname + "." + name + " invalidated, due to: " + e.getMessage());
    }
    return queryInstance;
  }
  
  public MetaInfo.Query execParameterizedQuery(AuthToken authToken, UUID queryId, Map<String, String>[] params)
    throws Exception
  {
    Context context = getContext(authToken);
    MetaInfo.Query query = (MetaInfo.Query)context.getObject(queryId);
    MetaInfo.Query dup = context.prepareQuery(query, makePropList(params));
    if (logger.isDebugEnabled()) {
      logger.debug("Query duplicated ready to run:  " + dup.getFullName() + " : UUID " + dup.appUUID);
    }
    context.startAdHocQuery(dup);
    return dup;
  }
  
  public MetaInfo.Query prepareQuery(AuthToken authToken, UUID queryId, Map<String, Object> params)
    throws Exception
  {
    Context context = getContext(authToken);
    Map<String, String>[] paramArgs = null;
    
    MetaInfo.Query query = (MetaInfo.Query)context.getObject(queryId);
    if (query == null) {
      throw new RuntimeException("Query doesn't exist.");
    }
    if (!query.getMetaInfoStatus().isValid()) {
      try
      {
        CreateAlterRecompileStmt(authToken, "QUERY", query.getFullName());
        query = (MetaInfo.Query)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.QUERY, query.getNsName(), query.getName(), null, authToken);
      }
      catch (Exception e)
      {
        throw new RuntimeException("Query " + query.getFullName() + " is not valid, potentially queried object doesn't exist.");
      }
    }
    if (params.isEmpty())
    {
      MetaInfo.CQ cq = (MetaInfo.CQ)this.metadataRepository.getMetaObjectByUUID(query.cqUUID, authToken);
      int paramCounter;
      if ((cq != null) && (cq.plan != null) && (cq.plan.paramsDesc != null) && (cq.plan.paramsDesc.size() > 0))
      {
        paramArgs = new HashMap[cq.plan.paramsDesc.size()];
        paramCounter = 0;
        for (ParamDesc d : cq.plan.paramsDesc)
        {
          Map<String, String> paramMapping = new HashMap();
          paramMapping.put(d.paramName, null);
          paramArgs[paramCounter] = paramMapping;
          paramCounter++;
        }
      }
      else
      {
        paramArgs = new HashMap[0];
      }
    }
    else
    {
      paramArgs = new HashMap[] { (HashMap)params };
    }
    paramArgs = checkIfAllParametersSet(query, paramArgs, authToken);
    MetaInfo.Query dup = context.prepareQuery(query, makePropList(paramArgs));
    if (logger.isDebugEnabled())
    {
      logger.debug("ORIGINAL QUERY : " + query);
      logger.debug("DUPLICATED QUERY :  " + dup.getFullName() + " : UUID " + dup.getUuid());
      logger.debug("QUERY OBJECTS : " + this.metadataRepository.getMetaObjectByUUID(query.appUUID, WASecurityManager.TOKEN).getDependencies());
      logger.debug("DUPLICATED QUERY OBJECTS :  " + dup.getFullName() + " : UUID " + this.metadataRepository.getMetaObjectByUUID(dup.appUUID, WASecurityManager.TOKEN).getDependencies());
      logger.debug("ORIGINAL CQ : " + this.metadataRepository.getMetaObjectByUUID(query.cqUUID, WASecurityManager.TOKEN));
      logger.debug("DUPLICATED CQ :  " + this.metadataRepository.getMetaObjectByUUID(dup.cqUUID, WASecurityManager.TOKEN));
    }
    dup.projectionFields = query.projectionFields;
    this.metadataRepository.updateMetaObject(dup, authToken);
    return dup;
  }
  
  Map<String, String>[] checkIfAllParametersSet(MetaInfo.Query query, Map<String, String>[] paramArgs, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    Set<String> expected = new HashSet();
    Set<String> calculated = new HashSet();
    
    MetaInfo.CQ cq = (MetaInfo.CQ)this.metadataRepository.getMetaObjectByUUID(query.cqUUID, authToken);
    if ((cq != null) && (cq.plan != null) && (cq.plan.paramsDesc != null) && (cq.plan.paramsDesc.size() > 0)) {
      for (ParamDesc d : cq.plan.paramsDesc) {
        expected.add(d.paramName);
      }
    }
    for (Map<String, String> paramMap : paramArgs) {
      for (Map.Entry<String, String> params : paramMap.entrySet()) {
        calculated.add(params.getKey());
      }
    }
    Set<String> missing = new HashSet();
    for (String string : expected) {
      if (!calculated.contains(string)) {
        missing.add(string);
      }
    }
    if (!missing.isEmpty())
    {
      Map<String, String>[] result = new HashMap[paramArgs.length + missing.size()];
      for (int i = 0; i < paramArgs.length; i++) {
        result[i] = paramArgs[i];
      }
      for (String missingParam : missing)
      {
        HashMap map = new HashMap();
        map.put(missingParam, null);
        result[i] = map;
        i++;
      }
      return result;
    }
    return paramArgs;
  }
  
  private static QueryManager.QueryParameters extractParametersInTQL(String word)
  {
    Pattern stringParamMatch = Pattern.compile("\"[:](\\w*)\"");
    Matcher stringMatch = stringParamMatch.matcher(word);
    
    Set<String> stringParams = new HashSet();
    while (stringMatch.find()) {
      stringParams.add(stringMatch.group(1));
    }
    Pattern paramMatch = Pattern.compile("(\\W)[:](\\w*)(\\W)");
    
    Matcher match = paramMatch.matcher(word);
    Set<String> params = new HashSet();
    while (match.find()) {
      if (!match.group(1).equals("\"")) {
        params.add(match.group(2));
      }
    }
    QueryManager.QueryParameters queryParameters = new QueryManager.QueryParameters();
    queryParameters.setStringParams(stringParams);
    queryParameters.setOtherParams(new HashSet());
    for (String string : params) {
      queryParameters.addOtherParams(string);
    }
    return queryParameters;
  }
  
  private static String setParametersInTQL(String word, Map<String, Object> params)
  {
    if (params == null)
    {
      QueryManager.QueryParameters queryParameters = extractParametersInTQL(word);
      Set<String> stringParams = queryParameters.getStringParams();
      Set<String> otherParams = queryParameters.getOtherParams();
      for (String paramName : stringParams)
      {
        word = word.replaceAll("[:]" + paramName + " IS NULL", "FALSE");
        word = word.replaceAll("(\")[:]" + paramName + "(\")", "$1$2");
      }
      for (String paramName : otherParams)
      {
        word = word.replaceAll("[:]" + paramName + " IS NULL", "FALSE");
        word = word.replaceAll("(\\W)[:]" + paramName + "(\\W)", "$10$2");
      }
      return word;
    }
    Set<String> paramNames = extractParametersInTQL(word).allParams();
    for (String paramName : paramNames) {
      if (params.containsKey(paramName))
      {
        word = word.replaceAll("[:]" + paramName + " IS NULL", "FALSE");
        word = word.replaceAll("(\\W)[:]" + paramName + "(\\W)", "$1" + params.get(paramName) + "$2");
      }
      else
      {
        word = word.replaceAll("[:]" + paramName + " IS NULL", "TRUE");
        word = word.replaceAll("(\\W)[:]" + paramName + "(\\W)", "$1NULL$2");
      }
    }
    return word;
  }
  
  private MetaInfo.Query getQuery(UUID queryId, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    return (MetaInfo.Query)this.metadataRepository.getMetaObjectByUUID(queryId, authToken);
  }
  
  private MetaInfo.Query getQuery(String queryName, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    return (MetaInfo.Query)this.metadataRepository.getMetaObjectByName(EntityType.QUERY, Utility.splitDomain(queryName), Utility.splitName(queryName), null, authToken);
  }
  
  public void deleteAdhocQuery(AuthToken authToken, UUID queryId)
    throws MetaDataRepositoryException
  {
    Context context = getContext(authToken);
    MetaInfo.Query query = getQuery(queryId, authToken);
    if ((query == null) || (!query.isAdhocQuery())) {
      return;
    }
    context.deleteQuery(query, authToken);
  }
  
  public void deleteNamedQueryByUUID(AuthToken authToken, UUID queryId)
    throws MetaDataRepositoryException
  {
    Context context = getContext(authToken);
    MetaInfo.Query query = getQuery(queryId, authToken);
    if ((query == null) || (query.isAdhocQuery())) {
      return;
    }
    context.deleteQuery(query, authToken);
  }
  
  public void deleteNamedQueryByName(AuthToken authToken, String queryName)
    throws MetaDataRepositoryException
  {
    Context context = getContext(authToken);
    MetaInfo.Query query = getQuery(queryName, authToken);
    if ((query == null) || (query.isAdhocQuery())) {
      return;
    }
    context.deleteQuery(query, authToken);
  }
  
  public MetaInfo.Query cloneNamedQueryFromAdhoc(AuthToken authToken, UUID queryId, String alias)
    throws Exception
  {
    MetaInfo.Query query = getQuery(queryId, authToken);
    if ((query == null) || (!query.isAdhocQuery())) {
      throw new FatalException("Wrong adhoc query");
    }
    String queryText = query.queryDefinition;
    stopAdhocQuery(authToken, queryId);
    deleteAdhocQuery(authToken, queryId);
    return createNamedQuery(authToken, "CREATE NAMEDQUERY " + alias + " " + queryText + ";");
  }
  
  public boolean startAdhocQuery(AuthToken authToken, UUID queryId)
    throws MetaDataRepositoryException
  {
    Context context = getContext(authToken);
    MetaInfo.Query query = getQuery(queryId, authToken);
    if (query == null) {
      throw new FatalException("No such query exists");
    }
    MetaInfo.Flow applicationMetaObject = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByUUID(query.appUUID, authToken);
    if (applicationMetaObject == null) {
      throw new FatalException("No such application exists");
    }
    try
    {
      context.startAdHocQuery(query);
    }
    catch (Exception e)
    {
      context.deleteQuery(query, authToken);
      throw new RuntimeException(findRootThrowable(e).getMessage());
    }
    return true;
  }
  
  private Throwable findRootThrowable(Throwable e)
  {
    if (e.getCause() == null) {
      return e;
    }
    return findRootThrowable(e.getCause());
  }
  
  public boolean startNamedQuery(AuthToken authToken, UUID queryId)
    throws MetaDataRepositoryException
  {
    Context context = getContext(authToken);
    MetaInfo.Query query = getQuery(queryId, authToken);
    if (query == null) {
      throw new FatalException("No such query exists");
    }
    MetaInfo.Flow applicationMetaObject = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByUUID(query.appUUID, authToken);
    if (applicationMetaObject == null) {
      throw new FatalException("No such application exists");
    }
    context.startAdHocQuery(query);
    return true;
  }
  
  public boolean stopAdhocQuery(AuthToken authToken, UUID queryId)
    throws MetaDataRepositoryException
  {
    Context context = getContext(authToken);
    MetaInfo.Query query = getQuery(queryId, authToken);
    if (query != null)
    {
      context.stopAdHocQuery(query);
      context.deleteQuery(query, authToken);
    }
    return true;
  }
  
  public boolean stopNamedQuery(AuthToken authToken, UUID queryId)
    throws MetaDataRepositoryException
  {
    Context context = getContext(authToken);
    MetaInfo.Query query = getQuery(queryId, authToken);
    if (query == null) {
      throw new FatalException("No such query exists");
    }
    context.stopAdHocQuery(query);
    return true;
  }
  
  public MetaInfo.Query createAdhocQueryFromJSON(AuthToken authToken, String queryDefinition)
    throws Exception
  {
    return createAdhocQuery(authToken, CQUtility.convertJSONToSQL(queryDefinition));
  }
  
  public MetaInfo.Query createNamedQueryFromJSON(AuthToken authToken, String queryDefinition)
    throws Exception
  {
    return createNamedQuery(authToken, CQUtility.convertJSONToSQL(queryDefinition));
  }
  
  public String exportDashboard(AuthToken authToken, String dashboardName)
    throws Exception
  {
    VisualizationArtifacts va = new VisualizationArtifacts();
    MetaInfo.Dashboard dashboard = (MetaInfo.Dashboard)this.metadataRepository.getMetaObjectByName(EntityType.DASHBOARD, Utility.splitDomain(dashboardName), Utility.splitName(dashboardName), null, authToken);
    if (dashboard == null) {
      throw new RuntimeException("No such dashboard exists");
    }
    va.setDashboard(dashboard);
    for (String pageName : dashboard.getPages())
    {
      MetaInfo.Page page = null;
      if (pageName.indexOf(".") == -1) {
        page = (MetaInfo.Page)this.metadataRepository.getMetaObjectByName(EntityType.PAGE, Utility.splitDomain(dashboardName), pageName, null, authToken);
      } else {
        page = (MetaInfo.Page)this.metadataRepository.getMetaObjectByName(EntityType.PAGE, Utility.splitDomain(pageName), Utility.splitName(pageName), null, authToken);
      }
      va.addPages(page);
      if (page == null) {
        throw new RuntimeException("No such Page exists");
      }
      for (String queryVisualizationName : page.getQueryVisualizations())
      {
        MetaInfo.QueryVisualization queryVisualization = null;
        if (queryVisualizationName.indexOf(".") == -1) {
          queryVisualization = (MetaInfo.QueryVisualization)this.metadataRepository.getMetaObjectByName(EntityType.QUERYVISUALIZATION, Utility.splitDomain(dashboardName), queryVisualizationName, null, authToken);
        } else {
          queryVisualization = (MetaInfo.QueryVisualization)this.metadataRepository.getMetaObjectByName(EntityType.QUERYVISUALIZATION, Utility.splitDomain(queryVisualizationName), Utility.splitName(queryVisualizationName), null, authToken);
        }
        if (queryVisualization == null) {
          throw new RuntimeException("No such QueryVisualization exists");
        }
        va.addQueryVisualization(queryVisualization);
        
        String queryName = queryVisualization.getQuery();
        MetaInfo.Query queryMetaObject = null;
        if ((queryName != null) && (!queryName.isEmpty()))
        {
          if (queryName.indexOf(".") == -1) {
            queryMetaObject = (MetaInfo.Query)this.metadataRepository.getMetaObjectByName(EntityType.QUERY, Utility.splitDomain(dashboardName), Utility.splitName(queryName), null, authToken);
          } else {
            queryMetaObject = (MetaInfo.Query)this.metadataRepository.getMetaObjectByName(EntityType.QUERY, Utility.splitDomain(queryName), Utility.splitName(queryName), null, authToken);
          }
          if (queryMetaObject == null) {
            throw new RuntimeException("No such Query exists");
          }
          va.addParametrizedQuery(queryMetaObject);
        }
      }
    }
    return va.convertToJSON();
  }
  
  public UUID importDashboard(AuthToken authToken, Boolean doReplace, String fileName, String fullJSON)
    throws Exception
  {
    Context context = getContext(authToken);
    if ((fullJSON == null) || (fullJSON.isEmpty()))
    {
      String json = Compiler.readFile((CreateDashboardStatement)AST.CreateDashboardStatement(doReplace, fileName));
      UUID dashboardUUID = null;
      try
      {
        dashboardUUID = context.createDashboard(json, null);
      }
      catch (Exception e)
      {
        logger.warn(e.getMessage());
      }
      return dashboardUUID;
    }
    return context.createDashboard(fullJSON, null);
  }
  
  public UUID createQueryIfNotExists(AuthToken authToken, String queryName, String queryDefinition)
    throws Exception
  {
    Context context = getContext(authToken);
    MetaInfo.Query query = (MetaInfo.Query)context.get(queryName, EntityType.QUERY);
    if (query != null) {
      return query.getUuid();
    }
    String queryTQL = "CREATE NAMEDQUERY " + queryName + " " + queryDefinition;
    try
    {
      query = createNamedQuery(authToken, queryTQL);
    }
    catch (Exception e)
    {
      logger.warn(e.getMessage());
    }
    return query.getUuid();
  }
  
  public UUID importDashboard(AuthToken authToken, String toNamespace, Boolean doReplace, String fileName, String fullJSON)
    throws Exception
  {
    Context context = getContext(authToken);
    if ((fullJSON == null) || (fullJSON.isEmpty()))
    {
      String json = Compiler.readFile((CreateDashboardStatement)AST.CreateDashboardStatement(doReplace, fileName));
      UUID dashboardUUID = null;
      try
      {
        dashboardUUID = context.createDashboard(json, toNamespace);
      }
      catch (Exception e)
      {
        logger.warn(e.getMessage());
      }
      return dashboardUUID;
    }
    return context.createDashboard(fullJSON, toNamespace);
  }
  
  public void CreateDashboard(AuthToken token, String objectName, String namespace, Boolean cor, ObjectNode data)
    throws JSONException, MetaDataRepositoryException
  {
    MetaInfo.Dashboard dashBoardMetaObject = (MetaInfo.Dashboard)this.metadataRepository.getMetaObjectByName(EntityType.DASHBOARD, namespace, objectName, null, token);
    boolean isNew;
    boolean isNew;
    if (dashBoardMetaObject == null)
    {
      assert (!cor.booleanValue());
      dashBoardMetaObject = new MetaInfo.Dashboard();
      MetaInfo.Namespace ns = (MetaInfo.Namespace)this.metadataRepository.getMetaObjectByName(EntityType.NAMESPACE, "Global", namespace, null, token);
      dashBoardMetaObject.construct(objectName, ns, EntityType.DASHBOARD);
      isNew = true;
    }
    else
    {
      assert (cor.booleanValue() == true);
      isNew = false;
    }
    if (data.has("title"))
    {
      JsonNode titleNode = data.get("title");
      if (titleNode != null) {
        dashBoardMetaObject.setTitle(titleNode.asText());
      }
    }
    if (data.has("defaultLandingPage"))
    {
      JsonNode defaultPageNode = data.get("defaultLandingPage");
      if (defaultPageNode != null) {
        dashBoardMetaObject.setDefaultLandingPage(defaultPageNode.asText());
      }
    }
    if (data.has("pages"))
    {
      JsonNode pagesNode = data.get("pages");
      if (pagesNode != null)
      {
        assert (pagesNode.isArray() == true);
        ArrayNode an = (ArrayNode)pagesNode;
        Iterator<JsonNode> itr = an.elements();
        List<String> allPages = new ArrayList();
        while (itr.hasNext())
        {
          JsonNode pNode = ((JsonNode)itr.next()).get("id");
          if (pNode == null) {
            throw new JSONException("Page Id can't be NULL");
          }
          allPages.add(pNode.asText());
        }
        dashBoardMetaObject.setPages(allPages);
      }
    }
    if (isNew) {
      this.metadataRepository.putMetaObject(dashBoardMetaObject, token);
    } else {
      this.metadataRepository.updateMetaObject(dashBoardMetaObject, token);
    }
  }
  
  public void CreatePage(AuthToken token, String objectName, String namespace, Boolean cor, ObjectNode data)
    throws MetaDataRepositoryException, JSONException
  {
    MetaInfo.Page p = (MetaInfo.Page)this.metadataRepository.getMetaObjectByName(EntityType.PAGE, namespace, objectName, null, token);
    boolean isNew = false;
    if (p == null)
    {
      assert (!cor.booleanValue());
      p = new MetaInfo.Page();
      MetaInfo.Namespace ns = (MetaInfo.Namespace)this.metadataRepository.getMetaObjectByName(EntityType.NAMESPACE, "Global", namespace, null, token);
      p.construct(objectName, ns, EntityType.PAGE);
      isNew = true;
    }
    else
    {
      assert (cor.booleanValue() == true);
      isNew = false;
    }
    if (data.has("title"))
    {
      JsonNode titleNode = data.get("title");
      if (titleNode != null) {
        p.setTitle(titleNode.asText(" "));
      }
    }
    if (data.has("queryVisualizations"))
    {
      ArrayNode visualizationsNode = (ArrayNode)data.get("queryVisualizations");
      if (visualizationsNode != null)
      {
        assert (visualizationsNode.isArray() == true);
        Iterator<JsonNode> itr = visualizationsNode.elements();
        List<String> allVisualizations = new ArrayList();
        while (itr.hasNext()) {
          allVisualizations.add(((JsonNode)itr.next()).textValue());
        }
        p.setQueryVisualizations(allVisualizations);
      }
    }
    if (data.has("gridJSON"))
    {
      JsonNode gridJSONNode = data.get("gridJSON");
      if (gridJSONNode != null) {
        p.setGridJSON(gridJSONNode.asText());
      }
    }
    if (isNew) {
      this.metadataRepository.putMetaObject(p, token);
    } else {
      this.metadataRepository.updateMetaObject(p, token);
    }
  }
  
  public void CreateQueryVisualization(AuthToken token, String objectName, String namespace, Boolean cor, ObjectNode data)
    throws MetaDataRepositoryException, JSONException
  {
    MetaInfo.QueryVisualization uic = (MetaInfo.QueryVisualization)this.metadataRepository.getMetaObjectByName(EntityType.QUERYVISUALIZATION, namespace, objectName, null, token);
    boolean isNew = false;
    if (uic == null)
    {
      uic = new MetaInfo.QueryVisualization();
      MetaInfo.Namespace ns = (MetaInfo.Namespace)this.metadataRepository.getMetaObjectByName(EntityType.NAMESPACE, "Global", namespace, null, token);
      uic.construct(objectName, ns, EntityType.QUERYVISUALIZATION);
      isNew = true;
    }
    if (data.has("title"))
    {
      JsonNode titleNode = data.get("title");
      if (titleNode != null) {
        uic.setTitle(titleNode.asText());
      }
    }
    if (data.has("query"))
    {
      JsonNode queryNode = data.get("query");
      String query = null;
      if (queryNode.has("name"))
      {
        String nsName = "";
        if (queryNode.has("nsName")) {
          nsName = queryNode.get("nsName").asText();
        }
        if ((nsName != null) && (!nsName.isEmpty())) {
          query = nsName + "." + queryNode.get("name").asText();
        } else {
          query = queryNode.get("name").asText();
        }
      }
      if (query == null) {
        query = queryNode.asText();
      }
      if (query != null) {
        uic.setQuery(query);
      }
    }
    if (data.has("visualizationType"))
    {
      JsonNode visualizationTypeNode = data.get("visualizationType");
      if (visualizationTypeNode != null) {
        uic.setVisualizationType(visualizationTypeNode.asText());
      }
    }
    if (data.has("config"))
    {
      JsonNode configNode = data.get("config");
      if (configNode != null) {
        uic.setConfig(configNode.toString());
      }
    }
    if (isNew) {
      this.metadataRepository.putMetaObject(uic, token);
    } else {
      this.metadataRepository.updateMetaObject(uic, token);
    }
  }
  
  public ObjectNode[] CRUDHandler(AuthToken token, String appOrFlowName, ClientOperations.CRUD operation, ObjectNode node)
    throws Exception
  {
    if (token == null) {
      throw new SecurityException("Can't process a request without an authentication token");
    }
    if (operation == null) {
      throw new InvalidPropertiesFormatException("Can't process a request without knowing the kind of operation to be executed");
    }
    JsonNode namespaceNode = node.get("nsName");
    String namespace = namespaceNode != null ? namespaceNode.asText(" ") : null;
    
    JsonNode entityNode = node.get("type");
    EntityType entityType = entityNode != null ? EntityType.valueOf(entityNode.asText(" ")) : null;
    if (entityType == null) {
      throw new InvalidPropertiesFormatException("Entity Type can't be null");
    }
    if ((entityType != null) && (entityType.equals(EntityType.UNKNOWN))) {
      throw new InvalidPropertiesFormatException(entityType + " is invalid Entity Type");
    }
    JsonNode name = node.get("name");
    StringBuilder stringToValidate = new StringBuilder();
    if (name != null)
    {
      if (StringUtils.isNotBlank(namespace)) {
        stringToValidate.append(namespace).append(".");
      }
      stringToValidate.append(name.asText());
      if (!isValidName(stringToValidate.toString())) {
        throw new InvalidPropertiesFormatException(stringToValidate.toString() + " is invalid name");
      }
    }
    if (logger.isInfoEnabled()) {
      logger.info("Operation: " + operation + ", Method Params: " + node.toString());
    }
    return this.clientOperations.CRUDWorker(this, token, namespace, appOrFlowName, entityType, operation, node);
  }
  
  public ObjectNode actionHandler(String entityType_string, AuthToken token)
    throws MetaDataRepositoryException, JsonProcessingException, SecurityException
  {
    MDConstants.checkNullParams("Entity Type and Auth token can't be NULL, EntityType: ".concat(entityType_string).concat(", Token: ").concat(token.toString()), new Object[] { entityType_string, token });
    if (WASecurityManager.get().isAuthenticated(token))
    {
      EntityType entityType = EntityType.valueOf(entityType_string);
      return MetaInfo.getActions(entityType);
    }
    throw new SecurityException("Unauthorized access to action handler");
  }
  
  public ObjectNode[] updateApplicationSettings(AuthToken token, UUID app_uuid, ObjectNode data)
    throws Exception
  {
    MetaInfo.Flow app_metaObject = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByUUID(app_uuid, token);
    if (app_metaObject != null)
    {
      List<Property> eHandlersList = Lists.newArrayList();
      JsonNode eHandlers = data.get("eHandlers");
      if (eHandlers != null) {
        for (ExceptionType exceptionType : ExceptionType.values())
        {
          String exceptionName = exceptionType.name();
          JsonNode handlerValue = eHandlers.get(exceptionName);
          if (handlerValue != null) {
            eHandlersList.add(new Property(exceptionName, handlerValue.asText()));
          }
        }
      }
      ExceptionHandler exceptionHandler = new ExceptionHandler(eHandlersList);
      
      Boolean encrypt = Boolean.valueOf(false);
      if (data.get("encrypted") != null) {
        encrypt = Boolean.valueOf(data.get("encrypted").asBoolean());
      }
      JsonNode recoveryType = data.get("recoveryType");
      JsonNode recoveryPeriod = data.get("recoveryPeriod");
      int recovType = 0;long recovPeriod = 0L;
      if (recoveryType != null) {
        recovType = recoveryType.asInt();
      }
      if (recoveryPeriod != null) {
        recovPeriod = recoveryPeriod.asLong();
      }
      JsonNode importStatements = data.get("importStatements");
      Set<String> importStatementsArray = null;
      if (importStatements != null)
      {
        assert (importStatements.isArray());
        importStatementsArray = ClientCreateOperation.arrayNodeToSet(importStatements);
      }
      app_metaObject.setEncrypted(encrypt.booleanValue());
      app_metaObject.setImportStatements(importStatementsArray);
      app_metaObject.setRecoveryPeriod(recovPeriod);
      try
      {
        Context temp_ctx = new Context(token);
        JsonNode namespaceNode = data.get("nsName");
        String namespace = namespaceNode != null ? namespaceNode.asText(" ") : null;
        if (namespace == null) {
          throw new InvalidPropertiesFormatException("Namespace can't be null while updating app settings");
        }
        Compiler temp_compiler = new Compiler(null, temp_ctx);
        temp_ctx.useNamespace(namespace);
        setUpdateMode(true);
        temp_ctx.setCurApp(app_metaObject);
        
        Map<String, Object> ekseptionHandler = temp_compiler.combineProperties(exceptionHandler.props);
        app_metaObject.setEhandlers(ekseptionHandler);
        
        this.metadataRepository.updateMetaObject(app_metaObject, token);
      }
      catch (Exception e)
      {
        logger.error("Unexpected error: ", e);
      }
      return this.clientOperations.formatResult(app_metaObject);
    }
    throw new MetaDataRepositoryException("No application found for ID: " + app_uuid);
  }
  
  public void LoadDataComponent(AuthToken authToken, MetaInfo.Flow application, DeploymentRule deploymentRule)
    throws MetaDataRepositoryException
  {
    createOrGetContext(authToken);
    compile(authToken, AST.CreateDeployStmt(EntityType.APPLICATION, deploymentRule, new ArrayList(), new ArrayList()));
    compile(authToken, AST.CreateStartStmt(application.getFullName(), EntityType.APPLICATION, null));
  }
  
  public void UnloadDataComponent(AuthToken authToken, Flow application, MetaInfo.MetaObject obj)
    throws MetaDataRepositoryException
  {
    createOrGetContext(authToken);
    compile(authToken, AST.CreateStopStmt(application.getMetaFullName(), EntityType.APPLICATION));
    compile(authToken, AST.CreateUndeployStmt(application.getMetaFullName(), EntityType.APPLICATION));
    this.metadataRepository.removeMetaObjectByUUID(application.getMetaID(), authToken);
    obj.getReverseIndexObjectDependencies().remove(application.getMetaID());
    this.metadataRepository.updateMetaObject(obj, authToken);
  }
  
  private Context createOrGetContext(AuthToken authToken)
  {
    if (this.contexts.containsKey(authToken)) {
      return (Context)this.contexts.get(authToken);
    }
    return (Context)this.contexts.put(authToken, new Context(authToken));
  }
  
  public Map<String, Boolean> getAllowedPermissions(AuthToken token, String userid, List<String> list)
    throws Exception
  {
    Map<String, Boolean> map = new HashMap();
    MetaInfo.User user = WASecurityManager.get().getUser(userid);
    if (!PermissionUtility.checkReadPermission(user, token, WASecurityManager.get())) {
      throw new SecurityException("Current user can't read :" + userid);
    }
    if ((list == null) || (list.isEmpty())) {
      return map;
    }
    for (String str : list)
    {
      boolean bool = WASecurityManager.get().userAllowed(user, new ObjectPermission(str));
      map.put(str, new Boolean(bool));
    }
    return map;
  }
  
  public List<String> getPermissionObjectTypeList()
  {
    List<String> vals = new ArrayList();
    for (ObjectPermission.ObjectType type : ObjectPermission.ObjectType.values()) {
      vals.add(type.name());
    }
    return vals;
  }
  
  public List<String> getPermissionActionList()
  {
    List<String> vals = new ArrayList();
    for (ObjectPermission.Action type : ObjectPermission.Action.values()) {
      vals.add(type.name());
    }
    return vals;
  }
  
  public List<String> getPermissionTypeList()
  {
    List<String> vals = new ArrayList();
    for (ObjectPermission.PermissionType type : ObjectPermission.PermissionType.values()) {
      vals.add(type.name());
    }
    return vals;
  }
  
  public static boolean isReservedKeyword(String text)
  {
    return Lexer.isKeyword(text);
  }
  
  public static boolean isValidName(String name)
    throws IOException
  {
    Lexer l = new Lexer(name);
    l.setAcceptNewLineInStringLiteral(false);
    boolean seenFirstIdentifier = false;
    boolean seenDot = false;
    isValid = false;
    try
    {
      for (;;)
      {
        Symbol s = l.nextToken();
        if (s.sym == 213)
        {
          if (isReservedKeyword(s.value.toString()))
          {
            isValid = false;
            break;
          }
          if (!seenFirstIdentifier) {
            seenFirstIdentifier = true;
          }
          if ((seenFirstIdentifier == true) && (seenDot == true))
          {
            isValid = true;
            break;
          }
        }
        else if (s.sym == 191)
        {
          if (!seenFirstIdentifier)
          {
            isValid = false;
            break;
          }
          seenDot = true;
        }
        else
        {
          isValid = false;
          break;
        }
      }
      return isValid;
    }
    catch (RuntimeException|IOException e)
    {
      isValid = false;
      if (l.nextToken().sym != 0) {
        return false;
      }
    }
  }
  
  public void AlterStreamStatement(AuthToken requestToken, String objectName, Boolean partitionEnable, String[] streamPartitionFields, Boolean persistEnable, StreamPersistencePolicy spp)
    throws MetaDataRepositoryException
  {
    Stmt stmt = AST.CreateAlterStmt(objectName, partitionEnable, Arrays.asList(streamPartitionFields), persistEnable, spp);
    if (logger.isInfoEnabled()) {
      logger.info("AlterStreamStatement is called - with arguments " + objectName + " , " + partitionEnable + " , " + Arrays.toString(streamPartitionFields) + " , " + persistEnable + " , " + spp);
    }
    compile(requestToken, stmt);
  }
}
