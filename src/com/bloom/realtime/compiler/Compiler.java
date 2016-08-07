package com.bloom.runtime.compiler;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.bloom.anno.AdapterType;
import com.bloom.anno.NotSet;
import com.bloom.appmanager.ApplicationStatusResponse;
import com.bloom.appmanager.ChangeApplicationStateResponse;
import com.bloom.appmanager.FlowUtil;
import com.bloom.classloading.WALoader;
import com.bloom.drop.DropMetaObject.DropRule;
import com.bloom.drop.DropMetaObject.DropSource;
import com.bloom.drop.DropMetaObject.DropStream;
import com.bloom.drop.DropMetaObject.DropTarget;
import com.bloom.event.SimpleEvent;
import com.bloom.exception.AlterException;
import com.bloom.exception.CompilationException;
import com.bloom.exception.FatalException;
import com.bloom.exception.SecurityException;
import com.bloom.exception.Warning;
import com.bloom.exceptionhandling.ExceptionType;
import com.bloom.intf.AuthLayer;
import com.bloom.kafkamessaging.StreamPersistencePolicy;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.metaRepository.PermissionUtility;
import com.bloom.metaRepository.RemoteCall;
import com.bloom.runtime.ActionType;
import com.bloom.runtime.Context;
import com.bloom.runtime.Context.Result;
import com.bloom.runtime.DistributedExecutionManager;
import com.bloom.runtime.ExceptionEvent;
import com.bloom.runtime.Interval;
import com.bloom.runtime.KafkaStreamUtils;
import com.bloom.runtime.Pair;
import com.bloom.runtime.Property;
import com.bloom.runtime.TraceOptions;
import com.bloom.runtime.WactionStorePersistencePolicy;
import com.bloom.runtime.compiler.custom.AggHandlerDesc;
import com.bloom.runtime.compiler.select.RSFieldDesc;
import com.bloom.runtime.compiler.select.SelectCompiler;
import com.bloom.runtime.compiler.stmts.ActionStmt;
import com.bloom.runtime.compiler.stmts.AdapterDescription;
import com.bloom.runtime.compiler.stmts.AlterDeploymentGroupStmt;
import com.bloom.runtime.compiler.stmts.AlterStmt;
import com.bloom.runtime.compiler.stmts.ConnectStmt;
import com.bloom.runtime.compiler.stmts.CreateAdHocSelectStmt;
import com.bloom.runtime.compiler.stmts.CreateAppOrFlowStatement;
import com.bloom.runtime.compiler.stmts.CreateCacheStmt;
import com.bloom.runtime.compiler.stmts.CreateCqStmt;
import com.bloom.runtime.compiler.stmts.CreateDashboardStatement;
import com.bloom.runtime.compiler.stmts.CreateDeploymentGroupStmt;
import com.bloom.runtime.compiler.stmts.CreateNamespaceStatement;
import com.bloom.runtime.compiler.stmts.CreatePropertySetStmt;
import com.bloom.runtime.compiler.stmts.CreatePropertyVariableStmt;
import com.bloom.runtime.compiler.stmts.CreateRoleStmt;
import com.bloom.runtime.compiler.stmts.CreateShowStreamStmt;
import com.bloom.runtime.compiler.stmts.CreateSorterStmt;
import com.bloom.runtime.compiler.stmts.CreateSourceOrTargetStmt;
import com.bloom.runtime.compiler.stmts.CreateStreamStmt;
import com.bloom.runtime.compiler.stmts.CreateTypeStmt;
import com.bloom.runtime.compiler.stmts.CreateUserStmt;
import com.bloom.runtime.compiler.stmts.CreateVisualizationStmt;
import com.bloom.runtime.compiler.stmts.CreateWASStmt;
import com.bloom.runtime.compiler.stmts.CreateWindowStmt;
import com.bloom.runtime.compiler.stmts.DeployStmt;
import com.bloom.runtime.compiler.stmts.DeploymentRule;
import com.bloom.runtime.compiler.stmts.DropStmt;
import com.bloom.runtime.compiler.stmts.EndBlockStmt;
import com.bloom.runtime.compiler.stmts.EventType;
import com.bloom.runtime.compiler.stmts.ExceptionHandler;
import com.bloom.runtime.compiler.stmts.ExecPreparedStmt;
import com.bloom.runtime.compiler.stmts.ExportAppStmt;
import com.bloom.runtime.compiler.stmts.ExportDataStmt;
import com.bloom.runtime.compiler.stmts.ExportStreamSchemaStmt;
import com.bloom.runtime.compiler.stmts.GrantPermissionToStmt;
import com.bloom.runtime.compiler.stmts.GrantRoleToStmt;
import com.bloom.runtime.compiler.stmts.ImportDataStmt;
import com.bloom.runtime.compiler.stmts.ImportStmt;
import com.bloom.runtime.compiler.stmts.LoadFileStmt;
import com.bloom.runtime.compiler.stmts.LoadUnloadJarStmt;
import com.bloom.runtime.compiler.stmts.MonitorStmt;
import com.bloom.runtime.compiler.stmts.RevokePermissionFromStmt;
import com.bloom.runtime.compiler.stmts.RevokeRoleFromStmt;
import com.bloom.runtime.compiler.stmts.SecurityStmt;
import com.bloom.runtime.compiler.stmts.Select;
import com.bloom.runtime.compiler.stmts.SetStmt;
import com.bloom.runtime.compiler.stmts.SorterInOutRule;
import com.bloom.runtime.compiler.stmts.Stmt;
import com.bloom.runtime.compiler.stmts.UpdateUserInfoStmt;
import com.bloom.runtime.compiler.stmts.UseStmt;
import com.bloom.runtime.compiler.stmts.UserProperty;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.components.MetaObjectPermissionChecker;
import com.bloom.runtime.meta.CQExecutionPlan;
import com.bloom.runtime.meta.CQExecutionPlan.CQExecutionPlanFactory;
import com.bloom.runtime.meta.CQExecutionPlan.DataSource;
import com.bloom.runtime.meta.IntervalPolicy;
import com.bloom.runtime.meta.IntervalPolicy.AttrBasedPolicy;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.CQ;
import com.bloom.runtime.meta.MetaInfo.Cache;
import com.bloom.runtime.meta.MetaInfo.DeploymentGroup;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.meta.MetaInfo.Flow.Detail;
import com.bloom.runtime.meta.MetaInfo.Flow.Detail.FailOverRule;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Namespace;
import com.bloom.runtime.meta.MetaInfo.PropertyDef;
import com.bloom.runtime.meta.MetaInfo.PropertySet;
import com.bloom.runtime.meta.MetaInfo.PropertyTemplateInfo;
import com.bloom.runtime.meta.MetaInfo.PropertyVariable;
import com.bloom.runtime.meta.MetaInfo.Query;
import com.bloom.runtime.meta.MetaInfo.Role;
import com.bloom.runtime.meta.MetaInfo.ShowStream;
import com.bloom.runtime.meta.MetaInfo.Sorter.SorterRule;
import com.bloom.runtime.meta.MetaInfo.Source;
import com.bloom.runtime.meta.MetaInfo.StatusInfo.Status;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.meta.MetaInfo.Target;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.meta.MetaInfo.User;
import com.bloom.runtime.meta.MetaInfo.Visualization;
import com.bloom.runtime.meta.MetaInfo.WActionStore;
import com.bloom.runtime.meta.MetaInfo.Window;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.utils.Factory;
import com.bloom.runtime.utils.NameHelper;
import com.bloom.runtime.utils.NamePolicy;
import com.bloom.runtime.utils.RuntimeUtils;
import com.bloom.security.LDAPAuthLayer;
import com.bloom.security.ObjectPermission.Action;
import com.bloom.security.ObjectPermission.ObjectType;
import com.bloom.security.Password;
import com.bloom.security.WASecurityManager;
import com.bloom.sourcefiltering.SourceSideFilteringHandler;
import com.bloom.sourcefiltering.SourceSideFilteringManager;
import com.bloom.tungsten.CluiMonitorView;
import com.bloom.tungsten.Tungsten;
import com.bloom.tungsten.Tungsten.PrintFormat;
import com.bloom.utility.Utility;
import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.Attributes.Name;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.commons.collections.map.HashedMap;
import org.apache.log4j.Logger;

public class Compiler
{
  private static Logger logger = Logger.getLogger(Compiler.class);
  private final Grammar parser;
  private final Context ctx;
  MDRepository metadata_repo = MetadataRepository.getINSTANCE();
  public static final String NAMED_QUERY_PREFIX = "namedQuery" + java.util.UUID.nameUUIDFromBytes("namedQuery".getBytes()).toString().split("-")[0];
  
  public Compiler(Grammar p, Context ctx)
  {
    this.parser = p;
    this.ctx = ctx;
  }
  
  public void error(String message, Object info)
  {
    if (this.parser != null) {
      this.parser.parseError(message, info);
    } else {
      throw new CompilationException(message);
    }
  }
  
  public String getExprText(Object o)
  {
    return this.parser == null ? "" : this.parser.lex.getExprText(o);
  }
  
  public Object compileStmt(Stmt s)
    throws MetaDataRepositoryException
  {
    return s.compile(this);
  }
  
  public static void main(String[] args)
    throws Exception
  {
    compile(AST.emptyStmt(), null, new ExecutionCallback()
    {
      public void execute(Stmt stmt, Compiler compiler)
        throws Exception
      {
        System.out.println(stmt.getClass());
      }
    });
  }
  
  public static void compile(String tqlText, Context context, ExecutionCallback cb)
    throws Exception
  {
    Lexer lexer = new Lexer(tqlText);
    Grammar parser = new Grammar(lexer, lexer.getSymbolFactory());
    
    List<Stmt> stmts = parser.parseStmt(false);
    Compiler compiler = new Compiler(parser, context);
    for (Stmt stmt : stmts) {
      cb.execute(stmt, compiler);
    }
  }
  
  public static void compile(Stmt stmt, Context context, ExecutionCallback cb)
    throws Exception
  {
    cb.execute(stmt, new Compiler(null, context));
  }
  
  public static void compile(String tqlText, Context context)
    throws Exception
  {
    compile(tqlText, context, new ExecutionCallback()
    {
      public void execute(Stmt stmt, Compiler compiler)
        throws MetaDataRepositoryException
      {
        if (Compiler.logger.isDebugEnabled()) {
          Compiler.logger.debug("parsed:\n" + stmt);
        }
        compiler.compileStmt(stmt);
      }
    });
  }
  
  public Context getContext()
  {
    return this.ctx;
  }
  
  public Class<?> getClass(TypeName typename)
  {
    try
    {
      String typeName = typename.name.toString();
      try
      {
        MetaInfo.Type t = getContext().getType(typeName);
        if ((t != null) && 
          (t.className != null)) {
          typeName = t.className;
        }
      }
      catch (MetaDataRepositoryException e) {}
      Class<?> c;
      Class<?> c;
      if (typename.array_dimensions > 0) {
        c = this.ctx.getClassWithoutReplacingPrimitives(typeName);
      } else {
        c = this.ctx.getClass(typeName);
      }
      for (int i = 0; i < typename.array_dimensions; i++) {
        c = CompilerUtils.toArrayType(c);
      }
      return c;
    }
    catch (ClassNotFoundException e)
    {
      error("no such type", typename.name);
    }
    return null;
  }
  
  public MetaInfo.Type createAnonType(UUID typeid, Class<?> klass)
  {
    MetaInfo.Type type = new MetaInfo.Type();
    type.construct(RuntimeUtils.genRandomName("type"), this.ctx.getCurNamespace(), klass.getName(), null, null, false);
    
    type.getMetaInfoStatus().setAnonymous(true);
    return type;
  }
  
  public Object compileCreateStreamStmt(CreateStreamStmt stmt)
    throws MetaDataRepositoryException
  {
    UUID typeid;
    UUID typeid;
    if (stmt.typeName != null)
    {
      assert (stmt.fields == null);
      MetaInfo.MetaObject type = this.ctx.getTypeInCurSchema(stmt.typeName);
      if (type == null) {
        try
        {
          Class<?> clazz = WALoader.get().loadClass(stmt.typeName);
          typeid = getTypeForCLass(clazz).uuid;
        }
        catch (ClassNotFoundException e)
        {
          error("no such type declaration", stmt.typeName);
          UUID typeid = null;
        }
      } else {
        typeid = type.uuid;
      }
    }
    else
    {
      assert (stmt.fields != null);
      assert (stmt.typeName == null);
      typeid = createType(false, RuntimeUtils.genRandomName("type"), stmt.fields, true);
    }
    checkPartitionFields(stmt.partitioning_fields, typeid, stmt.name);
    
    MetaInfo.Type typeObject = (MetaInfo.Type)this.ctx.getObject(typeid);
    MetaInfo.Stream streamObject;
    if (stmt.spp.getFullyQualifiedNameOfPropertyset() != null)
    {
      String namespace = Utility.splitDomain(stmt.spp.getFullyQualifiedNameOfPropertyset());
      if (namespace == null) {
        namespace = this.ctx.getCurNamespaceName();
      }
      String name = Utility.splitName(stmt.spp.getFullyQualifiedNameOfPropertyset());
      MetaInfo.PropertySet kpset = (MetaInfo.PropertySet)this.metadata_repo.getMetaObjectByName(EntityType.PROPERTYSET, namespace, name, null, WASecurityManager.TOKEN);
      if (kpset == null) {
        throw new CompilationException("Couldn't find property set : " + namespace.concat(".").concat(name));
      }
      streamObject = this.ctx.putStream(stmt.doReplace, makeObjectName(stmt.name), typeid, stmt.partitioning_fields, stmt.gracePeriod, kpset.getFullName(), null, null);
    }
    else
    {
      streamObject = this.ctx.putStream(stmt.doReplace, makeObjectName(stmt.name), typeid, stmt.partitioning_fields, stmt.gracePeriod, null, null, null);
    }
    setupKafka(streamObject, true);
    if (logger.isDebugEnabled()) {
      logger.debug("Added dependency: { from type " + typeObject.name + " => stream " + streamObject.name + " } ");
    }
    addDependency(typeObject, streamObject);
    addSourceTextToMetaObject(stmt, streamObject);
    addCurrentApplicationFlowDependencies(typeObject);
    addCurrentApplicationFlowDependencies(streamObject);
    return streamObject;
  }
  
  private void setupKafka(MetaInfo.Stream streamObject, boolean doDelete)
    throws MetaDataRepositoryException
  {
    if ((streamObject != null) && (streamObject.pset != null)) {
      try
      {
        boolean did_create;
        boolean did_create;
        if (HazelcastSingleton.isClientMember())
        {
          RemoteCall createTopic_executor = KafkaStreamUtils.getCreateTopicExecutor(streamObject);
          did_create = ((Boolean)DistributedExecutionManager.execOnAny(HazelcastSingleton.get(), createTopic_executor)).booleanValue();
        }
        else
        {
          did_create = KafkaStreamUtils.createTopic(streamObject);
        }
        if (did_create)
        {
          if (logger.isInfoEnabled()) {
            logger.info("Created kafka topic with name: " + streamObject.getFullName());
          }
          MetaInfo.PropertySet kafka_propset = KafkaStreamUtils.getPropertySet(streamObject);
          if (kafka_propset == null) {
            throw new Exception("PropertySet " + streamObject.pset + " does not exist. Please create the propertySet and try again.");
          }
          streamObject.setPropertySet(kafka_propset);
          String format = (String)kafka_propset.properties.get("dataformat");
          if ((format != null) && (format.equalsIgnoreCase("avro")))
          {
            String schema = getSchema(format, streamObject);
            streamObject.setAvroSchema(schema);
          }
          this.ctx.updateMetaObject(streamObject);
        }
        else
        {
          logger.error("Request to create kafka topic for stream: " + streamObject.getFullName() + " was successful but failed to verify its existence from zookeeper");
        }
      }
      catch (Exception e)
      {
        if (doDelete) {
          this.ctx.removeObject(streamObject);
        }
        throw new RuntimeException(e);
      }
    }
  }
  
  private String getSchema(String format, MetaInfo.Stream streamInfo)
    throws Exception
  {
    if (format.equalsIgnoreCase("avro"))
    {
      Map<String, Object> avroFormatter_Properties = new HashMap();
      String schemFileName = streamInfo.getFullName().concat("_schema.avsc");
      avroFormatter_Properties.put("schemaFileName", schemFileName);
      UUID type_uuid = streamInfo.getDataType();
      MetaInfo.Type type = (MetaInfo.Type)this.metadata_repo.getMetaObjectByUUID(type_uuid, WASecurityManager.TOKEN);
      avroFormatter_Properties.put("TypeName", type.getName());
      Class<?> typeClass = WALoader.get().loadClass(type.className);
      avroFormatter_Properties.put("EventType", "ContainerEvent");
      Field[] fields = typeClass.getDeclaredFields();
      Field[] typedEventFields = new Field[fields.length - 1];
      int i = 0;
      for (Field field : fields) {
        if ((java.lang.reflect.Modifier.isPublic(field.getModifiers())) && 
          (!"mapper".equals(field.getName())))
        {
          typedEventFields[i] = field;
          i++;
        }
      }
      fields = typedEventFields;
      
      String formatterClassName = "com.bloom.proc.AvroFormatter";
      Class<?> formatterClass = Class.forName(formatterClassName);
      formatterClass.getConstructor(new Class[] { Map.class, Field[].class }).newInstance(new Object[] { avroFormatter_Properties, fields });
      
      File schemaFile = new File(schemFileName);
      if ((schemaFile.exists()) && (!schemaFile.isDirectory()))
      {
        Schema schema = new Schema.Parser().parse(schemaFile);
        String schemaString = schema.toString();
        schemaFile.delete();
        return schemaString;
      }
      throw new Exception("Avro Schema file by name " + schemaFile + " not found!");
    }
    return null;
  }
  
  public Object exportStreamSchema(ExportStreamSchemaStmt exportStreamSchemaStmt)
    throws MetaDataRepositoryException
  {
    String[] fullName = this.ctx.splitNamespaceAndName(exportStreamSchemaStmt.streamName, EntityType.STREAM);
    MetaInfo.Stream streamMetaObject = (MetaInfo.Stream)this.metadata_repo.getMetaObjectByName(EntityType.STREAM, fullName[0], fullName[1], null, WASecurityManager.TOKEN);
    if (streamMetaObject == null) {
      throw new RuntimeException("Stream " + exportStreamSchemaStmt.streamName + " not found!");
    }
    if (streamMetaObject.avroSchema == null) {
      throw new RuntimeException("No schema found for Stream " + streamMetaObject.getFullName());
    }
    StringBuilder schemaFileNameBuilder = new StringBuilder();
    if (exportStreamSchemaStmt.optPath != null) {
      schemaFileNameBuilder.append(exportStreamSchemaStmt.optPath);
    } else {
      schemaFileNameBuilder.append(".");
    }
    if (exportStreamSchemaStmt.optFileName != null) {
      schemaFileNameBuilder.append("/").append(exportStreamSchemaStmt.optFileName).append(".avsc");
    } else {
      schemaFileNameBuilder.append("/").append(streamMetaObject.getNsName()).append("_").append(streamMetaObject.getName()).append("_schema.avsc");
    }
    try
    {
      FileWriter fileWriter = new FileWriter(schemaFileNameBuilder.toString());
      fileWriter.write(streamMetaObject.avroSchema);
      fileWriter.close();
      return "Wrote schema for " + streamMetaObject.getFullName() + " to " + schemaFileNameBuilder.toString();
    }
    catch (IOException e)
    {
      throw new RuntimeException(e.getMessage());
    }
  }
  
  private UUID createType(boolean doReplace, String typeName, List<TypeField> fields, boolean setAnonymous)
    throws MetaDataRepositoryException
  {
    return createType(doReplace, typeName, null, fields, setAnonymous);
  }
  
  private UUID createType(boolean doReplace, String typeName, String extendsType, List<TypeField> fields, boolean setAnonymous)
    throws MetaDataRepositoryException
  {
    Set<String> names = Factory.makeNameSet();
    Map<String, String> fieldMap = Factory.makeLinkedMap();
    List<String> keyFields = new ArrayList();
    for (TypeField tf : fields)
    {
      String fieldName = tf.fieldName;
      if (names.contains(fieldName))
      {
        error("duplicated field name", fieldName);
      }
      else
      {
        names.add(fieldName);
        Class<?> c = getClass(tf.fieldType);
        fieldMap.put(fieldName, c.getName());
        if (tf.isPartOfKey) {
          keyFields.add(fieldName);
        }
      }
    }
    MetaInfo.Type type = this.ctx.putType(doReplace, makeObjectName(typeName), extendsType, fieldMap, keyFields, setAnonymous);
    addCurrentApplicationFlowDependencies(type);
    
    return type.uuid;
  }
  
  public UUID compileCreateTypeStmt(CreateTypeStmt stmt)
    throws MetaDataRepositoryException
  {
    if ((stmt.typeDef.typeName != null) && (stmt.typeDef.typeDef == null)) {
      try
      {
        Class<?> clazz = WALoader.get().loadClass(stmt.typeDef.typeName);
        return getTypeForCLass(clazz).uuid;
      }
      catch (ClassNotFoundException e)
      {
        error("external class could not be found", stmt.typeDef.typeName);
        return null;
      }
    }
    UUID uuid = createType(stmt.doReplace, stmt.name, stmt.typeDef.extendsTypeName, stmt.typeDef.typeDef, false);
    MetaInfo.MetaObject type = this.ctx.getObject(uuid);
    addSourceTextToMetaObject(stmt, type);
    return uuid;
  }
  
  public List<Field> checkPartitionFields(List<String> partitionFields, UUID typeId, String streamName)
    throws MetaDataRepositoryException
  {
    List<Field> ret = new ArrayList();
    Class<?> streamType;
    Set<String> dups;
    if (partitionFields != null)
    {
      streamType = getTypeClass(typeId);
      dups = Factory.makeNameSet();
      for (String fieldName : partitionFields) {
        if (dups.contains(fieldName))
        {
          error("field name is duplicated", fieldName);
        }
        else
        {
          dups.add(fieldName);
          try
          {
            Field fld = NamePolicy.getField(streamType, fieldName);
            ret.add(fld);
          }
          catch (NoSuchFieldException e)
          {
            error("stream <" + streamName + "> has no such field", fieldName);
          }
          catch (SecurityException e)
          {
            logger.error(e);
          }
        }
      }
    }
    return ret;
  }
  
  private void validateWindowAttrPolicy(UUID typeid, String attrname)
  {
    MetaInfo.Type typeObj = null;
    try
    {
      typeObj = (MetaInfo.Type)this.ctx.getObject(typeid);
    }
    catch (MetaDataRepositoryException e)
    {
      error("Unable to get type for stream", null);
      return;
    }
    String attrType = (String)typeObj.fields.get(attrname);
    if (attrType == null) {
      error("Unable to find attribute in stream type.", attrname);
    } else if ((attrType.equalsIgnoreCase("java.lang.String")) || (attrType.equalsIgnoreCase("String"))) {
      error("Attribute type is not comparable, hence cannot be used for attribute time", attrname);
    }
  }
  
  public Object compileCreateWindowStmt(CreateWindowStmt stmt)
    throws MetaDataRepositoryException
  {
    MetaInfo.Stream stream = this.ctx.getStreamInCurSchema(stmt.stream_name);
    if (stream == null) {
      error("no such stream", stmt.stream_name);
    }
    Object stats = stmt.getOption("dumpstats");
    Integer dumpstatsVal;
    Integer dumpstatsVal;
    if ((stats instanceof Number))
    {
      dumpstatsVal = Integer.valueOf(((Number)stats).intValue());
    }
    else
    {
      Integer dumpstatsVal;
      if ((stats instanceof String))
      {
        dumpstatsVal = Integer.valueOf((String)stats);
      }
      else
      {
        Integer dumpstatsVal;
        if (stmt.haveOption("dumpstats")) {
          dumpstatsVal = Integer.valueOf(0);
        } else {
          dumpstatsVal = null;
        }
      }
    }
    if (stmt.window_len.first != null)
    {
      IntervalPolicy policy = (IntervalPolicy)stmt.window_len.first;
      if (policy.isAttrBased())
      {
        IntervalPolicy.AttrBasedPolicy attrPolicy = policy.getAttrPolicy();
        validateWindowAttrPolicy(stream.getDataType(), attrPolicy.getAttrName());
      }
    }
    checkPartitionFields(stmt.partitioning_fields, stream.dataType, stmt.stream_name);
    MetaInfo.Window windowObject = this.ctx.putWindow(stmt.doReplace, makeObjectName(stmt.name), stream.uuid, stmt.partitioning_fields, stmt.window_len, stmt.jumping, false, false, dumpstatsVal);
    if (logger.isDebugEnabled()) {
      logger.debug("Added dependency: { from stream " + stream.name + " => window " + windowObject.name + " } ");
    }
    addDependency(stream, windowObject);
    addCurrentApplicationFlowDependencies(stream);
    addCurrentApplicationFlowDependencies(windowObject);
    addSourceTextToMetaObject(stmt, windowObject);
    return null;
  }
  
  public static TraceOptions buildTraceOptions(Stmt stmt)
  {
    int traceFlags = 0;
    Object traceFilename = null;
    Object traceFilePath = null;
    if (stmt != null)
    {
      if (stmt.haveOption("dumpinput")) {
        traceFlags |= 0x1;
      }
      if (stmt.haveOption("dumpinputx")) {
        traceFlags |= 0x21;
      }
      if (stmt.haveOption("dumpoutput")) {
        traceFlags |= 0x2;
      }
      if (stmt.haveOption("dumpcode")) {
        traceFlags |= 0x4;
      }
      if (stmt.haveOption("dumpplan")) {
        traceFlags |= 0x8;
      }
      if (stmt.haveOption("dumpproc")) {
        traceFlags |= 0x10;
      }
      if (stmt.haveOption("DEBUGINFO")) {
        traceFlags |= 0x40;
      }
      traceFilename = stmt.getOption("dumptofile");
      traceFilePath = stmt.getOption("dumpcode");
    }
    TraceOptions traceOptions = new TraceOptions(traceFlags, (String)traceFilename, (String)traceFilePath);
    
    return traceOptions;
  }
  
  private String fixFieldName(String name)
  {
    if (name == null) {
      throw new RuntimeException("Field name is null");
    }
    StringBuilder fixedName = new StringBuilder();
    for (int i = 0; i < name.length(); i++) {
      if (((i == 0) && (Character.isJavaIdentifierStart(name.charAt(i)))) || ((i != 0) && (Character.isJavaIdentifierPart(name.charAt(i))))) {
        fixedName.append(name.charAt(i));
      }
    }
    return fixedName.toString();
  }
  
  private UUID genStreamType(CQExecutionPlan plan, CreateCqStmt stmt)
  {
    List<RSFieldDesc> rfields = plan.resultSetDesc;
    if (rfields.size() == 1)
    {
      Class<?> fldType = ((RSFieldDesc)rfields.get(0)).type;
      if (SimpleEvent.class.isAssignableFrom(fldType)) {
        try
        {
          return getTypeForCLass(fldType).uuid;
        }
        catch (MetaDataRepositoryException e) {}
      }
    }
    Map<String, String> fields = new LinkedHashMap();
    for (RSFieldDesc resultFields : rfields)
    {
      String name = resultFields.name;
      name = fixFieldName(name);
      int count = 2;
      while (fields.get(name) != null) {
        name = name + count++;
      }
      fields.put(name, resultFields.type.getCanonicalName());
    }
    try
    {
      MetaInfoStatus status = new MetaInfoStatus().setAnonymous(false).setGenerated(true);
      
      MetaInfo.Type typeGenerated = this.ctx.putType(true, makeObjectName(this.ctx.getNamespaceName(Utility.splitDomain(stmt.stream_name)), stmt.stream_name + "_Type"), null, fields, null, status);
      
      return typeGenerated.getUuid();
    }
    catch (MetaDataRepositoryException e)
    {
      logger.warn(e.getLocalizedMessage());
    }
    return null;
  }
  
  private MetaInfo.Stream createStreamFromCQ(CreateCqStmt stmt)
    throws Exception
  {
    Select copyOfSelect = stmt.select.copyDeep();
    
    CQExecutionPlan plan = SelectCompiler.compileSelect(this.ctx.getNamespaceName(Utility.splitDomain(stmt.name)) + "." + stmt.name, this.ctx.getNamespaceName(Utility.splitDomain(stmt.name)), this, copyOfSelect, buildTraceOptions(stmt));
    
    UUID streamType = genStreamType(plan, stmt);
    try
    {
      MetaInfoStatus status = new MetaInfoStatus().setAnonymous(true).setGenerated(true);
      
      return this.ctx.putStream(true, makeObjectName(this.ctx.getNamespaceName(Utility.splitDomain(stmt.stream_name)), stmt.stream_name), streamType, stmt.getPartitionFieldList(), null, null, null, status);
    }
    catch (MetaDataRepositoryException e)
    {
      logger.warn(e.getLocalizedMessage());
    }
    return null;
  }
  
  public Object compileCreateCqStmt(CreateCqStmt stmt)
    throws MetaDataRepositoryException
  {
    MetaInfo.MetaObject dataSink = this.ctx.getDataSourceInCurSchema(stmt.stream_name);
    if ((dataSink != null) && ((dataSink instanceof MetaInfo.Stream)) && (dataSink.getMetaInfoStatus().isAnonymous()) && (dataSink.getMetaInfoStatus().isGenerated()))
    {
      if ((stmt.doReplace) && 
        (!this.ctx.recompileMode)) {
        try
        {
          DropMetaObject.DropStream.drop(this.ctx, dataSink, DropMetaObject.DropRule.NONE, this.ctx.getAuthToken());
        }
        catch (Exception e)
        {
          if (logger.isDebugEnabled()) {
            logger.debug(e.getMessage());
          }
        }
      }
      dataSink = null;
    }
    if (dataSink == null) {
      try
      {
        dataSink = createStreamFromCQ(stmt);
      }
      catch (Exception e)
      {
        throw new RuntimeException(e);
      }
    }
    UUID outputTypeId = null;
    if ((dataSink instanceof MetaInfo.Stream)) {
      outputTypeId = ((MetaInfo.Stream)dataSink).dataType;
    } else if ((dataSink instanceof MetaInfo.WActionStore)) {
      outputTypeId = ((MetaInfo.WActionStore)dataSink).contextType;
    } else {
      error("data sink should be stream or wactionstore", stmt.stream_name);
    }
    MetaInfo.Type streamType = getTypeInfo(outputTypeId);
    Class<?> streamClass = getTypeClass(outputTypeId);
    Set<String> fieldSet = Factory.makeNameSet();
    List<Field> targetFields = new ArrayList();
    for (String fieldName : stmt.field_list) {
      if (fieldSet.contains(fieldName))
      {
        error("field name is duplicated", fieldName);
      }
      else
      {
        fieldSet.add(fieldName);
        try
        {
          Field f = NamePolicy.getField(streamClass, fieldName);
          targetFields.add(f);
        }
        catch (NoSuchFieldException|SecurityException e)
        {
          error("no such field in target stream", fieldName);
        }
      }
    }
    Map<String, Integer> targetFieldIndices = new HashMap();
    int index = 0;
    for (String fieldName : streamType.fields.keySet()) {
      targetFieldIndices.put(fieldName, Integer.valueOf(index++));
    }
    MetaInfo.CQ cqObject = null;
    UUID outputStreamId = null;
    try
    {
      CQExecutionPlan plan = SelectCompiler.compileSelectInto(Utility.splitName(stmt.name), this.ctx.getNamespaceName(Utility.splitDomain(stmt.name)), this, stmt.select, buildTraceOptions(stmt), streamClass, targetFields, targetFieldIndices, outputTypeId, false);
      if (this.ctx.isReadOnly()) {
        return null;
      }
      outputStreamId = dataSink.uuid;
      cqObject = this.ctx.putCQ(stmt.doReplace, makeObjectName(stmt.name), outputStreamId, plan, stmt.select_text, stmt.field_list, null);
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
    for (CQExecutionPlan.DataSource ds : cqObject.plan.dataSources)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("Added dependency: { from stream/window/cache " + ds.name + " => cq " + cqObject.name + " } ");
      }
      MetaInfo.MetaObject subTaskMetaObject = this.ctx.getDataSourceInCurSchema(ds.name);
      if (subTaskMetaObject == null)
      {
        UUID dsid = ds.getDataSourceID();
        subTaskMetaObject = this.ctx.getObject(dsid);
      }
      addDependency(subTaskMetaObject, cqObject);
      addCurrentApplicationFlowDependencies(subTaskMetaObject);
    }
    MetaInfo.MetaObject outputStreamMetaObject = this.ctx.getObject(outputStreamId);
    if (logger.isDebugEnabled()) {
      logger.debug("Added dependency: { from stream/wactionstore " + outputStreamMetaObject.name + " => cq " + cqObject.name + " } ");
    }
    addDependency(outputStreamMetaObject, cqObject);
    addCurrentApplicationFlowDependencies(outputStreamMetaObject);
    addCurrentApplicationFlowDependencies(cqObject);
    addSourceTextToMetaObject(stmt, cqObject);
    return cqObject;
  }
  
  private void addCurrentApplicationFlowDependencies(MetaInfo.MetaObject metaObject)
    throws MetaDataRepositoryException
  {
    MetaInfo.Flow currentApp = this.ctx.getCurApp();
    MetaInfo.Flow currentFlow = this.ctx.getCurFlow();
    if (currentApp != null)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("Added dependency: { from " + metaObject.type.toString() + " " + metaObject.name + " => currentApp " + currentApp.name + " } " + currentApp.uuid);
      }
      addDependency(metaObject, currentApp);
    }
    if (currentFlow != null)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("Added dependency: { from " + metaObject.type.toString() + " " + metaObject.name + " => currentFlow " + currentFlow.name + " } " + currentFlow.uuid);
      }
      addDependency(metaObject, currentFlow);
    }
  }
  
  private void addSourceTextToMetaObject(Stmt stmt, MetaInfo.MetaObject metaObject)
    throws MetaDataRepositoryException
  {
    if (logger.isDebugEnabled()) {
      logger.debug("addSourceTextToMetaObject() " + metaObject + " " + stmt.sourceText);
    }
    metaObject.setSourceText(stmt.sourceText);
    this.metadata_repo.updateMetaObject(metaObject, this.ctx.getSessionID());
  }
  
  public Object compileShowStreamStmt(CreateShowStreamStmt stmt)
    throws MetaDataRepositoryException
  {
    MetaInfo.Stream stream = this.ctx.getStreamInCurSchema(stmt.stream_name);
    if (stream == null) {
      error("Unable to find stream", stmt.stream_name);
    }
    UUID session_id = this.ctx.getSessionID();
    
    MetaInfo.ShowStream show_stream = new MetaInfo.ShowStream();
    show_stream.construct(true, stmt.line_count, stream.uuid, stmt.isTungsten, session_id);
    this.ctx.showStreamStmt(show_stream);
    return null;
  }
  
  public MetaInfo.Query compileCreateAdHocSelect(final CreateAdHocSelectStmt stmt)
    throws MetaDataRepositoryException
  {
    String alias = stmt.name;
    final boolean isAdhoc = alias == null;
    if (!isAdhoc)
    {
      MetaInfo.MetaObject obj = this.ctx.get(alias, EntityType.QUERY);
      if (obj != null) {
        throw new CompilationException(obj.type + " with name <" + obj.getFullName() + "> already exists");
      }
    }
    String copyOfAlias = alias;
    final String queryNS = this.ctx.makeObjectName(copyOfAlias).getNamespace();
    alias = this.ctx.makeObjectName(copyOfAlias).getName();
    String streamName;
    String queryName;
    String appName;
    final String cqName;
    String streamName;
    if (!isAdhoc)
    {
      String queryName = alias;
      String appName = NAMED_QUERY_PREFIX + alias;
      String cqName = NAMED_QUERY_PREFIX + alias;
      streamName = NAMED_QUERY_PREFIX + alias;
    }
    else
    {
      queryName = RuntimeUtils.genRandomName("adhocquery");
      appName = queryName;
      cqName = RuntimeUtils.genRandomName("cq");
      streamName = RuntimeUtils.genRandomName("stream");
    }
    try
    {
      CQExecutionPlan.CQExecutionPlanFactory planFactory = new CQExecutionPlan.CQExecutionPlanFactory()
      {
        public CQExecutionPlan createPlan()
          throws Exception
        {
          CQExecutionPlan plan = SelectCompiler.compileSelect(cqName, queryNS, Compiler.this, stmt.select, Compiler.buildTraceOptions(stmt), isAdhoc);
          
          return plan;
        }
      };
      return this.ctx.buildAdHocQuery(appName, streamName, cqName, queryName, planFactory, stmt.select_text, stmt.sourceText, isAdhoc, null, queryNS);
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }
  
  public Object compileDropStmt(DropStmt stmt)
    throws MetaDataRepositoryException
  {
    if (stmt.objectName.split("\\.").length > 2) {
      error(stmt.objectName + " format is wrong", stmt.objectName);
    }
    List<String> str = this.ctx.dropObject(stmt.objectName, stmt.objectType, stmt.dropRule);
    stmt.returnText = str;
    return null;
  }
  
  private Class<?> loadClass(String className)
  {
    try
    {
      return this.ctx.loadClass(className);
    }
    catch (ClassNotFoundException e) {}
    return null;
  }
  
  private static boolean canMethodBeImported(Method m)
  {
    if (javassist.Modifier.isStatic(m.getModifiers())) {
      return true;
    }
    if ((javassist.Modifier.isAbstract(m.getModifiers())) && (m.getAnnotation(AggHandlerDesc.class) != null)) {
      return true;
    }
    return false;
  }
  
  public Object compileImportStmt(ImportStmt importStmt)
  {
    Imports im = this.ctx.getImports();
    String name = importStmt.name;
    if (importStmt.hasStaticKeyword)
    {
      if (importStmt.hasStar)
      {
        String className = name;
        Class<?> c = loadClass(className);
        if (c == null)
        {
          error("cannot resolve type", name);
        }
        else
        {
          Field[] declFields = c.getFields();
          for (Field f : declFields) {
            if (javassist.Modifier.isStatic(f.getModifiers()))
            {
              Field pf = im.addStaticFieldRef(f);
              if (pf != null) {
                error("import of static field hides previous import", name);
              }
            }
          }
          Method[] declMethods = c.getDeclaredMethods();
          for (Method m : declMethods) {
            if (canMethodBeImported(m)) {
              im.addStaticMethod(c, m.getName());
            }
          }
        }
      }
      else
      {
        String className = NameHelper.getPrefix(name);
        String varName = NameHelper.getBasename(name);
        if (className.isEmpty())
        {
          error("invalid import declaration", name);
        }
        else
        {
          Class<?> c;
          if ((c = loadClass(className)) == null)
          {
            error("cannot resolve type " + className, name);
          }
          else
          {
            boolean hasField = false;
            boolean hasMethod = false;
            try
            {
              Field f = NamePolicy.getField(c, varName);
              if (javassist.Modifier.isStatic(f.getModifiers()))
              {
                Field pf = im.addStaticFieldRef(f);
                if (pf != null) {
                  error("import of static field hides previous import", name);
                }
                hasField = true;
              }
            }
            catch (NoSuchFieldException e) {}
            Method[] declMethods = c.getDeclaredMethods();
            for (Method m : declMethods) {
              if ((canMethodBeImported(m)) && (m.getName().equals(varName)))
              {
                im.addStaticMethod(c, m.getName());
                hasMethod = true;
              }
            }
            if ((!hasField) && (!hasMethod)) {
              error("type has not member " + varName, name);
            }
          }
        }
      }
    }
    else if (importStmt.hasStar)
    {
      im.importPackage(name.toString());
    }
    else
    {
      String className = name.toString();
      try
      {
        Class<?> c = this.ctx.loadClass(className);
        Class<?> pc = im.importClass(c);
        if (pc != null) {
          error("import of class hides previous import", name);
        }
      }
      catch (ClassNotFoundException e)
      {
        error("cannot resolve type", name);
      }
    }
    return null;
  }
  
  public Class<?> getTypeClass(UUID type)
    throws MetaDataRepositoryException
  {
    return this.ctx.getTypeClass(type);
  }
  
  public MetaInfo.Type getTypeInfo(UUID type)
    throws MetaDataRepositoryException
  {
    return this.ctx.getTypeInfo(type);
  }
  
  public Object compileUseStmt(UseStmt stmt)
    throws MetaDataRepositoryException
  {
    if (stmt.what == EntityType.NAMESPACE) {
      this.ctx.useNamespace(stmt.schemaName);
    } else if (stmt.what == EntityType.QUERY) {
      try
      {
        this.ctx.recompileQuery(stmt.schemaName);
      }
      catch (Exception e)
      {
        throw new RuntimeException(e.getMessage());
      }
    } else {
      this.ctx.alterAppOrFlow(stmt.what, stmt.schemaName, stmt.recompile);
    }
    return null;
  }
  
  private Map<String, Object> loadProperties(String name)
    throws MetaDataRepositoryException
  {
    MetaInfo.PropertySet props = this.ctx.getPropertySetInCurSchema(name);
    if (props == null)
    {
      error("cannot find properties", name);
      return Collections.emptyMap();
    }
    return props.properties;
  }
  
  public Map<String, Object> combineProperties(List<Property> propList)
    throws MetaDataRepositoryException
  {
    Map<String, Object> props = Factory.makeCaseInsensitiveMap();
    if (propList != null) {
      for (Property p : propList) {
        if (p.name.equals("#"))
        {
          Map<String, Object> pp = loadProperties((String)p.value);
          props.putAll(pp);
        }
        else if ((p.value instanceof List))
        {
          Object value = combineProperties((List)p.value);
          props.put(p.name, value);
        }
        else
        {
          props.put(p.name, p.value);
        }
      }
    }
    return encryptSensitiveInfo(props, propList);
  }
  
  public UUID compileCreatePropertySetStmt(CreatePropertySetStmt stmt)
    throws MetaDataRepositoryException
  {
    Map<String, Object> props = combineProperties(stmt.props);
    return this.ctx.putPropertySet(stmt.doReplace, stmt.name, props).uuid;
  }
  
  public UUID compileCreatePropertyVariableStmt(CreatePropertyVariableStmt stmt)
    throws MetaDataRepositoryException
  {
    Map<String, Object> props = combineProperties(stmt.props);
    return this.ctx.putPropertyVariable(stmt.doReplace, stmt.name, props).uuid;
  }
  
  private Map<String, Object> encryptSensitiveInfo(Map<String, Object> props, List<Property> propList)
    throws SecurityException, MetaDataRepositoryException
  {
    if ((props == null) || (props.isEmpty())) {
      return props;
    }
    Map<String, Object> temp = Factory.makeCaseInsensitiveMap();
    temp.putAll(props);
    for (String key : temp.keySet())
    {
      String key_flag = key + "_encrypted";
      if ((props.get(key_flag) != null) && 
        (!Utility.isValueEncryptionFlagSetToTrue(key, props)))
      {
        String val = (String)props.get(key);
        props.put(key, Password.getEncryptedStatic(val));
        
        props.put(key_flag, Boolean.valueOf(true));
        for (int ik = 0; ik < propList.size(); ik++)
        {
          if (((Property)propList.get(ik)).name.equalsIgnoreCase(key)) {
            propList.set(ik, new Property(key, Password.getEncryptedStatic(val)));
          }
          if (((Property)propList.get(ik)).name.equalsIgnoreCase(key_flag)) {
            propList.set(ik, new Property(key_flag, Boolean.valueOf(true)));
          }
        }
      }
    }
    return props;
  }
  
  public Object compileCreateCacheStmt(CreateCacheStmt stmt)
    throws MetaDataRepositoryException
  {
    String adapterTypeName = stmt.src.getAdapterTypeName();
    List<Property> handler_props = stmt.src.getProps();
    String parse_handler = stmt.parser != null ? stmt.parser.getAdapterTypeName() : null;
    
    List<Property> parse_handler_props = stmt.parser != null ? stmt.parser.getProps() : Collections.emptyList();
    
    Map<String, Object> reader_props = combineProperties(handler_props);
    Map<String, Object> parser_props = combineProperties(parse_handler_props);
    Map<String, Object> query_props = combineProperties(stmt.query_props);
    MetaInfo.Type type = this.ctx.getTypeInCurSchema(stmt.typename);
    if (type == null) {
      error("The specified type does not exist.", stmt.typename);
    }
    MetaInfo.PropertyTemplateInfo pt = this.ctx.getAdapterProperties(stmt.src.getAdapterTypeName());
    
    EntityType et = null;
    if (pt == null)
    {
      et = EntityType.forObject(stmt.src.getAdapterTypeName());
      if (et == null) {
        error("cannot load adapter", stmt.src.getAdapterTypeName());
      }
    }
    MetaInfo.MetaObject obj = null;
    if ((pt != null) && (pt.adapterType != AdapterType.source) && (pt.adapterType != AdapterType.internal))
    {
      error("The specified adapter is a " + pt.adapterType + " adapter, please use a source adapter for caches.", stmt.src.getAdapterTypeName());
    }
    else if (et != null)
    {
      if ((query_props.containsKey("refreshinterval")) && (query_props.containsKey("publishonrefresh"))) {
        error("Refresh interval and publish on refresh won't work with EventTable", stmt.query_props);
      }
      if (!reader_props.containsKey("NAME")) {
        error("No input stream name specified for EventTable", stmt.src.getProps());
      }
      obj = this.ctx.get((String)reader_props.get("NAME"), et);
      if (obj == null)
      {
        String streamName = (String)reader_props.get("NAME");
        obj = this.ctx.putStream(false, makeObjectName(streamName), type.getUuid(), null, null, null, null, null);
      }
    }
    if (pt != null)
    {
      validateAdapterPropsAndValues(pt, reader_props, handler_props);
      
      MetaInfo.PropertyTemplateInfo parserPropertyTemplate = null;
      if (parse_handler != null) {
        parserPropertyTemplate = this.ctx.getAdapterProperties(parse_handler);
      }
      if (parse_handler != null)
      {
        if (parserPropertyTemplate == null) {
          error("No such parser exists.", stmt.parser.getAdapterTypeName());
        }
        parser_props.put("handler", parserPropertyTemplate.className);
      }
      if ((pt.requiresParser == true) && (parserPropertyTemplate == null)) {
        error(stmt.src.getAdapterTypeName() + " requires parser.", stmt.src.getAdapterTypeName());
      }
      if (parserPropertyTemplate != null) {
        validateAdapterPropsAndValues(parserPropertyTemplate, parser_props, null);
      }
    }
    MetaInfo.Cache cacheObject = this.ctx.putCache(stmt.doReplace, makeObjectName(stmt.name), pt == null ? null : pt.className, reader_props, parser_props, query_props, type.uuid, pt == null ? null : pt.getInputClass());
    if (logger.isDebugEnabled()) {
      logger.debug("Added dependency: { from type " + type.name + " => cache " + cacheObject.name + " } ");
    }
    addDependency(type, cacheObject);
    addCurrentApplicationFlowDependencies(type);
    addCurrentApplicationFlowDependencies(cacheObject);
    stmt.sourceText = Utility.createCacheStatementText(stmt.name, Boolean.valueOf(stmt.doReplace), adapterTypeName, handler_props, parse_handler, parse_handler_props, stmt.query_props, stmt.typename);
    addSourceTextToMetaObject(stmt, cacheObject);
    return null;
  }
  
  private void addDependency(MetaInfo.MetaObject fromObject, MetaInfo.MetaObject toObject)
    throws MetaDataRepositoryException
  {
    if ((fromObject != null) && (toObject != null))
    {
      fromObject.addReverseIndexObjectDependencies(toObject.uuid);
      this.metadata_repo.updateMetaObject(fromObject, this.ctx.getSessionID());
    }
    else
    {
      logger.warn("Can't add dependency between " + fromObject + " and " + toObject);
    }
  }
  
  private MetaInfo.Type getTypeForCLass(Class<?> clazz)
    throws MetaDataRepositoryException
  {
    MetaInfo.Type type = null;
    if (!clazz.equals(NotSet.class))
    {
      type = this.ctx.getType("Global." + clazz.getSimpleName());
      if (type != null) {
        return type;
      }
      String typeName = this.ctx.addSchemaPrefix(null, clazz.getSimpleName());
      type = this.ctx.getType(typeName);
      if (type == null)
      {
        Map<String, String> fields = new LinkedHashMap();
        
        Field[] cFields = clazz.getDeclaredFields();
        for (Field f : cFields) {
          if (javassist.Modifier.isPublic(f.getModifiers())) {
            fields.put(f.getName(), f.getType().getCanonicalName());
          }
        }
        type = new MetaInfo.Type();
        type.construct(typeName, this.ctx.getNamespace(null), clazz.getName(), fields, null, false);
        this.ctx.putType(type);
        type = this.ctx.getType(typeName);
      }
    }
    return type;
  }
  
  private void validateAdapterPropsAndValues(MetaInfo.PropertyTemplateInfo pt, Map<String, Object> props, List<Property> propList)
  {
    Map<String, MetaInfo.PropertyDef> tps = pt.getPropertyMap();
    for (Map.Entry<String, MetaInfo.PropertyDef> tp : tps.entrySet())
    {
      String pname = (String)tp.getKey();
      MetaInfo.PropertyDef def = (MetaInfo.PropertyDef)tp.getValue();
      Class<?> ptype = def.type;
      boolean preq = def.required;
      String defVal = def.defaultValue;
      Object val = props.get(pname);
      if (val == null)
      {
        if (preq) {
          error("property " + pname + " is required, but is not specified", null);
        }
        val = castDefaultValueToProperType(pname, defVal, ptype, props, null);
        props.put(pname, val);
      }
      else if (((val instanceof String)) && (!ptype.equals(String.class)))
      {
        val = castDefaultValueToProperType(pname, (String)val, ptype, props, propList);
        props.put(pname, val);
      }
    }
  }
  
  private void checkCompoundStreamName(String name)
    throws MetaDataRepositoryException
  {
    if (name.indexOf('.') >= 0)
    {
      String[] names = name.split("\\.");
      if (this.ctx.getNamespace(names[0]) == null) {
        error("cannot find namespace <" + names[0] + ">", name);
      }
    }
  }
  
  private void addChannelNameForSubscriptions(CreateSourceOrTargetStmt stmt)
  {
    if (stmt.srcOrDest.getAdapterTypeName().equalsIgnoreCase("WebAlertAdapter"))
    {
      List<Property> handler_props = stmt.srcOrDest.getProps();
      boolean add = false;
      boolean channelNameGiven = false;
      for (Property property : handler_props)
      {
        if (property.name.equalsIgnoreCase("isSubscription")) {
          add = true;
        }
        if ((property.name.equalsIgnoreCase("channelName")) && 
          (property.value != null) && (!((String)property.value).isEmpty())) {
          channelNameGiven = true;
        }
      }
      if ((add) && (!channelNameGiven))
      {
        Property p = new Property("channelName", this.ctx.getNamespaceName(Utility.splitDomain(stmt.name)) + "_" + stmt.name);
        stmt.srcOrDest.getProps().add(p);
      }
    }
  }
  
  public MetaInfo.MetaObject compileCreateSourceOrTargetStmt(CreateSourceOrTargetStmt stmt)
    throws MetaDataRepositoryException
  {
    String adapterTypeName = stmt.srcOrDest.getAdapterTypeName();
    addChannelNameForSubscriptions(stmt);
    List<Property> handler_props = stmt.srcOrDest.getProps();
    
    String parserOrFormatterHandler = stmt.parserOrFormatter != null ? stmt.parserOrFormatter.getAdapterTypeName() : null;
    List<Property> parserOrFormatterProps = stmt.parserOrFormatter != null ? stmt.parserOrFormatter.getProps() : Collections.emptyList();
    MetaInfo.PropertyTemplateInfo pt = this.ctx.getAdapterProperties(adapterTypeName);
    if (pt == null) {
      error("cannot load adapter", adapterTypeName);
    }
    if (((pt.requiresParser == true) || (pt.requiresFormatter == true)) && (parserOrFormatterHandler == null)) {
      error(stmt.srcOrDest.getAdapterTypeName() + " requires parser.", stmt.srcOrDest.getAdapterTypeName());
    }
    if ((stmt.what == EntityType.SOURCE) && (pt.adapterType != AdapterType.source) && (pt.adapterType != AdapterType.internal)) {
      error("The specified adapter is a " + pt.adapterType + " adapter, please use a source adapter for sources.", adapterTypeName);
    }
    if ((stmt.what == EntityType.TARGET) && (pt.adapterType != AdapterType.target) && (pt.adapterType != AdapterType.internal)) {
      error("The specified adapter is a " + pt.adapterType + " adapter, please use a target adapter for targets.", adapterTypeName);
    }
    MetaInfo.PropertyTemplateInfo parserOrFormatterPropertyTemplate = null;
    if (parserOrFormatterHandler != null)
    {
      parserOrFormatterPropertyTemplate = this.ctx.getAdapterProperties(parserOrFormatterHandler);
      if (parserOrFormatterPropertyTemplate == null) {
        error("cannot find parser template", parserOrFormatterHandler);
      }
      if ((stmt.what == EntityType.SOURCE) && (parserOrFormatterPropertyTemplate.adapterType != AdapterType.parser) && (parserOrFormatterPropertyTemplate.adapterType != AdapterType.internal)) {
        error("The specified parser is a " + parserOrFormatterPropertyTemplate.adapterType + " adapter, please use a correct parser adapter for sources.", adapterTypeName);
      }
      if ((stmt.what == EntityType.TARGET) && (parserOrFormatterPropertyTemplate.adapterType != AdapterType.formatter) && (parserOrFormatterPropertyTemplate.adapterType != AdapterType.internal)) {
        error("The specified formatter is a " + parserOrFormatterPropertyTemplate.adapterType + " adapter, please use a correct formatter adapter for targets.", adapterTypeName);
      }
    }
    Map<String, Object> props = combineProperties(handler_props);
    Map<String, Object> parserOrFormatterProperties = combineProperties(parserOrFormatterProps);
    
    validateAdapterPropsAndValues(pt, props, handler_props);
    if (props != null) {
      props.put("adapterName", pt.name);
    }
    if (parserOrFormatterPropertyTemplate != null)
    {
      validateAdapterPropsAndValues(parserOrFormatterPropertyTemplate, parserOrFormatterProperties, null);
      parserOrFormatterProperties.put("handler", parserOrFormatterPropertyTemplate.className);
      if (stmt.what == EntityType.SOURCE) {
        parserOrFormatterProperties.put("parserName", parserOrFormatterPropertyTemplate.name);
      } else {
        parserOrFormatterProperties.put("formatterName", parserOrFormatterPropertyTemplate.name);
      }
    }
    Class<?> inputTypeClass = parserOrFormatterPropertyTemplate == null ? pt.getInputClass() : parserOrFormatterPropertyTemplate.getInputClass();
    MetaInfo.Type inputType = getTypeForCLass(inputTypeClass);
    Class<?> outputTypeClass = pt.getOutputClass();
    MetaInfo.Type outputType = getTypeForCLass(outputTypeClass);
    
    assert ((stmt.what == EntityType.SOURCE) || (stmt.what == EntityType.TARGET));
    UUID resType = stmt.what == EntityType.SOURCE ? inputType.uuid : outputType.uuid;
    
    UUID oldSourceUUID = null;
    UUID oldTargetUUID = null;
    UUID oldStreamUUID = null;
    if ((stmt.doReplace) && (
      (this.ctx.getSourceInCurSchema(stmt.name) != null) || (this.ctx.getTargetInCurSchema(stmt.name) != null))) {
      if (this.ctx.recompileMode)
      {
        if (stmt.what == EntityType.SOURCE)
        {
          MetaInfo.Source sourceMetaObject = this.ctx.getSourceInCurSchema(stmt.name);
          oldSourceUUID = sourceMetaObject.uuid;
          if (this.ctx.getStreamInCurSchema(stmt.getStreamName()).getMetaInfoStatus().isAnonymous())
          {
            oldStreamUUID = this.ctx.getStreamInCurSchema(stmt.getStreamName()).uuid;
            this.ctx.removeObject(this.ctx.getStreamInCurSchema(stmt.getStreamName()));
          }
          this.ctx.removeObject(sourceMetaObject);
        }
        if (stmt.what == EntityType.TARGET)
        {
          MetaInfo.Target targetMetaObject = this.ctx.getTargetInCurSchema(stmt.name);
          oldTargetUUID = targetMetaObject.uuid;
          MetaInfo.Stream streamMetaObject = this.ctx.getStreamInCurSchema(stmt.getStreamName());
          if (((!streamMetaObject.getMetaInfoStatus().isGenerated()) || (!streamMetaObject.getMetaInfoStatus().isValid())) && 
            (streamMetaObject.getMetaInfoStatus().isAnonymous()))
          {
            oldStreamUUID = this.ctx.getStreamInCurSchema(stmt.getStreamName()).uuid;
            this.ctx.removeObject(this.ctx.getStreamInCurSchema(stmt.getStreamName()));
          }
          this.ctx.removeObject(targetMetaObject);
        }
      }
      else if (stmt.what == EntityType.SOURCE)
      {
        DropMetaObject.DropSource.drop(this.ctx, this.ctx.getSourceInCurSchema(stmt.name), DropMetaObject.DropRule.NONE, this.ctx.getSessionID());
      }
      else
      {
        DropMetaObject.DropTarget.drop(this.ctx, this.ctx.getTargetInCurSchema(stmt.name), DropMetaObject.DropRule.NONE, this.ctx.getSessionID());
      }
    }
    MetaInfo.MetaObject stream = this.ctx.getDataSourceInCurSchema(stmt.getStreamName());
    if (stream == null)
    {
      checkCompoundStreamName(stmt.getStreamName());
      checkPartitionFields(stmt.getPartitionFields(), resType, stmt.getStreamName());
      if ((this.ctx.recompileMode) && (oldStreamUUID != null)) {
        stream = this.ctx.putStream(false, makeObjectName(stmt.getStreamName()), resType, stmt.getPartitionFields(), null, null, oldStreamUUID, null);
      } else {
        stream = this.ctx.putStream(false, makeObjectName(stmt.getStreamName()), resType, stmt.getPartitionFields(), null, null, null, null);
      }
      stream.getMetaInfoStatus().setGenerated(true);
      this.metadata_repo.updateMetaObject(stream, this.ctx.getSessionID());
    }
    MetaInfo.MetaObject metaObject = null;
    if (stmt.what == EntityType.SOURCE) {
      metaObject = this.ctx.putSource(stmt.doReplace, makeObjectName(stmt.name), pt.className, props, parserOrFormatterProperties, stream.uuid, oldSourceUUID);
    } else {
      metaObject = this.ctx.putTarget(stmt.doReplace, makeObjectName(stmt.name), pt.className, props, parserOrFormatterProperties, stream.uuid, oldTargetUUID);
    }
    MetaInfo.Type type = stmt.what == EntityType.SOURCE ? inputType : outputType;
    if (logger.isDebugEnabled()) {
      logger.debug("Added dependency: { from stream " + stream.name + " => source/target " + metaObject.name + " } ");
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Added dependency: { from type " + type.name + " => source/target " + metaObject.name + " } ");
    }
    addDependency(type, stream);
    addDependency(stream, metaObject);
    addCurrentApplicationFlowDependencies(metaObject);
    addCurrentApplicationFlowDependencies(stream);
    if (stmt.what == EntityType.TARGET) {
      stmt.sourceText = Utility.createTargetStatementText(stmt.name, Boolean.valueOf(stmt.doReplace), adapterTypeName, handler_props, parserOrFormatterHandler, parserOrFormatterProps, stmt.getStreamName());
    } else if (stmt.what == EntityType.SOURCE) {
      stmt.sourceText = Utility.createSourceStatementText(stmt.name, Boolean.valueOf(stmt.doReplace), adapterTypeName, handler_props, parserOrFormatterHandler, parserOrFormatterProps, stmt.getStreamName(), stmt.getGeneratedStream());
    }
    addSourceTextToMetaObject(stmt, metaObject);
    return metaObject;
  }
  
  private Object castDefaultValueToProperType(String key, String value, Class<?> type, Map<String, Object> props, List<Property> propList)
  {
    Object correctType = null;
    if ((value == null) || (value.isEmpty())) {
      return null;
    }
    if (value.startsWith("$"))
    {
      if (type == Password.class) {
        return new Password(value);
      }
      return value;
    }
    if (type == Character.class)
    {
      correctType = Character.valueOf(value.charAt(0));
    }
    else if (type == Boolean.class)
    {
      if ((value.equalsIgnoreCase("true")) || (value.equalsIgnoreCase("yes"))) {
        correctType = Boolean.valueOf(true);
      } else {
        correctType = Boolean.valueOf(false);
      }
    }
    else if (type == Password.class)
    {
      correctType = new Password();
      if (Utility.isValueEncryptionFlagSetToTrue(key, props))
      {
        ((Password)correctType).setEncrypted(value);
      }
      else
      {
        ((Password)correctType).setPlain(value);
        updateMapAndListWithEncryptionFlag(key, value, props, propList);
      }
    }
    else
    {
      try
      {
        if (type.isPrimitive()) {
          type = CompilerUtils.getBoxingType(type);
        }
        correctType = type.getConstructor(new Class[] { String.class }).newInstance(new Object[] { value });
      }
      catch (InstantiationException|IllegalAccessException|IllegalArgumentException|InvocationTargetException|NoSuchMethodException|SecurityException e)
      {
        logger.error(e.getMessage());
      }
    }
    return correctType;
  }
  
  private void updateMapAndListWithEncryptionFlag(String key, String value, Map<String, Object> props, List<Property> propList)
  {
    boolean update = false;
    boolean add = false;
    if ((propList != null) && (propList.size() > 0))
    {
      for (int ik = 0; ik < propList.size(); ik++) {
        if (((Property)propList.get(ik)).name.equalsIgnoreCase(key))
        {
          propList.set(ik, new Property(key, Password.getEncryptedStatic(value)));
          if (props.get(key + "_encrypted") == null) {
            add = true;
          } else {
            update = true;
          }
        }
      }
      if (add) {
        propList.add(new Property(key + "_encrypted", Boolean.valueOf(true)));
      }
      if (update) {
        for (int ik = 0; ik < propList.size(); ik++) {
          if (((Property)propList.get(ik)).name.equalsIgnoreCase(key + "_encrypted")) {
            propList.set(ik, new Property(key + "_encrypted", Boolean.valueOf(true)));
          }
        }
      }
    }
  }
  
  public Object compileCreateAppOrFlowStatement(CreateAppOrFlowStatement stmt)
    throws MetaDataRepositoryException
  {
    Map<EntityType, LinkedHashSet<UUID>> objects = null;
    if (stmt.entities != null)
    {
      objects = Factory.makeMap();
      for (Pair<EntityType, String> p : stmt.entities)
      {
        String name = this.ctx.addSchemaPrefix(null, (String)p.second);
        MetaInfo.MetaObject obj = this.ctx.get(name, (EntityType)p.first);
        if (obj == null)
        {
          error(((EntityType)p.first).name().toLowerCase() + " [" + (String)p.second + "]does not exist", p);
        }
        else
        {
          LinkedHashSet<UUID> list = (LinkedHashSet)objects.get(obj.type);
          if (list == null)
          {
            list = new LinkedHashSet();
            objects.put(obj.type, list);
          }
          list.add(obj.uuid);
        }
      }
    }
    Map<String, Object> ehprops = null;
    if (stmt.eh != null)
    {
      ehprops = combineProperties(stmt.eh.props);
      if (!ExceptionType.validate(ehprops))
      {
        logger.error("validation of exceptionhandler details failed. enter proper values and recompile applicaiton");
        throw new CompilationException("validation of exceptionhandler details failed. enter proper values and recompile applicaiton");
      }
    }
    MetaInfo.Flow flowOrApp = this.ctx.putFlow(stmt.what, stmt.doReplace, makeObjectName(stmt.name), stmt.encrypted, stmt.recoveryDesc, ehprops, objects, new HashSet());
    
    Set<Map.Entry<EntityType, LinkedHashSet<UUID>>> objectsByType = flowOrApp.getObjects().entrySet();
    Iterator<Map.Entry<EntityType, LinkedHashSet<UUID>>> ii;
    if ((objectsByType != null) && (!objectsByType.isEmpty())) {
      for (ii = objectsByType.iterator(); ii.hasNext();)
      {
        Map.Entry<EntityType, LinkedHashSet<UUID>> entry = (Map.Entry)ii.next();
        for (UUID uuid : (LinkedHashSet)entry.getValue())
        {
          MetaInfo.MetaObject objectInFlow = this.ctx.getObject(uuid);
          addDependency(objectInFlow, flowOrApp);
        }
      }
    }
    if (stmt.what == EntityType.FLOW)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("Added dependency: { from flow " + stmt.name + " => app " + this.ctx.getCurApp().name + " } ");
      }
      if (this.ctx.getCurApp() != null) {
        addDependency(flowOrApp, this.ctx.getCurApp());
      } else {
        logger.warn("Trying to add dependency between a Flow:" + flowOrApp + " and App: Null");
      }
    }
    return null;
  }
  
  public Object compileCreateDeploymentGroupStmt(CreateDeploymentGroupStmt stmt)
    throws MetaDataRepositoryException
  {
    this.ctx.putDeploymentGroup(stmt.name, stmt.deploymentGroup, stmt.minServers);
    return null;
  }
  
  public Object compileAlterDeploymentGroupStmt(AlterDeploymentGroupStmt stmt)
    throws MetaDataRepositoryException
  {
    this.ctx.alterDeploymentGroup(stmt.add, stmt.groupname, stmt.deploymentGroup);
    return null;
  }
  
  public static boolean compareKeyFields(List<String> k1, List<String> k2)
  {
    return k1.equals(k2);
  }
  
  public Object compileCreateWASStmt(CreateWASStmt stmt)
    throws MetaDataRepositoryException
  {
    TypeDefOrName typeDef = stmt.typeDef;
    UUID typeid;
    UUID typeid;
    if (typeDef.typeName != null)
    {
      assert (typeDef.typeDef == null);
      MetaInfo.MetaObject type = this.ctx.getTypeInCurSchema(typeDef.typeName);
      if (type == null) {
        error("no such type declaration", typeDef.typeName);
      }
      typeid = type.uuid;
    }
    else
    {
      assert (typeDef.typeDef != null);
      
      typeid = createType(false, RuntimeUtils.genRandomName("type"), typeDef.typeDef, true);
    }
    List<EventType> eventTypes = stmt.evenTypes;
    List<UUID> eventTypeIDs = new ArrayList();
    List<String> eventTypeKeys = new ArrayList();
    List<MetaInfo.Type> eventTypesList = new ArrayList();
    Map<String, List<String>> ets = new HashMap();
    for (EventType et : eventTypes)
    {
      MetaInfo.Type type = this.ctx.getTypeInCurSchema(et.typeName);
      if (type == null) {
        error("no such event type declared", et.typeName);
      }
      eventTypeIDs.add(type.uuid);
      eventTypeKeys.add(et.keyFields.get(0));
      eventTypesList.add(type);
      ets.put(et.typeName, et.keyFields);
    }
    Interval how = stmt.howToPersist;
    Map<String, Object> props = combineProperties(stmt.properties);
    MetaInfo.WActionStore wactionStroreObject = this.ctx.putWActionStore(stmt.doReplace, makeObjectName(stmt.name), typeid, how, eventTypeIDs, eventTypeKeys, props);
    
    MetaInfo.Type contextTypeMetaObject = (MetaInfo.Type)this.ctx.getObject(typeid);
    if (logger.isDebugEnabled()) {
      logger.debug("Added dependency: { from type " + contextTypeMetaObject.name + " => stream " + wactionStroreObject.name + " } ");
    }
    addDependency(contextTypeMetaObject, wactionStroreObject);
    addCurrentApplicationFlowDependencies(contextTypeMetaObject);
    if (eventTypesList != null) {
      for (MetaInfo.Type eventType : eventTypesList)
      {
        if (logger.isDebugEnabled()) {
          logger.debug("Added dependency: { from type " + eventType.name + " => stream " + wactionStroreObject.name + " } ");
        }
        addDependency(eventType, wactionStroreObject);
        addCurrentApplicationFlowDependencies(eventType);
      }
    }
    addCurrentApplicationFlowDependencies(wactionStroreObject);
    
    WactionStorePersistencePolicy policy = new WactionStorePersistencePolicy(stmt.howToPersist, stmt.properties);
    String ival = null;
    if (how != null) {
      ival = String.valueOf(how.value / 1000000L) + " SECOND ";
    }
    stmt.sourceText = Utility.createWASStatementText(stmt.name, Boolean.valueOf(stmt.doReplace), stmt.typeDef.typeName, ets, ival, policy);
    addSourceTextToMetaObject(stmt, wactionStroreObject);
    return null;
  }
  
  public Object compileCreateNamespaceStatement(CreateNamespaceStatement stmt)
    throws MetaDataRepositoryException
  {
    this.ctx.putNamespace(stmt.doReplace, stmt.name);
    return null;
  }
  
  public Object compileCreateUserStmt(CreateUserStmt stmt)
  {
    if (stmt.prop.lroles != null) {
      for (String rname : stmt.prop.lroles)
      {
        Pattern pattern = Pattern.compile("\\s");
        Matcher matcher = pattern.matcher(rname);
        boolean found = matcher.find();
        if (found == true) {
          error("Space in the ROLE NAME. ", stmt.prop.lroles);
        }
      }
    }
    if (stmt.prop.ldap != null)
    {
      AuthLayer al = LDAPAuthLayer.get(Utility.convertToFullQualifiedName(this.ctx.getCurNamespace(), stmt.prop.ldap));
      if (al == null)
      {
        error("no ldap exists with name", stmt.prop.ldap);
        return null;
      }
      if (!al.find(stmt.name))
      {
        error("No user found with ldap server, name", stmt.name);
        return null;
      }
    }
    try
    {
      this.ctx.putUser(stmt.name, stmt.password, stmt.prop);
    }
    catch (Exception e)
    {
      throw new Warning(e.getMessage());
    }
    return null;
  }
  
  public Object compileCreateRoleStmt(CreateRoleStmt stmt)
  {
    Pattern pattern = Pattern.compile("\\s");
    Matcher matcher = pattern.matcher(stmt.name);
    boolean found = matcher.find();
    if (stmt.name.split(":").length != 2) {
      error("Rolename has wrong format ", stmt.name);
    }
    if (found == true) {
      error("Space in the ROLE NAME.", stmt.name);
    }
    try
    {
      this.ctx.putRole(stmt.name);
    }
    catch (Exception e)
    {
      throw new RuntimeException(e.getMessage());
    }
    return null;
  }
  
  public Object compileConnectStmt(ConnectStmt stmt)
    throws MetaDataRepositoryException
  {
    this.ctx.Connect(stmt.name, stmt.password, stmt.clusterid, stmt.host);
    return null;
  }
  
  public Object compileImportDataStmt(ImportDataStmt stmt)
  {
    if ("METADATA".equalsIgnoreCase(stmt.what)) {
      try
      {
        File f = new File(stmt.where);
        FileReader fr = new FileReader(f);
        StringBuilder jsonBuilder = new StringBuilder();
        char[] cbuf = new char['��'];
        for (int result = fr.read(cbuf); result != -1; result = fr.read(cbuf)) {
          jsonBuilder.append(cbuf, 0, result);
        }
        String json = jsonBuilder.toString();
        this.metadata_repo.importMetadataFromJson(json, stmt.replace);
        fr.close();
      }
      catch (IOException e)
      {
        logger.error(e);
      }
    } else {
      logger.info("Cannot export data becasue of unknown datatype: " + stmt.what);
    }
    return null;
  }
  
  public Object compileExportDataStmt(ExportDataStmt stmt)
  {
    if ("METADATA".equalsIgnoreCase(stmt.what))
    {
      if ((stmt.where == null) || (stmt.where.equals("STDOUT")))
      {
        String json = this.metadata_repo.exportMetadataAsJson();
        System.out.print(json);
      }
      else
      {
        try
        {
          String json = this.metadata_repo.exportMetadataAsJson();
          File f = new File(stmt.where);
          
          FileWriter fw = new FileWriter(f);
          fw.write(json);
          fw.close();
        }
        catch (IOException e)
        {
          logger.error(e.getMessage(), e);
        }
      }
    }
    else {
      logger.info("Cannot export data becasue of unknown datatype: " + stmt.what);
    }
    return null;
  }
  
  public Object compileSetStmt(SetStmt stmt)
  {
    if ((stmt.paramname == null) && (stmt.paramvalue == null))
    {
      Tungsten.listLogLevels();
    }
    else if (stmt.paramname.equalsIgnoreCase("PRINTFORMAT"))
    {
      if ((stmt.paramvalue instanceof String)) {
        if (stmt.paramvalue.toString().equalsIgnoreCase("JSON")) {
          Tungsten.currentFormat = Tungsten.PrintFormat.JSON;
        } else if (stmt.paramvalue.toString().equalsIgnoreCase("ROW_FORMAT")) {
          Tungsten.currentFormat = Tungsten.PrintFormat.ROW_FORMAT;
        } else {
          Tungsten.currentFormat = Tungsten.PrintFormat.JSON;
        }
      }
    }
    else if (stmt.paramname.equalsIgnoreCase("LOGLEVEL"))
    {
      if ((this.ctx != null) && (this.ctx.getSessionID() != null)) {
        try
        {
          MetaInfo.User user = WASecurityManager.get().getAuthenticatedUser(this.ctx.getSessionID());
          MetaInfo.Role rl = new MetaInfo.Role();
          rl.construct(MetaInfo.GlobalNamespace, "admin");
          if (user.getRoles().contains(rl))
          {
            Utility.changeLogLevel(stmt.paramvalue, true);
            this.ctx.SetStmtRemoteCall(stmt.paramname, stmt.paramvalue);
          }
          else
          {
            throw new RuntimeException("No permissions to set log level for the user : " + user);
          }
        }
        catch (MetaDataRepositoryException e)
        {
          if (logger.isInfoEnabled()) {
            logger.info(e.getMessage());
          }
          throw new RuntimeException(e.getMessage());
        }
      }
    }
    else if (stmt.paramname.equalsIgnoreCase("MONITOR"))
    {
      IMap<String, Serializable> clusterSettings = HazelcastSingleton.get().getMap("#ClusterSettings");
      Boolean newValue = new Boolean((stmt.paramvalue != null) && (stmt.paramvalue.toString().equalsIgnoreCase("TRUE")));
      if (!newValue.equals(clusterSettings.get("com.bloom.config.enable-monitor"))) {
        clusterSettings.put("com.bloom.config.enable-monitor", newValue);
      }
    }
    else if (stmt.paramname.equalsIgnoreCase("MONITORPERSISTENCE"))
    {
      IMap<String, Serializable> clusterSettings = HazelcastSingleton.get().getMap("#ClusterSettings");
      Boolean newValue = new Boolean((stmt.paramvalue != null) && (stmt.paramvalue.toString().equalsIgnoreCase("TRUE")));
      if (!newValue.equals(clusterSettings.get("com.bloom.config.enable-monitor-persistence"))) {
        clusterSettings.put("com.bloom.config.enable-monitor-persistence", newValue);
      }
    }
    else if (stmt.paramname.equalsIgnoreCase("MONDBMAX"))
    {
      IMap<String, Serializable> clusterSettings = HazelcastSingleton.get().getMap("#ClusterSettings");
      String noCommas = stmt.paramvalue.toString().replace(",", "");
      Integer newValue = null;
      try
      {
        newValue = Integer.valueOf(Integer.parseInt(noCommas));
        if (!newValue.equals(clusterSettings.get("com.bloom.config.monitor-db-max"))) {
          clusterSettings.put("com.bloom.config.monitor-db-max", newValue);
        }
      }
      catch (NumberFormatException e)
      {
        System.out.println("Could not parse as integer: " + stmt.paramvalue);
      }
    }
    else
    {
      logger.warn("'SET' param name '" + stmt.paramname + "' not recognized");
    }
    return null;
  }
  
  public Object compileCreateVisualizationStmt(CreateVisualizationStmt stmt)
    throws MetaDataRepositoryException
  {
    int count = stmt.fileName.split(".").length - 1;
    if (count > 1) {
      error("File name " + stmt.fileName + " is problem", stmt.name);
    }
    File f = new File(stmt.fileName);
    if (!f.exists()) {
      error("File name " + stmt.fileName + " does not exist", stmt.fileName);
    }
    MetaInfo.Visualization viz = this.ctx.putVisualization(stmt.name, stmt.fileName);
    addCurrentApplicationFlowDependencies(viz);
    return null;
  }
  
  public Object UpdateUserInfoStmt(UpdateUserInfoStmt stmt)
  {
    try
    {
      Map<String, Object> props_toupdate = combineProperties(stmt.properties);
      this.ctx.UpdateUserInfoStmt(stmt.username, props_toupdate);
    }
    catch (SecurityException|UnsupportedEncodingException|GeneralSecurityException e)
    {
      error("Cannot update user information", stmt.username);
    }
    catch (Exception e)
    {
      error(e.getMessage(), stmt.username);
    }
    return null;
  }
  
  private static String COLON = ":";
  
  private List<String> generatePermissions(SecurityStmt stmt)
  {
    String name = stmt.getName();
    List<ObjectPermission.Action> listOfPrivilege = stmt.getListOfPrivilege();
    List<ObjectPermission.ObjectType> objectType = stmt.getObjectType();
    
    ObjectName fullObjectName = makeObjectName(name);
    String objectName = fullObjectName.getName();
    String objectNamespace = fullObjectName.getNamespace();
    
    List<String> permissions = new ArrayList();
    for (int i = 0; i < listOfPrivilege.size(); i++)
    {
      ObjectPermission.Action action = (ObjectPermission.Action)listOfPrivilege.get(i);
      if ((objectType != null) && (!objectType.isEmpty())) {
        for (int j = 0; j < objectType.size(); j++)
        {
          ObjectPermission.ObjectType objectTypeInstance = (ObjectPermission.ObjectType)objectType.get(j);
          permissions.add(objectNamespace + COLON + (action == ObjectPermission.Action.all ? "*" : action) + COLON + objectTypeInstance + COLON + objectName);
        }
      } else {
        permissions.add(objectNamespace + COLON + (action == ObjectPermission.Action.all ? "*" : action) + COLON + "*" + COLON + objectName);
      }
    }
    return permissions;
  }
  
  public Object GrantPermissionToStmt(GrantPermissionToStmt stmt)
  {
    List<String> permissions = generatePermissions(stmt);
    if (permissions != null) {
      for (String permission : permissions)
      {
        if (permission.split(":").length != 4) {
          error("Permission has wrong format :", permission);
        }
        Pattern pattern = Pattern.compile("\\s");
        Matcher matcher = pattern.matcher(permission);
        boolean found = matcher.find();
        if (found == true) {
          error("Space in the ROLE NAME.", permission);
        }
      }
    }
    if ((stmt.towhat == EntityType.ROLE) && (
      (stmt.name == null) || (stmt.name.split(":").length != 2))) {
      error("Role name format is wrong. Ex. NameSpaceName:roleName", stmt.name);
    }
    this.ctx.GrantPermissionToStmt(stmt, permissions);
    return null;
  }
  
  public Object GrantRoleToStmt(GrantRoleToStmt stmt)
  {
    Pattern pattern = Pattern.compile("\\s");
    for (String rolename : stmt.rolename)
    {
      Matcher matcher = pattern.matcher(rolename);
      if (rolename.split(":").length != 2) {
        error("Rolename has wrong format ", stmt.rolename);
      }
      boolean found = matcher.find();
      if (found == true) {
        error("Space in the ROLE NAME.", stmt.rolename);
      }
    }
    this.ctx.GrantRoleToStmt(stmt);
    return null;
  }
  
  public Object RevokePermissionFromStmt(RevokePermissionFromStmt stmt)
  {
    List<String> permissions = generatePermissions(stmt);
    if (permissions != null) {
      for (String permission : permissions)
      {
        if ((permission.split(":").length < 4) || (permission.split(":").length > 5)) {
          error("Permission has wrong format :", permission);
        }
        Pattern pattern = Pattern.compile("\\s");
        Matcher matcher = pattern.matcher(permission);
        boolean found = matcher.find();
        if (found == true) {
          error("Space in the ROLE NAME.", permission);
        }
      }
    }
    if ((stmt.towhat == EntityType.ROLE) && (
      (stmt.name == null) || (stmt.name.split(":").length != 2))) {
      error("Role name format is wrong. Ex. NameSpaceName:roleName", stmt.name);
    }
    this.ctx.RevokePermissionFromStmt(stmt, permissions);
    return null;
  }
  
  public Object RevokeRoleFromStmt(RevokeRoleFromStmt stmt)
  {
    Pattern pattern = Pattern.compile("\\s");
    for (String rolename : stmt.rolename)
    {
      Matcher matcher = pattern.matcher(rolename);
      if (rolename.split(":").length != 2) {
        error("Rolename has wrong format ", stmt.rolename);
      }
      boolean found = matcher.find();
      if (found == true) {
        error("Space in the ROLE NAME.", stmt.rolename);
      }
    }
    this.ctx.RevokeRoleFromStmt(stmt);
    return null;
  }
  
  public Object compileLoadOrUnloadJar(LoadUnloadJarStmt stmt)
  {
    WALoader loader = WALoader.get();
    String ns = this.ctx.getCurNamespace().name;
    int i = stmt.pathToJar.lastIndexOf('/');
    String name;
    String name;
    if (i == -1) {
      name = stmt.pathToJar;
    } else {
      name = stmt.pathToJar.substring(i + 1);
    }
    if (stmt.doLoad) {
      try
      {
        loader.addJar(ns, stmt.pathToJar, name);
      }
      catch (Exception e)
      {
        error(e.getMessage(), stmt.pathToJar);
      }
    } else {
      try
      {
        loader.removeJar(ns, name);
      }
      catch (Exception e)
      {
        error(e.getMessage(), stmt.pathToJar);
      }
    }
    return null;
  }
  
  public Object compileEndStmt(EndBlockStmt stmt)
    throws MetaDataRepositoryException
  {
    MetaInfo.Flow flow = this.ctx.getFlowInCurSchema(stmt.name, stmt.type);
    if (flow == null) {
      error("cannot find " + stmt.type, stmt.name);
    }
    assert (flow.type == stmt.type);
    String errMsg = this.ctx.endBlock(flow);
    if (errMsg != null) {
      error(errMsg, stmt.name);
    }
    return null;
  }
  
  private Pair<MetaInfo.Flow, MetaInfo.DeploymentGroup> checkDeploymentRule(DeploymentRule rule, MetaInfo.Flow parent, EntityType type)
    throws MetaDataRepositoryException
  {
    MetaInfo.Flow flow = this.ctx.getFlowInCurSchema(rule.flowName, type);
    if (flow == null) {
      error("cannot find such " + type, rule.flowName);
    }
    if (parent != null)
    {
      if (parent.uuid.equals(flow.uuid)) {
        error(type + " cannot be part of itself", rule.flowName);
      }
      Set<UUID> list = parent.getObjects(EntityType.FLOW);
      if (!list.contains(flow.uuid)) {
        error(type + " is not part of <" + parent.name + ">", rule.flowName);
      }
    }
    MetaInfo.DeploymentGroup dg = this.ctx.getDeploymentGroup(rule.deploymentGroup);
    if (dg == null) {
      error("cannot find such deployment group", rule.deploymentGroup);
    }
    return Pair.make(flow, dg);
  }
  
  public Object compileDeployStmt(DeployStmt stmt)
    throws MetaDataRepositoryException
  {
    if (stmt.appOrFlow.isAccessible())
    {
      MetaInfo.DeploymentGroup dg = this.ctx.getDeploymentGroup(stmt.appRule.deploymentGroup);
      if (dg == null) {
        error("cannot find such deployment group", stmt.appRule.deploymentGroup);
      }
      MetaInfo.MetaObject metaObject = this.ctx.get(stmt.appRule.flowName, stmt.appOrFlow);
      this.ctx.remoteCallOnObject(ActionType.LOAD, metaObject, new Object[] { stmt.appRule });
      return null;
    }
    List<MetaInfo.Flow.Detail> deploymentPlan = new ArrayList();
    this.ctx.resetAppScope();
    Pair<MetaInfo.Flow, MetaInfo.DeploymentGroup> appRule = checkDeploymentRule(stmt.appRule, null, stmt.appOrFlow);
    
    MetaInfo.Flow app = (MetaInfo.Flow)appRule.first;
    if (!PermissionUtility.checkPermission(app, ObjectPermission.Action.deploy, getContext().getSessionID(), false)) {
      error("Insufficient permissions to deploy " + app.getName(), app.getName());
    }
    if (!app.getMetaInfoStatus().isValid())
    {
      for (Map.Entry<EntityType, LinkedHashSet<UUID>> metaObjectsBelongToApp : app.objects.entrySet())
      {
        System.out.print(metaObjectsBelongToApp.getKey() + " => [ ");
        for (UUID metaObjectUUID : (LinkedHashSet)metaObjectsBelongToApp.getValue())
        {
          MetaInfo.MetaObject metaObjectDefinition = this.ctx.getObject(metaObjectUUID);
          if (metaObjectDefinition != null) {
            if (!metaObjectDefinition.getMetaInfoStatus().isValid()) {
              System.out.print(metaObjectDefinition.name + " ");
            }
          }
        }
        System.out.println("]");
      }
      error(stmt.appOrFlow + " is missing components! (Invalid " + stmt.appOrFlow + ")", stmt.appRule.flowName);
    }
    MetaInfo.Flow.Detail.FailOverRule failOverRule = MetaInfo.Flow.Detail.FailOverRule.NONE;
    if (stmt.options != null) {
      for (Pair<String, String> opt : stmt.options) {
        if (((String)opt.first).equalsIgnoreCase("FAILOVER"))
        {
          if (((String)opt.second).equalsIgnoreCase("AUTO")) {
            failOverRule = MetaInfo.Flow.Detail.FailOverRule.AUTO;
          } else if (((String)opt.second).equalsIgnoreCase("MANUAL")) {
            failOverRule = MetaInfo.Flow.Detail.FailOverRule.MANUAL;
          }
        }
        else {
          error((String)opt.first + " is not correct option to set for deploy.", stmt.options);
        }
      }
    }
    MetaInfo.Flow.Detail detail = new MetaInfo.Flow.Detail();
    detail.construct(stmt.appRule.strategy, ((MetaInfo.DeploymentGroup)appRule.second).uuid, ((MetaInfo.Flow)appRule.first).uuid, failOverRule);
    deploymentPlan.add(detail);
    for (DeploymentRule r : stmt.flowRules)
    {
      Pair<MetaInfo.Flow, MetaInfo.DeploymentGroup> flowRule = checkDeploymentRule(r, (MetaInfo.Flow)appRule.first, EntityType.FLOW);
      MetaInfo.Flow.Detail d = new MetaInfo.Flow.Detail();
      d.construct(r.strategy, ((MetaInfo.DeploymentGroup)flowRule.second).uuid, ((MetaInfo.Flow)flowRule.first).uuid, failOverRule);
      deploymentPlan.add(d);
    }
    app.setDeploymentPlan(deploymentPlan);
    try
    {
      Map<String, Object> params = new HashedMap();
      params.put("DEPLOYMENTPLAN", deploymentPlan);
      this.ctx.changeApplicationState(ActionType.DEPLOY, app, params);
      
      MetaInfo.StatusInfo.Status status = getStatus(app.uuid, this.ctx.getAuthToken());
      
      int count = 0;
      while (((status == MetaInfo.StatusInfo.Status.NOT_ENOUGH_SERVERS) || (status == MetaInfo.StatusInfo.Status.CREATED)) && (count < 300))
      {
        Thread.sleep(1000L);
        count++;
        status = getStatus(app.uuid, this.ctx.getAuthToken());
      }
      status = getStatus(app.uuid, this.ctx.getAuthToken());
      if (status == MetaInfo.StatusInfo.Status.NOT_ENOUGH_SERVERS) {
        throw new RuntimeException("Did not get required number of servers for deployment to succeed");
      }
    }
    catch (InterruptedException e) {}catch (Exception e)
    {
      throw new RuntimeException(e);
    }
    return null;
  }
  
  private MetaInfo.StatusInfo.Status getStatus(UUID uuid, AuthToken authToken)
    throws Exception
  {
    return FlowUtil.getCurrentStatusFromAppManager(uuid, authToken);
  }
  
  public void compileDumpStmt(String cqname, int mode, int action, AuthToken clToken)
    throws MetaDataRepositoryException
  {
    this.ctx.changeCQInputOutput(cqname, mode, action, clToken);
  }
  
  private void printStatus(MetaInfo.Flow app, Collection<Context.Result> r)
  {
    MetaInfo.StatusInfo.Status stat = null;
    try
    {
      ChangeApplicationStateResponse result = this.ctx.changeApplicationState(ActionType.STATUS, app, null);
      stat = ((ApplicationStatusResponse)result).getStatus();
      System.out.println(app.name + " is " + stat);
      if (stat == MetaInfo.StatusInfo.Status.CRASH)
      {
        Set<ExceptionEvent> exceptionEvents = ((ApplicationStatusResponse)result).getExceptionEvents();
        System.out.println("Exception(s) leading to CRASH State:");
        for (ExceptionEvent event : exceptionEvents) {
          System.out.println("    " + event.userString());
        }
      }
      if ((stat == MetaInfo.StatusInfo.Status.DEPLOYED) || (stat == MetaInfo.StatusInfo.Status.RUNNING) || (stat == MetaInfo.StatusInfo.Status.CRASH))
      {
        System.out.println("Status per node....");
        System.out.println(r);
      }
    }
    catch (Exception e)
    {
      logger.error("Failed to get status with exception " + e.getMessage(), e);
    }
  }
  
  public Object compileActionStmt(ActionStmt stmt)
    throws MetaDataRepositoryException
  {
    if (stmt.type.isAccessible())
    {
      MetaInfo.MetaObject metaObject = this.ctx.get(stmt.name, stmt.type);
      if ((metaObject instanceof MetaObjectPermissionChecker)) {
        if (!((MetaObjectPermissionChecker)metaObject).checkPermissionForMetaPropertyVariable(getContext().getSessionID())) {
          logger.error("Problem accessing PropVariable. See if you have right permissions or check if the prop variable exists");
        }
      }
      this.ctx.remoteCallOnObject(ActionType.UNLOAD, metaObject, new Object[0]);
      return null;
    }
    MetaInfo.Flow app = this.ctx.getFlowInCurSchema(stmt.name, stmt.type);
    if (app == null) {
      throw new Warning("cannot find " + stmt.type + " " + stmt.name + " in current namespace <" + this.ctx.getCurNamespace().name + ">");
    }
    Set<UUID> allObjectsInApp = app.getDeepDependencies();
    for (UUID uuid : allObjectsInApp)
    {
      MetaInfo.MetaObject obj = this.metadata_repo.getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
      if ((obj instanceof MetaObjectPermissionChecker)) {
        if (!((MetaObjectPermissionChecker)obj).checkPermissionForMetaPropertyVariable(getContext().getSessionID())) {
          logger.error("Problem accessing PropVariable. See if you have right permissions or check if the prop variable exists");
        }
      }
    }
    ObjectPermission.Action action = null;
    switch (stmt.what)
    {
    case START: 
      action = ObjectPermission.Action.start;
      break;
    case STOP: 
      action = ObjectPermission.Action.stop;
      break;
    case UNDEPLOY: 
      action = ObjectPermission.Action.undeploy;
      break;
    case STATUS: 
      action = ObjectPermission.Action.read;
      break;
    case RESUME: 
      action = ObjectPermission.Action.resume;
      break;
    default: 
      error(stmt.what + " is not an allowable action for ", stmt.name);
    }
    if (!PermissionUtility.checkPermission(app, action, getContext().getSessionID(), false)) {
      error("Insufficient permissions to " + action + " " + stmt.name, stmt.name);
    }
    assert (app.type == stmt.type);
    Collection<Context.Result> r = null;
    if ((stmt.what == ActionType.START) || (stmt.what == ActionType.STOP) || (stmt.what == ActionType.UNDEPLOY) || (stmt.what == ActionType.RESUME))
    {
      Map<String, Object> params = new HashedMap();
      params.put("RECOVERYDESCRIPTION", stmt.recov);
      try
      {
        this.ctx.changeApplicationState(stmt.what, app, params);
      }
      catch (Exception e)
      {
        throw new Warning(e.getMessage(), e);
      }
    }
    else
    {
      r = this.ctx.changeFlowState(stmt.what, app);
    }
    if (stmt.what == ActionType.STATUS) {
      printStatus(app, r);
    }
    return null;
  }
  
  private Pair<MetaInfo.Sorter.SorterRule, Class<?>> makeSorterRule(SorterInOutRule inRule)
    throws MetaDataRepositoryException
  {
    MetaInfo.Stream inStream = this.ctx.getStreamInCurSchema(inRule.inStream);
    if (inStream == null) {
      error("no such stream", inRule.inStream);
    }
    List<Field> flds = checkPartitionFields(Collections.singletonList(inRule.inStreamField), inStream.dataType, inRule.inStream);
    
    String outName = inRule.outStream;
    MetaInfo.Stream outStream = this.ctx.getStreamInCurSchema(outName);
    if (outStream == null)
    {
      checkCompoundStreamName(outName);
      outStream = this.ctx.putStream(false, makeObjectName(outName), inStream.dataType, null, null, null, null, null);
    }
    else if (!outStream.dataType.equals(inStream.dataType))
    {
      error("input and output streams should have same data type", outName);
    }
    MetaInfo.Sorter.SorterRule rule = new MetaInfo.Sorter.SorterRule(inStream.uuid, outStream.uuid, inRule.inStreamField, inStream.dataType);
    
    Class<?> keyFldType = ((Field)flds.get(0)).getType();
    return Pair.make(rule, keyFldType);
  }
  
  public Object compileCreateSorterStmt(CreateSorterStmt stmt)
    throws MetaDataRepositoryException
  {
    List<MetaInfo.Sorter.SorterRule> inOutRules = new ArrayList();
    
    Class<?> keyFldType = null;
    for (SorterInOutRule sr : stmt.inOutRules)
    {
      Pair<MetaInfo.Sorter.SorterRule, Class<?>> r = makeSorterRule(sr);
      inOutRules.add(r.first);
      if (keyFldType == null)
      {
        keyFldType = (Class)r.second;
      }
      else if (keyFldType != r.second)
      {
        String firstFld = ((SorterInOutRule)stmt.inOutRules.get(0)).inStreamField;
        String firstFldType = keyFldType.getCanonicalName();
        String thisFldType = ((Class)r.second).getCanonicalName();
        error("key field has type [" + thisFldType + "] incompatible with type [" + firstFldType + "] of first key field <" + firstFld + ">", sr.inStreamField);
      }
    }
    MetaInfo.Stream errorStream = this.ctx.getStreamInCurSchema(stmt.errorStream);
    if (errorStream == null)
    {
      checkCompoundStreamName(stmt.errorStream);
      errorStream = this.ctx.putStream(false, makeObjectName(stmt.errorStream), UUID.nilUUID(), null, null, null, null, null);
    }
    this.ctx.putSorter(stmt.doReplace, makeObjectName(stmt.name), stmt.sortTimeInterval, inOutRules, errorStream.uuid);
    
    return null;
  }
  
  public Object compileCreateDashboardStatement(CreateDashboardStatement stmt)
    throws MetaDataRepositoryException
  {
    String json = readFile(stmt);
    try
    {
      this.ctx.createDashboard(json, null);
    }
    catch (Exception e)
    {
      throw e;
    }
    return null;
  }
  
  public static String readFile(CreateDashboardStatement stmt)
  {
    File f = new File(stmt.getFileName());
    if (!f.exists()) {
      throw new RuntimeException("File name " + stmt.getFileName() + " does not exist");
    }
    Scanner scanner = null;
    try
    {
      scanner = new Scanner(f);
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
    return json;
  }
  
  public Object execPreparedQuery(ExecPreparedStmt stmt)
    throws MetaDataRepositoryException
  {
    MetaInfo.Query q = (MetaInfo.Query)this.ctx.get(stmt.queryName, EntityType.QUERY);
    if (q == null) {
      error("no such name query", stmt.queryName);
    }
    if (!q.getMetaInfoStatus().isValid()) {
      throw new FatalException("Query is not valid, either metaobject is changed/missing. \nTo make query executable, please recompile query by: \nW (...) > alter namedquery " + stmt.queryName + " recompile.");
    }
    MetaInfo.Query dup = this.ctx.prepareQuery(q, stmt.params);
    return dup;
  }
  
  void validateNestedTQL(File filePointer)
    throws FileNotFoundException
  {
    byte[] encoded;
    try
    {
      encoded = Files.readAllBytes(Paths.get(filePointer.toURI()));
    }
    catch (IOException e)
    {
      throw new FileNotFoundException(e.getMessage());
    }
    String tqlText = Charset.defaultCharset().decode(ByteBuffer.wrap(encoded)).toString();
    
    Lexer lexer = new Lexer(tqlText);
    Grammar parser = new Grammar(lexer, lexer.getSymbolFactory());
    try
    {
      List<Stmt> stmts = parser.parseStmt(false);
      if (stmts != null) {
        for (Stmt stmt : stmts) {
          if ((stmt instanceof LoadFileStmt))
          {
            File file = new File(((LoadFileStmt)stmt).getFileName());
            validateNestedTQL(file);
          }
        }
      }
    }
    catch (Exception e)
    {
      if ((e instanceof FileNotFoundException)) {
        throw ((FileNotFoundException)e);
      }
      throw new CompilationException(e.getMessage());
    }
  }
  
  public Object compileLoadFileStmt(LoadFileStmt loadFileStmt)
    throws Exception
  {
    File filePointer = new File(loadFileStmt.getFileName());
    
    byte[] encoded = Files.readAllBytes(Paths.get(filePointer.toURI()));
    String text = Charset.defaultCharset().decode(ByteBuffer.wrap(encoded)).toString();
    
    validateNestedTQL(filePointer);
    if (HazelcastSingleton.isClientMember()) {
      Tungsten.processWithContext(text, this.ctx);
    } else {
      compile(text, this.ctx, new ExecutionCallback()
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
            Compiler.logger.warn(e.getMessage());
          }
        }
      });
    }
    return null;
  }
  
  public void compileMonitorStmt(MonitorStmt monitorStmt)
    throws MetaDataRepositoryException
  {
    CluiMonitorView.handleMonitorRequest(monitorStmt.params);
  }
  
  public Object compileExportTypesStmt(ExportAppStmt exportAppStmt)
    throws MetaDataRepositoryException
  {
    String namespace = this.ctx.getNamespace(Utility.splitDomain(exportAppStmt.appName)).getName();
    String appName = Utility.splitName(exportAppStmt.appName);
    File targetFile = new File(exportAppStmt.filepath);
    try
    {
      List<MetaInfo.MetaObject> moList = MetadataRepository.getINSTANCE().getAllObjectsInApp(namespace, appName, false, WASecurityManager.TOKEN);
      if (moList.size() == 0) {
        throw new Exception("No application found with the name '" + namespace + "." + appName + "'");
      }
      if (targetFile.isDirectory()) {
        throw new Exception("Specified path is a directory.Specify path of the jar file to be generated.");
      }
      if (targetFile.exists()) {
        targetFile.delete();
      }
      FileOutputStream fos = new FileOutputStream(targetFile);
      if (exportAppStmt.format.equals("JAR")) {
        generateTypesAsJar(moList, fos, namespace);
      }
      fos.close();
      System.out.println("Exported application jar to " + targetFile.getAbsolutePath() + " ");
    }
    catch (Exception e)
    {
      if (targetFile.exists()) {
        targetFile.delete();
      }
      throw new MetaDataRepositoryException("Failed to export types : " + e.getMessage());
    }
    return null;
  }
  
  public void generateTypesAsJar(List<MetaInfo.MetaObject> moList, FileOutputStream fos, String namespace)
    throws Exception
  {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    
    JarOutputStream target = new JarOutputStream(fos, manifest);
    addJarEntryForPackage("wa." + namespace + ".", target);
    for (MetaInfo.MetaObject mo : moList) {
      if (mo.getType() == EntityType.TYPE)
      {
        String tn = "wa." + mo.getNsName() + "." + mo.getName() + "_1_0";
        addJarEntryForTypeClass(tn, target);
        System.out.println(mo.getNsName() + "." + mo.getName() + " exported successfully ");
      }
    }
    target.close();
  }
  
  static void addJarEntryForTypeClass(String typeName, JarOutputStream target)
    throws IOException
  {
    String fqn = typeName.replace('.', '/');
    JarEntry jarEntry = new JarEntry(fqn + ".class");
    jarEntry.setTime(System.currentTimeMillis());
    target.putNextEntry(jarEntry);
    byte[] typeClassByteCode = WALoader.get().getClassBytes(typeName);
    target.write(typeClassByteCode, 0, typeClassByteCode.length);
    target.closeEntry();
  }
  
  static void addJarEntryForPackage(String typeName, JarOutputStream target)
    throws IOException
  {
    String packageName = typeName.replace('.', '/');
    if (!packageName.endsWith("/")) {
      packageName = packageName + "/";
    }
    JarEntry jarEntry = new JarEntry(packageName);
    jarEntry.setTime(System.currentTimeMillis());
    target.putNextEntry(jarEntry);
    target.closeEntry();
  }
  
  public ObjectName makeObjectName(String name)
  {
    return this.ctx.makeObjectName(name);
  }
  
  public ObjectName makeObjectName(String namespace, String name)
  {
    return this.ctx.makeObjectName(namespace, name);
  }
  
  public MetaInfo.Type prepareSource(String adapterTypeName, AdapterDescription sourceProperties, AdapterDescription parserProperties)
    throws MetaDataRepositoryException
  {
    List<Property> handler_props = sourceProperties.getProps();
    
    String parserOrFormatterHandler = parserProperties != null ? parserProperties.getAdapterTypeName() : null;
    List<Property> parserOrFormatterProps = parserProperties != null ? parserProperties.getProps() : Collections.emptyList();
    MetaInfo.PropertyTemplateInfo pt = this.ctx.getAdapterProperties(adapterTypeName);
    if (pt == null) {
      error("cannot load adapter", adapterTypeName);
    }
    if (((pt.requiresParser == true) || (pt.requiresFormatter == true)) && (parserOrFormatterHandler == null)) {
      error(sourceProperties.getAdapterTypeName() + " requires parser.", sourceProperties.getAdapterTypeName());
    }
    if ((pt.adapterType != AdapterType.source) && (pt.adapterType != AdapterType.internal)) {
      error("The specified adapter is a " + pt.adapterType + " adapter, please use a source adapter for sources.", adapterTypeName);
    }
    MetaInfo.PropertyTemplateInfo parserOrFormatterPropertyTemplate = null;
    if (parserOrFormatterHandler != null)
    {
      parserOrFormatterPropertyTemplate = this.ctx.getAdapterProperties(parserOrFormatterHandler);
      if (parserOrFormatterPropertyTemplate == null) {
        error("cannot find parser template", parserOrFormatterHandler);
      }
      if ((parserOrFormatterPropertyTemplate.adapterType != AdapterType.parser) && (parserOrFormatterPropertyTemplate.adapterType != AdapterType.internal)) {
        error("The specified parser is a " + parserOrFormatterPropertyTemplate.adapterType + " adapter, please use a correct parser adapter for sources.", adapterTypeName);
      }
    }
    Map<String, Object> props = combineProperties(handler_props);
    Map<String, Object> parserOrFormatterProperties = combineProperties(parserOrFormatterProps);
    if (parserOrFormatterHandler != null) {
      parserOrFormatterProperties.put("handler", parserOrFormatterPropertyTemplate.className);
    }
    validateAdapterPropsAndValues(pt, props, handler_props);
    if (parserOrFormatterPropertyTemplate != null) {
      validateAdapterPropsAndValues(parserOrFormatterPropertyTemplate, parserOrFormatterProperties, null);
    }
    Class<?> inputTypeClass = parserOrFormatterPropertyTemplate == null ? pt.getInputClass() : parserOrFormatterPropertyTemplate.getInputClass();
    MetaInfo.Type inputType = getTypeForCLass(inputTypeClass);
    
    return inputType;
  }
  
  public Object createFilteredSource(CreateSourceOrTargetStmt createSourceWithImplicitCQStmt)
    throws MetaDataRepositoryException
  {
    return new SourceSideFilteringManager().receive(new SourceSideFilteringHandler(), this, createSourceWithImplicitCQStmt);
  }
  
  public Object compileAlterStmt(AlterStmt alterStmt)
  {
    try
    {
      MetaInfo.Stream streamMO = (MetaInfo.Stream)this.ctx.get(makeObjectName(alterStmt.getObjectName()).getFullName(), EntityType.STREAM);
      if ((alterStmt.getPartitionBy() != null) && (!alterStmt.getPartitionBy().isEmpty()))
      {
        checkPartitionFields(alterStmt.getPartitionBy(), streamMO.getDataType(), streamMO.getFullName());
        if (alterStmt.getEnablePartitionBy().booleanValue())
        {
          if (alterStmt.getPartitionBy() != null)
          {
            Set partitionBySet = new LinkedHashSet(streamMO.partitioningFields);
            partitionBySet.addAll(alterStmt.getPartitionBy());
            streamMO.partitioningFields = new ArrayList(partitionBySet);
          }
          else
          {
            throw new CompilationException("Alter operation failed, please retry again");
          }
        }
        else {
          streamMO.partitioningFields.removeAll(alterStmt.getPartitionBy());
        }
      }
      if (alterStmt.getEnablePersistence() != null) {
        if (alterStmt.getEnablePersistence().booleanValue())
        {
          String namespace = Utility.splitDomain(alterStmt.getPersistencePolicy().getFullyQualifiedNameOfPropertyset());
          String name = Utility.splitName(alterStmt.getPersistencePolicy().getFullyQualifiedNameOfPropertyset());
          MetaInfo.PropertySet kpset = (MetaInfo.PropertySet)this.metadata_repo.getMetaObjectByName(EntityType.PROPERTYSET, namespace, name, null, WASecurityManager.TOKEN);
          if (kpset == null) {
            return null;
          }
          streamMO.setPset(kpset.getFullName());
          setupKafka(streamMO, false);
        }
        else
        {
          try
          {
            destroyTopic(streamMO);
          }
          catch (Exception e) {}
          streamMO.setPset(null);
          streamMO.setPropertySet(null);
        }
      }
      this.ctx.updateMetaObject(streamMO);
    }
    catch (Exception e)
    {
      throw new AlterException(e.getMessage(), e);
    }
    return null;
  }
  
  private void destroyTopic(MetaInfo.Stream streamMO)
  {
    try
    {
      if (HazelcastSingleton.isClientMember())
      {
        RemoteCall deleteTopic_executor = KafkaStreamUtils.getDeleteTopicExecutor(streamMO);
        DistributedExecutionManager.execOnAny(HazelcastSingleton.get(), deleteTopic_executor);
      }
      else
      {
        KafkaStreamUtils.deleteTopic(streamMO);
      }
    }
    catch (Exception e)
    {
      throw new RuntimeException("Failed to delete kafka topics associated with stream: " + streamMO.getFullName() + ", Reason: " + e.getMessage(), e);
    }
  }
  
  public static abstract interface ExecutionCallback
  {
    public abstract void execute(Stmt paramStmt, Compiler paramCompiler)
      throws Exception;
  }
}
