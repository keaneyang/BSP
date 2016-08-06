package com.bloom.proc;

import com.bloom.drop.DropMetaObject;
import com.bloom.drop.DropMetaObject.DropRule;
import com.bloom.drop.DropMetaObject.DropType;
import com.bloom.intf.SourceMetadataProvider;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.runtime.Context;
import com.bloom.runtime.TypeGenerator;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.TypeDefOrName;
import com.bloom.runtime.compiler.Compiler.ExecutionCallback;
import com.bloom.runtime.compiler.stmts.CreateTypeStmt;
import com.bloom.runtime.compiler.stmts.Stmt;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.components.Flow;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Source;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.security.WASecurityManager;
import com.bloom.usagemetrics.MetricsCollector;
import com.bloom.usagemetrics.SourceMetricsCollector;
import com.bloom.uuid.UUID;
import com.bloom.recovery.Position;
import com.bloom.recovery.SourcePosition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.log4j.Logger;

public abstract class SourceProcess
  extends BaseProcess
{
  private static final Logger logger = Logger.getLogger(SourceProcess.class);
  protected UUID sourceUUID;
  protected String distributionID;
  protected SourceMetricsCollector metricsCollector;
  protected Flow ownerFlow;
  protected Map<String, UUID> typeUUIDForCDCTables;
  
  public SourceProcess()
  {
    this.sourceUUID = null;
    
    this.metricsCollector = new MetricsCollector();
  }
  
  public void onCompile(Compiler c, MetaInfo.Source sourceInfo)
    throws Exception
  {}
  
  public void onUndeploy()
    throws Exception
  {
    if (this.typeUUIDForCDCTables != null)
    {
      MetaInfo.Source sourceInfo = (MetaInfo.Source)MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.sourceUUID, WASecurityManager.TOKEN);
      
      Context ctx = Context.createContext(WASecurityManager.TOKEN);
      for (Map.Entry<String, UUID> entry : this.typeUUIDForCDCTables.entrySet())
      {
        MetaInfo.Type typeObject = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID((UUID)entry.getValue(), WASecurityManager.TOKEN);
        List<String> result = DropMetaObject.DropType.drop(ctx, typeObject, DropMetaObject.DropRule.NONE, WASecurityManager.TOKEN);
        if (logger.isDebugEnabled()) {
          logger.debug(result.toString());
        }
      }
      this.typeUUIDForCDCTables = null;
    }
  }
  
  public synchronized void init(Map<String, Object> properties, Map<String, Object> parserProperties, UUID uuid, String distributionID)
    throws Exception
  {
    properties.put("UUID", uuid);
    super.init(properties, parserProperties);
    this.sourceUUID = uuid;
    this.distributionID = distributionID;
    String mode = (String)properties.get("SessionType");
    if ((mode == null) || (!mode.equalsIgnoreCase("METADATA"))) {
      createTypesFromTableDef();
    }
  }
  
  public Position getCheckpoint()
  {
    return null;
  }
  
  public Flow getOwnerFlow()
  {
    return this.ownerFlow;
  }
  
  public void setOwnerFlow(Flow ownerFlow)
  {
    this.ownerFlow = ownerFlow;
  }
  
  public void close()
    throws Exception
  {
    super.close();
    this.metricsCollector.flush();
  }
  
  public void setSourcePosition(SourcePosition sourcePosition) {}
  
  public boolean requiresPartitionedSourcePosition()
  {
    return false;
  }
  
  public void createTypesFromTableDef()
    throws Exception
  {
    if ((this instanceof SourceMetadataProvider))
    {
      Map<String, TypeDefOrName> metaMap = ((SourceMetadataProvider)this).getMetadata();
      if (metaMap != null)
      {
        MetaInfo.Source sourceInfo = (MetaInfo.Source)MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.sourceUUID, WASecurityManager.TOKEN);
        Context ctx = Context.createContext(WASecurityManager.TOKEN);
        this.typeUUIDForCDCTables = new HashMap();
        for (Map.Entry<String, TypeDefOrName> entry : metaMap.entrySet())
        {
          String modifiedTableName = TypeGenerator.getValidJavaIdentifierName((String)entry.getKey());
          String typeName = TypeGenerator.getTypeName(sourceInfo.nsName, sourceInfo.name, (String)entry.getKey());
          
          TypeDefOrName typeDef = (TypeDefOrName)entry.getValue();
          String typeNameWithOutNameSpace = typeName.substring(typeName.indexOf('.') + 1);
          MetaInfo.Type dataType = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.TYPE, sourceInfo.nsName, typeNameWithOutNameSpace, null, WASecurityManager.TOKEN);
          if (dataType != null)
          {
            if ((!modifiedTableName.equals(entry.getKey())) && (metaMap.containsKey(modifiedTableName)))
            {
              logger.warn("Type " + typeName + " already exists. Cannot create type for table " + (String)entry.getKey() + "(Modified table name after replacing special character(s) is " + modifiedTableName + ").");
            }
            else
            {
              this.typeUUIDForCDCTables.put(entry.getKey(), dataType.uuid);
              if (logger.isDebugEnabled()) {
                logger.debug("Type " + dataType.name + " already exists. Skipping type creation for table " + (String)entry.getKey());
              }
            }
          }
          else
          {
            CreateTypeStmt ctStmt = new CreateTypeStmt(typeName, Boolean.valueOf(true), typeDef);
            CallBackExecutor cb = new CallBackExecutor();
            Compiler.compile(ctStmt, ctx, cb);
            this.typeUUIDForCDCTables.put(entry.getKey(), cb.uuid);
          }
        }
        ctx = null;
      }
    }
  }
  
  public UUID getTypeUUIDForCDCTables(String table)
  {
    return (UUID)this.typeUUIDForCDCTables.get(table);
  }
  
  private class CallBackExecutor
    implements Compiler.ExecutionCallback
  {
    UUID uuid;
    
    private CallBackExecutor() {}
    
    public void execute(Stmt stmt, Compiler compiler)
      throws Exception
    {
      this.uuid = ((UUID)compiler.compileStmt(stmt));
    }
  }
}
