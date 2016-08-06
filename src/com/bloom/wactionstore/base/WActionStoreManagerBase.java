package com.bloom.wactionstore.base;

import com.bloom.wactionstore.CheckpointManager;
import com.bloom.wactionstore.Utility;
import com.bloom.wactionstore.WActionStore;
import com.bloom.wactionstore.WActionStoreManager;
import com.bloom.wactionstore.checkpoints.Manager;
import com.bloom.wactionstore.constants.Capability;
import com.bloom.wactionstore.constants.NameType;
import com.bloom.wactionstore.elasticsearch.IndexOperationsManager;
import com.bloom.wactionstore.exceptions.CapabilityException;
import com.bloom.wactionstore.exceptions.QuerySchemaException;
import com.bloom.wactionstore.exceptions.WActionStoreException;
import com.bloom.wactionstore.exceptions.WActionStoreMissingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.fge.jsonschema.core.report.ProcessingMessage;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;

public abstract class WActionStoreManagerBase
  implements WActionStoreManager
{
  private static final Class<WActionStoreManagerBase> thisClass = WActionStoreManagerBase.class;
  private static final Logger logger = Logger.getLogger(thisClass);
  private static final String CLASS_NAME = thisClass.getSimpleName();
  private static final JsonSchema wActionQuerySchema = Utility.createJsonSchema("/WActionStoreQuery.json");
  private static final Level invalidQueryLoggingLevel = Level.DEBUG;
  private static final boolean isInvalidQueryLoggingEnabled = logger.isEnabledFor(invalidQueryLoggingLevel);
  private final EnumSet<Capability> capabilities = EnumSet.noneOf(Capability.class);
  private final String providerName;
  private final String instanceName;
  private final Manager checkpointManager;
  private IndexOperationsManager indexManager = null;
  
  protected WActionStoreManagerBase(String providerName, String instanceName, Capability... capabilities)
  {
    this.providerName = providerName;
    this.instanceName = instanceName;
    this.checkpointManager = new Manager(this);
    this.indexManager = new IndexOperationsManager(this);
    Collections.addAll(this.capabilities, capabilities);
  }
  
  public IndexOperationsManager getIndexManager()
  {
    return this.indexManager;
  }
  
  public String getInstanceName()
  {
    return this.instanceName;
  }
  
  public String getProviderName()
  {
    return this.providerName;
  }
  
  public Set<Capability> getCapabilities()
  {
    return Collections.unmodifiableSet(this.capabilities);
  }
  
  public CheckpointManager getCheckpointManager()
  {
    return this.checkpointManager;
  }
  
  public WActionStore getOrCreate(String wActionStoreName, Map<String, Object> properties)
  {
    WActionStore result = getUsingAlias(wActionStoreName, properties);
    if (result == null)
    {
      result = create(wActionStoreName, properties);
      if (result == null)
      {
        result = get(wActionStoreName, properties);
        if (result == null) {
          throw new WActionStoreException(String.format("Unable to create WActionStore '%s'", new Object[] { wActionStoreName }));
        }
      }
    }
    return result;
  }
  
  protected void validateQuery(JsonNode queryJson)
    throws WActionStoreException
  {
    if (wActionQuerySchema == null) {
      throw new IllegalArgumentException("wActionQuerySchema");
    }
    if (queryJson == null) {
      throw new IllegalArgumentException("queryJson");
    }
    ProcessingReport report = wActionQuerySchema.validateUnchecked(queryJson);
    if ((report != null) && (!report.isSuccess()))
    {
      processInvalidQuery(queryJson, report);
      return;
    }
    verifyPresent(queryJson, "select");
    verifyMissing(queryJson, "delete");
    
    validateCapability(queryJson, "select", Capability.SELECT);
    validateCapability(queryJson, "from", Capability.FROM);
    validateCapability(queryJson, "where", Capability.WHERE);
    validateCapability(queryJson, "orderby", Capability.ORDER_BY);
    validateCapability(queryJson, "groupby", Capability.GROUP_BY);
    validateCapability(queryJson, "having", Capability.HAVING);
    validateSelectClause(queryJson);
    validateFromClause(queryJson);
  }
  
  protected void validateDelete(JsonNode queryJson)
    throws WActionStoreException
  {
    if (wActionQuerySchema == null) {
      throw new IllegalArgumentException("wActionQuerySchema");
    }
    if (queryJson == null) {
      throw new IllegalArgumentException("queryJson");
    }
    ProcessingReport report = wActionQuerySchema.validateUnchecked(queryJson);
    if ((report != null) && (!report.isSuccess()))
    {
      processInvalidQuery(queryJson, report);
      return;
    }
    verifyPresent(queryJson, "delete");
    verifyMissing(queryJson, "select");
    
    validateCapability(queryJson, "delete", Capability.DELETE);
    validateCapability(queryJson, "from", Capability.FROM);
    validateCapability(queryJson, "where", Capability.WHERE);
    verifyMissing(queryJson, "orderby");
    verifyMissing(queryJson, "groupby");
    verifyMissing(queryJson, "having");
    validateDeleteClause(queryJson);
    validateFromClause(queryJson);
  }
  
  private static void processInvalidQuery(JsonNode queryJson, Iterable<ProcessingMessage> report)
    throws QuerySchemaException
  {
    int messageNumber;
    if (isInvalidQueryLoggingEnabled)
    {
      logger.log(invalidQueryLoggingLevel, String.format("Query : %s", new Object[] { queryJson }));
      messageNumber = 1;
      for (ProcessingMessage message : report)
      {
        JsonNode instanceNode = message.asJson().path("instance").path("pointer");
        String instanceNodeName = instanceNode.isMissingNode() ? "<Unknown>" : instanceNode.asText().replace("/", "");
        String messageText = message.getMessage();
        logger.log(invalidQueryLoggingLevel, String.format("Error %2d: %s : %s", new Object[] { Integer.valueOf(messageNumber), instanceNodeName, messageText }));
        messageNumber++;
      }
    }
    throw new QuerySchemaException(queryJson, report);
  }
  
  private void validateCapability(JsonNode queryJson, String feature, Capability capability)
    throws CapabilityException
  {
    if ((!queryJson.path(feature).isMissingNode()) && (!this.capabilities.contains(capability))) {
      throw new CapabilityException(getProviderName(), capability.name());
    }
  }
  
  private static void verifyPresent(JsonNode queryJson, String feature)
    throws CapabilityException
  {
    if (queryJson.path(feature).isMissingNode()) {
      throw new QuerySchemaException(queryJson, String.format("JSON does not contain '%s' node, which is required", new Object[] { feature }));
    }
  }
  
  private static void verifyMissing(JsonNode queryJson, String feature)
    throws CapabilityException
  {
    if (!queryJson.path(feature).isMissingNode()) {
      throw new QuerySchemaException(queryJson, String.format("JSON contains '%s' node, which is not allowed", new Object[] { feature }));
    }
  }
  
  private void validateSelectClause(JsonNode queryJson)
    throws CapabilityException
  {
    if (!this.capabilities.contains(Capability.PROJECTIONS))
    {
      ArrayNode selectNode = (ArrayNode)queryJson.path("select");
      if ((selectNode.size() != 1) || (!"*".equals(selectNode.get(0).asText()))) {
        throw new CapabilityException(getProviderName(), Capability.PROJECTIONS.name());
      }
    }
  }
  
  private static void validateDeleteClause(JsonNode queryJson)
    throws CapabilityException
  {
    ArrayNode deleteNode = (ArrayNode)queryJson.path("delete");
    if ((deleteNode.size() != 1) || (!"*".equals(deleteNode.get(0).asText()))) {
      throw new QuerySchemaException(queryJson, String.format("JSON contains '%s' node, which is not allowed", new Object[] { "*" }));
    }
  }
  
  private void validateFromClause(JsonNode queryJson)
    throws WActionStoreException
  {
    validateMultipleFrom(queryJson);
    validateWActionStoreNames(queryJson);
  }
  
  private void validateMultipleFrom(JsonNode queryJson)
    throws CapabilityException
  {
    if (!this.capabilities.contains(Capability.FROM_MULTIPLE))
    {
      ArrayNode fromNode = (ArrayNode)queryJson.path("from");
      if (fromNode.size() != 1) {
        throw new CapabilityException(getProviderName(), Capability.FROM_MULTIPLE.name());
      }
    }
  }
  
  private void validateWActionStoreNames(JsonNode queryJson)
    throws WActionStoreMissingException
  {
    Set<String> wActionStoreNames = Sets.newHashSet(getNames());
    ImmutableOpenMap<String, ImmutableOpenMap<String, AliasMetaData>> map_alias = this.indexManager.getAliasMapFromES();
    for (JsonNode fromName : queryJson.path("from"))
    {
      String wActionStoreName = fromName.asText();
      String translatedName = translateName(NameType.WACTIONSTORE, wActionStoreName);
      if ((!wActionStoreNames.contains(translatedName)) && (!map_alias.containsKey("search_alias_" + translatedName))) {
        throw new WActionStoreMissingException(getProviderName(), getInstanceName(), wActionStoreName);
      }
    }
  }
  
  public String toString()
  {
    return CLASS_NAME + '{' + "provider=" + this.providerName + "instance=" + this.instanceName + "capabilities=" + this.capabilities + '}';
  }
}
