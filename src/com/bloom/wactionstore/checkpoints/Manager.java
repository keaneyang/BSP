package com.bloom.wactionstore.checkpoints;

import com.bloom.runtime.BaseServer;
import com.bloom.wactionstore.CheckpointManager;
import com.bloom.wactionstore.DataType;
import com.bloom.wactionstore.InternalType;
import com.bloom.wactionstore.WAction;
import com.bloom.wactionstore.WActionQuery;
import com.bloom.wactionstore.WActionStore;
import com.bloom.wactionstore.WActionStoreManager;
import com.bloom.wactionstore.constants.NameType;
import com.bloom.wactionstore.exceptions.WActionStoreException;
import com.bloom.wactionstore.exceptions.WActionStoreMissingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.bloom.recovery.Path;
import com.bloom.recovery.Position;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import javax.xml.bind.DatatypeConverter;
import org.apache.log4j.Logger;

public class Manager
  implements CheckpointManager
{
  private static final Class<Manager> thisClass = Manager.class;
  private static final Logger logger = Logger.getLogger(thisClass);
  private static final String nodeName = BaseServer.getServerName();
  private final WActionStoreManager manager;
  private final Map<String, Details> detailsMap = new HashMap();
  private DataType checkpointType;
  
  public Manager(WActionStoreManager manager)
  {
    this.manager = manager;
  }
  
  public void add(String wActionStoreName, WAction wAction, Position wActionPosition)
  {
    if ((wActionPosition != null) && (wActionPosition.values() != null))
    {
      Details details = getWActionStoreDetails(wActionStoreName);
      wAction.put("$node_name", nodeName);
      wAction.put("$checkpoint", details.timestamp);
      details.wActions += 1L;
      if (logger.isDebugEnabled())
      {
        JsonNode wActionIDJson = wAction.get("$id");
        if (wActionIDJson != null)
        {
          String wActionID = wActionIDJson.asText();
          logger.debug(String.format("WAction %d in checkpoint #%d is '%s'", new Object[] { Long.valueOf(details.wActions), Long.valueOf(details.timestamp), wActionID }));
        }
      }
      details.position.mergeHigherPositions(wActionPosition);
    }
  }
  
  private Details getWActionStoreDetails(String wActionStoreName)
  {
    Details result = (Details)this.detailsMap.get(wActionStoreName);
    if (result == null)
    {
      result = new Details();
      this.detailsMap.put(wActionStoreName, result);
    }
    return result;
  }
  
  public void start(String wActionStoreName)
  {
    Details details = getWActionStoreDetails(wActionStoreName);
    details.start();
    logger.debug(String.format("Starting new checkpoint #%d for WActionStore '%s'", new Object[] { Long.valueOf(details.timestamp), wActionStoreName }));
  }
  
  public void flush(WActionStore wActionStore)
  {
    String wActionStoreName = wActionStore.getName();
    Map<String, Object> properties = wActionStore.getProperties();
    Details details;
    synchronized (this)
    {
      if (this.checkpointType == null) {
        this.checkpointType = InternalType.CHECKPOINT.getDataType(this.manager, properties);
      }
      details = getCurrentCheckpoint(wActionStoreName);
    }
    if ((details != null) && (details.position != null) && (details.position.values() != null))
    {
      Collection<Path> paths = details.position.values();
      WAction checkpointWAction = makeCheckpointWAction(wActionStoreName, paths, details);
      this.checkpointType.insert(checkpointWAction);
      wActionStore.fsync();
      this.checkpointType.fsync();
      logger.info(String.format("Wrote checkpoint #%d with %d paths for WActionStore '%s' for %d WActions", new Object[] { Long.valueOf(details.timestamp), Integer.valueOf(paths.size()), wActionStoreName, Long.valueOf(details.wActions) }));
    }
  }
  
  public void remove(String wActionStoreName)
  {
    String actualName = this.manager.translateName(NameType.WACTIONSTORE, wActionStoreName);
    JsonNode deleteStatement = com.bloom.wactionstore.Utility.readTree(String.format("{ \"delete\": [ \"*\" ],  \"from\":  [ \"$Internal.CHECKPOINT\" ],  \"where\": {     \"oper\": \"eq\",    \"attr\": \"WActionStoreName\",    \"value\": \"%s\" }}", new Object[] { actualName }));
    doDelete(deleteStatement, wActionStoreName);
    this.detailsMap.remove(wActionStoreName);
  }
  
  private void deleteWActionsPastCheckpoint(String wActionStoreName, long timestamp)
  {
    String actualName = this.manager.translateName(NameType.WACTIONSTORE, wActionStoreName);
    JsonNode deleteStatement = com.bloom.wactionstore.Utility.readTree(String.format("{ \"delete\": [ \"*\" ],  \"from\":  [ \"%s\" ],  \"where\": {     \"and\": [    { \"oper\": \"eq\",      \"attr\": \"$node_name\",      \"value\": \"%s\" },    { \"oper\": \"gt\",      \"attr\": \"$checkpoint\",      \"value\": %d }    ]  }}", new Object[] { actualName, nodeName, Long.valueOf(timestamp) }));
    doDelete(deleteStatement, wActionStoreName);
  }
  
  private void doDelete(JsonNode deleteStatement, String wActionStoreName)
  {
    try
    {
      this.manager.delete(deleteStatement);
    }
    catch (WActionStoreMissingException ignored) {}catch (WActionStoreException exception)
    {
      logger.warn(String.format("Failed to remove checkpoint for WActionStore '%s'", new Object[] { wActionStoreName }), exception);
    }
  }
  
  private WAction makeCheckpointWAction(String wActionStoreName, Collection<Path> paths, Details details)
  {
    String actualName = this.manager.translateName(NameType.WACTIONSTORE, wActionStoreName);
    WAction result = new WAction();
    Path[] pathsArray = (Path[])paths.toArray(new Path[paths.size()]);
    String pathsString = com.bloom.wactionstore.Utility.serializeToBase64String(pathsArray);
    
    result.put("$id", getHash(new String[] { nodeName + '.' + actualName }));
    result.put("$timestamp", details.timestamp);
    result.put("NodeName", nodeName);
    result.put("WActionStoreName", actualName);
    result.put("WActionCount", details.wActions);
    result.put("PathCount", paths.size());
    result.put("Paths", pathsString);
    
    return result;
  }
  
  private static String getHash(String... values)
  {
    String result = null;
    try
    {
      MessageDigest md = MessageDigest.getInstance("MD5");
      for (String value : values)
      {
        byte[] valueBytes = value.getBytes();
        md.update(valueBytes);
      }
      byte[] digest = md.digest();
      result = DatatypeConverter.printHexBinary(digest);
    }
    catch (NoSuchAlgorithmException exception)
    {
      logger.error(String.format("Cannot create hash using algorithm '%s'", new Object[] { "MD5" }), exception);
    }
    return result;
  }
  
  private Details getCurrentCheckpoint(String wActionStoreName)
  {
    Details result = null;
    Details details = (Details)this.detailsMap.get(wActionStoreName);
    if (details != null)
    {
      result = new Details(details);
      details.start();
    }
    return result;
  }
  
  private long getLastTimestamp(String wActionStoreName)
  {
    long result = 0L;
    try
    {
      String actualName = this.manager.translateName(NameType.WACTIONSTORE, wActionStoreName);
      ObjectNode queryJson = (ObjectNode)com.bloom.wactionstore.Utility.readTree(String.format("{ \"select\":[ \"$timestamp\" ],  \"from\":  [ \"$Internal.CHECKPOINT\" ],  \"where\": {     \"and\": [    { \"oper\": \"eq\",      \"attr\": \"NodeName\",      \"value\": \"%s\" },    { \"oper\": \"eq\",      \"attr\": \"WActionStoreName\",      \"value\": \"%s\" }    ]  }}", new Object[] { nodeName, actualName }));
      WActionQuery query = this.manager.prepareQuery(queryJson);
      Iterator<WAction> checkpoints = query.execute().iterator();
      if (checkpoints.hasNext())
      {
        WAction wAction = (WAction)checkpoints.next();
        result = wAction.get("$timestamp").asLong(0L);
      }
    }
    catch (WActionStoreMissingException exception)
    {
      String checkpointStoreName = InternalType.CHECKPOINT.getWActionStoreName();
      if (!checkpointStoreName.equals(exception.wActionStoreName)) {
        throw exception;
      }
    }
    return result;
  }
  
  public Position get(String wActionStoreName)
  {
    Position result = new Position();
    boolean isCheckpointFound = false;
    try
    {
      String actualName = this.manager.translateName(NameType.WACTIONSTORE, wActionStoreName);
      String queryString = String.format("{ \"select\":[ \"Paths\" ],  \"from\":  [ \"$Internal.CHECKPOINT\" ],  \"where\":   { \"oper\": \"eq\",    \"attr\": \"WActionStoreName\",    \"value\": \"%s\" } },  \"orderby\": [    { \"attr\": \"$timestamp\", \"ascending\": true }  ]}", new Object[] { actualName });
      WActionQuery query = this.manager.prepareQuery(com.bloom.wactionstore.Utility.readTree(queryString));
      for (WAction checkpoint : query.execute())
      {
        isCheckpointFound = true;
        String pathsString = checkpoint.get("Paths").asText();
        Object pathsObject = com.bloom.wactionstore.Utility.serializeFromBase64String(pathsString);
        if (!(pathsObject instanceof Path[]))
        {
          logger.error(String.format("Malformed checkpoint for WActionStore '%s': %s", new Object[] { wActionStoreName, checkpoint }));
        }
        else
        {
          Path[] pathsArray = (Path[])pathsObject;
          if (result == null) {
            result = new Position(new HashSet(Arrays.asList(pathsArray)));
          } else {
            result.mergeHigherPositions(new Position(new HashSet(Arrays.asList(pathsArray))));
          }
        }
      }
      if (isCheckpointFound)
      {
        logger.info(String.format("Read a valid checkpoint for WActionStore '%s'", new Object[] { wActionStoreName }));
        Details details = getWActionStoreDetails(wActionStoreName);
        details.position = result;
        if (logger.isDebugEnabled()) {
          com.bloom.utility.Utility.prettyPrint(details.position);
        }
      }
      else
      {
        logger.info(String.format("Checkpoint not available for WActionStore '%s'", new Object[] { wActionStoreName }));
      }
    }
    catch (WActionStoreMissingException ignored)
    {
      logger.warn(String.format("Checkpoint not available for WActionStore '%s'", new Object[] { wActionStoreName }));
    }
    return result;
  }
  
  public void removeInvalidWActions(String wActionStoreName)
  {
    long lastTimestamp = getLastTimestamp(wActionStoreName);
    
    WActionStore wActionStore = this.manager.get(wActionStoreName, null);
    if (wActionStore == null) {
      return;
    }
    long deletedWActionCount = wActionStore.getWActionCount();
    deleteWActionsPastCheckpoint(wActionStoreName, lastTimestamp);
    deletedWActionCount -= wActionStore.getWActionCount();
    if (deletedWActionCount > 0L) {
      logger.debug(String.format("Purged %d WActions past checkpoint #%d in WActionStore '%s'", new Object[] { Long.valueOf(deletedWActionCount), Long.valueOf(lastTimestamp), wActionStoreName }));
    }
  }
  
  public void closeWactionStore(String wActionStoreName)
  {
    this.detailsMap.remove(wActionStoreName);
  }
  
  public void writeBlankCheckpoint(String wActionStoreName)
  {
    WActionStore wActionStore = this.manager.get(wActionStoreName, null);
    Details details = getWActionStoreDetails(wActionStoreName);
    details.position = new Position();
    flush(wActionStore);
    logger.info(String.format("Wrote checkpoint #%d with %d paths for WActionStore '%s' for %d WActions", new Object[] { Long.valueOf(details.timestamp), Integer.valueOf(0), wActionStoreName, Long.valueOf(details.wActions) }));
  }
}
