package com.bloom.wactionstore.elasticsearch;

import com.bloom.persistence.WactionStore;
import com.bloom.runtime.components.EntityType;
import com.bloom.wactionstore.BackgroundTask;
import com.bloom.wactionstore.Utility;
import com.bloom.wactionstore.WAction;
import com.bloom.wactionstore.base.DataTypeBase;
import com.bloom.wactionstore.constants.Constants;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.bloom.common.exc.SystemException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.jctools.queues.MpscCompoundQueue;

public class DataType
  extends DataTypeBase<WActionStore>
{
  private static final Class<DataType> thisClass = DataType.class;
  private static final Logger logger = Logger.getLogger(thisClass);
  private static final int QUEUED_WACTIONS_MIN = 100;
  private static final int QUEUED_WACTIONS_MAX = 1024;
  private static final long QUEUED_WACTIONS_TIMEOUT_MS = 1000L;
  private static final long BACKGROUND_THREAD_TIMEOUT = 3000L;
  private final BlockingQueue<WAction> queuedWActions = new MpscCompoundQueue(1024);
  private final BackgroundTask backgroundTask;
  
  DataType(WActionStore wActionStore, String name, JsonNode schema, WactionStore ws)
  {
    super(wActionStore, name, schema);
    this.backgroundTask = new BackgroundTask(wActionStore.getName(), getName(), 3000L, ws)
    {
      public void run()
      {
        List<WAction> wActions = new ArrayList(1024);
        while (!isTerminationRequested())
        {
          waitForSignal();
          boolean first = true;
          for (;;)
          {
            DataType.this.queuedWActions.drainTo(wActions);
            if (wActions.size() <= (first ? 0 : 100)) {
              break;
            }
            DataType.this.insert(wActions, getWactionStoreObject());
            wActions.clear();
            first = false;
          }
        }
        DataType.this.queuedWActions.drainTo(wActions);
        if (!wActions.isEmpty()) {
          DataType.this.insert(wActions, getWactionStoreObject());
        }
      }
    };
  }
  
  public boolean insert(List<WAction> wActions, WactionStore ws)
  {
    if (Constants.IsEsRollingIndexEnabled.equalsIgnoreCase("true"))
    {
      WActionStore wActionStore = (WActionStore)getWActionStore();
      WActionStoreManager manager = (WActionStoreManager)wActionStore.getManager();
      IndexOperationsManager indexManager = manager.getIndexManager();
      indexManager.checkForRollingIndex(wActionStore);
    }
    boolean result = false;
    String errorMessage = null;
    Exception processingException = null;
    int wActionCount = 0;
    
    long elapsedTime = 0L;
    try
    {
      BulkRequestBuilder bulkRequest = createBulkInsertRequest(wActions);
      wActionCount = bulkRequest.numberOfActions();
      if (wActionCount > 0)
      {
        BulkResponse bulkResponse = (BulkResponse)bulkRequest.execute().actionGet();
        elapsedTime = bulkResponse.getTookInMillis();
        if (bulkResponse.hasFailures())
        {
          if (ws != null)
          {
            String failureReason = "";
            if (bulkResponse.buildFailureMessage().contains("UnavailableShardsException")) {
              failureReason = failureReason + " because elasticsearch index " + ((WActionStore)getWActionStore()).getIndexName() + " is throwing UnavailableShardsException. Please check the index status";
            }
            logger.warn("Failed to insert wactions into " + ws.getMetaFullName() + failureReason);
            ws.notifyAppMgr(EntityType.WACTIONSTORE, ws.getMetaName(), ws.getMetaID(), new SystemException("Failed to insert wactions into " + ws.getMetaFullName() + failureReason), "Insert into waction store failure", new Object[0]);
          }
          return false;
        }
        result = true;
      }
    }
    catch (MapperParsingException|JsonProcessingException exception)
    {
      processingException = exception;
    }
    reportInsertResults(result, wActionCount, errorMessage, processingException, elapsedTime);
    return result;
  }
  
  private BulkRequestBuilder createBulkInsertRequest(Iterable<WAction> wActions)
    throws JsonProcessingException
  {
    WActionStore wActionStore = (WActionStore)getWActionStore();
    WActionStoreManager manager = (WActionStoreManager)wActionStore.getManager();
    String wActionStoreName = getName();
    Client client = manager.getClient();
    
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    bulkRequest.setTimeout(new TimeValue(Constants.ELASTICSEARCH_INTIAL_TIME_TO_WAIT_UNTIL_INDEX_IS_READY, TimeUnit.MINUTES));
    ObjectMapper mapper = Utility.objectMapper;
    Map<String, Object> wActionMap = new HashMap(1);
    for (WAction wAction : wActions) {
      if (isValid(wAction))
      {
        String indexName = wActionStore.getIndexName();
        IndexRequestBuilder index = client.prepareIndex(indexName, wActionStoreName);
        wActionMap = (Map)mapper.treeToValue(wAction, wActionMap.getClass());
        bulkRequest.add(index.setSource(wActionMap));
        if (logger.isDebugEnabled()) {
          logger.debug(String.format("Inserting WAction into WActionStore '%s' - '%s'", new Object[] { wActionStoreName, wAction }));
        }
      }
    }
    return bulkRequest;
  }
  
  private void reportInsertResults(boolean result, int wActionCount, String errorMessage, Exception processingException, long elapsedTime)
  {
    if (processingException != null) {
      logger.warn(String.format("Caught mapping exception - %s", new Object[] { processingException.getMessage() }));
    }
    if (!result) {
      logger.error(String.format("Failed to insert %d WActions - %s", new Object[] { Integer.valueOf(wActionCount), errorMessage }));
    }
    if ((elapsedTime > 750L) && (logger.isInfoEnabled()))
    {
      String wActionStoreName = ((WActionStore)getWActionStore()).getName();
      logger.info(String.format("Inserted %d WActions successfully into WActionStore '%s' in %d ms", new Object[] { Integer.valueOf(wActionCount), wActionStoreName, Long.valueOf(elapsedTime) }));
    }
    if ((result) && (logger.isDebugEnabled()))
    {
      String wActionStoreName = ((WActionStore)getWActionStore()).getName();
      logger.debug(String.format("Inserted %d WActions successfully into WActionStore '%s' in %d ms", new Object[] { Integer.valueOf(wActionCount), wActionStoreName, Long.valueOf(elapsedTime) }));
    }
  }
  
  public void queue(WAction wAction)
  {
    if (!this.backgroundTask.isAlive()) {
      this.backgroundTask.start();
    }
    if (this.queuedWActions.remainingCapacity() <= 1) {
      this.backgroundTask.signalBackgroundThread();
    }
    for (;;)
    {
      try
      {
        this.queuedWActions.put(wAction);
      }
      catch (InterruptedException ignored) {}
    }
  }
  
  public void queue(Iterable<WAction> wActions)
  {
    for (WAction waction : wActions) {
      queue(waction);
    }
  }
  
  public void flush()
  {
    flushQueuedWActions();
    WActionStore wActionStore = (WActionStore)getWActionStore();
    wActionStore.flush();
  }
  
  public void fsync()
  {
    flushQueuedWActions();
    WActionStore wActionStore = (WActionStore)getWActionStore();
    wActionStore.fsync();
  }
  
  private void flushQueuedWActions()
  {
    if (this.backgroundTask.isAlive()) {
      this.backgroundTask.terminate();
    }
  }
}
