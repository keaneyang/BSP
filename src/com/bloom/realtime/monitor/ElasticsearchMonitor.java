package com.bloom.runtime.monitor;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsNodes;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.search.stats.SearchStats.Stats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.monitor.fs.FsStats.Info;

import com.bloom.wactionstore.WActionStores;
import com.bloom.wactionstore.elasticsearch.WActionStoreManager;

public class ElasticsearchMonitor
  implements Runnable
{
  public ElasticsearchMonitor()
  {
    if (Logger.getLogger("Monitor").isDebugEnabled()) {
      Logger.getLogger("Monitor").debug("Initializing ElasticsearchMonitor");
    }
  }
  
  Long prevTimestamp = null;
  Long prevDocs = null;
  Long prevQueries = null;
  Long prevFetches = null;
  Long prevSize = null;
  
  public void run()
  {
    try
    {
      List<MonitorEvent> monEvs = new ArrayList();
      long timestamp = System.currentTimeMillis();
      WActionStoreManager instance = WActionStores.getAnyElasticsearchInstance();
      if (instance != null)
      {
        Client client = instance.getClient();
        ClusterStatsResponse clusterStats = (ClusterStatsResponse)client.admin().cluster().prepareClusterStats().get();
        FsStats.Info info = clusterStats.getNodesStats().getFs();
        
        monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ELASTICSEARCH_FREE, info.getFree().bytes() / 1000000000L + "GB", Long.valueOf(timestamp)));
        monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_FREE_BYTES, Long.valueOf(info.getFree().bytes()), Long.valueOf(timestamp)));
        monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_TOTAL_BYTES, Long.valueOf(info.getTotal().bytes()), Long.valueOf(timestamp)));
        
        NodesStatsResponse nodestats = (NodesStatsResponse)client.admin().cluster().prepareNodesStats(new String[0]).get();
        long docs = 0L;
        long queries = 0L;
        long fetches = 0L;
        long size = 0L;
        for (NodeStats n : (NodeStats[])nodestats.getNodes())
        {
          docs += n.getIndices().getDocs().getCount();
          queries += n.getIndices().getSearch().getTotal().getQueryCount();
          fetches += n.getIndices().getSearch().getTotal().getFetchCount();
          size += n.getIndices().getStore().getSizeInBytes();
        }
        monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_DOCS, Long.valueOf(docs), Long.valueOf(timestamp)));
        monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_QUERIES, Long.valueOf(queries), Long.valueOf(timestamp)));
        monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_FETCHES, Long.valueOf(fetches), Long.valueOf(timestamp)));
        monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_SIZE_BYTES, Long.valueOf(size), Long.valueOf(timestamp)));
        if (this.prevDocs != null)
        {
          long delta = Math.abs(docs - this.prevDocs.longValue());
          Long rate = Long.valueOf(1000L * delta / (timestamp - this.prevTimestamp.longValue()));
          monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_DOCS_RATE, rate, Long.valueOf(timestamp)));
        }
        this.prevDocs = Long.valueOf(docs);
        if (this.prevQueries != null)
        {
          long delta = Math.abs(queries - this.prevQueries.longValue());
          Long rate = Long.valueOf(1000L * delta / (timestamp - this.prevTimestamp.longValue()));
          monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_QUERY_RATE, rate, Long.valueOf(timestamp)));
        }
        this.prevQueries = Long.valueOf(queries);
        if (this.prevFetches != null)
        {
          long delta = Math.abs(fetches - this.prevFetches.longValue());
          Long rate = Long.valueOf(1000L * delta / (timestamp - this.prevTimestamp.longValue()));
          monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_FETCH_RATE, rate, Long.valueOf(timestamp)));
        }
        this.prevFetches = Long.valueOf(fetches);
        if (this.prevSize != null)
        {
          long delta = Math.abs(size - this.prevSize.longValue());
          Long rate = Long.valueOf(1000L * delta / (timestamp - this.prevTimestamp.longValue()));
          monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_SIZE_BYTES_RATE, rate, Long.valueOf(timestamp)));
        }
        this.prevSize = Long.valueOf(size);
        this.prevTimestamp = Long.valueOf(timestamp);
      }
      MonitorBatchEvent batchEvent = new MonitorBatchEvent(timestamp, monEvs);
      MonitorModel.processBatch(batchEvent);
    }
    catch (Exception e)
    {
      Logger.getLogger("Monitor").error("ElasticsearchMonitor could not send batch to MonitorModel", e);
    }
  }
}
