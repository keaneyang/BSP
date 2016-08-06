package com.bloom.wactionstore.elasticsearch;

import com.bloom.wactionstore.WAction;
import com.bloom.wactionstore.exceptions.CapabilityException;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class WActionDeleteByQuery
  extends WActionQuery
{
  WActionDeleteByQuery(JsonNode queryJson, WActionStore... wActionStores)
    throws CapabilityException
  {
    super(queryJson, "delete", wActionStores);
  }
  
  public Iterable<WAction> execute()
    throws CapabilityException
  {
    WActionStore wActionStore = (WActionStore)getWActionStore(0);
    WActionStoreManager manager = (WActionStoreManager)wActionStore.getManager();
    Client client = manager.getClient();
    
    JsonNode whereClause = getQuery().get("where");
    QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
    if (whereClause != null)
    {
      FilterBuilder filterBuilder = getLogicalOperation(whereClause);
      queryBuilder = new FilteredQueryBuilder(queryBuilder, filterBuilder);
    }
    DeleteByQueryRequestBuilder requestBuilder = client.prepareDeleteByQuery(new String[0]);
    requestBuilder.setQuery(queryBuilder);
    requestBuilder.setIndices(new String[] { wActionStore.getActualName() });
    requestBuilder.execute().actionGet();
    
    final WAction result = new WAction();
    result.put("Result", 0);
    
    return new Iterable()
    {
      public Iterator<WAction> iterator()
      {
        WAction[] wActions = { result };
        return Arrays.asList(wActions).iterator();
      }
    };
  }
}
