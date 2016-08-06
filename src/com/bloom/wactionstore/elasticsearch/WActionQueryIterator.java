package com.bloom.wactionstore.elasticsearch;

import com.bloom.wactionstore.Utility;
import com.bloom.wactionstore.WAction;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Iterator;
import java.util.Map;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

class WActionQueryIterator
  implements Iterator<WAction>
{
  private static final Class<WActionQueryIterator> thisClass = WActionQueryIterator.class;
  private static final Logger logger = Logger.getLogger(thisClass);
  private static final TimeValue scrollTimeout = new TimeValue(60000L);
  private static final int scrollSize = 100;
  private final Client client;
  private SearchResponse searchResponse;
  private Iterator<SearchHit> results;
  
  WActionQueryIterator(SearchRequestBuilder searchRequest, Client client)
  {
    this.client = client;
    SearchRequestBuilder scrollRequest = modifyRequestForScrolling(searchRequest);
    this.searchResponse = ((SearchResponse)scrollRequest.execute().actionGet());
    Utility.reportExecutionTime(logger, this.searchResponse.getTookInMillis());
    this.results = this.searchResponse.getHits().iterator();
  }
  
  private SearchRequestBuilder modifyRequestForScrolling(SearchRequestBuilder searchRequest)
  {
    return searchRequest.setSize(100).setScroll(scrollTimeout);
  }
  
  public boolean hasNext()
  {
    boolean result = this.results.hasNext();
    if (!result)
    {
      String scrollId = this.searchResponse.getScrollId();
      this.searchResponse = ((SearchResponse)this.client.prepareSearchScroll(scrollId).setScroll(scrollTimeout).execute().actionGet());
      this.results = this.searchResponse.getHits().iterator();
      result = this.results.hasNext();
    }
    return result;
  }
  
  public WAction next()
  {
    WAction wAction = null;
    SearchHit result = (SearchHit)this.results.next();
    if (result != null)
    {
      wAction = new WAction();
      Map<String, Object> source = result.getSource();
      ObjectNode node = (ObjectNode)Utility.objectMapper.valueToTree(source);
      wAction.setAll(node);
    }
    if (logger.isDebugEnabled()) {
      logger.debug(String.format("Returning WAction '%s'", new Object[] { wAction }));
    }
    return wAction;
  }
  
  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}
