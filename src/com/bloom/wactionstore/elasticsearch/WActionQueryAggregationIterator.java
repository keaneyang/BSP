package com.bloom.wactionstore.elasticsearch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;

import com.bloom.wactionstore.Utility;
import com.bloom.wactionstore.WAction;

class WActionQueryAggregationIterator
  implements Iterator<WAction>
{
  private static final Class<WActionQueryAggregationIterator> thisClass = WActionQueryAggregationIterator.class;
  private static final Logger logger = Logger.getLogger(thisClass);
  private final Collection<WAction> wActions = new ArrayList(0);
  private final Iterator<WAction> results;
  
  WActionQueryAggregationIterator(SearchRequestBuilder searchRequest)
  {
    SearchResponse searchResponse = (SearchResponse)searchRequest.execute().actionGet();
    Utility.reportExecutionTime(logger, searchResponse.getTookInMillis());
    collectWActions(new WAction(), searchResponse.getAggregations());
    this.results = this.wActions.iterator();
  }
  
  public boolean hasNext()
  {
    return this.results.hasNext();
  }
  
  public WAction next()
  {
    WAction wAction = (WAction)this.results.next();
    if (logger.isDebugEnabled()) {
      logger.debug(String.format("Returning WAction '%s'", new Object[] { wAction }));
    }
    return wAction;
  }
  
  public void remove()
  {
    throw new UnsupportedOperationException();
  }
  
  private void collectWActions(WAction wAction, Aggregations aggregations)
  {
    for (Aggregation aggregation : aggregations)
    {
      if ((aggregation instanceof Terms))
      {
        collectionWActionGroups(wAction, (Terms)aggregation);
        return;
      }
      if ((aggregation instanceof InternalNumericMetricsAggregation.SingleValue))
      {
        String aggregationName = aggregation.getName();
        double aggregationValue = ((InternalNumericMetricsAggregation.SingleValue)aggregation).value();
        wAction.put(aggregationName, aggregationValue <= Double.NEGATIVE_INFINITY ? null : Double.valueOf(aggregationValue));
      }
      if ((aggregation instanceof InternalValueCount))
      {
        String aggregationName = aggregation.getName();
        long aggregationValue = ((ValueCount)aggregation).getValue();
        wAction.put(aggregationName, aggregationValue);
      }
    }
    this.wActions.add(wAction);
  }
  
  private void collectionWActionGroups(WAction baseWAction, Terms terms)
  {
    String groupByName = terms.getName().replace(":groupby", "");
    for (Terms.Bucket bucket : terms.getBuckets())
    {
      WAction wAction = new WAction(baseWAction);
      String termValue = bucket.getKey();
      wAction.put(groupByName, termValue);
      collectWActions(wAction, bucket.getAggregations());
    }
  }
}
