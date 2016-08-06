package com.bloom.wactionstore.elasticsearch;

import com.bloom.wactionstore.Utility;
import com.bloom.wactionstore.WAction;
import com.bloom.wactionstore.base.WActionQueryBase;
import com.bloom.wactionstore.constants.Capability;
import com.bloom.wactionstore.exceptions.CapabilityException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Joiner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountBuilder;
import org.elasticsearch.search.sort.SortOrder;

class WActionQuery
  extends WActionQueryBase<WActionStore>
{
  private static final Class<WActionQuery> thisClass = WActionQuery.class;
  private static final Logger logger = Logger.getLogger(thisClass);
  private static final Pattern REGEX_META_CHARACTERS = Pattern.compile("[.^$?+*\\[\\]\\{\\}\\(\\)]");
  private static final Pattern REGEX_PERCENT = Pattern.compile("(^|[^\\\\])[%]+");
  private static final Pattern REGEX_UNDERSCORE = Pattern.compile("(^|[^\\\\])[_]");
  private static final Pattern REGEX_ESCAPED_LIKE = Pattern.compile("[\\\\]([%_])");
  private static final Pattern REGEX_META_BACKSLASH = Pattern.compile("[\\\\]");
  private static final Pattern REGEX_LITERAL_META_CHARS = Pattern.compile("[\\[]([.^$?+*\\[\\]\\{\\}\\(\\)])[\\]]");
  private boolean containsAggregations = false;
  private TermsBuilder groupByAggregations = null;
  private static final Map<String, String> mappedAttributeNames = new HashMap(1);
  private static final String META_CHARACTERS = ".^$?+*\\[\\]\\{\\}\\(\\)";
  private static final String REPLACE_META_CHARACTERS = "[$0]";
  private static final String REPLACE_PERCENT = "$1.*";
  private static final String REPLACE_UNDERSCORE = "$1.";
  private static final String REPLACE_ESCAPED_LIKE = "$1";
  private static final String REPLACE_META_BACKSLASH = "[\\\\$0]";
  private static final String REPLACE_LITERAL_META_CHARS = "[\\\\$1]";
  private final SearchRequestBuilder searchRequest;
  
  static
  {
    mappedAttributeNames.put("*any", "_all");
  }
  
  WActionQuery(JsonNode queryJson, WActionStore... wActionStores)
    throws CapabilityException
  {
    super(queryJson, Arrays.asList(wActionStores));
    
    WActionStore wActionStore = (WActionStore)getWActionStore(0);
    WActionStoreManager manager = (WActionStoreManager)wActionStore.getManager();
    
    this.searchRequest = manager.prepareSearch(wActionStore, getQuery());
    if (this.searchRequest != null)
    {
      if (logger.isDebugEnabled()) {
        logger.debug(String.format("WActionStore  query:   '%s'", new Object[] { queryJson }));
      }
      processProjection();
      processFiltration();
      processSortOrder();
      if (logger.isDebugEnabled())
      {
        String queryString = this.searchRequest.toString();
        try
        {
          queryString = Utility.objectMapper.readTree(queryString).toString();
        }
        catch (IOException ignored) {}
        String indexList = Joiner.on(',').join(this.searchRequest.request().indices());
        String typeList = Joiner.on(',').join(this.searchRequest.request().types());
        
        logger.debug(String.format("Elasticsearch query:   '%s'", new Object[] { queryString }));
        logger.debug(String.format("Elasticsearch indices: '%s'", new Object[] { indexList }));
        logger.debug(String.format("Elasticsearch types:   '%s'", new Object[] { typeList }));
      }
    }
  }
  
  WActionQuery(JsonNode queryJson, String feature, WActionStore... wActionStores)
  {
    super(queryJson, Arrays.asList(wActionStores));
    this.searchRequest = null;
  }
  
  static FilterBuilder getLogicalOperation(JsonNode operation)
    throws CapabilityException
  {
    String operationType = (String)operation.fieldNames().next();
    switch (operationType)
    {
    case "and": 
      return getLogicalOperationAnd(operation.get(operationType));
    case "or": 
      return getLogicalOperationOr(operation.get(operationType));
    case "not": 
      return getLogicalOperationNot(operation.get(operationType));
    }
    return getSimpleCondition(operation);
  }
  
  private static FilterBuilder getLogicalOperationAnd(JsonNode operation)
  {
    FilterBuilder[] filters = getFilterArray(operation);
    return FilterBuilders.andFilter(filters);
  }
  
  private static FilterBuilder getLogicalOperationOr(JsonNode operation)
  {
    FilterBuilder[] filters = getFilterArray(operation);
    return FilterBuilders.orFilter(filters);
  }
  
  private static FilterBuilder[] getFilterArray(JsonNode operation)
  {
    int operations = operation.size();
    FilterBuilder[] filters = new FilterBuilder[operation.size()];
    for (int index = 0; index < operations; index++) {
      filters[index] = getLogicalOperation(operation.get(index));
    }
    return filters;
  }
  
  private static FilterBuilder getLogicalOperationNot(JsonNode operation)
  {
    FilterBuilder filter = getLogicalOperation(operation);
    return FilterBuilders.notFilter(filter);
  }
  
  private static FilterBuilder getSimpleCondition(JsonNode operation)
    throws CapabilityException
  {
    String operationName = operation.get("oper").asText();
    String attributeName = getWhereClauseAttributeName(operation);
    FilterBuilder result = null;
    JsonNode value;
    JsonNode values;
    switch (operationName)
    {
    case "gt": 
    case "lt": 
    case "gte": 
    case "lte": 
      value = getOperationValue(operation, attributeName, "value");
      result = getSimpleConditionBinaryCompare(operationName, attributeName, value);
      break;
    case "eq": 
    case "neq": 
      value = getOperationValue(operation, attributeName, "value");
      result = getSimpleConditionBinaryEquality(operationName, attributeName, value);
      break;
    case "like": 
    case "regex": 
      value = getOperationValue(operation, attributeName, "value");
      result = getSimpleConditionPatternMatch(operationName, attributeName, value);
      break;
    case "between": 
      values = getOperationValue(operation, attributeName, "values");
      result = getSimpleConditionTrinary(operationName, attributeName, values.get(0), values.get(1));
      break;
    case "in": 
      values = getOperationValue(operation, attributeName, "values");
      result = getSimpleConditionSet(operationName, attributeName, values);
      break;
    }
    return result;
  }
  
  private static JsonNode getOperationValue(JsonNode operation, String attributeName, String valueName)
  {
    JsonNode value = operation.get(valueName);
    if ("_all".equals(attributeName)) {
      if (value.isTextual())
      {
        value = TextNode.valueOf(value.asText().toLowerCase());
      }
      else if (value.isArray())
      {
        ArrayNode valueArray = (ArrayNode)value;
        for (int index = 0; index < valueArray.size(); index++)
        {
          JsonNode node = valueArray.get(index);
          if (node.isTextual()) {
            valueArray.set(index, TextNode.valueOf(node.asText().toLowerCase()));
          }
        }
        value = valueArray;
      }
    }
    return value;
  }
  
  private static String getWhereClauseAttributeName(JsonNode operation)
  {
    String attributeName = operation.get("attr").asText();
    String mappedAttributeName = (String)mappedAttributeNames.get(attributeName);
    return mappedAttributeName == null ? attributeName : mappedAttributeName;
  }
  
  private static FilterBuilder getSimpleConditionBinaryCompare(String operationName, String attributeName, JsonNode value)
  {
    RangeFilterBuilder result = FilterBuilders.rangeFilter(attributeName);
    switch (operationName)
    {
    case "gt": 
      result = result.gt(getNodeValue(value));
      break;
    case "lt": 
      result = result.lt(getNodeValue(value));
      break;
    case "gte": 
      result = result.gte(getNodeValue(value));
      break;
    case "lte": 
      result = result.lte(getNodeValue(value));
      break;
    }
    return result;
  }
  
  private static FilterBuilder getSimpleConditionBinaryEquality(String operationName, String attributeName, JsonNode value)
  {
    Object valueObject = getNodeValue(value);
    FilterBuilder result = valueObject == null ? FilterBuilders.missingFilter(attributeName) : FilterBuilders.termFilter(attributeName, valueObject);
    if ("neq".equals(operationName)) {
      result = FilterBuilders.notFilter(result);
    }
    return result;
  }
  
  private static FilterBuilder getSimpleConditionPatternMatch(String operationName, String attributeName, JsonNode value)
  {
    String valueString = getNodeValue(value).toString();
    if ("like".equals(operationName))
    {
      valueString = REGEX_META_CHARACTERS.matcher(valueString).replaceAll("[$0]");
      valueString = REGEX_PERCENT.matcher(valueString).replaceAll("$1.*");
      for (int loop = 1; loop <= 2; loop++) {
        valueString = REGEX_UNDERSCORE.matcher(valueString).replaceAll("$1.");
      }
      valueString = REGEX_ESCAPED_LIKE.matcher(valueString).replaceAll("$1");
      valueString = REGEX_META_BACKSLASH.matcher(valueString).replaceAll("[\\\\$0]");
      valueString = REGEX_LITERAL_META_CHARS.matcher(valueString).replaceAll("[\\\\$1]");
      
      logger.debug(String.format("Transformed match filter from LIKE '%s' to regular expression '%s'", new Object[] { getNodeValue(value).toString(), valueString }));
    }
    return FilterBuilders.regexpFilter(attributeName, valueString);
  }
  
  private static FilterBuilder getSimpleConditionTrinary(String operationName, String attributeName, JsonNode jsonNodeLeft, JsonNode jsonNodeRight)
  {
    FilterBuilder result = null;
    if ("between".equals(operationName)) {
      result = FilterBuilders.rangeFilter(attributeName).gte(getNodeValue(jsonNodeLeft)).lte(getNodeValue(jsonNodeRight));
    }
    return result;
  }
  
  private static FilterBuilder getSimpleConditionSet(String operationName, String attributeName, JsonNode values)
  {
    FilterBuilder result = null;
    if ("in".equals(operationName))
    {
      Collection<Object> valueList = new ArrayList(values.size());
      for (JsonNode valueNode : values)
      {
        Object valueObject = getNodeValue(valueNode);
        valueList.add(valueObject);
      }
      result = FilterBuilders.termsFilter(attributeName, valueList);
    }
    return result;
  }
  
  private static Object getNodeValue(JsonNode value)
  {
    Object result = null;
    switch (value.getNodeType())
    {
    case STRING: 
      result = value.asText();
      break;
    case BOOLEAN: 
      result = Boolean.valueOf(value.asBoolean());
      break;
    case NUMBER: 
      result = value.isIntegralNumber() ? Long.valueOf(value.asLong()) : Double.valueOf(value.asDouble());
      break;
    }
    return result;
  }
  
  private void processProjection()
    throws CapabilityException
  {
    JsonNode attributes = getQuery().get("select");
    if (!"*".equals(attributes.get(0).asText()))
    {
      buildGroupByAggregations();
      List<String> fetchSource = new ArrayList(attributes.size());
      boolean hasSource = false;
      for (int index = 0; index < attributes.size(); index++)
      {
        JsonNode attribute = attributes.get(index);
        if (attribute.isValueNode())
        {
          fetchSource.add(attribute.asText());
          hasSource = true;
        }
        else
        {
          processProjectionFunction(attribute);
          this.containsAggregations = true;
        }
      }
      if (hasSource) {
        this.searchRequest.setFetchSource((String[])fetchSource.toArray(new String[fetchSource.size()]), null);
      }
    }
  }
  
  private void buildGroupByAggregations()
  {
    JsonNode query = getQuery();
    JsonNode groupByNodes = query.get("groupby");
    JsonNode orderByNodes = query.get("orderby");
    if (groupByNodes != null) {
      for (JsonNode groupByNode : groupByNodes)
      {
        String groupByAttributeName = groupByNode.asText();
        String groupByAggregateName = groupByAttributeName + ':' + "groupby";
        Terms.Order order = getSortOrder(orderByNodes, groupByAttributeName);
        TermsBuilder groupByAggregation = ((TermsBuilder)AggregationBuilders.terms(groupByAggregateName).field(groupByAttributeName)).size(Integer.MAX_VALUE).order(order);
        if (this.groupByAggregations == null) {
          this.searchRequest.addAggregation(groupByAggregation);
        } else {
          this.groupByAggregations.subAggregation(groupByAggregation);
        }
        this.groupByAggregations = groupByAggregation;
      }
    }
  }
  
  private static Terms.Order getSortOrder(JsonNode orderByNodes, String groupByAttributeName)
  {
    if (orderByNodes != null) {
      for (JsonNode orderByNode : orderByNodes)
      {
        String orderByAttributeName = orderByNode.get("attr").asText();
        if (groupByAttributeName.equals(orderByAttributeName))
        {
          boolean orderByAscending = orderByNode.get("ascending").asBoolean(true);
          return Terms.Order.term(orderByAscending);
        }
      }
    }
    return Terms.Order.term(true);
  }
  
  private void processProjectionFunction(JsonNode function)
    throws CapabilityException
  {
    String functionName = function.get("oper").asText();
    String attributeName = function.get("attr").asText();
    switch (functionName)
    {
    case "min": 
    case "max": 
    case "count": 
    case "sum": 
    case "avg": 
      addAggregation(functionName, attributeName);
      break;
    case "first": 
      throw new CapabilityException(Capability.AGGREGATION_FIRST.name());
    case "last": 
      throw new CapabilityException(Capability.AGGREGATION_LAST.name());
    }
  }
  
  private void addAggregation(String functionName, String attributeName)
  {
    AbstractAggregationBuilder aggregation = null;
    String aggregationName = attributeName + ':' + functionName;
    switch (functionName)
    {
    case "min": 
      aggregation = AggregationBuilders.min(aggregationName).field(attributeName);
      break;
    case "max": 
      aggregation = AggregationBuilders.max(aggregationName).field(attributeName);
      break;
    case "count": 
      aggregation = AggregationBuilders.count(aggregationName).field(attributeName);
      break;
    case "sum": 
      aggregation = AggregationBuilders.sum(aggregationName).field(attributeName);
      break;
    case "avg": 
      aggregation = AggregationBuilders.avg(aggregationName).field(attributeName);
      break;
    }
    if (this.groupByAggregations != null) {
      this.groupByAggregations.subAggregation(aggregation);
    } else {
      this.searchRequest.addAggregation(aggregation);
    }
  }
  
  private void processFiltration()
  {
    JsonNode whereClause = getQuery().get("where");
    QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
    if (whereClause != null)
    {
      FilterBuilder filterBuilder = getLogicalOperation(whereClause);
      queryBuilder = new FilteredQueryBuilder(queryBuilder, filterBuilder);
    }
    this.searchRequest.setQuery(queryBuilder);
  }
  
  private void processSortOrder()
  {
    JsonNode attributes = getQuery().get("orderby");
    if ((attributes != null) && (getQuery().get("groupby") == null)) {
      for (JsonNode attribute : attributes)
      {
        String field = attribute.get("attr").asText();
        JsonNode ascending = attribute.get("ascending");
        SortOrder order = (ascending == null) || (ascending.asBoolean(true)) ? SortOrder.ASC : SortOrder.DESC;
        this.searchRequest.addSort(field, order);
      }
    }
  }
  
  public Iterable<WAction> execute()
    throws CapabilityException
  {
    return new Iterable()
    {
      public Iterator<WAction> iterator()
      {
        if (WActionQuery.this.containsAggregations)
        {
          WActionQuery.this.searchRequest.setSize(0);
          return new WActionQueryAggregationIterator(WActionQuery.this.searchRequest);
        }
        WActionStore wActionStore = (WActionStore)WActionQuery.this.getWActionStore(0);
        WActionStoreManager manager = (WActionStoreManager)wActionStore.getManager();
        Client client = manager.getClient();
        WActionQuery.this.searchRequest.setSize(Integer.MAX_VALUE);
        return new WActionQueryIterator(WActionQuery.this.searchRequest, client);
      }
    };
  }
}
