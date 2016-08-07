package com.bloom.runtime.meta;

import com.bloom.runtime.Interval;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class IntervalPolicy
  implements Serializable
{
  private static final long serialVersionUID = 5645537094795872797L;
  public Kind kind;
  public CountBasedPolicy countBasedPolicy;
  public TimeBasedPolicy timeBasedPolicy;
  public AttrBasedPolicy attrBasedPolicy;
  public IntervalPolicy() {}
  
  public static class CountBasedPolicy
    implements Serializable
  {
    private static final long serialVersionUID = -8721320131044552238L;
    public int count;
    
    public CountBasedPolicy() {}
    
    public CountBasedPolicy(int cnt)
    {
      this.count = cnt;
    }
    
    @JsonIgnore
    public int getCountInterval()
    {
      return this.count;
    }
    
    public String toString()
    {
      return this.count + " ROWS";
    }
  }
  
  public static class TimeBasedPolicy
    implements Serializable
  {
    private static final long serialVersionUID = 7377334123916855969L;
    public Interval time;
    
    public TimeBasedPolicy() {}
    
    public TimeBasedPolicy(Interval time)
    {
      this.time = time;
    }
    
    @JsonIgnore
    public long getTimeInterval()
    {
      return this.time.value;
    }
    
    public String toString()
    {
      return "WITHIN " + this.time.toHumanReadable();
    }
  }
  
  public static class AttrBasedPolicy
    implements Serializable
  {
    private static final long serialVersionUID = 6109839268916036301L;
    public long range;
    public String name;
    
    public AttrBasedPolicy() {}
    
    public AttrBasedPolicy(String name, long range)
    {
      this.range = range;
      this.name = name;
    }
    
    @JsonIgnore
    public long getAttrValueRange()
    {
      return this.range;
    }
    
    @JsonIgnore
    public String getAttrName()
    {
      return this.name;
    }
    
    public String toString()
    {
      return "RANGE " + this.range + " ON " + this.name;
    }
  }
  
  public static enum Kind
  {
    COUNT,  TIME,  ATTR,  TIME_COUNT,  TIME_ATTR;
    
    private Kind() {}
  }
  
  public IntervalPolicy(Kind kind, CountBasedPolicy cp, TimeBasedPolicy tp, AttrBasedPolicy ap)
  {
    this.kind = kind;
    this.countBasedPolicy = cp;
    this.timeBasedPolicy = tp;
    this.attrBasedPolicy = ap;
  }
  
  public String toString()
  {
    StringBuilder sb = new StringBuilder(this.kind.toString());
    if (isTimeBased()) {
      sb.append(" " + getTimePolicy());
    }
    if (isCountBased()) {
      sb.append(" " + getCountPolicy());
    }
    if (isAttrBased()) {
      sb.append(" " + getAttrPolicy());
    }
    return sb.toString();
  }
  
  @JsonIgnore
  public CountBasedPolicy getCountPolicy()
  {
    return this.countBasedPolicy;
  }
  
  @JsonIgnore
  public TimeBasedPolicy getTimePolicy()
  {
    return this.timeBasedPolicy;
  }
  
  @JsonIgnore
  public AttrBasedPolicy getAttrPolicy()
  {
    return this.attrBasedPolicy;
  }
  
  @JsonIgnore
  public boolean isCountBased()
  {
    return getCountPolicy() != null;
  }
  
  @JsonIgnore
  public boolean isTimeBased()
  {
    return getTimePolicy() != null;
  }
  
  @JsonIgnore
  public boolean isAttrBased()
  {
    return getAttrPolicy() != null;
  }
  
  @JsonIgnore
  public Kind getKind()
  {
    return this.kind;
  }
  
  private static CountBasedPolicy makeCountPolicy(int count)
  {
    return new CountBasedPolicy(count);
  }
  
  private static TimeBasedPolicy makeTimePolicy(Interval time)
  {
    return new TimeBasedPolicy(time);
  }
  
  private static AttrBasedPolicy makeAttrPolicy(String name, long range)
  {
    return new AttrBasedPolicy(name, range);
  }
  
  private static IntervalPolicy makePolicy(Kind kind, CountBasedPolicy cp, TimeBasedPolicy tp, AttrBasedPolicy ap)
  {
    return new IntervalPolicy(kind, cp, tp, ap);
  }
  
  public static IntervalPolicy createCountPolicy(int count)
  {
    return makePolicy(Kind.COUNT, makeCountPolicy(count), null, null);
  }
  
  public static IntervalPolicy createAttrPolicy(String name, long range)
  {
    return makePolicy(Kind.ATTR, null, null, makeAttrPolicy(name, range));
  }
  
  public static IntervalPolicy createTimePolicy(Interval time)
  {
    return makePolicy(Kind.TIME, null, makeTimePolicy(time), null);
  }
  
  public static IntervalPolicy createTimeCountPolicy(Interval time, int count)
  {
    return makePolicy(Kind.TIME_COUNT, makeCountPolicy(count), makeTimePolicy(time), null);
  }
  
  public static IntervalPolicy createTimeAttrPolicy(Interval time, String name, long range)
  {
    return makePolicy(Kind.TIME_ATTR, null, makeTimePolicy(time), makeAttrPolicy(name, range));
  }
  
  @JsonIgnore
  public boolean isCount1()
  {
    return (getKind() == Kind.COUNT) && (getCountPolicy().getCountInterval() == 1);
  }
  
  @JsonIgnore
  public static Kind getKindFromString(String kind)
  {
    if (kind.equals("COUNT")) {
      return Kind.COUNT;
    }
    if (kind.equals("ATTR")) {
      return Kind.ATTR;
    }
    if (kind.equals("TIME")) {
      return Kind.TIME;
    }
    if (kind.equals("TIME_COUNT")) {
      return Kind.TIME_COUNT;
    }
    if (kind.equals("TIME_ATTR")) {
      return Kind.TIME_ATTR;
    }
    return null;
  }
  
  public static class IntervalPolicyDeserializer
    extends StdDeserializer<IntervalPolicy>
  {
    public IntervalPolicyDeserializer()
    {
      super();
    }
    
    public IntervalPolicy deserialize(JsonParser jp, DeserializationContext dctx)
      throws IOException, JsonProcessingException
    {
      ObjectMapper mapper = new ObjectMapper();
      mapper.setConfig(dctx.getConfig());
      jp.setCodec(mapper);
      if (jp.hasCurrentToken())
      {
        JsonNode winPolacyIntNode = (JsonNode)jp.readValueAsTree();
        return IntervalPolicy.deserialize(winPolacyIntNode);
      }
      return null;
    }
  }
  
  public static IntervalPolicy deserialize(JsonNode jsonNode)
    throws IOException, JsonProcessingException
  {
    if (jsonNode == null) {
      return null;
    }
    Integer countSize = null;
    Interval timeLimit = null;
    List<String> fields = null;
    JsonNode ctNode = jsonNode.get("count_interval");
    if (ctNode != null) {
      countSize = ctNode.toString().equalsIgnoreCase("null") ? null : new Integer(ctNode.asInt());
    }
    JsonNode tiNode = jsonNode.get("time_interval");
    if (tiNode != null)
    {
      JsonNode valNode = tiNode.get("value");
      if (valNode != null) {
        timeLimit = valNode.toString().equalsIgnoreCase("null") ? null : new Interval(valNode.asLong());
      }
    }
    JsonNode optionalAttrNode = jsonNode.get("optional_attr");
    if ((optionalAttrNode != null) && (optionalAttrNode.isArray()) && 
      (optionalAttrNode.get(0) != null))
    {
      fields = new ArrayList();
      fields.add(optionalAttrNode.get(0).asText());
    }
    if ((countSize != null) && (timeLimit != null)) {
      return createTimeCountPolicy(timeLimit, countSize.intValue());
    }
    if ((fields != null) && (!fields.isEmpty()) && (timeLimit != null)) {
      return createAttrPolicy((String)fields.get(0), timeLimit.value);
    }
    if (countSize != null) {
      return createCountPolicy(countSize.intValue());
    }
    if (timeLimit != null) {
      return createTimePolicy(timeLimit);
    }
    return null;
  }
}
