package com.bloom.runtime.monitor;

import com.bloom.tungsten.CluiMonitorView;
import com.bloom.uuid.UUID;
import com.bloom.wactionstore.WAction;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.JsonNode;
import com.bloom.event.SimpleEvent;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.log4j.Logger;

public class MonitorEvent
  extends SimpleEvent
  implements Serializable, KryoSerializable
{
  private static final long serialVersionUID = 7197475144117417143L;
  public static final String SERVER_ID = "serverID";
  public static final String ENTITY_ID = "entityID";
  public static final String VALUE_LONG = "valueLong";
  public static final String VALUE_STRING = "valueString";
  public static final String TIME_STAMP = "timeStamp";
  public static final String TYPE = "type";
  private static Logger logger = Logger.getLogger(MonitorEvent.class);
  public static final UUID typeID = new UUID("7b1f8438-947c-4d10-9fe3-ac8f4a34af27");
  public static final UUID RollupUUID = new UUID("8c209549-058d-5e21-a0f4-bd905b45b038");
  private static final int MAX_STR_LEN = 4095;
  public static final String MONITORING_SCHEMA = "{  \"context\": [     { \"name\": \"serverID\", \"type\": \"string\", \"nullable\": false , \"index\": \"not_analyzed\" },    { \"name\": \"entityID\", \"type\": \"string\", \"nullable\": false , \"index\": \"not_analyzed\" },    { \"name\": \"valueLong\", \"type\": \"long\", \"nullable\": true , \"index\": \"no\" },    { \"name\": \"valueString\", \"type\": \"string\", \"nullable\": true , \"index\": \"no\" },    { \"name\": \"timeStamp\", \"type\": \"long\", \"nullable\": false , \"index\": \"not_analyzed\" },    { \"name\": \"type\", \"type\": \"string\", \"nullable\": false , \"index\": \"no\" }  ],  \"metadata\": [ \"id\"  , \"timestamp\"  , \"ttl\" ]}";
  public static final String ORDERBY_CLAUSE = " , \"orderby\": [    { \"attr\": \"timeStamp\", \"ascending\": %s }  ]";
  
  public static String getOrderByClause(String isAscending)
  {
    return String.format(" , \"orderby\": [    { \"attr\": \"timeStamp\", \"ascending\": %s }  ]", new Object[] { isAscending });
  }
  
  public MonitorEvent() {}
  
  public static enum Type
  {
    CACHE_REFRESH,  CACHE_SIZE,  CLUSTER_NAME,  COMMENTS,  CORES,  CPU_PER_NODE,  CPU_RATE,  CPU_THREAD,  DISK_FREE,  ELASTICSEARCH_FREE,  ES_FREE_BYTES,  ES_TOTAL_BYTES,  ES_TX_BYTES,  ES_RX_BYTES,  ES_QUERIES,  ES_FETCHES,  ES_DOCS,  ES_SIZE_BYTES,  ES_QUERY_RATE,  ES_FETCH_RATE,  ES_DOCS_RATE,  ES_SIZE_BYTES_RATE,  INPUT,  INPUT_RATE,  KAFKA_BROKERS,  KAFKA_BYTES,  KAFKA_BYTES_RATE,  KAFKA_BYTES_RATE_LONG,  KAFKA_MSGS,  KAFKA_MSGS_RATE,  KAFKA_MSGS_RATE_LONG,  LATEST_ACTIVITY,  LOCAL_HITS,  LOCAL_HITS_RATE,  LOCAL_MISSES,  LOCAL_MISSES_RATE,  LOG_ERROR,  LOG_WARN,  LOOKUPS,  LOOKUPS_RATE,  MEMORY_FREE,  MEMORY_MAX,  MEMORY_TOTAL,  NUM_LOG_ERRORS,  NUM_LOG_WARNS,  NUM_PARTITIONS,  OUTPUT,  OUTPUT_RATE,  PROCESSED,  PROCESSED_RATE,  RANGE_HEAD,  RANGE_TAIL,  RATE,  RECEIVED,  RECEIVED_RATE,  REMOTE_HITS,  REMOTE_HITS_RATE,  REMOTE_MISSES,  REMOTE_MISSES_RATE,  SOURCE_INPUT,  SOURCE_RATE,  STATUS_CHANGE,  STREAM_FULL,  TARGET_ACKED,  TARGET_RATE,  TARGET_OUTPUT,  UPTIME,  VERSION,  WACTIONS_CREATED,  WACTIONS_CREATED_RATE,  WINDOW_SIZE;
    
    private Type() {}
  }
  
  public static final Type[] KAFKA_TYPES = { Type.KAFKA_BROKERS, Type.KAFKA_BYTES_RATE, Type.KAFKA_MSGS_RATE };
  public static final Type[] ES_TYPES = { Type.ES_FREE_BYTES, Type.ES_RX_BYTES, Type.ES_TOTAL_BYTES, Type.ES_TX_BYTES };
  public long id;
  public UUID serverID;
  public UUID entityID;
  public Type type;
  public Long valueLong;
  public String valueString;
  
  public MonitorEvent(MonitorEvent other)
  {
    this.timeStamp = other.timeStamp;
    this._wa_SimpleEvent_ID = new UUID();
    this.serverID = other.serverID;
    this.entityID = other.entityID;
    this.type = other.type;
    this.valueLong = other.valueLong;
    this.valueString = other.valueString;
  }
  
  public MonitorEvent(long timestamp)
  {
    super(timestamp);
    this.serverID = null;
    this.entityID = null;
    this.type = null;
    this.valueLong = null;
    this.timeStamp = -1L;
  }
  
  public MonitorEvent(UUID serverID, UUID entityID, Type type, Long valueLong, Long timeStamp)
  {
    super(System.currentTimeMillis());
    if (serverID == null) {
      throw new RuntimeException("serverID cannot be null");
    }
    if (entityID == null) {
      throw new RuntimeException("entityID cannot be null");
    }
    this.serverID = serverID;
    this.entityID = entityID;
    this.type = type;
    this.valueLong = valueLong;
    this.timeStamp = timeStamp.longValue();
  }
  
  public MonitorEvent(UUID serverID, UUID componentID, Type type, String valueString, Long timeStamp)
  {
    super(System.currentTimeMillis());
    this.serverID = serverID;
    this.entityID = componentID;
    this.type = type;
    this.valueString = valueString;
    if (this.valueString.length() > 4095)
    {
      if (logger.isInfoEnabled()) {
        logger.info("MonitorEvent truncating this string to 4095 characters: " + valueString);
      }
      this.valueString = this.valueString.substring(0, 4095);
    }
    this.timeStamp = timeStamp.longValue();
    if (serverID == null) {
      throw new RuntimeException("serverID cannot be null");
    }
    if (this.entityID == null) {
      throw new RuntimeException("entityID cannot be null");
    }
  }
  
  public MonitorEvent(WAction wAction)
  {
    super(System.currentTimeMillis());
    this.serverID = (!wAction.get("serverID").isNull() ? new UUID(wAction.get("serverID").asText()) : null);
    this.entityID = (!wAction.get("entityID").isNull() ? new UUID(wAction.get("entityID").asText()) : null);
    this.type = (!wAction.get("type").isNull() ? Type.valueOf(wAction.get("type").asText()) : null);
    this.valueString = (!wAction.get("valueString").isNull() ? wAction.get("valueString").asText() : null);
    this.valueLong = (!wAction.get("valueLong").isNull() ? Long.valueOf(wAction.get("valueLong").asLong()) : null);
    if ((this.valueString != null) && (this.valueString.length() > 4095))
    {
      if (logger.isInfoEnabled()) {
        logger.info("MonitorEvent truncating this string to 4095 characters: " + this.valueString);
      }
      this.valueString = this.valueString.substring(0, 4095);
    }
    this.timeStamp = (!wAction.get("timeStamp").isNull() ? Long.valueOf(wAction.get("timeStamp").asLong()) : null).longValue();
    if (this.serverID == null) {
      throw new RuntimeException("serverID cannot be null");
    }
    if (this.entityID == null) {
      throw new RuntimeException("entityID cannot be null");
    }
  }
  
  public String toString()
  {
    Date d = new Date(this.timeStamp);
    String timeStr = CluiMonitorView.DATE_FORMAT_MILLIS.format(d);
    String serverIDString = RollupUUID.equals(this.serverID) ? "<ROLLUP>" : this.serverID.getUUIDString();
    
    return "MonitorEvent { serverID:" + serverIDString + ", entityID:" + this.entityID + ", timestamp:" + timeStr + " (" + this.timeStamp + ")}" + ", type:" + this.type + ", value:" + (this.valueString != null ? this.valueString : this.valueLong);
  }
  
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MonitorEvent)) {
      return false;
    }
    MonitorEvent that = (MonitorEvent)obj;
    return (this.id == that.id) && (this.serverID == that.serverID) && (this.entityID == that.entityID) && (this.type == that.type) && (this.valueLong == that.valueLong) && (this.valueString == that.valueString) && (this.timeStamp == that.timeStamp);
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(this.id).append(this.serverID).append(this.entityID).append(this.type.hashCode()).append(this.valueLong).append(this.valueString).append(this.timeStamp).toHashCode();
  }
  
  public void write(Kryo kryo, Output output)
  {
    super.write(kryo, output);
    output.writeLong(this.id);
    kryo.writeObjectOrNull(output, this.serverID, UUID.class);
    kryo.writeObjectOrNull(output, this.entityID, UUID.class);
    output.writeString(this.type.name());
    kryo.writeObjectOrNull(output, this.valueLong, Long.class);
    output.writeString(this.valueString);
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    this.id = input.readLong();
    this.serverID = ((UUID)kryo.readObjectOrNull(input, UUID.class));
    this.entityID = ((UUID)kryo.readObjectOrNull(input, UUID.class));
    this.type = Type.valueOf(input.readString());
    this.valueLong = ((Long)kryo.readObjectOrNull(input, Long.class));
    this.valueString = input.readString();
  }
  
  public WAction getwAction()
  {
    WAction wAction = new WAction();
    wAction.put("serverID", this.serverID != null ? this.serverID.toString() : null);
    wAction.put("entityID", this.entityID != null ? this.entityID.toString() : null);
    wAction.put("valueLong", this.valueLong);
    wAction.put("valueString", this.valueString);
    wAction.put("timeStamp", this.timeStamp);
    wAction.put("type", this.type.name());
    return wAction;
  }
  
  public static String getFields()
  {
    return "*";
  }
  
  public static String getWhereClauseJson(Map<String, Object> params)
  {
    if (params.isEmpty()) {
      return "";
    }
    StringBuilder result = new StringBuilder();
    result.append(",  \"where\": {     \"and\": [");
    
    Iterator<String> entries = params.keySet().iterator();
    while (entries.hasNext())
    {
      String entry = (String)entries.next();
      if (entry.equalsIgnoreCase("serverID"))
      {
        result.append("    { \"oper\": \"eq\",");
        result.append("      \"attr\": \"serverID\",");
        result.append("      \"value\": \"" + params.get(entry) + "\" }");
      }
      if (entry.equalsIgnoreCase("entityID"))
      {
        result.append("    { \"oper\": \"eq\",");
        result.append("      \"attr\": \"entityID\",");
        result.append("      \"value\": \"" + params.get(entry) + "\" }");
      }
      if (entry.equalsIgnoreCase("startTime"))
      {
        result.append("    { \"oper\": \"gte\",");
        result.append("      \"attr\": \"timeStamp\",");
        result.append("      \"value\": " + params.get(entry) + " }");
      }
      if (entry.equalsIgnoreCase("datumNames"))
      {
        result.append("    { \"oper\": \"in\",");
        result.append("      \"attr\": \"type\",");
        result.append("      \"value\": \"" + params.get(entry) + "\" }");
      }
      if (entry.equalsIgnoreCase("endTime"))
      {
        result.append("    { \"oper\": \"lte\",");
        result.append("      \"attr\": \"timeStamp\",");
        result.append("      \"value\": " + params.get(entry) + " }");
      }
      if (entries.hasNext()) {
        result.append(",");
      }
    }
    result.append("    ]  }");
    
    return result.toString();
  }
}
