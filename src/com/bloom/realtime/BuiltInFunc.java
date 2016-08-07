package com.bloom.runtime;

import cern.colt.list.DoubleArrayList;

import com.bloom.classloading.WALoader;
import com.bloom.geo.GeoSearchCoverTree;
import com.bloom.geo.Point;
import com.bloom.geo.Polygon;
import com.bloom.iplookup.IPLookup2;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.proc.events.JsonNodeEvent;
import com.bloom.proc.events.StringArrayEvent;
import com.bloom.runtime.compiler.custom.AcceptWildcard;
import com.bloom.runtime.compiler.custom.AccessPrevEventInBuffer;
import com.bloom.runtime.compiler.custom.AggHandlerDesc;
import com.bloom.runtime.compiler.custom.AnyAttrLike;
import com.bloom.runtime.compiler.custom.CustomFunction;
import com.bloom.runtime.compiler.custom.DataSetItertorTrans;
import com.bloom.runtime.compiler.custom.EventListGetter;
import com.bloom.runtime.compiler.custom.StreamGeneratorDef;
import com.bloom.runtime.compiler.custom.matchTranslator;
import com.bloom.runtime.components.HeartBeatGenerator;
import com.bloom.runtime.components.HeartBeatGenerator.HeartBeatEvent;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.monitor.MonitorBatchEvent;
import com.bloom.runtime.monitor.MonitorModel;
import com.bloom.runtime.utils.DateParser;
import com.bloom.security.WASecurityManager;
import com.bloom.utility.SnmpPayload;
import com.bloom.uuid.UUID;
import com.bloom.waction.Waction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.bloom.event.Event;
import com.bloom.event.SimpleEvent;
import com.bloom.proc.events.WAEvent;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.util.Utf8;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.joda.time.DateTime;
import org.joda.time.DateTime.Property;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.LocalDate.Property;
import org.joda.time.Period;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimePrinter;
import org.joda.time.format.ISODateTimeFormat;

public abstract class BuiltInFunc
{
  private static Logger logger = Logger.getLogger(BuiltInFunc.class);
  
  public static StreamGeneratorDef heartbeat(ClassLoader c, long interval)
  {
    return new StreamGeneratorDef(HeartBeatGenerator.HeartBeatEvent.uuid, HeartBeatGenerator.HeartBeatEvent.class, new Object[] { Long.valueOf(interval) }, HeartBeatGenerator.class.getName());
  }
  
  public static StreamGeneratorDef heartbeat(ClassLoader c, long interval, int times)
  {
    return new StreamGeneratorDef(HeartBeatGenerator.HeartBeatEvent.uuid, HeartBeatGenerator.HeartBeatEvent.class, new Object[] { Long.valueOf(interval), Integer.valueOf(times) }, HeartBeatGenerator.class.getName());
  }
  
  public static StreamGeneratorDef heartbeat(ClassLoader c, long interval, int times, int batchSize)
  {
    return new StreamGeneratorDef(HeartBeatGenerator.HeartBeatEvent.uuid, HeartBeatGenerator.HeartBeatEvent.class, new Object[] { Long.valueOf(interval), Integer.valueOf(times), Integer.valueOf(batchSize) }, HeartBeatGenerator.class.getName());
  }
  
  public static StreamGeneratorDef heartbeat(ClassLoader c, long interval, int times, int batchSize, int seqsize)
  {
    return new StreamGeneratorDef(HeartBeatGenerator.HeartBeatEvent.uuid, HeartBeatGenerator.HeartBeatEvent.class, new Object[] { Long.valueOf(interval), Integer.valueOf(times), Integer.valueOf(batchSize), Integer.valueOf(seqsize) }, HeartBeatGenerator.class.getName());
  }
  
  private static DateTimeFormatter makeDefaultFormatter()
  {
    DateTimeParser tp = ISODateTimeFormat.timeElementParser().getParser();
    DateTimeParser otp = new DateTimeFormatterBuilder().appendLiteral(' ').appendOptional(tp).toParser();
    
    DateTimeParser[] parsers = { new DateTimeFormatterBuilder().append(ISODateTimeFormat.dateElementParser().getParser()).appendOptional(otp).toParser(), new DateTimeFormatterBuilder().append(DateTimeFormat.forPattern("yyyy/MM/dd")).appendOptional(otp).toParser(), new DateTimeFormatterBuilder().append(DateTimeFormat.forPattern("yyyy-MMM-dd")).appendOptional(otp).toParser(), ISODateTimeFormat.dateOptionalTimeParser().getParser(), tp };
    
    DateTimePrinter dp = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss.SSS").getPrinter();
    DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(dp, parsers).toFormatter();
    
    return formatter;
  }
  
  private static DateTimeFormatter defaultDateTimeFormatter = makeDefaultFormatter();
  
  public static DateTime TO_DATE(Object obj)
  {
    if (obj == null) {
      return null;
    }
    if ((obj instanceof DateTime)) {
      return (DateTime)obj;
    }
    if ((obj instanceof Long))
    {
      Long val = (Long)obj;
      return new DateTime(val.longValue());
    }
    if ((obj instanceof Date))
    {
      Date val = (Date)obj;
      return new DateTime(val);
    }
    if ((obj instanceof LocalDate)) {
      return ((LocalDate)obj).toDateTimeAtStartOfDay();
    }
    if ((obj instanceof String))
    {
      String val = (String)obj;
      try
      {
        return DateTime.parse(val, defaultDateTimeFormatter);
      }
      catch (IllegalArgumentException iae)
      {
        Long l = Long.valueOf(Long.parseLong(val));
        return new DateTime(l);
      }
    }
    if ((obj instanceof SnmpPayload)) {
      return getSnmpDateTime(obj);
    }
    throw new IllegalArgumentException("Passed Parameter of type " + obj.getClass().getName() + " cannot be converted to DateTime");
  }
  
  public static DateTime TO_DATE(long val)
  {
    return new DateTime(val);
  }
  
  private static Map<String, DateTimeFormatter> dtfs = new ConcurrentHashMap();
  
  public static DateTime TO_DATE(Object obj, String format)
  {
    String value;
    if ((obj instanceof String)) {
      value = (String)obj;
    } else {
      value = obj.toString();
    }
    DateTimeFormatter dtf = (DateTimeFormatter)dtfs.get(format);
    if (dtf == null)
    {
      dtf = DateTimeFormat.forPattern(format);
      dtfs.put(format, dtf);
    }
    return dtf.parseDateTime(value);
  }
  
  private static Map<String, DateParser> dps = new ConcurrentHashMap();
  
  public static DateTime TO_DATEF(Object obj, String format)
  {
    String value;
    String value;
    if ((obj instanceof String)) {
      value = (String)obj;
    } else {
      value = obj.toString();
    }
    DateParser dp = (DateParser)dps.get(format);
    if (dp == null)
    {
      dp = new DateParser(format);
      dps.put(format, dp);
    }
    return dp.parse(value);
  }
  
  public static String TO_STRING(DateTime value, String format)
  {
    DateTimeFormatter dtf = (DateTimeFormatter)dtfs.get(format);
    if (dtf == null)
    {
      dtf = DateTimeFormat.forPattern(format);
      dtfs.put(format, dtf);
    }
    return dtf.print(value);
  }
  
  public static DateTime DNOW()
  {
    return DateTime.now();
  }
  
  public static int DYEARS(Object date)
  {
    if ((date instanceof DateTime)) {
      return ((DateTime)date).year().get();
    }
    if ((date instanceof LocalDate)) {
      return ((LocalDate)date).year().get();
    }
    throw new IllegalArgumentException("Passed Parameter of type " + date.getClass().getName() + " cannot be converted to DateTime or LocalDate");
  }
  
  public static int DMONTHS(Object date)
  {
    if ((date instanceof DateTime)) {
      return ((DateTime)date).monthOfYear().get();
    }
    if ((date instanceof LocalDate)) {
      return ((LocalDate)date).monthOfYear().get();
    }
    throw new IllegalArgumentException("Passed Parameter of type " + date.getClass().getName() + " cannot be converted to DateTime or LocalDate");
  }
  
  public static int DDAYS(Object date)
  {
    if ((date instanceof DateTime)) {
      return ((DateTime)date).dayOfMonth().get();
    }
    if ((date instanceof LocalDate)) {
      return ((LocalDate)date).dayOfMonth().get();
    }
    throw new IllegalArgumentException("Passed Parameter of type " + date.getClass().getName() + " cannot be converted to DateTime or LocalDate");
  }
  
  public static int DHOURS(DateTime date)
  {
    return date.hourOfDay().get();
  }
  
  public static int DMINS(DateTime date)
  {
    return date.minuteOfHour().get();
  }
  
  public static int DSECS(DateTime date)
  {
    return date.secondOfMinute().get();
  }
  
  public static int DMILLIS(DateTime date)
  {
    return date.getMillisOfSecond();
  }
  
  public static Period DYEARS(int num)
  {
    return Period.years(num);
  }
  
  public static Period DMONTHS(int num)
  {
    return Period.months(num);
  }
  
  public static Period DDAYS(int num)
  {
    return Period.days(num);
  }
  
  public static Period DHOURS(int num)
  {
    return Period.hours(num);
  }
  
  public static Period DMINS(int num)
  {
    return Period.minutes(num);
  }
  
  public static Period DSECS(int num)
  {
    return Period.seconds(num);
  }
  
  public static Period DMILLIS(int num)
  {
    return Period.millis(num);
  }
  
  public static boolean DBEFORE(Object first, Object second)
  {
    if (((first instanceof DateTime)) && ((second instanceof DateTime))) {
      return ((DateTime)second).isBefore((DateTime)first);
    }
    if (((first instanceof LocalDate)) && ((second instanceof LocalDate))) {
      return ((LocalDate)second).isBefore((LocalDate)second);
    }
    throw new IllegalArgumentException("Passed Parameters of type " + first.getClass().getName() + " / " + second.getClass().getName() + " cannot be converted to DateTime or LocalDate");
  }
  
  public static boolean DAFTER(Object first, Object second)
  {
    if (((first instanceof DateTime)) && ((second instanceof DateTime))) {
      return ((DateTime)second).isAfter((DateTime)first);
    }
    if (((first instanceof LocalDate)) && ((second instanceof LocalDate))) {
      return ((LocalDate)second).isAfter((LocalDate)second);
    }
    throw new IllegalArgumentException("Passed Parameters of type " + first.getClass().getName() + " / " + second.getClass().getName() + " cannot be converted to DateTime or LocalDate");
  }
  
  public static boolean DBETWEEN(Object first, Object second, Object third)
  {
    if (((first instanceof DateTime)) && ((second instanceof DateTime))) {
      return (((DateTime)first).isAfter((DateTime)second)) && (((DateTime)first).isBefore((DateTime)third));
    }
    if (((first instanceof LocalDate)) && ((second instanceof LocalDate))) {
      return (((LocalDate)first).isAfter((LocalDate)second)) && (((LocalDate)first).isBefore((LocalDate)third));
    }
    throw new IllegalArgumentException("Passed Parameters of type " + first.getClass().getName() + " / " + second.getClass().getName() + " cannot be converted to DateTime or LocalDate");
  }
  
  public static DateTime DADD(DateTime date, Period after)
  {
    return date.plus(after);
  }
  
  public static LocalDate DADD(LocalDate date, ReadablePeriod after)
  {
    return date.plus(after);
  }
  
  public static DateTime DSUBTRACT(DateTime date, Period before)
  {
    return date.minus(before);
  }
  
  public static LocalDate DSUBTRACT(LocalDate date, ReadablePeriod before)
  {
    return date.minus(before);
  }
  
  public static Period DDIFF(DateTime first, DateTime second)
  {
    long firstM = first.getMillis();
    long secondM = second.getMillis();
    long diff = firstM > secondM ? firstM - secondM : secondM - firstM;
    
    return new Period(diff);
  }
  
  public static long DDIFF_LONG(DateTime first, DateTime second)
  {
    long firstM = first.getMillis();
    long secondM = second.getMillis();
    long diff = firstM > secondM ? firstM - secondM : secondM - firstM;
    
    return diff;
  }
  
  public static int DDIFF(LocalDate first, LocalDate second)
  {
    return Days.daysBetween(first, second).getDays();
  }
  
  public static Short TO_SHORT(Object obj)
  {
    if (obj == null) {
      return null;
    }
    if ((obj instanceof Number)) {
      return Short.valueOf(((Number)obj).shortValue());
    }
    if ((obj instanceof String)) {
      return Short.valueOf(Short.parseShort((String)obj));
    }
    throw new IllegalArgumentException("Passed Parameter of type " + obj.getClass().getName() + " cannot be converted to short");
  }
  
  public static Integer TO_INT(Object obj, Integer radix)
  {
    if ((obj == null) || (radix == null) || (radix.intValue() < 2) || (radix.intValue() > 36)) {
      return null;
    }
    if ((obj instanceof Number)) {
      return Integer.valueOf(((Number)obj).intValue());
    }
    if ((obj instanceof String)) {
      return Integer.valueOf(Integer.parseInt((String)obj, radix.intValue()));
    }
    throw new IllegalArgumentException("Passed Parameter of type " + obj.getClass().getName() + " cannot be converted to int");
  }
  
  public static Integer TO_INT(Object obj)
  {
    return TO_INT(obj, Integer.valueOf(10));
  }
  
  public static Long TO_LONG(Object obj, Integer radix)
  {
    if ((obj == null) || (radix == null) || (radix.intValue() < 2) || (radix.intValue() > 36)) {
      return null;
    }
    if ((obj instanceof Number)) {
      return Long.valueOf(((Number)obj).longValue());
    }
    if ((obj instanceof String)) {
      return Long.valueOf(Long.parseLong((String)obj, radix.intValue()));
    }
    if ((obj instanceof DateTime)) {
      return Long.valueOf(((DateTime)obj).getMillis());
    }
    if ((obj instanceof Period)) {
      return Long.valueOf(((Period)obj).getMillis());
    }
    if ((obj instanceof Date)) {
      return Long.valueOf(((Date)obj).getTime());
    }
    throw new IllegalArgumentException("Passed Parameter of type " + obj.getClass().getName() + " cannot be converted to long");
  }
  
  public static Long TO_LONG(Object obj)
  {
    return TO_LONG(obj, Integer.valueOf(10));
  }
  
  public static Float TO_FLOAT(Object obj)
  {
    if (obj == null) {
      return null;
    }
    if ((obj instanceof Number)) {
      return Float.valueOf(((Number)obj).floatValue());
    }
    if ((obj instanceof String)) {
      return Float.valueOf(Float.parseFloat((String)obj));
    }
    throw new IllegalArgumentException("Passed Parameter of type " + obj.getClass().getName() + " cannot be converted to float");
  }
  
  public static Double TO_DOUBLE(Object obj)
  {
    if (obj == null) {
      return null;
    }
    if ((obj instanceof Number)) {
      return Double.valueOf(((Number)obj).doubleValue());
    }
    if ((obj instanceof String)) {
      return Double.valueOf(Double.parseDouble((String)obj));
    }
    throw new IllegalArgumentException("Passed Parameter of type " + obj.getClass().getName() + " cannot be converted to double");
  }
  
  public static double ROUND_DOUBLE(Object obj, Object places)
  {
    int pos = TO_INT(places).intValue();
    if (pos < 0) {
      throw new IllegalArgumentException("Passed Parameter of type " + places.getClass().getName() + " cannot be converted to int");
    }
    long factor = Math.pow(10.0D, pos);
    double d = TO_DOUBLE(obj).doubleValue();
    d = Math.round(d * factor);
    return d / factor;
  }
  
  public static float ROUND_FLOAT(Object obj, Object places)
  {
    int pos = TO_INT(places).intValue();
    if (pos < 0) {
      throw new IllegalArgumentException("Passed Parameter of type " + places.getClass().getName() + " cannot be converted to int");
    }
    long factor = Math.pow(10.0D, pos);
    float f = TO_FLOAT(obj).floatValue();
    f = Math.round(f * (float)factor);
    return f / (float)factor;
  }
  
  public static Boolean TO_BOOLEAN(Object obj)
  {
    if (obj == null) {
      return null;
    }
    if ((obj instanceof Boolean)) {
      return Boolean.valueOf(((Boolean)obj).booleanValue());
    }
    if ((obj instanceof String)) {
      return Boolean.valueOf(Boolean.parseBoolean((String)obj));
    }
    throw new IllegalArgumentException("Passed Parameter of type " + obj.getClass().getName() + " cannot be converted to boolean");
  }
  
  public static String TO_STRING(Object obj)
  {
    if (obj == null) {
      return null;
    }
    if ((obj instanceof String)) {
      return (String)obj;
    }
    if ((obj instanceof DateTime)) {
      return defaultDateTimeFormatter.print((DateTime)obj);
    }
    if ((obj instanceof SnmpPayload)) {
      return ((SnmpPayload)obj).getSnmpStringValue();
    }
    return obj.toString();
  }
  
  public static String TO_STRING(Object o, String encoding)
  {
    if (o == null) {
      return null;
    }
    try
    {
      return new String((byte[])o, encoding);
    }
    catch (UnsupportedEncodingException e)
    {
      throw new IllegalArgumentException("Parameter of type " + o.getClass().getName() + " cannot be converted to String");
    }
  }
  
  public static String TO_HEX(Object o)
  {
    if (o == null) {
      return null;
    }
    return Hex.encodeHexString((byte[])o);
  }
  
  public static String SRIGHT(Object obj, int from)
  {
    if (obj == null) {
      return null;
    }
    String val = TO_STRING(obj);
    return val.substring(from);
  }
  
  public static String SLEFT(Object obj, int to)
  {
    if (obj == null) {
      return null;
    }
    String val = TO_STRING(obj);
    return val.substring(0, to);
  }
  
  public static Double ABS(Double num)
  {
    return Double.valueOf(num.doubleValue() >= 0.0D ? num.doubleValue() : -num.doubleValue());
  }
  
  public static Float ABS(Float num)
  {
    return Float.valueOf(num.floatValue() >= 0.0D ? num.floatValue() : -num.floatValue());
  }
  
  public static Long ABS(Long num)
  {
    return Long.valueOf(num.longValue() >= 0L ? num.longValue() : -num.longValue());
  }
  
  public static Integer ABS(Integer num)
  {
    return Integer.valueOf(num.intValue() >= 0 ? num.intValue() : -num.intValue());
  }
  
  public static Short ABS(Short num)
  {
    return Short.valueOf((short)(num.shortValue() >= 0 ? num.shortValue() : -num.shortValue()));
  }
  
  public static Object NVL(Object obj, Object nvl)
  {
    return obj == null ? nvl : obj;
  }
  
  public static boolean IS_NULL(Object data)
  {
    if (data == null) {
      return true;
    }
    return false;
  }
  
  static Map<Object, Object> PREVs = new ConcurrentHashMap();
  
  public static Object PREV(Object key, Object val)
  {
    Object prevval = PREVs.get(key);
    PREVs.put(key, val);
    
    return prevval == null ? val : prevval;
  }
  
  public static DateTime getSnmpDateTime(Object o)
  {
    byte[] bytes = ((SnmpPayload)o).getSnmpByteValue();
    
    long year = 0L;
    int i = 1;
    for (int shiftBy = 0; shiftBy < 16; shiftBy += 8)
    {
      year |= (bytes[(0 + i)] & 0xFF) << shiftBy;
      i--;
    }
    byte[] direction = new byte[1];
    System.arraycopy(bytes, 8, direction, 0, 1);
    String hrOffsetStr = new String(direction) + bytes[9];
    int hoursOffset = Integer.parseInt(hrOffsetStr);
    
    return new DateTime((int)year, bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], DateTimeZone.forOffsetHoursMinutes(hoursOffset, bytes[10]));
  }
  
  public static Object VALUE(WAEvent obj, String key)
  {
    Object val = ((Map)obj.data[0]).get(key);
    return val;
  }
  
  public static Object VALUE(WAEvent evt, int n)
  {
    if (n < evt.data.length) {
      return evt.data[n];
    }
    return null;
  }
  
  public static String TO_MACID(Object o, String del)
  {
    String str = "";
    if (o == null) {
      return str;
    }
    byte[] tmp = ((SnmpPayload)o).getSnmpByteValue();
    for (int itr = 0; itr < tmp.length; itr++) {
      if (itr != 0) {
        str = str + del + String.format("%02X", new Object[] { Integer.valueOf(tmp[itr] & 0xFF) });
      } else {
        str = str + String.format("%02X", new Object[] { Integer.valueOf(tmp[itr] & 0xFF) });
      }
    }
    return str;
  }
  
  public static Object META(WAEvent e, String key)
  {
    Object o = e.metadata.get(key);
    Object ret = o == null ? "" : o;
    return ret;
  }
  
  public static Object META(JsonNodeEvent jsonNodeEvent, String key)
  {
    Object value = jsonNodeEvent.metadata.get(key);
    value = value == null ? "" : value;
    return value;
  }
  
  public static int FIELDCOUNT(WAEvent waEvent)
  {
    return waEvent.data.length;
  }
  
  public static boolean IS_PRESENT(WAEvent e, Object[] p_array, int index)
  {
    int pos = index / 7;
    int offset = index % 7;
    int val = 0;
    byte b = (byte)(1 << offset);
    if (p_array == e.data) {
      val = (e.dataPresenceBitMap[pos] & b) >> offset;
    } else if (p_array == e.before) {
      val = (e.beforePresenceBitMap[pos] & b) >> offset;
    }
    return val == 1;
  }
  
  private static HashMap<UUID, Field[]> typeUUIDCache = new HashMap();
  
  public static HashMap<String, Object> DATA(WAEvent event)
  {
    if (event.data != null) {
      return getDataOrBeforeArrayAsMap(event, event.data);
    }
    return null;
  }
  
  public static HashMap<String, Object> BEFORE(WAEvent event)
  {
    if (event.before != null) {
      return getDataOrBeforeArrayAsMap(event, event.before);
    }
    return null;
  }
  
  private static HashMap<String, Utf8> stringToUtf8Mapper = new HashMap();
  
  public static Object VALUE(Object object, String keyString)
  {
    if ((object instanceof HashMap))
    {
      HashMap<Utf8, ?> mp = (HashMap)object;
      if (stringToUtf8Mapper.containsKey(keyString)) {
        return mp.get(stringToUtf8Mapper.get(keyString));
      }
      Utf8 Utf8String = new Utf8(keyString);
      stringToUtf8Mapper.put(keyString, Utf8String);
      return mp.get(Utf8String);
    }
    return null;
  }
  
  private static HashMap<String, Object> getDataOrBeforeArrayAsMap(WAEvent event, Object[] dataOrBeforeArray)
  {
    Field[] fieldsOfThisTable = null;
    if (event.typeUUID != null) {
      if (typeUUIDCache.containsKey(event.typeUUID)) {
        fieldsOfThisTable = (Field[])typeUUIDCache.get(event.typeUUID);
      } else {
        try
        {
          MetaInfo.Type dataType = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(event.typeUUID, WASecurityManager.TOKEN);
          Class<?> typeClass = WALoader.get().loadClass(dataType.className);
          fieldsOfThisTable = typeClass.getDeclaredFields();
          typeUUIDCache.put(event.typeUUID, fieldsOfThisTable);
        }
        catch (MetaDataRepositoryException|ClassNotFoundException e)
        {
          logger.warn("Unable to fetch the type for table " + event.metadata.get("TableName") + e);
        }
      }
    }
    HashMap<String, Object> dataOrBeforeArrayMap = new HashMap();
    boolean isPresent;
    Integer localInteger1;
    for (Integer i = Integer.valueOf(0); i.intValue() < dataOrBeforeArray.length; localInteger1 = i = Integer.valueOf(i.intValue() + 1))
    {
      isPresent = IS_PRESENT(event, dataOrBeforeArray, i.intValue());
      if (isPresent) {
        dataOrBeforeArrayMap.put(fieldsOfThisTable[i.intValue()].getName(), dataOrBeforeArray[i.intValue()] != null ? dataOrBeforeArray[i.intValue()] : null);
      }
      isPresent = i;
    }
    return dataOrBeforeArrayMap;
  }
  
  public static String IP_COUNTRY(String ip)
  {
    return IPLookup2.lookupCountry(ip);
  }
  
  public static String IP_CITY(String ip)
  {
    return IPLookup2.lookupCity(ip);
  }
  
  public static double IP_LAT(String ip)
  {
    String strVal = IPLookup2.lookupLat(ip);
    if (strVal == null) {
      return 0.0D;
    }
    return Double.parseDouble(strVal);
  }
  
  public static double IP_LON(String ip)
  {
    String strVal = IPLookup2.lookupLon(ip);
    if (strVal == null) {
      return 0.0D;
    }
    return Double.parseDouble(strVal);
  }
  
  public static Pattern compile__like__pattern(String pat)
  {
    String re = pat;
    re = re.replace(".", "\\.");
    re = re.replace("*", "\\*");
    re = re.replace("?", "\\?");
    re = re.replace("(\\?", "(?");
    re = re.replace("^", "\\^");
    re = re.replace("$", "\\$");
    re = re.replace("%", ".*");
    re = re.replace("_", ".");
    Pattern p = Pattern.compile(re);
    return p;
  }
  
  public static Timestamp toTimestamp(long millisec, int nanosec)
  {
    Timestamp t = new Timestamp(millisec);
    t.setNanos(nanosec);
    return t;
  }
  
  public static DateTime addInterval(DateTime t, long interval)
  {
    return t.plus(interval / 1000L);
  }
  
  public static String ar(String[] data, int index)
  {
    try
    {
      return data[index];
    }
    catch (ArrayIndexOutOfBoundsException e) {}
    return "";
  }
  
  public static int arlen(Object[] data)
  {
    return data.length;
  }
  
  public static StringArrayEvent func(SimpleEvent e)
  {
    return new StringArrayEvent(System.currentTimeMillis());
  }
  
  public static int castToInt(String s)
  {
    int x = Integer.parseInt(s);
    return x;
  }
  
  public static double castToDouble(String s)
  {
    double x = Double.parseDouble(s);
    return x;
  }
  
  public static int strIndexOf(String s, String search)
  {
    int x = s.indexOf(search);
    return x;
  }
  
  public static int strLastIndexOf(String s, String search)
  {
    int x = s.lastIndexOf(search);
    return x;
  }
  
  public static int strLength(String s)
  {
    int x = s.length();
    return x;
  }
  
  public static String trimStr(String s)
  {
    if (s == null) {
      return s;
    }
    return s.trim();
  }
  
  public static String substr(String s, int start, int count)
  {
    return s.substring(start, count);
  }
  
  public static String castToString(int s)
  {
    return Integer.toString(s);
  }
  
  public static String castToString(Object s)
  {
    return (String)s;
  }
  
  public static StringArrayEvent castToStringArray(SimpleEvent inpEvent)
  {
    StringArrayEvent evt = new StringArrayEvent(System.currentTimeMillis());
    Object[] objarr = inpEvent.getPayload();
    String[] strdata = null;
    
    String[] dataArr = (String[])objarr[0];
    strdata = new String[dataArr.length];
    for (int i = 0; i < dataArr.length; i++) {
      strdata[i] = dataArr[i];
    }
    evt.setData(strdata);
    return evt;
  }
  
  public static String[] getEventData(SimpleEvent ev)
  {
    return getStringArrayData(castToStringArray(ev));
  }
  
  public static String[] getStringArrayData(StringArrayEvent str)
  {
    return str.getData();
  }
  
  public static long castToLong(String s)
  {
    return Long.parseLong(s);
  }
  
  public static String textFromFields(String text, SimpleEvent event)
  {
    Object[] objarr = event.getPayload();
    String ret = text;
    for (int i = objarr.length - 1; i >= 0; i--) {
      if (ret.indexOf("$" + (i + 1)) != -1)
      {
        String value = objarr[i].toString();
        
        String numVal = "" + (i + 1);
        String numReg = "";
        for (int j = 0; j < numVal.length(); j++) {
          numReg = numReg + "[" + numVal.charAt(j) + "]";
        }
        ret = ret.replaceAll("[$]" + numReg, value);
      }
    }
    return ret;
  }
  
  @CustomFunction(translator=matchTranslator.class)
  public abstract boolean match2(String paramString1, String paramString2);
  
  public static boolean match2impl(String a, String regex)
  {
    Pattern p = Pattern.compile(regex);
    Matcher m = p.matcher(a);
    return m.find();
  }
  
  public static boolean match2impl(String a, Pattern pat)
  {
    Matcher m = pat.matcher(a);
    return m.find();
  }
  
  public static String matchUser(String s)
  {
    String regex = "user\\W+(\\w+)";
    Pattern p = Pattern.compile(regex);
    Matcher m = p.matcher(s);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }
  
  public static String matchPort(String s)
  {
    String regex = "port\\W+(\\w+)";
    Pattern p = Pattern.compile(regex);
    Matcher m = p.matcher(s);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }
  
  static Map<String, Pattern> patterns = new HashMap();
  
  public static String match(String s, String regex)
  {
    return match(s, regex, 1);
  }
  
  public static String match(String s, String regex, int groupNumber)
  {
    Pattern p = (Pattern)patterns.get(regex);
    if (p == null)
    {
      p = Pattern.compile(regex);
      patterns.put(regex, p);
    }
    String ret = null;
    try
    {
      Matcher m = p.matcher(s);
      if (m.find()) {
        ret = m.group(groupNumber);
      }
    }
    catch (Exception e)
    {
      System.err.println(e);
    }
    return ret;
  }
  
  static HashMap<String, HashMap<String, Pattern>> all_patterns = new HashMap();
  
  public static boolean MATCH_ANY_ADD_PATTERN(String group, String pat)
  {
    HashMap<String, Pattern> group_patterns = (HashMap)all_patterns.get(group);
    if (group_patterns == null)
    {
      group_patterns = new HashMap();
      all_patterns.put(group, group_patterns);
    }
    if (group_patterns.containsKey(pat)) {
      return false;
    }
    if (pat.equals("<RESET>"))
    {
      group_patterns.clear();
      return false;
    }
    Pattern p = Pattern.compile(pat);
    group_patterns.put(pat, p);
    
    return true;
  }
  
  public static String MATCH_ANY_FIND_PATTERN(String group, String to_match)
  {
    String ret = null;
    
    HashMap<String, Pattern> group_patterns = (HashMap)all_patterns.get(group);
    if (group_patterns == null) {
      return ret;
    }
    for (Map.Entry<String, Pattern> entry : group_patterns.entrySet()) {
      if (((Pattern)entry.getValue()).matcher(to_match).matches())
      {
        ret = (String)entry.getKey();
        break;
      }
    }
    return ret;
  }
  
  public static String MATCH_ANY_EXACT_PATTERN(String group, String to_match)
  {
    String ret = null;
    
    HashMap<String, Pattern> group_patterns = (HashMap)all_patterns.get(group);
    if (group_patterns == null) {
      return ret;
    }
    for (Map.Entry<String, Pattern> entry : group_patterns.entrySet()) {
      if (((String)entry.getKey()).equals(to_match))
      {
        ret = (String)entry.getKey();
        break;
      }
    }
    return ret;
  }
  
  static Map<String, Map<String, Integer>> uidSets = new HashMap();
  
  public static int uid(String set, String name)
  {
    Map<String, Integer> uids = (Map)uidSets.get(set);
    if (uids == null)
    {
      uids = new HashMap();
      uidSets.put(set, uids);
    }
    Integer val = (Integer)uids.get(name);
    if (val == null)
    {
      val = Integer.valueOf(uids.size() + 1);
      uids.put(name, val);
    }
    return val.intValue();
  }
  
  public static String matchIP(String s)
  {
    Pattern p = Pattern.compile("([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})");
    Matcher m = p.matcher(s);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }
  
  public static String parseMonthToNum(String month)
  {
    if (month.equalsIgnoreCase("jan")) {
      return new String("01");
    }
    if (month.equalsIgnoreCase("feb")) {
      return new String("02");
    }
    if (month.equalsIgnoreCase("mar")) {
      return new String("03");
    }
    if (month.equalsIgnoreCase("apr")) {
      return new String("04");
    }
    if (month.equalsIgnoreCase("may")) {
      return new String("05");
    }
    if (month.equalsIgnoreCase("jun")) {
      return new String("06");
    }
    if (month.equalsIgnoreCase("jul")) {
      return new String("07");
    }
    if (month.equalsIgnoreCase("aug")) {
      return new String("08");
    }
    if (month.equalsIgnoreCase("sep")) {
      return new String("09");
    }
    if (month.equalsIgnoreCase("oct")) {
      return new String("10");
    }
    if (month.equalsIgnoreCase("nov")) {
      return new String("11");
    }
    if (month.equalsIgnoreCase("dec")) {
      return new String("12");
    }
    logger.warn("Invalid month");
    
    return null;
  }
  
  @CustomFunction(translator=EventListGetter.class)
  public static List<SimpleEvent> eventList(Object o)
  {
    Waction wa = (Waction)o;
    return wa.getEvents();
  }
  
  public static ArrayNode eventList(JsonNodeEvent event)
  {
    ArrayNode ret = JsonNodeFactory.instance.arrayNode();
    ObjectNode x = (ObjectNode)event.data;
    Iterator<JsonNode> ait;
    for (Iterator<Map.Entry<String, JsonNode>> it = x.fields(); it.hasNext();)
    {
      Map.Entry<String, JsonNode> e = (Map.Entry)it.next();
      String name = (String)e.getKey();
      JsonNode n = (JsonNode)e.getValue();
      if ((n.isArray()) && (name.contains(":"))) {
        for (ait = ((ArrayNode)n).elements(); ait.hasNext();) {
          ret.add((JsonNode)ait.next());
        }
      }
    }
    
    return ret;
  }
  
  public static MonitorBatchEvent processBatch(MonitorBatchEvent event)
  {
    try
    {
      return MonitorModel.processBatch(event);
    }
    catch (Exception e)
    {
      if ((e instanceof SearchPhaseExecutionException)) {
        logger.warn("Failed to process MonitorBatchEvent with exception " + e.getMessage());
      } else {
        logger.error("Failed to process MonitorBatchEvent with exception ", e);
      }
    }
    return null;
  }
  
  @CustomFunction(translator=AccessPrevEventInBuffer.class)
  public abstract Object prev();
  
  @CustomFunction(translator=AccessPrevEventInBuffer.class)
  public abstract Object prev(int paramInt);
  
  @CustomFunction(translator=AnyAttrLike.class)
  public abstract boolean anyattrlike(Object paramObject, String paramString);
  
  @CustomFunction(translator=DataSetItertorTrans.class)
  public abstract Iterator<SimpleEvent> it(Object paramObject);
  
  public static String windump(Iterator<SimpleEvent> it)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("windump {\n");
    while (it.hasNext())
    {
      SimpleEvent e = (SimpleEvent)it.next();
      sb.append(e + "\n");
    }
    sb.append("}\n");
    return sb.toString();
  }
  
  public static JsonNode makeJSON(String jsonText)
  {
    try
    {
      return new ObjectMapper().readTree(jsonText);
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }
  }
  
  public static List<?> makeList(Object... objs)
  {
    return new ArrayList(Arrays.asList(objs));
  }
  
  public static SimpleEvent makeSimpleEvent(Object... objs)
  {
    SimpleEvent e = new SimpleEvent(System.currentTimeMillis());
    e.payload = objs;
    return e;
  }
  
  public static StringArrayEvent makeStringArrayEvent(Object... objs)
  {
    StringArrayEvent e = new StringArrayEvent(System.currentTimeMillis());
    String[] data = new String[objs.length];
    int i = 0;
    for (Object o : objs) {
      data[(i++)] = o.toString();
    }
    e.setData(data);
    return e;
  }
  
  public static WAEvent makeWAEvent(Object... objs)
  {
    WAEvent e = new WAEvent();
    e.data = new Object[objs.length];
    e.dataPresenceBitMap = new byte[(objs.length + 7) / 8];
    int i = 0;
    for (Object o : objs) {
      e.setData(i++, o);
    }
    return e;
  }
  
  public static boolean Pause(Long period, SimpleEvent e)
  {
    LockSupport.parkNanos(period.longValue() * 1000L);
    return true;
  }
  
  public static Map<String, GeoSearchCoverTree<Event>> spatialIndices = new HashMap();
  @AggHandlerDesc(handler=SumInt.class)
  public abstract Integer sum(Integer paramInteger);
  
  @AggHandlerDesc(handler=SumLong.class)
  public abstract Long sum(Long paramLong);
  
  @AggHandlerDesc(handler=SumFloat.class)
  public abstract Float sum(Float paramFloat);
  
  @AggHandlerDesc(handler=SumDouble.class)
  public abstract Double sum(Double paramDouble);
  
  @AggHandlerDesc(handler=SumBigDecimal.class)
  public abstract BigDecimal sum(BigDecimal paramBigDecimal);
  
  @AggHandlerDesc(handler=AvgInt.class)
  public abstract Integer avg(Integer paramInteger);
  
  @AggHandlerDesc(handler=AvgLong.class)
  public abstract Long avg(Long paramLong);
  
  @AggHandlerDesc(handler=AvgFloat.class)
  public abstract Float avg(Float paramFloat);
  
  @AggHandlerDesc(handler=AvgDouble.class)
  public abstract Double avg(Double paramDouble);
  
  @AggHandlerDesc(handler=AvgNumber.class)
  public abstract Double avgD(Number paramNumber);
  
  @AggHandlerDesc(handler=StdDevPNumber.class)
  public abstract Double StdDevP(Number paramNumber);
  
  @AggHandlerDesc(handler=StdDevSNumber.class)
  public abstract Double StdDevS(Number paramNumber);
  
  @AggHandlerDesc(handler=CountNotNull.class, distinctHandler=CountNotNullDistinct.class)
  public abstract int count(Object paramObject);
  
  @AggHandlerDesc(handler=CountAny.class)
  public abstract int count();
  
  @AggHandlerDesc(handler=FirstAny.class)
  public abstract Object first(Object paramObject);
  
  @AcceptWildcard
  @AggHandlerDesc(handler=LastAny.class)
  public abstract Object last(Object paramObject);
  
  @AggHandlerDesc(handler=MinInt.class)
  public abstract Integer min(Integer paramInteger);
  
  @AggHandlerDesc(handler=MinLong.class)
  public abstract Long min(Long paramLong);
  
  @AggHandlerDesc(handler=MinFloat.class)
  public abstract Float min(Float paramFloat);
  
  @AggHandlerDesc(handler=MinDouble.class)
  public abstract Double min(Double paramDouble);
  
  @AggHandlerDesc(handler=MinString.class)
  public abstract String min(String paramString);
  
  @AggHandlerDesc(handler=LastInt.class)
  public abstract Integer last(Integer paramInteger);
  
  @AggHandlerDesc(handler=LastLong.class)
  public abstract Long last(Long paramLong);
  
  @AggHandlerDesc(handler=LastFloat.class)
  public abstract Float last(Float paramFloat);
  
  @AggHandlerDesc(handler=LastDouble.class)
  public abstract Double last(Double paramDouble);
  
  @AggHandlerDesc(handler=LastString.class)
  public abstract String last(String paramString);
  
  @AggHandlerDesc(handler=LastDateTime.class)
  public abstract DateTime last(DateTime paramDateTime);
  
  @AggHandlerDesc(handler=FirstInt.class)
  public abstract Integer first(Integer paramInteger);
  
  @AggHandlerDesc(handler=FirstLong.class)
  public abstract Long first(Long paramLong);
  
  @AggHandlerDesc(handler=FirstFloat.class)
  public abstract Float first(Float paramFloat);
  
  @AggHandlerDesc(handler=FirstDouble.class)
  public abstract Double first(Double paramDouble);
  
  @AggHandlerDesc(handler=FirstString.class)
  public abstract String first(String paramString);
  
  @AggHandlerDesc(handler=FirstDateTime.class)
  public abstract DateTime first(DateTime paramDateTime);
  
  @AggHandlerDesc(handler=MaxInt.class)
  public abstract Integer max(Integer paramInteger);
  
  @AggHandlerDesc(handler=MaxLong.class)
  public abstract Long max(Long paramLong);
  
  @AggHandlerDesc(handler=MaxFloat.class)
  public abstract Float max(Float paramFloat);
  
  @AggHandlerDesc(handler=MaxDouble.class)
  public abstract Double max(Double paramDouble);
  
  @AggHandlerDesc(handler=MaxString.class)
  public abstract String max(String paramString);
  
  @AggHandlerDesc(handler=MaxOccursString.class)
  public abstract String maxOccurs(String paramString);
  
  @AggHandlerDesc(handler=DblArray.class)
  public abstract DoubleArrayList dblarray(double paramDouble);
  
  @AggHandlerDesc(handler=GenList.class)
  public abstract List list(Object... paramVarArgs);
  
  @AggHandlerDesc(handler=GeometryConv.class)
  public abstract LocationArrayList locInfoList(double paramDouble1, double paramDouble2, String paramString1, String paramString2, String paramString3);
  
  @AggHandlerDesc(handler=Spatial.class)
  public abstract GeoSearchCoverTree<Event> SPATIAL_INDEX(String paramString, Event paramEvent, double paramDouble1, double paramDouble2);
  
  public static class Spatial
  {
    private GeoSearchCoverTree<Event> search = null;
    
    public GeoSearchCoverTree<Event> getAggValue()
    {
      return this.search;
    }
    
    public void incAggValue(String indexName, Event e, double lat, double lon)
    {
      if (this.search == null)
      {
        this.search = ((GeoSearchCoverTree)BuiltInFunc.spatialIndices.get(indexName));
        if (this.search == null)
        {
          this.search = new GeoSearchCoverTree();
          BuiltInFunc.spatialIndices.put(indexName, this.search);
        }
      }
      this.search.add(e, lat, lon);
    }
    
    public void decAggValue(String indexName, Event e, double lat, double lon)
    {
      if (this.search == null)
      {
        this.search = ((GeoSearchCoverTree)BuiltInFunc.spatialIndices.get(indexName));
        if (this.search == null) {
          return;
        }
      }
      this.search.remove(e, lat, lon);
    }
  }
  
  public static List<Event> SPATIAL_FIND_IN_RADIUS(String indexName, double lat, double lon, double radiusInM)
  {
    GeoSearchCoverTree<Event> search = (GeoSearchCoverTree)spatialIndices.get(indexName);
    if (search == null) {
      return Collections.EMPTY_LIST;
    }
    return search.queryRadius(lat, lon, radiusInM);
  }
  
  public static double SPATIAL_DISTANCE_M(double lat1, double lon1, double lat2, double lon2)
  {
    return GeoSearchCoverTree.distFrom(lat1, lon1, lat2, lon2);
  }
  
  public static double SPATIAL_DISTANCE_MILES(double lat1, double lon1, double lat2, double lon2)
  {
    return 6.21371E-4D * GeoSearchCoverTree.distFrom(lat1, lon1, lat2, lon2);
  }
  
  public static Point SPATIAL_POINT(double lat, double lon)
  {
    return new Point(lat, lon);
  }
  
  public static Point SPATIAL_POINT(Object obj)
  {
    if ((obj instanceof String)) {
      return new Point((String)obj);
    }
    if ((obj instanceof JsonNode)) {
      return new Point((JsonNode)obj);
    }
    if ((obj instanceof double[])) {
      return new Point((double[])obj);
    }
    throw new RuntimeException("Can only convert String, JsonNode or double[] to Point, got " + obj.getClass());
  }
  
  public static Polygon SPATIAL_POLYGON(Object obj)
  {
    if ((obj instanceof String)) {
      return new Polygon((String)obj);
    }
    if ((obj instanceof JsonNode)) {
      return new Polygon((JsonNode)obj);
    }
    if ((obj instanceof double[])) {
      return new Polygon((double[])obj);
    }
    if ((obj instanceof double[][])) {
      return new Polygon((double[][])obj);
    }
    throw new RuntimeException("Can only convert String, JsonNode, double[] or double[][] to Polygon, got " + obj.getClass());
  }
  
  public static boolean SPATIAL_POINT_IN_POLYGON(Point point, Polygon polygon)
  {
    return polygon.isInside(point);
  }
  
  public static class GeometryConv
  {
    LocationArrayList value = new LocationArrayList();
    
    /* Error */
    public LocationArrayList getAggValue()
    {
      // Byte code:
      //   0: aload_0
      //   1: getfield 4	com/bloom/runtime/BuiltInFunc$GeometryConv:value	Lcom/bloom/runtime/LocationArrayList;
      //   4: dup
      //   5: astore_1
      //   6: monitorenter
      //   7: new 2	com/bloom/runtime/LocationArrayList
      //   10: dup
      //   11: aload_0
      //   12: getfield 4	com/bloom/runtime/BuiltInFunc$GeometryConv:value	Lcom/bloom/runtime/LocationArrayList;
      //   15: invokespecial 5	com/bloom/runtime/LocationArrayList:<init>	(Lcom/bloom/runtime/LocationArrayList;)V
      //   18: aload_1
      //   19: monitorexit
      //   20: areturn
      //   21: astore_2
      //   22: aload_1
      //   23: monitorexit
      //   24: aload_2
      //   25: athrow
      // Line number table:
      //   Java source line #1293	-> byte code offset #0
      //   Java source line #1294	-> byte code offset #7
      //   Java source line #1295	-> byte code offset #21
      // Local variable table:
      //   start	length	slot	name	signature
      //   0	26	0	this	GeometryConv
      //   5	18	1	Ljava/lang/Object;	Object
      //   21	4	2	localObject1	Object
      // Exception table:
      //   from	to	target	type
      //   7	20	21	finally
      //   21	24	21	finally
    }
    
    public void incAggValue(double latVal, double longVal, String city, String zip, String companyName)
    {
      synchronized (this.value)
      {
        this.value.add(new LocationInfo(latVal, longVal, city, zip, companyName));
      }
    }
    
    public void decAggValue(double latVal, double longVal, String city, String zip, String companyName)
    {
      synchronized (this.value)
      {
        this.value.remove(new LocationInfo(latVal, longVal, city, zip, companyName));
      }
    }
  }
  
  public static class DblArray
  {
    DoubleArrayList value = new DoubleArrayList();
    
    /* Error */
    public DoubleArrayList getAggValue()
    {
      // Byte code:
      //   0: aload_0
      //   1: getfield 4	com/bloom/runtime/BuiltInFunc$DblArray:value	Lcern/colt/list/DoubleArrayList;
      //   4: dup
      //   5: astore_1
      //   6: monitorenter
      //   7: aload_0
      //   8: getfield 4	com/bloom/runtime/BuiltInFunc$DblArray:value	Lcern/colt/list/DoubleArrayList;
      //   11: invokevirtual 5	cern/colt/list/DoubleArrayList:copy	()Lcern/colt/list/DoubleArrayList;
      //   14: aload_1
      //   15: monitorexit
      //   16: areturn
      //   17: astore_2
      //   18: aload_1
      //   19: monitorexit
      //   20: aload_2
      //   21: athrow
      // Line number table:
      //   Java source line #1315	-> byte code offset #0
      //   Java source line #1316	-> byte code offset #7
      //   Java source line #1317	-> byte code offset #17
      // Local variable table:
      //   start	length	slot	name	signature
      //   0	22	0	this	DblArray
      //   5	14	1	Ljava/lang/Object;	Object
      //   17	4	2	localObject1	Object
      // Exception table:
      //   from	to	target	type
      //   7	16	17	finally
      //   17	20	17	finally
    }
    
    public void incAggValue(double arg)
    {
      synchronized (this.value)
      {
        this.value.add(arg);
      }
    }
    
    public void decAggValue(double arg)
    {
      synchronized (this.value)
      {
        assert (!this.value.isEmpty());
        assert (this.value.getQuick(0) == arg);
        this.value.remove(0);
      }
    }
  }
  
  public static class GenList
  {
    List value = new CopyOnWriteArrayList();
    boolean allEvents = true;
    
    public List getAggValue()
    {
      if (this.allEvents) {
        Collections.sort(this.value, new Comparator()
        {
          public int compare(Event o1, Event o2)
          {
            if ((o1 == null) && (o2 == null)) {
              return 0;
            }
            if ((o1 != null) && (o2 == null)) {
              return 1;
            }
            if ((o1 == null) && (o2 != null)) {
              return -1;
            }
            UUID u1 = o1.get_wa_SimpleEvent_ID();
            UUID u2 = o2.get_wa_SimpleEvent_ID();
            if ((u1 == null) && (u2 == null)) {
              return 0;
            }
            if ((u1 != null) && (u2 == null)) {
              return 1;
            }
            if ((u1 == null) && (u2 != null)) {
              return -1;
            }
            return u1.compareTo(u2);
          }
        });
      }
      return this.value;
    }
    
    public void incAggValue(Object... arg)
    {
      for (Object a : arg) {
        if (a != null)
        {
          this.value.add(a);
          if (!(a instanceof Event)) {
            this.allEvents = false;
          }
        }
      }
    }
    
    public void decAggValue(Object... arg)
    {
      for (Object a : arg) {
        if (a != null) {
          this.value.remove(a);
        }
      }
    }
  }
  
  public static class SumInt
  {
    int count = 0;
    int value = 0;
    
    public Integer getAggValue()
    {
      return Integer.valueOf(this.count == 0 ? 0 : this.value);
    }
    
    public void incAggValue(Integer arg)
    {
      if (arg != null)
      {
        this.count += 1;
        this.value += arg.intValue();
      }
    }
    
    public void decAggValue(Integer arg)
    {
      if (arg != null)
      {
        this.count -= 1;
        this.value -= arg.intValue();
      }
    }
  }
  
  public static class SumLong
  {
    int count = 0;
    long value = 0L;
    
    public Long getAggValue()
    {
      return Long.valueOf(this.count == 0 ? 0L : this.value);
    }
    
    public void incAggValue(Long arg)
    {
      if (arg != null)
      {
        this.count += 1;
        this.value += arg.longValue();
      }
    }
    
    public void decAggValue(Long arg)
    {
      if (arg != null)
      {
        this.count -= 1;
        this.value -= arg.longValue();
      }
    }
  }
  
  public static class SumFloat
  {
    int count = 0;
    float value = 0.0F;
    
    public Float getAggValue()
    {
      return Float.valueOf(this.count == 0 ? 0.0F : this.value);
    }
    
    public void incAggValue(Float arg)
    {
      if (arg != null)
      {
        this.count += 1;
        this.value += arg.floatValue();
      }
    }
    
    public void decAggValue(Float arg)
    {
      if (arg != null)
      {
        this.count -= 1;
        this.value -= arg.floatValue();
      }
    }
  }
  
  public static class SumDouble
  {
    int count = 0;
    double value = 0.0D;
    
    public Double getAggValue()
    {
      return Double.valueOf(this.count == 0 ? 0.0D : this.value);
    }
    
    public void incAggValue(Double arg)
    {
      if (arg != null)
      {
        this.count += 1;
        this.value += arg.doubleValue();
      }
    }
    
    public void decAggValue(Double arg)
    {
      if (arg != null)
      {
        this.count -= 1;
        this.value -= arg.doubleValue();
      }
    }
  }
  
  public static class SumBigDecimal
  {
    int count = 0;
    BigDecimal value = BigDecimal.ZERO;
    
    public BigDecimal getAggValue()
    {
      return this.count == 0 ? BigDecimal.ZERO : this.value;
    }
    
    public void incAggValue(BigDecimal arg)
    {
      if (arg != null)
      {
        this.count += 1;
        this.value = this.value.add(arg);
      }
    }
    
    public void decAggValue(BigDecimal arg)
    {
      if (arg != null)
      {
        this.count -= 1;
        this.value = this.value.subtract(arg);
      }
    }
  }
  
  public static class AvgInt
  {
    int value = 0;
    int count = 0;
    
    public Integer getAggValue()
    {
      return Integer.valueOf(this.count == 0 ? 0 : this.value / this.count);
    }
    
    public void incAggValue(Integer arg)
    {
      if (arg != null)
      {
        this.count += 1;
        this.value += arg.intValue();
      }
    }
    
    public void decAggValue(Integer arg)
    {
      if (arg != null)
      {
        this.count -= 1;
        this.value -= arg.intValue();
      }
    }
  }
  
  public static class AvgLong
  {
    long value = 0L;
    int count = 0;
    
    public Long getAggValue()
    {
      return Long.valueOf(this.count == 0 ? 0L : this.value / this.count);
    }
    
    public void incAggValue(Long arg)
    {
      if (arg != null)
      {
        this.count += 1;
        this.value += arg.longValue();
      }
    }
    
    public void decAggValue(Long arg)
    {
      if (arg != null)
      {
        this.count -= 1;
        this.value -= arg.longValue();
      }
    }
  }
  
  public static class AvgFloat
  {
    float value = 0.0F;
    int count = 0;
    
    public Float getAggValue()
    {
      return Float.valueOf(this.count == 0 ? 0.0F : this.value / this.count);
    }
    
    public void incAggValue(Float arg)
    {
      if (arg != null)
      {
        this.count += 1;
        this.value += arg.floatValue();
      }
    }
    
    public void decAggValue(Float arg)
    {
      if (arg != null)
      {
        this.count -= 1;
        this.value -= arg.floatValue();
      }
    }
  }
  
  public static class AvgDouble
  {
    double value = 0.0D;
    int count = 0;
    
    public Double getAggValue()
    {
      return Double.valueOf(this.count == 0 ? 0.0D : this.value / this.count);
    }
    
    public void incAggValue(Double arg)
    {
      if (arg != null)
      {
        this.count += 1;
        this.value += arg.doubleValue();
      }
    }
    
    public void decAggValue(Double arg)
    {
      if (arg != null)
      {
        this.count -= 1;
        this.value -= arg.doubleValue();
      }
    }
  }
  
  public static class AvgNumber
  {
    double value = 0.0D;
    int count = 0;
    
    public Double getAggValue()
    {
      return Double.valueOf(this.count == 0 ? 0.0D : this.value / this.count);
    }
    
    public void incAggValue(Number arg)
    {
      if (arg != null)
      {
        this.count += 1;
        this.value += arg.doubleValue();
      }
    }
    
    public void decAggValue(Number arg)
    {
      if (arg != null)
      {
        this.count -= 1;
        this.value -= arg.doubleValue();
      }
    }
  }
  
  public static class StdDevPNumber
  {
    Double m = Double.valueOf(0.0D);
    Double S = Double.valueOf(0.0D);
    int count = 0;
    
    public Double getAggValue()
    {
      return Double.valueOf(this.count == 0 ? 0.0D : Math.sqrt(this.S.doubleValue() / this.count));
    }
    
    public void incAggValue(Number arg)
    {
      if (arg != null)
      {
        Double prev_mean = this.m;
        double argd = arg.doubleValue();
        this.count += 1;
        this.m = Double.valueOf(this.m.doubleValue() + (argd - this.m.doubleValue()) / this.count);
        this.S = Double.valueOf(this.S.doubleValue() + (argd - this.m.doubleValue()) * (argd - prev_mean.doubleValue()));
      }
    }
    
    public void decAggValue(Number arg)
    {
      if (arg != null)
      {
        Double prev_mean = this.m;
        double argd = arg.doubleValue();
        this.m = Double.valueOf(this.m.doubleValue() - (argd - this.m.doubleValue()) / this.count);
        this.S = Double.valueOf(this.S.doubleValue() - (argd - this.m.doubleValue()) * (argd - prev_mean.doubleValue()));
        this.count -= 1;
      }
    }
  }
  
  public static class StdDevSNumber
  {
    Double m = Double.valueOf(0.0D);
    Double S = Double.valueOf(0.0D);
    int count = 0;
    
    public Double getAggValue()
    {
      return Double.valueOf(this.count <= 1 ? 0.0D : Math.sqrt(this.S.doubleValue() / (this.count - 1)));
    }
    
    public void incAggValue(Number arg)
    {
      if (arg != null)
      {
        Double prev_mean = this.m;
        double argd = arg.doubleValue();
        this.count += 1;
        this.m = Double.valueOf(this.m.doubleValue() + (argd - this.m.doubleValue()) / this.count);
        this.S = Double.valueOf(this.S.doubleValue() + (argd - this.m.doubleValue()) * (argd - prev_mean.doubleValue()));
      }
    }
    
    public void decAggValue(Number arg)
    {
      if (arg != null)
      {
        Double prev_mean = this.m;
        double argd = arg.doubleValue();
        this.m = Double.valueOf(this.m.doubleValue() - (argd - this.m.doubleValue()) / this.count);
        this.S = Double.valueOf(this.S.doubleValue() - (argd - this.m.doubleValue()) * (argd - prev_mean.doubleValue()));
        this.count -= 1;
      }
    }
  }
  
  public static class CountNotNull
  {
    int count = 0;
    
    public int getAggValue()
    {
      return this.count;
    }
    
    public void incAggValue(Object arg)
    {
      if (arg != null) {
        this.count += 1;
      }
    }
    
    public void decAggValue(Object arg)
    {
      if (arg != null) {
        this.count -= 1;
      }
    }
  }
  
  public static class CountNotNullDistinct
  {
    Map<Object, Integer> map = new HashMap();
    
    public int getAggValue()
    {
      return this.map.size();
    }
    
    public void incAggValue(Object arg)
    {
      if (arg != null)
      {
        Integer i = (Integer)this.map.get(arg);
        if (i == null) {
          this.map.put(arg, Integer.valueOf(1));
        } else {
          this.map.put(arg, Integer.valueOf(i.intValue() + 1));
        }
      }
    }
    
    public void decAggValue(Object arg)
    {
      if (arg != null)
      {
        Integer i = (Integer)this.map.get(arg);
        if (i.intValue() == 1) {
          this.map.remove(arg);
        } else {
          this.map.put(arg, Integer.valueOf(i.intValue() - 1));
        }
      }
    }
  }
  
  public static class CountAny
  {
    int count = 0;
    
    public int getAggValue()
    {
      return this.count;
    }
    
    public void incAggValue()
    {
      this.count += 1;
    }
    
    public void decAggValue()
    {
      this.count -= 1;
    }
  }
  
  public static class FirstAny
  {
    LinkedList<Object> list = new LinkedList();
    
    public Object getAggValue()
    {
      if (this.list.isEmpty()) {
        return null;
      }
      return this.list.getFirst();
    }
    
    public void incAggValue(Object arg)
    {
      this.list.addLast(arg);
    }
    
    public void decAggValue(Object arg)
    {
      if (!this.list.isEmpty()) {
        this.list.removeFirst();
      }
    }
  }
  
  public static class FirstInt
  {
    LinkedList<Integer> list = new LinkedList();
    
    public Integer getAggValue()
    {
      if (this.list.isEmpty()) {
        return null;
      }
      return (Integer)this.list.getFirst();
    }
    
    public void incAggValue(Integer arg)
    {
      this.list.addLast(arg);
    }
    
    public void decAggValue(Integer arg)
    {
      if (!this.list.isEmpty()) {
        this.list.removeFirst();
      }
    }
  }
  
  public static class FirstLong
  {
    LinkedList<Long> list = new LinkedList();
    
    public Long getAggValue()
    {
      if (this.list.isEmpty()) {
        return null;
      }
      return (Long)this.list.getFirst();
    }
    
    public void incAggValue(Long arg)
    {
      this.list.addLast(arg);
    }
    
    public void decAggValue(Long arg)
    {
      if (!this.list.isEmpty()) {
        this.list.removeFirst();
      }
    }
  }
  
  public static class FirstFloat
  {
    LinkedList<Float> list = new LinkedList();
    
    public Float getAggValue()
    {
      if (this.list.isEmpty()) {
        return null;
      }
      return (Float)this.list.getFirst();
    }
    
    public void incAggValue(Float arg)
    {
      this.list.addLast(arg);
    }
    
    public void decAggValue(Float arg)
    {
      if (!this.list.isEmpty()) {
        this.list.removeFirst();
      }
    }
  }
  
  public static class FirstDouble
  {
    LinkedList<Double> list = new LinkedList();
    
    public Double getAggValue()
    {
      if (this.list.isEmpty()) {
        return null;
      }
      return (Double)this.list.getFirst();
    }
    
    public void incAggValue(Double arg)
    {
      this.list.addLast(arg);
    }
    
    public void decAggValue(Double arg)
    {
      if (!this.list.isEmpty()) {
        this.list.removeFirst();
      }
    }
  }
  
  public static class FirstString
  {
    LinkedList<String> list = new LinkedList();
    
    public String getAggValue()
    {
      if (this.list.isEmpty()) {
        return null;
      }
      return (String)this.list.getFirst();
    }
    
    public void incAggValue(String arg)
    {
      this.list.addLast(arg);
    }
    
    public void decAggValue(String arg)
    {
      if (!this.list.isEmpty()) {
        this.list.removeFirst();
      }
    }
  }
  
  public static class FirstDateTime
  {
    LinkedList<DateTime> list = new LinkedList();
    
    public DateTime getAggValue()
    {
      if (this.list.isEmpty()) {
        return null;
      }
      return (DateTime)this.list.getFirst();
    }
    
    public void incAggValue(DateTime arg)
    {
      this.list.addLast(arg);
    }
    
    public void decAggValue(DateTime arg)
    {
      if (!this.list.isEmpty()) {
        this.list.removeFirst();
      }
    }
  }
  
  public static class LastAny
  {
    Object last;
    
    public Object getAggValue()
    {
      return this.last;
    }
    
    public void incAggValue(Object arg)
    {
      this.last = arg;
    }
    
    public void decAggValue(Object arg) {}
  }
  
  public static class LastInt
  {
    Integer last;
    
    public Integer getAggValue()
    {
      return this.last;
    }
    
    public void incAggValue(Integer arg)
    {
      this.last = arg;
    }
    
    public void decAggValue(Integer arg) {}
  }
  
  public static class LastLong
  {
    Long last;
    
    public Long getAggValue()
    {
      return this.last;
    }
    
    public void incAggValue(Long arg)
    {
      this.last = arg;
    }
    
    public void decAggValue(Long arg) {}
  }
  
  public static class LastFloat
  {
    Float last;
    
    public Float getAggValue()
    {
      return this.last;
    }
    
    public void incAggValue(Float arg)
    {
      this.last = arg;
    }
    
    public void decAggValue(Float arg) {}
  }
  
  public static class LastDouble
  {
    Double last;
    
    public Double getAggValue()
    {
      return this.last;
    }
    
    public void incAggValue(Double arg)
    {
      this.last = arg;
    }
    
    public void decAggValue(Double arg) {}
  }
  
  public static class LastString
  {
    String last;
    
    public String getAggValue()
    {
      return this.last;
    }
    
    public void incAggValue(String arg)
    {
      this.last = arg;
    }
    
    public void decAggValue(String arg) {}
  }
  
  public static class LastDateTime
  {
    DateTime last;
    
    public DateTime getAggValue()
    {
      return this.last;
    }
    
    public void incAggValue(DateTime arg)
    {
      this.last = arg;
    }
    
    public void decAggValue(DateTime arg) {}
  }
  
  public static class MinInt
  {
    MinMaxTree<Integer> tree = new MinMaxTree();
    
    public Integer getAggValue()
    {
      return (Integer)this.tree.getMin();
    }
    
    public void incAggValue(Integer arg)
    {
      this.tree.addKey(arg);
    }
    
    public void decAggValue(Integer arg)
    {
      this.tree.removeKey(arg);
    }
  }
  
  public static class MinLong
  {
    MinMaxTree<Long> tree = new MinMaxTree();
    
    public Long getAggValue()
    {
      return (Long)this.tree.getMin();
    }
    
    public void incAggValue(Long arg)
    {
      this.tree.addKey(arg);
    }
    
    public void decAggValue(Long arg)
    {
      this.tree.removeKey(arg);
    }
  }
  
  public static class MinFloat
  {
    MinMaxTree<Float> tree = new MinMaxTree();
    
    public Float getAggValue()
    {
      return (Float)this.tree.getMin();
    }
    
    public void incAggValue(Float arg)
    {
      this.tree.addKey(arg);
    }
    
    public void decAggValue(Float arg)
    {
      this.tree.removeKey(arg);
    }
  }
  
  public static class MinDouble
  {
    MinMaxTree<Double> tree = new MinMaxTree();
    
    public Double getAggValue()
    {
      return (Double)this.tree.getMin();
    }
    
    public void incAggValue(Double arg)
    {
      this.tree.addKey(arg);
    }
    
    public void decAggValue(Double arg)
    {
      this.tree.removeKey(arg);
    }
  }
  
  public static class MinString
  {
    MinMaxTree<String> tree = new MinMaxTree();
    
    public String getAggValue()
    {
      return (String)this.tree.getMin();
    }
    
    public void incAggValue(String arg)
    {
      this.tree.addKey(arg);
    }
    
    public void decAggValue(String arg)
    {
      this.tree.removeKey(arg);
    }
  }
  
  public static class MaxInt
  {
    MinMaxTree<Integer> tree = new MinMaxTree();
    
    public Integer getAggValue()
    {
      return (Integer)this.tree.getMax();
    }
    
    public void incAggValue(Integer arg)
    {
      this.tree.addKey(arg);
    }
    
    public void decAggValue(Integer arg)
    {
      this.tree.removeKey(arg);
    }
  }
  
  public static class MaxLong
  {
    MinMaxTree<Long> tree = new MinMaxTree();
    
    public Long getAggValue()
    {
      return (Long)this.tree.getMax();
    }
    
    public void incAggValue(Long arg)
    {
      this.tree.addKey(arg);
    }
    
    public void decAggValue(Long arg)
    {
      this.tree.removeKey(arg);
    }
  }
  
  public static class MaxFloat
  {
    MinMaxTree<Float> tree = new MinMaxTree();
    
    public Float getAggValue()
    {
      return (Float)this.tree.getMax();
    }
    
    public void incAggValue(Float arg)
    {
      this.tree.addKey(arg);
    }
    
    public void decAggValue(Float arg)
    {
      this.tree.removeKey(arg);
    }
  }
  
  public static class MaxDouble
  {
    MinMaxTree<Double> tree = new MinMaxTree();
    
    public Double getAggValue()
    {
      return (Double)this.tree.getMax();
    }
    
    public void incAggValue(Double arg)
    {
      this.tree.addKey(arg);
    }
    
    public void decAggValue(Double arg)
    {
      this.tree.removeKey(arg);
    }
  }
  
  public static class MaxString
  {
    MinMaxTree<String> tree = new MinMaxTree();
    
    public String getAggValue()
    {
      return (String)this.tree.getMax();
    }
    
    public void incAggValue(String arg)
    {
      this.tree.addKey(arg);
    }
    
    public void decAggValue(String arg)
    {
      this.tree.removeKey(arg);
    }
  }
  
  public static class MaxOccursString
  {
    MinMaxTree<String> tree = new MinMaxTree();
    
    public String getAggValue()
    {
      return (String)this.tree.getMaxOccurs();
    }
    
    public void incAggValue(String arg)
    {
      this.tree.addKey(arg);
    }
    
    public void decAggValue(String arg)
    {
      this.tree.removeKey(arg);
    }
  }
}
