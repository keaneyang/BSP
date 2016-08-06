package com.bloom.utility;

import com.bloom.anno.PropertyTemplate;
import com.bloom.kafkamessaging.OffsetPosition;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.runtime.Interval;
import com.bloom.runtime.Property;
import com.bloom.runtime.WactionStorePersistencePolicy;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.TypeField;
import com.bloom.runtime.compiler.TypeName;
import com.bloom.runtime.compiler.stmts.MappedStream;
import com.bloom.runtime.compiler.stmts.OutputClause;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.IntervalPolicy;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.IntervalPolicy.AttrBasedPolicy;
import com.bloom.runtime.meta.IntervalPolicy.CountBasedPolicy;
import com.bloom.runtime.meta.IntervalPolicy.TimeBasedPolicy;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Namespace;
import com.bloom.runtime.meta.MetaInfo.WAStoreView;
import com.bloom.runtime.meta.MetaInfo.Window;
import com.bloom.runtime.meta.MetaInfo.Flow.Detail;
import com.bloom.runtime.utils.Factory;
import com.bloom.security.ObjectPermission;
import com.bloom.security.WASecurityManager;
import com.bloom.security.ObjectPermission.ObjectType;
import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;
import com.bloom.wactionstore.Type;
import com.bloom.recovery.Path;
import com.bloom.recovery.Path.Item;
import com.bloom.recovery.Path.ItemList;
import com.bloom.recovery.Position;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.joda.time.IllegalFieldValueException;
import org.joda.time.LocalTime;

public class Utility
{
  private static Logger logger = Logger.getLogger(Utility.class);
  
  public static String convertToFullQualifiedName(MetaInfo.Namespace namespace, String name)
  {
    if (name.indexOf(".") == -1)
    {
      if (namespace == null) {
        throw new RuntimeException("Name is not fully qualified name and also namespace is not supplied");
      }
      return namespace.getName() + "." + name;
    }
    return name;
  }
  
  public static List<String> convertStringToRoleFormat(List<String> roles)
  {
    List<String> convertedResult = new ArrayList();
    for (String string : roles) {
      convertedResult.add(convertStringToRoleFormat(string));
    }
    return convertedResult;
  }
  
  public static String convertStringToRoleFormat(String role)
  {
    String[] splitRole = null;
    if ((splitRole = role.split("\\.")).length == 2) {
      return splitRole[0] + ":" + splitRole[1];
    }
    if ((splitRole = role.split(":")).length == 2) {
      return role;
    }
    throw new RuntimeException("Wrong role format, either chose 'namespacename:rolename' or namespacename.rolename");
  }
  
  public static ObjectPermission.ObjectType getPermissionObjectType(EntityType et)
  {
    switch (et)
    {
    case UNKNOWN: 
      return ObjectPermission.ObjectType.unknown;
    case APPLICATION: 
      return ObjectPermission.ObjectType.application;
    case FLOW: 
      return ObjectPermission.ObjectType.flow;
    case STREAM: 
      return ObjectPermission.ObjectType.stream;
    case WINDOW: 
      return ObjectPermission.ObjectType.window;
    case TYPE: 
      return ObjectPermission.ObjectType.type;
    case CQ: 
      return ObjectPermission.ObjectType.cq;
    case SOURCE: 
      return ObjectPermission.ObjectType.source;
    case TARGET: 
      return ObjectPermission.ObjectType.target;
    case PROPERTYSET: 
      return ObjectPermission.ObjectType.propertyset;
    case PROPERTYVARIABLE: 
      return ObjectPermission.ObjectType.propertyvariable;
    case WACTIONSTORE: 
      return ObjectPermission.ObjectType.wactionstore;
    case PROPERTYTEMPLATE: 
      return ObjectPermission.ObjectType.propertytemplate;
    case CACHE: 
      return ObjectPermission.ObjectType.cache;
    case ALERTSUBSCRIBER: 
      return ObjectPermission.ObjectType.alertsubscriber;
    case SERVER: 
      return ObjectPermission.ObjectType.server;
    case USER: 
      return ObjectPermission.ObjectType.user;
    case ROLE: 
      return ObjectPermission.ObjectType.role;
    case INITIALIZER: 
      return ObjectPermission.ObjectType.initializer;
    case DG: 
      return ObjectPermission.ObjectType.deploymentgroup;
    case VISUALIZATION: 
      return ObjectPermission.ObjectType.visualization;
    case NAMESPACE: 
      return ObjectPermission.ObjectType.namespace;
    case STREAM_GENERATOR: 
      return ObjectPermission.ObjectType.stream_generator;
    }
    return null;
  }
  
  public static String splitName(String name)
  {
    if (!checkIfFullName(name)) {
      return name;
    }
    String[] splitName = name.split("\\.");
    if (splitName.length == 2) {
      return splitName[1];
    }
    return name;
  }
  
  public static String splitDomain(String name)
  {
    if (!checkIfFullName(name)) {
      return null;
    }
    String[] splitName = name.split("\\.");
    if (splitName.length >= 2) {
      return splitName[0];
    }
    return name;
  }
  
  public static boolean checkIfFullName(String name)
  {
    if (name == null) {
      return false;
    }
    if (name.indexOf(".") == -1) {
      return false;
    }
    return true;
  }
  
  public static void removeDroppedFlow(List<MetaInfo.Flow> result)
  {
    for (Iterator<MetaInfo.Flow> it = result.iterator(); it.hasNext();)
    {
      MetaInfo.MetaObject metaObject = (MetaInfo.MetaObject)it.next();
      if (metaObject.getMetaInfoStatus().isDropped()) {
        it.remove();
      }
    }
  }
  
  public static void removeAdhocNamedQuery(List<MetaInfo.Flow> applicationsList)
  {
    for (Iterator<MetaInfo.Flow> it = applicationsList.iterator(); it.hasNext();)
    {
      MetaInfo.MetaObject metaObject = (MetaInfo.MetaObject)it.next();
      if (metaObject.getMetaInfoStatus().isAnonymous()) {
        it.remove();
      }
    }
  }
  
  public static Set<? extends MetaInfo.MetaObject> removeInternalApplicationsWithTypes(Set<MetaInfo.MetaObject> obj, boolean allowAnonTypes)
  {
    for (Iterator<MetaInfo.MetaObject> it = obj.iterator(); it.hasNext();)
    {
      MetaInfo.MetaObject metaObject = (MetaInfo.MetaObject)it.next();
      if ((metaObject.getMetaInfoStatus().isAdhoc()) || (metaObject.getName().startsWith(Compiler.NAMED_QUERY_PREFIX))) {
        it.remove();
      } else if ((metaObject.getMetaInfoStatus().isAnonymous()) && (!metaObject.getMetaInfoStatus().isGenerated())) {
        if (metaObject.getType() != EntityType.TYPE) {
          it.remove();
        } else if (!allowAnonTypes) {
          it.remove();
        }
      }
    }
    return obj;
  }
  
  public static Set<? extends MetaInfo.MetaObject> removeInternalApplications(Set<MetaInfo.MetaObject> obj)
  {
    return removeInternalApplicationsWithTypes(obj, false);
  }
  
  public static void removeDroppedObjects(List<MetaInfo.MetaObject> result)
  {
    for (Iterator<MetaInfo.MetaObject> it = result.iterator(); it.hasNext();)
    {
      MetaInfo.MetaObject metaObject = (MetaInfo.MetaObject)it.next();
      if (metaObject.getMetaInfoStatus().isDropped()) {
        it.remove();
      }
    }
  }
  
  public static String prettyPrintMap(List<Property> propertyList)
  {
    String str = "";
    if ((propertyList == null) || (propertyList.isEmpty())) {
      return str;
    }
    Iterator<Property> i = propertyList.iterator();
    boolean firstPass = true;
    while (i.hasNext())
    {
      Property e = (Property)i.next();
      String key = e.name;
      Object value = e.value;
      if (value != null)
      {
        if (!firstPass) {
          str = str + ",\n";
        }
        firstPass = false;
        if ((value instanceof Boolean)) {
          str = str + "  " + key + ": " + value.toString();
        } else if ((value instanceof Integer))
        {
          if (((Integer)value).intValue() < 0) {
            str = str + "  " + key + ": '" + value.toString() + "'";
          } else {
            str = str + "  " + key + ": " + value.toString() + "";
          }
        }
        else if (!key.equals("directory")) {
          str = str + "  " + key + ": '" + StringEscapeUtils.escapeJava(value.toString()) + "'";
        } else {
          str = str + "  " + key + ": '" + value + "'";
        }
      }
    }
    str = str + "\n";
    return str;
  }
  
  public static String createCQStatementText(String cq_name, Boolean doReplace, String dest_stream_name, String[] field_name_list, String selectText)
  {
    String str = "CREATE ";
    if (doReplace.booleanValue()) {
      str = str + "OR REPLACE";
    }
    str = str + " CQ " + cq_name + " ";
    str = str + "\nINSERT INTO ";
    str = str + splitName(dest_stream_name) + "\n";
    if (field_name_list.length > 0) {
      str = str + "( " + join(field_name_list) + " ) " + "\n";
    }
    str = str + selectText + "\n";
    
    return str;
  }
  
  private static String join(String[] arr)
  {
    StringBuilder sb = new StringBuilder();
    String delim = "";
    for (String i : arr)
    {
      sb.append(delim).append(i);
      delim = ",";
    }
    return sb.toString();
  }
  
  public static String join(List<String> list)
  {
    String[] arr = (String[])list.toArray(new String[list.size()]);
    return join(arr);
  }
  
  public static String createSourceStatementText(String sourceNameWithoutDomain, Boolean doReplace, String adapType, List<Property> adapProps, String parserType, List<Property> parserProps, String instream, MappedStream generated_streams)
  {
    String str = "CREATE ";
    if (doReplace.booleanValue()) {
      str = str + "OR REPLACE";
    }
    str = str + " SOURCE " + splitName(sourceNameWithoutDomain) + " ";
    str = str + "USING " + splitName(adapType) + " ( \n";
    str = str + prettyPrintMap(adapProps);
    str = str + " ) \n";
    if (parserType != null)
    {
      str = str + " PARSE USING " + splitName(parserType) + " ( \n";
      if (parserProps != null) {
        str = str + prettyPrintMap(parserProps);
      }
      str = str + " ) \n";
    }
    str = str + "OUTPUT TO " + splitName(instream);
    if (generated_streams != null) {
      str = str + " ,\n" + generated_streams.streamName + " MAP ( \n" + prettyPrintMap(makePropList(generated_streams.mappingProperties)) + ")";
    }
    return str;
  }
  
  public static String createTargetStatementText(String sourceNameWithoutDomain, Boolean doReplace, String adapType, List<Property> adapProps, String formatterType, List<Property> formatterProps, String instream)
  {
    String str = "CREATE ";
    if (doReplace.booleanValue()) {
      str = str + "OR REPLACE";
    }
    str = str + " TARGET " + sourceNameWithoutDomain + " ";
    str = str + "USING " + splitName(adapType) + " ( \n";
    str = str + prettyPrintMap(adapProps);
    str = str + " ) \n";
    if (formatterType != null)
    {
      str = str + "FORMAT USING " + splitName(formatterType) + " ( ";
      str = str + prettyPrintMap(formatterProps);
      str = str + " ) \n";
    }
    str = str + "INPUT FROM " + splitName(instream);
    return str;
  }
  
  public static String createSubscriptionStatementText(String sourceNameWithoutDomain, Boolean doReplace, String adapType, List<Property> adapProps, String formatterType, List<Property> formatterProps, String instream)
  {
    String str = "CREATE ";
    if (doReplace.booleanValue()) {
      str = str + "OR REPLACE";
    }
    str = str + " SUBSCRIPTION " + sourceNameWithoutDomain + " ";
    str = str + "USING " + adapType + " ( \n";
    str = str + prettyPrintMap(adapProps);
    str = str + " ) \n";
    if (formatterType != null)
    {
      str = str + "FORMAT USING " + splitName(formatterType) + " ( ";
      str = str + prettyPrintMap(formatterProps);
      str = str + " ) \n";
    }
    str = str + "INPUT FROM " + splitName(instream);
    return str;
  }
  
  public static String convertAdapterClassToName(String adapterClassName)
  {
    Class adapterCls = null;
    PropertyTemplate adapterAnno = null;
    try
    {
      adapterCls = Class.forName(adapterClassName);
      adapterAnno = (PropertyTemplate)adapterCls.getAnnotation(PropertyTemplate.class);
    }
    catch (ClassNotFoundException e)
    {
      if (logger.isInfoEnabled()) {
        logger.info(e.getMessage());
      }
    }
    return adapterAnno == null ? null : adapterAnno.name();
  }
  
  public static String prettyPrintEventType(Map<String, List<String>> ets)
  {
    String str = "EVENT TYPES ( ";
    boolean firstTime = true;
    for (Map.Entry<String, List<String>> et : ets.entrySet())
    {
      if (!firstTime) {
        str = str + ", ";
      }
      String eventType = (String)et.getKey();
      List<String> keyFields = (List)et.getValue();
      
      str = str + splitName(eventType) + " KEY ( " + join((String[])keyFields.toArray(new String[keyFields.size()])) + " ) ";
      firstTime = false;
    }
    str = str + " ) ";
    
    return str;
  }
  
  public static String createWASStatementText(String wasNameWithoutDomain, Boolean doReplace, String typeName, Map<String, List<String>> ets, String howLong, WactionStorePersistencePolicy wactionStorePersistencePolicy)
  {
    String str = "CREATE ";
    if (doReplace.booleanValue()) {
      str = str + "OR REPLACE ";
    }
    str = str + "WACTIONSTORE " + wasNameWithoutDomain + " ";
    str = str + " CONTEXT OF " + splitName(typeName) + "\n";
    if (!ets.isEmpty()) {
      str = str + prettyPrintEventType(ets);
    }
    String persistence = new String();
    switch (wactionStorePersistencePolicy.type)
    {
    case IN_MEMORY: 
      persistence = " USING MEMORY";
      break;
    case STANDARD: 
      if ((wactionStorePersistencePolicy.properties.size() == 1) && (((Property)wactionStorePersistencePolicy.properties.get(0)).name.equalsIgnoreCase(Type.STANDARD.typeName()))) {
        persistence = " ";
      } else {
        persistence = " USING ( " + prettyPrintMap(wactionStorePersistencePolicy.properties) + " ) ";
      }
      break;
    case INTERVAL: 
      String interval = null;
      if (wactionStorePersistencePolicy.howOften == null) {
        interval = "NONE";
      } else {
        interval = wactionStorePersistencePolicy.howOften.value != 0L ? "EVERY " + howLong : "IMMEDIATE";
      }
      persistence = " PERSIST " + interval + " USING ( " + prettyPrintMap(wactionStorePersistencePolicy.properties) + " ) ";
    }
    str = str + persistence;
    return str;
  }
  
  public static String createStreamStatementText(String streamNameWithoutDomain, Boolean doReplace, String typeNameWithoutDomain, String[] partition_fields)
  {
    String str = "CREATE ";
    if (doReplace.booleanValue()) {
      str = str + "OR REPLACE ";
    }
    str = str + "STREAM " + streamNameWithoutDomain + " ";
    str = str + "OF " + typeNameWithoutDomain;
    if (partition_fields.length > 0) {
      str = str + " PARTITION BY " + join(partition_fields);
    }
    return str;
  }
  
  public static String createTypeStatementText(String typeNameWithoutDomain, Boolean doReplace, List<TypeField> makeFieldList)
  {
    String str = "CREATE ";
    if (doReplace.booleanValue()) {
      str = str + "OR REPLACE";
    }
    str = str + " TYPE " + typeNameWithoutDomain + " ";
    str = str + " ( ";
    for (int i = 0; i < makeFieldList.size(); i++) {
      if (i + 1 == makeFieldList.size())
      {
        if (((TypeField)makeFieldList.get(i)).isPartOfKey) {
          str = str + ((TypeField)makeFieldList.get(i)).fieldName + " " + ((TypeField)makeFieldList.get(i)).fieldType.name + " KEY  \n";
        } else {
          str = str + ((TypeField)makeFieldList.get(i)).fieldName + " " + ((TypeField)makeFieldList.get(i)).fieldType.name + "  \n";
        }
      }
      else if (((TypeField)makeFieldList.get(i)).isPartOfKey) {
        str = str + ((TypeField)makeFieldList.get(i)).fieldName + " " + ((TypeField)makeFieldList.get(i)).fieldType.name + " KEY , \n";
      } else {
        str = str + ((TypeField)makeFieldList.get(i)).fieldName + " " + ((TypeField)makeFieldList.get(i)).fieldType.name + " , \n";
      }
    }
    str = str + " ) ";
    return str;
  }
  
  public static String createWindowStatementText(String windowNameWithoutDomain, Boolean doReplace, String stream_name, Map<String, Object> window_len, boolean isJumping, String[] partition_fields, boolean isPersistent)
  {
    String str = "CREATE ";
    if (doReplace.booleanValue()) {
      str = str + "OR REPLACE";
    }
    if (isJumping) {
      str = str + " JUMPING";
    }
    str = str + " WINDOW " + windowNameWithoutDomain + " ";
    str = str + "OVER " + splitName(stream_name) + " KEEP ";
    
    Object val = window_len.get("range");
    if (val != null) {
      str = str + "RANGE " + val.toString() + " ";
    }
    val = window_len.get("count");
    if (val != null)
    {
      Integer i = null;
      if ((val instanceof String)) {
        i = Integer.valueOf((String)val);
      } else if ((val instanceof Integer)) {
        i = (Integer)val;
      } else {
        throw new RuntimeException("invalid count interval");
      }
      str = str + i.toString() + " ROWS ";
    }
    val = window_len.get("time");
    if (val != null)
    {
      Interval interval = new Interval(Long.parseLong((String)val));
      str = str + "WITHIN " + interval.toHumanReadable() + " ";
    }
    Object attr = window_len.get("on");
    if (attr != null) {
      if ((attr instanceof String)) {
        str = str + "ON " + attr;
      } else {
        throw new RuntimeException("invalid ON attribute");
      }
    }
    if (partition_fields.length != 0) {
      str = str + " PARTITION BY " + join(partition_fields);
    }
    return str;
  }
  
  public static String createCacheStatementText(String cacheNameWithoutDomain, Boolean doReplace, String adapType, List<Property> readerPropList, String parserType, List<Property> parserPropList, List<Property> queryPropList, String typename)
  {
    String str = "CREATE ";
    if (doReplace.booleanValue()) {
      str = str + "OR REPLACE";
    }
    if (adapType.equalsIgnoreCase("STREAM")) {
      str = str + " EVENTTABLE " + splitName(cacheNameWithoutDomain) + " ";
    } else {
      str = str + " CACHE " + splitName(cacheNameWithoutDomain) + " ";
    }
    str = str + "USING " + splitName(adapType) + " ( \n";
    str = str + prettyPrintMap(readerPropList);
    str = str + " ) \n";
    if (parserType != null) {
      if (!adapType.equalsIgnoreCase("STREAM"))
      {
        str = str + "PARSE USING " + parserType + " ( \n";
        str = str + prettyPrintMap(parserPropList);
        str = str + " ) \n";
      }
    }
    str = str + "QUERY ( \n";
    str = str + prettyPrintMap(queryPropList);
    str = str + " ) \n";
    str = str + " OF  " + splitName(typename);
    
    return str;
  }
  
  public static Interval parseInterval(String intervalString)
  {
    if (intervalString.equalsIgnoreCase("NONE")) {
      return null;
    }
    Pattern p = Pattern.compile("\\s*([\\d\\s.:-]+)\\s+(day|hour|minute|second)(\\s+to\\s+(day|hour|minute|second))?\\s*", 2);
    Matcher m = p.matcher(intervalString);
    if (!m.matches()) {
      throw new IllegalArgumentException("Invalid interval string: " + intervalString);
    }
    String literal = m.group(1);
    String measurement1 = m.group(2);
    String measurement2 = m.group(4);
    int flags = string2flag(measurement1);
    if (measurement2 != null)
    {
      int flag2 = string2flag(measurement2);
      if (flags <= flag2) {
        throw new IllegalArgumentException("Invalid interval string: " + intervalString);
      }
      flags = (flags << 1) - 1 & (flag2 - 1 ^ 0xFFFFFFFF);
    }
    Interval i = Interval.parseDSInterval(literal, flags);
    if (i == null) {
      throw new IllegalArgumentException("Invalid interval string: " + intervalString);
    }
    return i;
  }
  
  public static LocalTime parseLocalTime(String localTime)
  {
    String[] split = localTime.split(":");
    String expectedFormat = "hh:mm:ss";
    String errorMsg = "Invalid input for Localtime. Given input: " + localTime + " Expected format: " + expectedFormat;
    if (split.length != 3) {
      throw new IllegalArgumentException(errorMsg);
    }
    int hrs = Integer.parseInt(split[0]);
    int minutes = Integer.parseInt(split[1]);
    int seconds = Integer.parseInt(split[2]);
    LocalTime time;
    try
    {
      time = new LocalTime(hrs, minutes, seconds);
    }
    catch (IllegalFieldValueException ex)
    {
      throw new IllegalArgumentException(errorMsg, ex);
    }
    return time;
  }
  
  public static int string2flag(String val)
  {
    switch (val.toUpperCase())
    {
    case "DAY": 
      return 8;
    case "HOUR": 
      return 4;
    case "MINUTE": 
      return 2;
    case "SECOND": 
      return 1;
    }
    return 0;
  }
  
  public static UUID getDeploymentGroupFromMetaObject(UUID metaObjectUUID, AuthToken token)
    throws MetaDataRepositoryException
  {
    MetadataRepository rep = MetadataRepository.getINSTANCE();
    MetaInfo.MetaObject metaObject = rep.getMetaObjectByUUID(metaObjectUUID, token);
    if (metaObject == null) {
      return null;
    }
    Set<UUID> deps = metaObject.getReverseIndexObjectDependencies();
    if ((metaObject instanceof MetaInfo.WAStoreView))
    {
      UUID wactionStoreUUID = ((MetaInfo.WAStoreView)metaObject).wastoreID;
      metaObject = rep.getMetaObjectByUUID(wactionStoreUUID, token);
      deps = metaObject.getReverseIndexObjectDependencies();
    }
    if (deps.isEmpty())
    {
      if ((metaObject instanceof MetaInfo.Window))
      {
        UUID streamUUID = ((MetaInfo.Window)metaObject).stream;
        metaObject = rep.getMetaObjectByUUID(streamUUID, token);
      }
      else
      {
        return null;
      }
      deps = metaObject.getReverseIndexObjectDependencies();
    }
    List<MetaInfo.Flow> possibleFlow = new ArrayList();
    List<MetaInfo.Flow> possibleApp = new ArrayList();
    for (UUID reverseIndex : deps)
    {
      MetaInfo.MetaObject reverseIndexObject = rep.getMetaObjectByUUID(reverseIndex, token);
      if (reverseIndexObject != null)
      {
        if (reverseIndexObject.type == EntityType.FLOW) {
          possibleFlow.add((MetaInfo.Flow)reverseIndexObject);
        }
        if (reverseIndexObject.type == EntityType.APPLICATION) {
          possibleApp.add((MetaInfo.Flow)reverseIndexObject);
        }
      }
    }
    MetaInfo.Flow flow;
    UUID deploymentGroup = null;
    Iterator i$;
    if (!possibleFlow.isEmpty()) {
      for (i$ = possibleFlow.iterator(); i$.hasNext();)
      {
        flow = (MetaInfo.Flow)i$.next();
        for (MetaInfo.Flow application : possibleApp) {
          if (application.deploymentPlan != null) {
            for (MetaInfo.Flow.Detail d : application.deploymentPlan) {
              if ((d != null) && (d.flow.equals(flow.getUuid())))
              {
                deploymentGroup = d.deploymentGroup;
                if (deploymentGroup != null) {
                  break;
                }
              }
            }
          }
        }
      }
    }
    
    if (deploymentGroup == null) {
      for (MetaInfo.Flow flow1 : possibleApp) {
        if (flow1.deploymentPlan != null)
        {
          deploymentGroup = ((MetaInfo.Flow.Detail)flow1.deploymentPlan.get(0)).deploymentGroup;
          break;
        }
      }
    }
    return deploymentGroup;
  }
  
  public static boolean isValueEncryptionFlagSetToTrue(String key, Map<String, Object> props)
  {
    Map<String, Object> temp = Factory.makeCaseInsensitiveMap();
    temp.putAll(props);
    key = key + "_encrypted";
    if (temp.get(key) != null)
    {
      Object obj = temp.get(key);
      if ((obj instanceof String))
      {
        if (((String)obj).equalsIgnoreCase("true")) {
          return true;
        }
      }
      else if ((obj instanceof Boolean)) {
        return ((Boolean)obj).booleanValue();
      }
    }
    return false;
  }
  
  public static boolean isValueEncryptionFlagExists(String key, Map<String, Object> props)
  {
    Map<String, Object> temp = Factory.makeCaseInsensitiveMap();
    temp.putAll(props);
    key = key + "_encrypted";
    return temp.containsKey(key);
  }
  
  public static Map<String, Object> makePropertyMap(List<Property> list)
  {
    if (list == null) {
      return null;
    }
    if (list.isEmpty()) {
      return Factory.makeCaseInsensitiveMap();
    }
    Map<String, Object> map = Factory.makeCaseInsensitiveMap();
    for (Property property : list) {
      map.put(property.name, property.value);
    }
    return map;
  }
  
  public static List<Property> makePropList(Map<String, Object> properties)
  {
    if (properties == null) {
      return null;
    }
    List<Property> plist = new ArrayList();
    for (Map.Entry<String, Object> prop : properties.entrySet())
    {
      Property p = new Property((String)prop.getKey(), prop.getValue());
      plist.add(p);
    }
    return plist;
  }
  
  public static void changeLogLevel(Object paramvalue, boolean printMessage)
  {
    if ((paramvalue instanceof String))
    {
      String loglevel = paramvalue.toString();
      LogManager.getRootLogger().setLevel(Level.toLevel(loglevel));
    }
    else if ((paramvalue instanceof List))
    {
      for (Property prop : (List)paramvalue) {
        if (LogManager.exists(prop.name) != null) {
          LogManager.getLogger(prop.name).setLevel(Level.toLevel(prop.value.toString()));
        } else if (printMessage) {
          System.out.println("Logger for class " + prop.name + " does not exist.");
        }
      }
    }
  }
  
  public static synchronized void prettyPrint(Position position)
  {
    StringBuilder output = new StringBuilder();
    if (position == null)
    {
      output.append("\n- Position: {null}");
    }
    else if (position.isEmpty())
    {
      output.append("\n- Position: {empty}");
    }
    else
    {
      output.append("\n- Position:");
      for (Path path : position.values())
      {
        output.append("\n- * Path <" + path.getPathHash() + ">");
        for (int i = 0; i < path.getPathItems().size(); i++)
        {
          Path.Item item = path.getPathComponent(i);
          if (item == null) {
            output.append("\n-      -- (unexpected null)\n");
          } else {
            try
            {
              MetaInfo.MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByUUID(item.getComponentUUID(), WASecurityManager.TOKEN);
              if (mo != null) {
                output.append(String.format("%n-   ~ %-12s %-50s [%s]", new Object[] { mo.type, mo.name + "/" + (item.getDistributionID() == null ? "~" : item.getDistributionID()), mo.uuid }));
              } else {
                output.append(String.format("%n-   ~ %s", new Object[] { "Could not find metadata for " + item.getComponentUUID() }));
              }
            }
            catch (MetaDataRepositoryException e)
            {
              logger.error("Error when trying to pretty print " + item.getComponentUUID(), e);
              output.append(String.format("%n-   ~ %-12s %-50s [%s]", new Object[] { "TypeNotFound", "ComponentNotFound/" + (item.getDistributionID() == null ? "~" : item.getDistributionID()), "UUID Unknown" }));
            }
            catch (Exception e)
            {
              logger.error("Error when trying to pretty print " + item.getComponentUUID(), e);
              output.append(String.format("%n-   ~ %-12s %-50s [%s]", new Object[] { "TypeNotFound/Null", "Component/Null" + (item.getDistributionID() == null ? "~" : item.getDistributionID()), "UUID Unknown" }));
            }
          }
        }
        output.append("\n-    @ " + path.getSourcePosition());
      }
    }
    output.append("\n- End Of Position");
    logger.error(output.toString());
  }
  
  public static synchronized void prettyPrint(OffsetPosition position)
  {
    StringBuilder output = new StringBuilder();
    if (position == null)
    {
      output.append("\n- OffsetPosition: {null} ");
    }
    else if (position.isEmpty())
    {
      output.append("\n- OffsetPosition: {empty} Offset=").append(position.getOffset());
    }
    else
    {
      output.append("\n- OffsetPosition: Offset=").append(position.getOffset());
      for (Path path : position.values())
      {
        output.append("\n- * Path <" + path.getPathHash() + ">");
        for (int i = 0; i < path.getPathItems().size(); i++)
        {
          Path.Item item = path.getPathComponent(i);
          if (item == null) {
            output.append("\n-      -- (unexpected null)\n");
          } else {
            try
            {
              MetaInfo.MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByUUID(item.getComponentUUID(), WASecurityManager.TOKEN);
              if (mo != null) {
                output.append(String.format("%n-   ~ %-12s %-50s [%s]", new Object[] { mo.type, mo.name + "/" + (item.getDistributionID() == null ? "~" : item.getDistributionID()), mo.uuid }));
              } else {
                output.append(String.format("%n-   ~ %s", new Object[] { "Could not find metadata for " + item.getComponentUUID() }));
              }
            }
            catch (MetaDataRepositoryException e)
            {
              logger.error("Error when trying to pretty print " + item.getComponentUUID(), e);
              output.append(String.format("%n-   ~ %-12s %-50s [%s]", new Object[] { "TypeNotFound", "ComponentNotFound/" + (item.getDistributionID() == null ? "~" : item.getDistributionID()), "UUID Unknown" }));
            }
            catch (Exception e)
            {
              logger.error("Error when trying to pretty print " + item.getComponentUUID(), e);
              output.append(String.format("%n-   ~ %-12s %-50s [%s]", new Object[] { "TypeNotFound/Null", "Component/Null" + (item.getDistributionID() == null ? "~" : item.getDistributionID()), "UUID Unknown" }));
            }
          }
        }
        output.append("\n-    @ " + path.getSourcePosition());
      }
    }
    output.append("\n- End Of Position");
    logger.error(output.toString());
  }
  
  public static synchronized void prettyPrint(Path path)
  {
    StringBuilder output = new StringBuilder();
    if (path == null)
    {
      output.append("\n- * Path <null>");
    }
    else
    {
      output.append("\n- * Path <" + path.getPathHash() + "> @" + path.getSourcePosition());
      for (int i = 0; i < path.getPathItems().size(); i++)
      {
        Path.Item item = path.getPathComponent(i);
        if (item == null) {
          output.append("\n-      -- (unexpected null)\n");
        } else {
          try
          {
            MetaInfo.MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByUUID(item.getComponentUUID(), WASecurityManager.TOKEN);
            output.append(String.format("%n-   ~ %-12s %-50s [%s]", new Object[] { mo.type, mo.name + "/" + (item.getDistributionID() == null ? "~" : item.getDistributionID()), mo.uuid }));
          }
          catch (MetaDataRepositoryException e)
          {
            logger.error("Error when trying to pretty print " + item.getComponentUUID(), e);
            output.append(String.format("%n-   ~ %-12s %-50s [%s]", new Object[] { "TypeNotFound", "ComponentNotFound/" + (item.getDistributionID() == null ? "~" : item.getDistributionID()), "UUID Unknown" }));
          }
          catch (Exception e)
          {
            logger.error("Error when trying to pretty print " + item.getComponentUUID(), e);
            output.append(String.format("%n-   ~ %-12s %-50s [%s]", new Object[] { "TypeNotFound/Null", "Component/Null" + (item.getDistributionID() == null ? "~" : item.getDistributionID()), "UUID Unknown" }));
          }
        }
      }
    }
    logger.error(output.toString());
  }
  
  public static Boolean getPropertyBoolean(Map<String, Object> properties, String propertyName)
  {
    Boolean result = null;
    Object property = properties != null ? properties.get(propertyName) : null;
    if (property != null) {
      result = getBooleanValue(property);
    }
    return result;
  }
  
  private static Boolean getBooleanValue(Object property)
  {
    return Boolean.valueOf(property.toString());
  }
  
  public static String createSourceStatementText(String sourceNameWithoutDomain, Boolean doReplace, String adapterType, List<Property> adapterProperty, String parserType, List<Property> parserProperty, List<OutputClause> outputClauses)
  {
    String str = "CREATE ";
    if (doReplace.booleanValue()) {
      str = str + "OR REPLACE";
    }
    str = str + " SOURCE " + splitName(sourceNameWithoutDomain) + " ";
    str = str + "USING " + splitName(adapterType) + " ( \n";
    str = str + prettyPrintMap(adapterProperty);
    str = str + " ) \n";
    if (parserType != null)
    {
      str = str + " PARSE USING " + splitName(parserType) + " ( \n";
      if (parserProperty != null) {
        str = str + prettyPrintMap(parserProperty);
      }
      str = str + " ) \n";
    }
    for (int i = 0; i < outputClauses.size(); i++)
    {
      OutputClause innerOutputClause = (OutputClause)outputClauses.get(i);
      if (innerOutputClause.getStreamName() != null) {
        str = str + "OUTPUT TO " + splitName(innerOutputClause.getStreamName());
      }
      if (innerOutputClause.getGeneratedStream() != null) {
        str = str + "OUTPUT TO " + splitName(innerOutputClause.getGeneratedStream().streamName);
      }
      str = buildFilterClause(str, innerOutputClause.getGeneratedStream(), innerOutputClause.getTypeDefinition(), innerOutputClause.getFilterText());
      if (i + 1 < outputClauses.size()) {
        str = str + ", \n";
      }
    }
    return str;
  }
  
  private static String buildFilterClause(String base, MappedStream mappedStreams, List<TypeField> typeFields, String selectText)
  {
    if ((typeFields != null) && (!typeFields.isEmpty()))
    {
      Iterator<TypeField> iterator = typeFields.iterator();
      base = base + "(";
      while (iterator.hasNext())
      {
        TypeField entry = (TypeField)iterator.next();
        base = base + " " + entry.fieldName + " " + entry.fieldType.name + " ";
        if (iterator.hasNext()) {
          base = base + ", ";
        }
      }
      base = base + ")";
    }
    if (mappedStreams != null)
    {
      base = base + " MAP ( ";
      for (Map.Entry<String, Object> entry : mappedStreams.mappingProperties.entrySet()) {
        base = base + (String)entry.getKey() + ":'" + entry.getValue() + "'";
      }
      base = base + ")";
    }
    base = base + " ";
    if (selectText != null) {
      base = base + cleanUpSelectStatement(selectText) + " ";
    }
    return base;
  }
  
  public static String cleanUpSelectStatement(String selectTQL)
  {
    StringBuilder sb = new StringBuilder();
    if (selectTQL != null)
    {
      String slctxt = selectTQL.trim();
      slctxt = org.apache.commons.lang.StringUtils.strip(slctxt);
      if (slctxt.endsWith(",")) {
        sb.append(slctxt.substring(0, slctxt.length() - 1));
      } else {
        sb.append(slctxt);
      }
    }
    return sb.toString();
  }
  
  public static String getWindowType(IntervalPolicy policy)
  {
    Integer count = null;
    Long time = null;
    Long timeout = null;
    String onField = null;
    if (policy.getCountPolicy() != null) {
      count = Integer.valueOf(policy.getCountPolicy().getCountInterval());
    }
    if (policy.getTimePolicy() != null) {
      time = Long.valueOf(policy.getTimePolicy().getTimeInterval());
    }
    if (policy.getAttrPolicy() != null)
    {
      timeout = Long.valueOf(policy.getAttrPolicy().getAttrValueRange());
      onField = policy.getAttrPolicy().getAttrName();
    }
    if ((timeout != null) && (org.apache.commons.lang3.StringUtils.isNotBlank(onField)) && ((count != null) || (time != null))) {
      return "hybrid";
    }
    if ((count != null) && (time != null)) {
      return "hybrid";
    }
    if (count != null) {
      return "count";
    }
    if ((time != null) || (timeout != null)) {
      return "time";
    }
    return null;
  }
  
  public static String appendOutputClause(String str, List<OutputClause> outputClauses)
  {
    for (int i = 0; i < outputClauses.size(); i++)
    {
      OutputClause innerOutputClause = (OutputClause)outputClauses.get(i);
      if (innerOutputClause.getStreamName() != null) {
        str = str + "OUTPUT TO " + splitName(innerOutputClause.getStreamName());
      }
      if (innerOutputClause.getGeneratedStream() != null) {
        str = str + "OUTPUT TO " + splitName(innerOutputClause.getGeneratedStream().streamName);
      }
      str = buildFilterClause(str, innerOutputClause.getGeneratedStream(), innerOutputClause.getTypeDefinition(), innerOutputClause.getFilterText());
      if (i + 1 < outputClauses.size()) {
        str = str + " \n";
      }
    }
    return str;
  }
}
