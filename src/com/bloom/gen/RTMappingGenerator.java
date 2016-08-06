package com.bloom.gen;

import com.bloom.persistence.WactionStore;
import com.bloom.persistence.WactionStore.TARGETDATABASE;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Type;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import org.apache.log4j.Logger;
import org.eclipse.eclipselink.xsds.persistence.orm.AccessMethods;
import org.eclipse.eclipselink.xsds.persistence.orm.AccessType;
import org.eclipse.eclipselink.xsds.persistence.orm.Attributes;
import org.eclipse.eclipselink.xsds.persistence.orm.Basic;
import org.eclipse.eclipselink.xsds.persistence.orm.Column;
import org.eclipse.eclipselink.xsds.persistence.orm.Converter;
import org.eclipse.eclipselink.xsds.persistence.orm.DataFormatType;
import org.eclipse.eclipselink.xsds.persistence.orm.Entity;
import org.eclipse.eclipselink.xsds.persistence.orm.EntityMappings;
import org.eclipse.eclipselink.xsds.persistence.orm.FetchAttribute;
import org.eclipse.eclipselink.xsds.persistence.orm.FetchGroup;
import org.eclipse.eclipselink.xsds.persistence.orm.Id;
import org.eclipse.eclipselink.xsds.persistence.orm.Index;
import org.eclipse.eclipselink.xsds.persistence.orm.NoSql;
import org.eclipse.eclipselink.xsds.persistence.orm.ObjectFactory;
import org.eclipse.eclipselink.xsds.persistence.orm.Table;
import org.eclipse.eclipselink.xsds.persistence.orm.TemporalType;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

public class RTMappingGenerator
{
  private static Logger logger = Logger.getLogger(RTMappingGenerator.class);
  static ConcurrentMap<String, String> ormMappings = new ConcurrentHashMap();
  private static String XML_NS = "org.eclipse.eclipselink.xsds.persistence.orm";
  private static String VERSION = "2.4";
  private static final String WACTION_KEY = "wactionkey";
  private static final String EL_NS = "http://www.eclipse.org/eclipselink/xsds/persistence/orm";
  private static final String DEFAULT_JPA_NOSQL_FILE = "default_jpa_orm_nosql.xml";
  private static final String DEFAULT_JPA_NOSQL_ONDB_FILE = "default_jpa_orm_nosql_ondb.xml";
  private static final String DEFAULT_JPA_RDBMS_FILE = "default_jpa_orm_rdbms.xml";
  private static final String DEFAULT_JPA_RDBMS_FILE_NEW = "default_jpa_orm_rdbms_new.xml";
  
  public static void addMappings(String name, String xml)
  {
    ormMappings.put(name, xml);
  }
  
  public static String getMappings(String name)
  {
    return (String)ormMappings.get(name);
  }
  
  public static String createOrmMappingforRTWactionRDBMSNew(String className, String tableName, Map<String, String> contextFields, MetaInfo.Type eventType, String eventTableName, Map<String, Object> wsProps)
  {
    Map<String, Object> props = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    if (wsProps != null) {
      props.putAll(wsProps);
    }
    String contextXml = createEntityORMXMLBlockForRuntimeWaction(true, className, tableName, contextFields, props);
    contextXml = mergeEntityAttributes("default_jpa_orm_rdbms_new.xml", contextXml);
    
    String eventClassName = eventType.className;
    String eventName = eventType.name;
    if (eventTableName == null) {
      eventTableName = tableName + "_" + eventName;
    }
    contextXml = contextXml.replace("$WACTION_ENTITY", tableName);
    contextXml = contextXml.replace("$WACTION_TABLENAME", tableName.toUpperCase());
    contextXml = contextXml.replace("$RUNTIME_WACTION_CLASS_NAME", className);
    contextXml = contextXml.replace("$EVENT_TARGET_CLASS", eventClassName);
    contextXml = contextXml.replace("$EVENTS_TABLENAME", eventTableName.toUpperCase());
    contextXml = contextXml.replace("$CHECKPOINT_TABLENAME", tableName.toUpperCase() + "_CHECKPOINT");
    int colLen = userSpecifiedLength("wactionkey", tableName, props);
    if (colLen != -1) {
      contextXml = contextXml.replace("$WACTION_KEY_LENGTH", Integer.toString(colLen));
    } else {
      contextXml = contextXml.replace("$WACTION_KEY_LENGTH", Integer.toString(255));
    }
    String eventXml = createEntityORMXMLBlockForRuntimeEvent(false, eventClassName, eventTableName, eventType.fields, props);
    if (eventXml.contains("<entity>")) {
      eventXml = eventXml.replace("<entity>", "<embeddable>");
    } else {
      eventXml = eventXml.replace("<entity ", "<embeddable ");
    }
    eventXml = eventXml.replace("</entity>", "</embeddable>");
    
    String[] mappings = { contextXml, eventXml };
    String mergedXml = mergeAllEntityMappings(mappings);
    
    return mergedXml;
  }
  
  private static String mergeEntityAttributes(String fileName, String xml)
  {
    String resultXml = "";
    try
    {
      InputStream in1 = ClassLoader.getSystemResourceAsStream(fileName);
      SAXBuilder saxBuilder = new SAXBuilder();
      Document doc1 = saxBuilder.build(in1);
      Element rootNode = doc1.getRootElement();
      Namespace ns1 = rootNode.getNamespace();
      List<Element> list = rootNode.getChildren("entity", ns1);
      
      Element attributes1 = null;
      Element fetchGroup1 = null;
      for (int ie = 0; ie < list.size(); ie++)
      {
        String nameAttr = ((Element)list.get(ie)).getAttributeValue("name");
        if ((nameAttr != null) && (nameAttr.equalsIgnoreCase("$WACTION_ENTITY")))
        {
          attributes1 = (Element)((Element)list.get(ie)).getChildren("attributes", ns1).get(0);
          fetchGroup1 = (Element)((Element)list.get(ie)).getChildren("fetch-group", ns1).get(0);
          
          InputStream in2 = new ByteArrayInputStream(xml.getBytes());
          Document doc2 = saxBuilder.build(in2);
          Element root2 = doc2.getRootElement();
          Namespace ns2 = root2.getNamespace();
          Element entity2 = (Element)root2.getChildren("entity", ns2).get(0);
          Element attribute2 = (Element)entity2.getChildren("attributes", ns2).get(0);
          Element fetchGroup2 = (Element)entity2.getChildren("fetch-group", ns2).get(0);
          
          List<Element> basic2 = attribute2.getChildren();
          int rtfields = basic2.size();
          if (logger.isDebugEnabled()) {
            logger.debug("merging " + rtfields + " context fields to existing waction orm xml.");
          }
          for (int rr = rtfields - 1; rr >= 0; rr--) {
            attributes1.addContent(((Element)basic2.get(rr)).detach());
          }
          attributes1.coalesceText(true);
          List<Element> fgas = fetchGroup2.getChildren();
          int fgfields = fgas.size();
          for (int rr = fgfields - 1; rr >= 0; rr--) {
            fetchGroup1.addContent(((Element)fgas.get(rr)).detach());
          }
          fetchGroup1.coalesceText(true);
        }
        Document newdoc = new Document();
        newdoc.addContent(rootNode.detach());
        XMLOutputter xmlOut = new XMLOutputter();
        resultXml = xmlOut.outputString(newdoc);
      }
    }
    catch (Exception e)
    {
      logger.error("error merging mapping xml files. ", e);
    }
    return resultXml;
  }
  
  public static int userSpecifiedLength(String fieldName, String tableName, Map<String, Object> props)
  {
    for (String key : props.keySet()) {
      if (key.equalsIgnoreCase("lengthof_" + tableName + "_" + fieldName))
      {
        Object obj = props.get(key);
        if (obj == null) {
          return -1;
        }
        try
        {
          if ((obj instanceof String)) {
            return Integer.parseInt((String)props.get(key));
          }
          if ((obj instanceof Integer)) {
            return ((Integer)obj).intValue();
          }
        }
        catch (NumberFormatException ne) {}
      }
    }
    return -1;
  }
  
  public static boolean userRequestedIndex(String fieldName, String tableName, Map<String, Object> props)
  {
    for (String key : props.keySet()) {
      if (key.equalsIgnoreCase("indexon_" + tableName))
      {
        String obj = (String)props.get(key);
        if ((obj == null) || (obj.isEmpty())) {
          return false;
        }
        String[] indexedKeys = obj.split(",");
        if ((indexedKeys != null) && (indexedKeys.length > 0)) {
          for (int kk = 0; kk < indexedKeys.length; kk++)
          {
            String s1 = indexedKeys[kk];
            if (s1.trim().equalsIgnoreCase(fieldName)) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }
  
  private static String createEntityORMXMLBlockForRuntimeWaction(boolean useAccessMethods, String className, String tableName, Map<String, String> fields, Map<String, Object> props)
  {
    String xml = "";
    ObjectFactory factory = new ObjectFactory();
    EntityMappings entityMappings = factory.createEntityMappings();
    entityMappings.setVersion(VERSION);
    
    Entity entity = factory.createEntity();
    entity.setName(tableName);
    entity.setClazz(className);
    entity.setAccess(AccessType.FIELD);
    
    FetchGroup fg = factory.createFetchGroup();
    fg.setName("noEvents");
    
    Attributes attrs = factory.createAttributes();
    for (Map.Entry<String, String> field : fields.entrySet())
    {
      String fieldName = (String)field.getKey();
      String fieldType = (String)field.getValue();
      if (!fieldType.contains("com.bloom."))
      {
        if (logger.isDebugEnabled()) {
          logger.debug("adding field name : " + fieldName + ", type is : " + fieldType);
        }
        int colLen = userSpecifiedLength(fieldName, tableName, props);
        boolean indexed = userRequestedIndex(fieldName, tableName, props);
        attrs.getBasic().add(getBasic(useAccessMethods, factory, fieldName, fieldType, colLen, indexed));
        FetchAttribute fa = factory.createFetchAttribute();
        fa.setName(fieldName);
        fg.getAttribute().add(fa);
      }
    }
    entity.getFetchGroup().add(fg);
    entity.setAttributes(attrs);
    entityMappings.getEntity().add(entity);
    try
    {
      JAXBContext jcontext = JAXBContext.newInstance(XML_NS);
      Marshaller marshaller = jcontext.createMarshaller();
      marshaller.setProperty("jaxb.formatted.output", Boolean.TRUE);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      marshaller.marshal(entityMappings, baos);
      xml = baos.toString();
    }
    catch (JAXBException e)
    {
      logger.error("error creating orm xml mapping for beanDef : " + className, e);
    }
    if (logger.isInfoEnabled()) {
      logger.info("xml mapping creatred for " + className + " is :\n" + xml);
    }
    return xml;
  }
  
  private static String createEntityORMXMLBlockForRuntimeEvent(boolean useAccessMethods, String className, String tableName, Map<String, String> fields, Map<String, Object> props)
  {
    String xml = "";
    ObjectFactory factory = new ObjectFactory();
    EntityMappings entityMappings = factory.createEntityMappings();
    entityMappings.setVersion(VERSION);
    
    Entity entity = factory.createEntity();
    entity.setName(tableName);
    entity.setClazz(className);
    entity.setAccess(AccessType.FIELD);
    Attributes attrs = factory.createAttributes();
    
    Basic basicForEventUUID = factory.createBasic();
    basicForEventUUID.setName("_wa_SimpleEvent_ID");
    basicForEventUUID.setAttributeType("String");
    Column column = factory.createColumn();
    column.setName("EVENT_UUID");
    column.setNullable(Boolean.valueOf(true));
    basicForEventUUID.setColumn(column);
    AccessMethods am = new AccessMethods();
    am.setGetMethod("getIDString");
    am.setSetMethod("setIDString");
    basicForEventUUID.setAccessMethods(am);
    attrs.getBasic().add(basicForEventUUID);
    for (Map.Entry<String, String> field : fields.entrySet())
    {
      String fieldName = (String)field.getKey();
      String fieldType = (String)field.getValue();
      if (!fieldType.contains("com.bloom."))
      {
        if (logger.isDebugEnabled()) {
          logger.debug("adding field name : " + fieldName + ", type is : " + fieldType);
        }
        int colLen = userSpecifiedLength(fieldName, tableName, props);
        boolean indexed = userRequestedIndex(fieldName, tableName, props);
        attrs.getBasic().add(getBasic(useAccessMethods, factory, fieldName, fieldType, colLen, indexed));
      }
    }
    entity.setAttributes(attrs);
    entityMappings.getEntity().add(entity);
    try
    {
      JAXBContext jcontext = JAXBContext.newInstance(XML_NS);
      Marshaller marshaller = jcontext.createMarshaller();
      marshaller.setProperty("jaxb.formatted.output", Boolean.TRUE);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      marshaller.marshal(entityMappings, baos);
      xml = baos.toString();
    }
    catch (JAXBException e)
    {
      logger.error("error creating orm xml mapping for beanDef : " + className, e);
    }
    if (logger.isInfoEnabled()) {
      logger.info("xml mapping creatred for " + className + " is :\n" + xml);
    }
    return xml;
  }
  
  public static String createOrmMappingforRTWactionRDBMS(String className, String tableName, MetaInfo.Type typeInfo, String eventTableName, Map<String, Object> wsProps)
  {
    Map<String, Object> props = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    if (wsProps != null) {
      props.putAll(wsProps);
    }
    String xml = "<entity-mappings/>";
    if (typeInfo == null)
    {
      logger.warn("beandef is empty, no mappings are created");
      return "<entity-mappings/>";
    }
    ObjectFactory factory = new ObjectFactory();
    EntityMappings entityMappings = factory.createEntityMappings();
    entityMappings.setVersion(VERSION);
    
    Entity entity = factory.createEntity();
    entity.setName(tableName);
    entity.setClazz(className);
    entity.setAccess(AccessType.FIELD);
    Attributes attrs = factory.createAttributes();
    for (Map.Entry<String, String> field : typeInfo.fields.entrySet())
    {
      String fieldName = (String)field.getKey();
      String fieldType = (String)field.getValue();
      if (!fieldType.contains("com.bloom."))
      {
        if (logger.isDebugEnabled()) {
          logger.debug("adding field name : " + fieldName + ", type is : " + fieldType);
        }
        attrs.getBasic().add(getBasic(factory, fieldName, fieldType));
      }
    }
    entity.setAttributes(attrs);
    entityMappings.getEntity().add(entity);
    try
    {
      JAXBContext jcontext = JAXBContext.newInstance(XML_NS);
      Marshaller marshaller = jcontext.createMarshaller();
      marshaller.setProperty("jaxb.formatted.output", Boolean.TRUE);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      marshaller.marshal(entityMappings, baos);
      xml = baos.toString();
    }
    catch (JAXBException e)
    {
      logger.error("error creating orm xml mapping for beanDef : " + typeInfo.name, e);
    }
    if (logger.isInfoEnabled()) {
      logger.info("xml mapping creatred for " + typeInfo.className + " is :\n" + xml);
    }
    String mergedXml = "<entity-mappings/>";
    try
    {
      InputStream in = ClassLoader.getSystemResourceAsStream("default_jpa_orm_rdbms.xml");
      SAXBuilder builder = new SAXBuilder();
      Document document = builder.build(in);
      Element rootNode = document.getRootElement();
      
      InputStream bis = new ByteArrayInputStream(xml.getBytes());
      Document runtimeDoc = builder.build(bis);
      Element rtRoot = runtimeDoc.getRootElement();
      List<Element> rtList = rtRoot.getChildren();
      int rtListSize = rtList.size();
      for (int rr = rtListSize - 1; rr >= 0; rr--)
      {
        Element entityEle = (Element)rtList.get(rr);
        if (entityEle.getName().equals("entity")) {
          rootNode.addContent(entityEle.detach());
        }
      }
      Document merged = new Document();
      merged.addContent(rootNode.detach());
      XMLOutputter xmlOut = new XMLOutputter();
      mergedXml = xmlOut.outputString(merged);
      if (eventTableName == null) {
        mergedXml = mergedXml.replace("$EVENTS_TABLENAME", tableName.toUpperCase() + "_EVENTS");
      } else {
        mergedXml = mergedXml.replace("$EVENTS_TABLENAME", eventTableName.toUpperCase());
      }
      mergedXml = mergedXml.replace("$CHECKPOINT_TABLENAME", tableName.toUpperCase() + "_CHECKPOINT");
      
      int colLen = userSpecifiedLength("wactionkey", tableName, props);
      if (colLen != -1) {
        mergedXml = mergedXml.replace("$WACTION_KEY_LENGTH", Integer.toString(colLen));
      } else {
        mergedXml = mergedXml.replace("$WACTION_KEY_LENGTH", Integer.toString(255));
      }
    }
    catch (JDOMException|IOException e)
    {
      logger.error(e);
    }
    if (logger.isInfoEnabled()) {
      logger.info("merged xml mappings are : \n" + mergedXml);
    }
    return mergedXml;
  }
  
  public static String createOrmMappingforRTWactionRDBMS(String wsName, MetaInfo.Type typeInfo)
  {
    if (typeInfo == null)
    {
      logger.warn("beandef is empty, no mappings are created");
      return "<entity-mappings/>";
    }
    ObjectFactory factory = new ObjectFactory();
    EntityMappings entityMappings = factory.createEntityMappings();
    entityMappings.setVersion(VERSION);
    
    Entity entity = factory.createEntity();
    entity.setName("WACTION_" + wsName.toUpperCase());
    entity.setClazz("wa.Waction_" + wsName);
    entity.setAccess(AccessType.FIELD);
    Attributes attrs = factory.createAttributes();
    for (Map.Entry<String, String> field : typeInfo.fields.entrySet())
    {
      String fieldName = (String)field.getKey();
      String fieldType = (String)field.getValue();
      if (!fieldType.contains("com.bloom."))
      {
        if (logger.isDebugEnabled()) {
          logger.debug("adding field name : " + fieldName + ", type is : " + fieldType);
        }
        attrs.getBasic().add(getBasic(factory, fieldName, fieldType));
      }
    }
    entity.setAttributes(attrs);
    entityMappings.getEntity().add(entity);
    
    String xml = "<entity-mappings/>";
    try
    {
      JAXBContext jcontext = JAXBContext.newInstance(XML_NS);
      Marshaller marshaller = jcontext.createMarshaller();
      marshaller.setProperty("jaxb.formatted.output", Boolean.TRUE);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      marshaller.marshal(entityMappings, baos);
      xml = baos.toString();
    }
    catch (JAXBException e)
    {
      logger.error("error creating orm xml mapping for beanDef : " + typeInfo.name, e);
    }
    if (logger.isInfoEnabled()) {
      logger.info("xml mapping creatred for " + typeInfo.className + " is :\n" + xml);
    }
    String mergedXml = "<entity-mappings/>";
    try
    {
      InputStream in = ClassLoader.getSystemResourceAsStream("default-orm-rdbms.xml");
      SAXBuilder builder = new SAXBuilder();
      Document document = builder.build(in);
      Element rootNode = document.getRootElement();
      
      InputStream bis = new ByteArrayInputStream(xml.getBytes());
      Document runtimeDoc = builder.build(bis);
      Element rtRoot = runtimeDoc.getRootElement();
      List<Element> rtList = rtRoot.getChildren();
      int rtListSize = rtList.size();
      for (int rr = rtListSize - 1; rr >= 0; rr--)
      {
        Element entityEle = (Element)rtList.get(rr);
        if (entityEle.getName().equals("entity")) {
          rootNode.addContent(entityEle.detach());
        }
      }
      Document merged = new Document();
      merged.addContent(rootNode.detach());
      XMLOutputter xmlOut = new XMLOutputter();
      mergedXml = xmlOut.outputString(merged);
    }
    catch (JDOMException|IOException e)
    {
      logger.error(e);
    }
    if (logger.isInfoEnabled()) {
      logger.info("merged xml mappings are : \n" + mergedXml);
    }
    return mergedXml;
  }
  
  public static String createOrmMappingforRTWactionNoSQL(String className, String tableName, MetaInfo.Type typeInfo, WactionStore.TARGETDATABASE tdb)
  {
    String xml = "<entity-mappings/>";
    if (typeInfo == null)
    {
      logger.warn("beandef is empty, no mappings are created");
      return "<entity-mappings/>";
    }
    String fileName = "";
    if (tdb.equals(WactionStore.TARGETDATABASE.ONDB)) {
      fileName = "default_jpa_orm_nosql_ondb.xml";
    } else {
      fileName = "default_jpa_orm_nosql.xml";
    }
    String jpaNoSQLMappings = "";
    try
    {
      InputStream in = ClassLoader.getSystemResourceAsStream(fileName);
      SAXBuilder builder = new SAXBuilder();
      Document document = builder.build(in);
      
      Namespace ns = Namespace.getNamespace("http://www.eclipse.org/eclipselink/xsds/persistence/orm");
      
      Element rootNode = document.getRootElement();
      List<Element> entityElms = rootNode.getChildren();
      Element entityElm = null;
      if ((entityElms != null) && (entityElms.size() > 0)) {
        for (Element e : entityElms) {
          if (e.getName().equalsIgnoreCase("entity")) {
            entityElm = e;
          }
        }
      }
      entityElm.setAttribute("name", tableName);
      entityElm.setAttribute("class", className);
      List<Element> attributeElms = entityElm.getChildren();
      Element attributeElm = null;
      if (attributeElms != null) {
        for (Element e1 : attributeElms) {
          if (e1.getName().equalsIgnoreCase("attributes")) {
            attributeElm = e1;
          }
        }
      }
      for (Map.Entry<String, String> field : typeInfo.fields.entrySet())
      {
        String fieldName = (String)field.getKey();
        String fieldType = (String)field.getValue();
        if (logger.isTraceEnabled()) {
          logger.trace("adding field name : " + fieldName + ", type is : " + fieldType);
        }
        if (!fieldType.contains("com.bloom."))
        {
          Element basicElm = new Element("basic", ns);
          if (fieldType.equals("org.joda.time.DateTime")) {
            basicElm.setAttribute("name", fieldName + "AsLong");
          } else {
            basicElm.setAttribute("name", fieldName);
          }
          basicElm.setAttribute("attribute-type", fieldType);
          
          Element column = new Element("column", ns);
          column.setAttribute("name", "RT_" + fieldName.toUpperCase());
          column.setAttribute("nullable", "true");
          basicElm.addContent(column);
          
          Element acessMethodElm = new Element("access-methods", ns);
          acessMethodElm.setAttribute("get-method", "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1));
          acessMethodElm.setAttribute("set-method", "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1));
          basicElm.addContent(acessMethodElm);
          
          attributeElm.addContent(basicElm);
        }
      }
      XMLOutputter out = new XMLOutputter(Format.getPrettyFormat());
      jpaNoSQLMappings = out.outputString(document);
      return jpaNoSQLMappings.replace("$CHECKPOINT_TABLENAME", tableName.toUpperCase() + "_CHECKPOINT");
    }
    catch (Exception ex)
    {
      logger.error("error creating JPA-ORM xml mapping for NoSQL db:" + ex);
    }
    return "<entity-mappings/>";
  }
  
  public static String createOrmMappingforRTWactionNoSQL(String wsName, MetaInfo.Type typeInfo)
  {
    String xml = "<entity-mappings/>";
    if (typeInfo == null)
    {
      logger.warn("beandef is empty, no mappings are created");
      return "<entity-mappings/>";
    }
    ObjectFactory factory = new ObjectFactory();
    EntityMappings entityMappings = factory.createEntityMappings();
    entityMappings.setVersion(VERSION);
    
    Entity entity = factory.createEntity();
    entity.setName("WACTION_" + wsName.toUpperCase());
    entity.setClazz("wa.Waction_" + wsName);
    entity.setAccess(AccessType.FIELD);
    
    NoSql nosql = factory.createNoSql();
    nosql.setDataFormat(DataFormatType.MAPPED);
    entity.setNoSql(nosql);
    Attributes attrs = factory.createAttributes();
    for (Map.Entry<String, String> field : typeInfo.fields.entrySet())
    {
      String fieldName = (String)field.getKey();
      String fieldType = (String)field.getValue();
      if (!fieldType.contains("com.bloom."))
      {
        if (logger.isDebugEnabled()) {
          logger.debug("adding field name : " + fieldName + ", type is : " + fieldType);
        }
        attrs.getBasic().add(getBasic(factory, fieldName, fieldType));
      }
    }
    entity.setAttributes(attrs);
    entityMappings.getEntity().add(entity);
    try
    {
      JAXBContext jcontext = JAXBContext.newInstance(XML_NS);
      Marshaller marshaller = jcontext.createMarshaller();
      marshaller.setProperty("jaxb.formatted.output", Boolean.TRUE);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      marshaller.marshal(entityMappings, baos);
      xml = baos.toString();
    }
    catch (JAXBException e)
    {
      logger.error("error creating orm xml mapping for beanDef : " + typeInfo.name, e);
    }
    if (logger.isInfoEnabled()) {
      logger.info("xml mapping creatred for " + typeInfo.className + " is :\n" + xml);
    }
    String mergedXml = "<entity-mappings/>";
    try
    {
      InputStream in = ClassLoader.getSystemResourceAsStream("default-orm-mongodb.xml");
      SAXBuilder builder = new SAXBuilder();
      Document document = builder.build(in);
      Element rootNode = document.getRootElement();
      
      InputStream bis = new ByteArrayInputStream(xml.getBytes());
      Document runtimeDoc = builder.build(bis);
      Element rtRoot = runtimeDoc.getRootElement();
      List<Element> rtList = rtRoot.getChildren();
      int rtListSize = rtList.size();
      for (int rr = rtListSize - 1; rr >= 0; rr--)
      {
        Element entityEle = (Element)rtList.get(rr);
        if (entityEle.getName().equals("entity")) {
          rootNode.addContent(entityEle.detach());
        }
      }
      Document merged = new Document();
      merged.addContent(rootNode.detach());
      XMLOutputter xmlOut = new XMLOutputter();
      mergedXml = xmlOut.outputString(merged);
    }
    catch (JDOMException|IOException e)
    {
      logger.error(e);
    }
    if (logger.isInfoEnabled()) {
      logger.info("merged xml mappings are : \n" + mergedXml);
    }
    return mergedXml;
  }
  
  public static String createOrmMappingforClassRDBMS(String tableName, MetaInfo.Type typeInfo)
  {
    if (typeInfo == null)
    {
      logger.warn("beandef is empty, no mappings are created");
      return "<entity-mappings/>";
    }
    ObjectFactory factory = new ObjectFactory();
    EntityMappings entityMappings = factory.createEntityMappings();
    entityMappings.setVersion(VERSION);
    
    List<Converter> convList = entityMappings.getConverter();
    Converter converter1 = new Converter();
    converter1.setClazz("com.bloom.runtime.converters.JodaDateConverter2");
    converter1.setName("JodaDateConverter2");
    convList.add(converter1);
    
    Entity entity = factory.createEntity();
    entity.setName(tableName);
    entity.setClazz(typeInfo.className);
    entity.setAccess(AccessType.FIELD);
    Table table = factory.createTable();
    table.setName(tableName);
    entity.setTable(table);
    Attributes attrs = factory.createAttributes();
    for (String keyField : typeInfo.keyFields)
    {
      String fieldName = keyField;
      String fieldType = (String)typeInfo.fields.get(keyField);
      attrs.getId().add(getId(factory, fieldName, fieldType));
    }
    for (Map.Entry<String, String> field : typeInfo.fields.entrySet())
    {
      String fieldName = (String)field.getKey();
      if (!typeInfo.keyFields.contains(fieldName))
      {
        String fieldType = (String)field.getValue();
        if (!fieldType.contains("com.bloom."))
        {
          if (logger.isDebugEnabled()) {
            logger.debug("adding field name : " + fieldName + ", type is : " + fieldType);
          }
          attrs.getBasic().add(getBasicForJPAWriter(factory, fieldName, fieldType));
        }
      }
    }
    entity.setAttributes(attrs);
    entityMappings.getEntity().add(entity);
    
    String xml = "<entity-mappings/>";
    try
    {
      JAXBContext jcontext = JAXBContext.newInstance(XML_NS);
      Marshaller marshaller = jcontext.createMarshaller();
      marshaller.setProperty("jaxb.formatted.output", Boolean.TRUE);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      marshaller.marshal(entityMappings, baos);
      xml = baos.toString();
    }
    catch (JAXBException e)
    {
      logger.error("error creating orm xml mapping for beanDef : " + typeInfo.name, e);
    }
    if (logger.isInfoEnabled()) {
      logger.info("xml mapping creatred for " + typeInfo.className + " is :\n" + xml);
    }
    return xml;
  }
  
  public static String mappingType = "mongo";
  private static final String DEFAULT_HIB_MAP_FILE = "default_hibernate_waction.xml";
  
  public static String mergeAllEntityMappings(String[] mappings)
  {
    String mergedXml = "<entity-mappings/>";
    if ((mappings == null) || (mappings.length == 0)) {
      return mergedXml;
    }
    if (mappings.length == 1) {
      return mappings[0];
    }
    SAXBuilder builder = new SAXBuilder();
    try
    {
      Document document = builder.build(new StringReader(mappings[0]));
      Element rootNode = document.getRootElement();
      for (int gg = 1; gg < mappings.length; gg++)
      {
        Document document2 = builder.build(new StringReader(mappings[gg]));
        Element rootNode2 = document2.getRootElement();
        List<Element> list2 = rootNode2.getChildren();
        for (int rr = 0; rr < list2.size(); rr++)
        {
          Element entityEle = (Element)list2.get(rr);
          if (entityEle.getName().equals("entity")) {
            rootNode.addContent(entityEle.detach());
          }
          if (entityEle.getName().equals("embeddable")) {
            rootNode.addContent(entityEle.detach());
          }
        }
      }
      Document merged = new Document();
      merged.addContent(rootNode.detach());
      XMLOutputter xmlOut = new XMLOutputter();
      mergedXml = xmlOut.outputString(merged);
    }
    catch (JDOMException|IOException e)
    {
      logger.error(e);
    }
    if (logger.isInfoEnabled()) {
      logger.info("merged xml mappings are : \n" + mergedXml);
    }
    return mergedXml;
  }
  
  private static Id getId(ObjectFactory factory, String fname, String ftype)
  {
    Id id = factory.createId();
    id.setName(fname);
    if (ftype.equals("java.lang.String")) {
      id.setAttributeType("String");
    } else if (ftype.equals("java.util.Date")) {
      id.setTemporal(TemporalType.TIMESTAMP);
    } else if (ftype.equals("org.joda.time.DateTime")) {
      id.setAttributeType("long");
    } else {
      id.setAttributeType(ftype);
    }
    Column column = factory.createColumn();
    column.setName(fname);
    column.setNullable(Boolean.valueOf(false));
    id.setColumn(column);
    String getMethod = "";
    String setMethod = "";
    if (ftype.equals("org.joda.time.DateTime"))
    {
      getMethod = "get" + fname.substring(0, 1).toUpperCase() + fname.substring(1) + "AsLong";
      setMethod = "set" + fname.substring(0, 1).toUpperCase() + fname.substring(1) + "AsLong";
    }
    else
    {
      getMethod = "get" + fname.substring(0, 1).toUpperCase() + fname.substring(1);
      setMethod = "set" + fname.substring(0, 1).toUpperCase() + fname.substring(1);
    }
    AccessMethods am = new AccessMethods();
    am.setGetMethod(getMethod);
    am.setSetMethod(setMethod);
    
    return id;
  }
  
  private static Basic getBasicForJPAWriter(ObjectFactory factory, String fname, String ftype)
  {
    Basic basic = factory.createBasic();
    basic.setName(fname);
    if (ftype.equals("java.lang.String")) {
      basic.setAttributeType("String");
    } else if (ftype.equals("java.util.Date")) {
      basic.setTemporal(TemporalType.TIMESTAMP);
    } else if (ftype.equals("org.joda.time.DateTime")) {
      basic.setConvert("JodaDateConverter2");
    } else {
      basic.setAttributeType(ftype);
    }
    Column column = factory.createColumn();
    column.setName(fname);
    column.setNullable(Boolean.valueOf(true));
    basic.setColumn(column);
    String getMethod = "";
    String setMethod = "";
    if (ftype.equals("org.joda.time.DateTime")) {
      return basic;
    }
    getMethod = "get" + fname.substring(0, 1).toUpperCase() + fname.substring(1);
    setMethod = "set" + fname.substring(0, 1).toUpperCase() + fname.substring(1);
    AccessMethods am = new AccessMethods();
    am.setGetMethod(getMethod);
    am.setSetMethod(setMethod);
    basic.setAccessMethods(am);
    return basic;
  }
  
  private static Basic getBasic(ObjectFactory factory, String fname, String ftype)
  {
    Basic basic = factory.createBasic();
    basic.setName(fname);
    if (ftype.equals("java.lang.String")) {
      basic.setAttributeType("String");
    } else if (ftype.equals("java.util.Date")) {
      basic.setTemporal(TemporalType.TIMESTAMP);
    } else if (ftype.equals("org.joda.time.DateTime")) {
      basic.setAttributeType("long");
    } else {
      basic.setAttributeType(ftype);
    }
    Column column = factory.createColumn();
    column.setName(fname);
    column.setNullable(Boolean.valueOf(true));
    basic.setColumn(column);
    String getMethod = "";
    String setMethod = "";
    if (ftype.equals("org.joda.time.DateTime"))
    {
      getMethod = "get" + fname.substring(0, 1).toUpperCase() + fname.substring(1) + "AsLong";
      setMethod = "set" + fname.substring(0, 1).toUpperCase() + fname.substring(1) + "AsLong";
    }
    else
    {
      getMethod = "get" + fname.substring(0, 1).toUpperCase() + fname.substring(1);
      setMethod = "set" + fname.substring(0, 1).toUpperCase() + fname.substring(1);
    }
    AccessMethods am = new AccessMethods();
    am.setGetMethod(getMethod);
    am.setSetMethod(setMethod);
    basic.setAccessMethods(am);
    return basic;
  }
  
  private static Basic getBasic(boolean useAccessMethods, ObjectFactory factory, String fname, String ftype, int colLen, boolean indexed)
  {
    Basic basic = factory.createBasic();
    basic.setName(fname);
    if (ftype.equals("java.lang.String")) {
      basic.setAttributeType("String");
    } else if (ftype.equals("java.util.Date")) {
      basic.setTemporal(TemporalType.TIMESTAMP);
    } else if (ftype.equals("org.joda.time.DateTime")) {
      basic.setAttributeType("long");
    } else if (ftype.equals("com.fasterxml.jackson.databind.JsonNode")) {
      basic.setAttributeType("String");
    } else {
      basic.setAttributeType(ftype);
    }
    Column column = factory.createColumn();
    if (colLen != -1) {
      column.setLength(Integer.valueOf(colLen));
    }
    column.setName(fname);
    column.setNullable(Boolean.valueOf(true));
    basic.setColumn(column);
    if (ftype.equals("org.joda.time.DateTime")) {
      basic.setConvert("JodaDateConverter");
    }
    if (ftype.equals("com.fasterxml.jackson.databind.JsonNode"))
    {
      basic.setConvert("JsonNodeConverter");
      useAccessMethods = false;
    }
    if (useAccessMethods)
    {
      String getMethod = "";
      String setMethod = "";
      getMethod = "get" + fname.substring(0, 1).toUpperCase() + fname.substring(1);
      setMethod = "set" + fname.substring(0, 1).toUpperCase() + fname.substring(1);
      AccessMethods am = new AccessMethods();
      am.setGetMethod(getMethod);
      am.setSetMethod(setMethod);
      basic.setAccessMethods(am);
    }
    if (indexed)
    {
      Index index = factory.createIndex();
      basic.setIndex(index);
    }
    return basic;
  }
  
  public static String createHibernateMappings(String className, MetaInfo.Type typeInfo)
  {
    String hibernateMappings = new String();
    try
    {
      InputStream in = ClassLoader.getSystemResourceAsStream("default_hibernate_waction.xml");
      SAXBuilder builder = new SAXBuilder();
      Document document = builder.build(in);
      Element rootNode = document.getRootElement();
      
      List<Element> classElms = rootNode.getChildren("class");
      Element clElm = (Element)classElms.get(0);
      clElm.setAttribute("name", className);
      clElm.setAttribute("table", className.replace("wa.", "").toUpperCase());
      for (Map.Entry<String, String> field : typeInfo.fields.entrySet())
      {
        String fieldName = (String)field.getKey();
        String fieldType = (String)field.getValue();
        if (!fieldType.contains("com.bloom."))
        {
          if (logger.isTraceEnabled()) {
            logger.trace("adding field name : " + fieldName + ", type is : " + fieldType);
          }
          Element property = new Element("property");
          if (fieldType.equals("org.joda.time.DateTime")) {
            property.setAttribute("name", fieldName + "AsLong");
          } else {
            property.setAttribute("name", fieldName);
          }
          property.setAttribute("type", getHibernateType(fieldType));
          Element column = new Element("column");
          column.setAttribute("name", "RT_" + fieldName.toUpperCase());
          column.setAttribute("not-null", "false");
          if (getHibernateType(fieldType).equalsIgnoreCase("string")) {
            column.setAttribute("length", "50");
          }
          property.setContent(column);
          clElm.addContent(property);
        }
      }
      XMLOutputter out = new XMLOutputter(Format.getPrettyFormat());
      hibernateMappings = out.outputString(document);
    }
    catch (JDOMException|IOException e)
    {
      logger.error("error creating hibernate mapping for " + className + "\n" + e);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("hibernate xml mapping :\n" + hibernateMappings);
    }
    return hibernateMappings;
  }
  
  private static String getHibernateType(String type)
  {
    switch (type)
    {
    case "java.lang.String": 
      return "string";
    case "org.joda.time.DateTime": 
      return "long";
    }
    return type;
  }
}
