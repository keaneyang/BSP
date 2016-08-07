package com.bloom.runtime;

import com.bloom.anno.NotSet;
import com.bloom.anno.PropertyTemplate;
import com.bloom.anno.PropertyTemplateProperty;
import com.bloom.classloading.WALoader;
import com.bloom.exception.ServerException;
import com.bloom.exception.Warning;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.Compiler.ExecutionCallback;
import com.bloom.runtime.compiler.stmts.Stmt;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.meta.MetaInfo.Initializer;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Namespace;
import com.bloom.runtime.meta.MetaInfo.Page;
import com.bloom.runtime.meta.MetaInfo.PropertyDef;
import com.bloom.runtime.meta.MetaInfo.PropertyTemplateInfo;
import com.bloom.runtime.meta.MetaInfo.Query;
import com.bloom.runtime.meta.MetaInfo.QueryVisualization;
import com.bloom.runtime.meta.MetaInfo.Role;
import com.bloom.runtime.meta.MetaInfo.Source;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.meta.MetaInfo.Target;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.meta.MetaInfo.User;
import com.bloom.runtime.utils.Factory;
import com.bloom.security.WASecurityManager;
import com.bloom.utility.Utility;
import com.bloom.uuid.UUID;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import com.hazelcast.core.HazelcastInstance;
import com.bloom.event.ObjectMapperFactory;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;

public class ServerUpgradeUtility
{
  private static enum MODE
  {
    UNKNOWN,  EXPORT,  IMPORT;
    
    private MODE() {}
  }
  
  private static MODE action = MODE.UNKNOWN;
  private static String jsonFileName = "mcd.json";
  private MDRepository metaDataRepository;
  private static Logger logger = Logger.getLogger(ServerUpgradeUtility.class);
  private static String UPGRADE_ERROR = null;
  private static Map<String, Object> errors = new TreeMap(String.CASE_INSENSITIVE_ORDER);
  static boolean needsRecompile = false;
  static boolean needsQueryCompile = false;
  List<String> flows = new CopyOnWriteArrayList();
  private Set<MetaInfo.MetaObject> moSet;
  static Context ctx = null;
  public static boolean isUpgrading;
  
  public ServerUpgradeUtility()
  {
    if (needsRecompile) {
      loadPropertyTemplates();
    }
    isUpgrading = true;
  }
  
  protected MetaInfo.MetaObject getObject(EntityType type, String namespace, String name)
    throws MetaDataRepositoryException
  {
    if (this.metaDataRepository != null) {
      return this.metaDataRepository.getMetaObjectByName(type, namespace, name, null, WASecurityManager.TOKEN);
    }
    return null;
  }
  
  public MetaInfo.MetaObject getObject(UUID uuid)
    throws MetaDataRepositoryException
  {
    return this.metaDataRepository.getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
  }
  
  public <T extends MetaInfo.MetaObject> T getObjectInfo(UUID uuid, EntityType type)
    throws ServerException, MetaDataRepositoryException
  {
    MetaInfo.MetaObject o = getObject(uuid);
    if ((o != null) && (o.type == type)) {
      return o;
    }
    throw new ServerException("cannot find metadata for " + type.name() + " " + uuid);
  }
  
  private void genDefaultClasses(PropertyTemplate pt)
    throws MetaDataRepositoryException
  {
    Map<String, String> fields = new LinkedHashMap();
    Class<?> inputType = pt.inputType();
    Class<?> outputType = pt.outputType();
    MetaInfo.Type type = null;
    if (!inputType.equals(NotSet.class))
    {
      Field[] cFields = inputType.getDeclaredFields();
      for (Field f : cFields) {
        if (Modifier.isPublic(f.getModifiers())) {
          fields.put(f.getName(), f.getType().getCanonicalName());
        }
      }
      if (getObject(EntityType.TYPE, "Global", inputType.getSimpleName()) == null)
      {
        type = new MetaInfo.Type();
        type.construct("Global." + inputType.getSimpleName(), MetaInfo.GlobalNamespace, inputType.getName(), fields, null, false);
      }
    }
    else
    {
      Field[] cFields = outputType.getDeclaredFields();
      for (Field f : cFields) {
        if (Modifier.isPublic(f.getModifiers())) {
          fields.put(f.getName(), f.getType().getCanonicalName());
        }
      }
      if (getObject(EntityType.TYPE, "Global", outputType.getSimpleName()) == null)
      {
        type = new MetaInfo.Type();
        type.construct("Global." + outputType.getSimpleName(), MetaInfo.GlobalNamespace, outputType.getName(), fields, null, false);
      }
    }
    if (type != null) {
      putObject(type);
    }
  }
  
  protected MetaInfo.MetaObject putObject(MetaInfo.MetaObject obj)
    throws MetaDataRepositoryException
  {
    if (this.metaDataRepository != null)
    {
      this.metaDataRepository.putMetaObject(obj, WASecurityManager.TOKEN);
      return this.metaDataRepository.getMetaObjectByUUID(obj.getUuid(), WASecurityManager.TOKEN);
    }
    return null;
  }
  
  public void loadPropertyTemplates()
  {
    String homeList = System.getProperty("com.bloom.platform.home");
    String[] platformHome = new String[2];
    if ((homeList != null) && (homeList.contains(";"))) {
      platformHome = homeList.split(";");
    } else {
      platformHome[0] = homeList;
    }
    Set<URL> result = Sets.newHashSet();
    for (int i = 0; i < platformHome.length; i++)
    {
      File platformHomeFile;
      if (platformHome[i] == null) {
        platformHomeFile = new File(".");
      } else {
        platformHomeFile = new File(platformHome[i]);
      }
      String platformHomeAbs;
      try
      {
        platformHomeAbs = platformHomeFile.getCanonicalPath();
      }
      catch (IOException e)
      {
        platformHomeAbs = platformHomeFile.getAbsolutePath();
      }
      ClassLoader[] loaders = ClasspathHelper.classLoaders(new ClassLoader[0]);
      for (ClassLoader classLoader : loaders) {
        while (classLoader != null)
        {
          if ((classLoader instanceof URLClassLoader))
          {
            URL[] urls = ((URLClassLoader)classLoader).getURLs();
            if (urls != null) {
              for (URL url : urls) {
                if (url.getPath().contains(platformHomeAbs)) {
                  result.add(url);
                }
              }
            }
          }
          classLoader = classLoader.getParent();
        }
      }
    }
    Reflections refs = new Reflections(result.toArray());
    Set<Class<?>> annotatedClasses = refs.getTypesAnnotatedWith(PropertyTemplate.class);
    for (Class<?> c : annotatedClasses)
    {
      PropertyTemplate pt = (PropertyTemplate)c.getAnnotation(PropertyTemplate.class);
      
      PropertyTemplateProperty[] ptp = pt.properties();
      Map<String, MetaInfo.PropertyDef> props = Factory.makeMap();
      for (PropertyTemplateProperty val : ptp)
      {
        MetaInfo.PropertyDef def = new MetaInfo.PropertyDef();
        def.construct(val.required(), val.type(), val.defaultValue(), val.label(), val.description());
        props.put(val.name(), def);
      }
      Lock propTemplateLock = HazelcastSingleton.get().getLock("propTemplateLock");
      
      propTemplateLock.lock();
      try
      {
        genDefaultClasses(pt);
        MetaInfo.PropertyTemplateInfo pti = (MetaInfo.PropertyTemplateInfo)getObject(EntityType.PROPERTYTEMPLATE, "Global", pt.name());
        
        boolean isUnique = false;
        Map<String, MetaInfo.PropertyDef> defCurrent;
        if (pti != null)
        {
          defCurrent = pti.propertyMap;
          for (Map.Entry<String, MetaInfo.PropertyDef> entry : props.entrySet()) {
            if (!((MetaInfo.PropertyDef)entry.getValue()).equals((MetaInfo.PropertyDef)defCurrent.get(entry.getKey())))
            {
              isUnique = true;
              break;
            }
          }
        }
        else
        {
          isUnique = true;
        }
        if (isUnique)
        {
          pti = new MetaInfo.PropertyTemplateInfo();
          pti.construct(pt.name(), pt.type(), props, pt.inputType().getName(), pt.outputType().getName(), c.getName(), pt.requiresParser(), pt.requiresFormatter());
          
          putObject(pti);
        }
      }
      catch (Exception e)
      {
        logger.error(e.getLocalizedMessage());
      }
      finally
      {
        propTemplateLock.unlock();
      }
    }
  }
  
  private void initialize()
    throws MetaDataRepositoryException
  {
    BaseServer.setMetaDataDbProviderDetails();
    NodeStartUp nsu = new NodeStartUp(true);
    MetaInfo.Initializer ini = nsu.getInitializer();
    HazelcastSingleton.setDBDetailsForMetaDataRepository(ini.MetaDataRepositoryLocation, ini.MetaDataRepositoryDBname, ini.MetaDataRepositoryUname, ini.MetaDataRepositoryPass);
    
    printf("Getting a metadata repository instance and initializing.....\n");
    this.metaDataRepository = MetadataRepository.getINSTANCE();
    this.metaDataRepository.initialize();
  }
  
  private void upgrade()
  {
    printf("Starting upgrade process.... \n");
    String json = getJsonString(jsonFileName);
    if (json == null)
    {
      UPGRADE_ERROR = "Aborting upgrade process. Total errors :" + errors.size() + "\n";
      return;
    }
    this.moSet = getMetaObjectsFromJson(json);
    try
    {
      checkImportStmts();
    }
    catch (Exception e)
    {
      logger.warn(e);
    }
    if (errors.size() > 0)
    {
      UPGRADE_ERROR = "Error importing json. Aborting upgrade process. Total errors :" + errors.size() + "\n";
      printf(UPGRADE_ERROR);
      for (String key : errors.keySet()) {
        printf("node[" + key + "] : " + ((Exception)errors.get(key)).getMessage() + "\n");
      }
      printf("\n\nUpgrade process went bad. Look at above errors.\n\n");
    }
    else
    {
      printf("\n\nUpgrade process went OK without errors.\n\n");
      deleteMetadata();
      importMetadata(this.moSet);
      if (needsRecompile) {
        fixSubscription(this.moSet);
      }
      printf("\n\nDone upgrading. Start server.");
    }
  }
  
  public void checkImportStmts()
    throws Exception
  {
    if ((this.moSet == null) || (this.moSet.isEmpty())) {
      return;
    }
    for (MetaInfo.MetaObject obj : this.moSet) {
      if (obj.getType() == EntityType.APPLICATION)
      {
        Set<String> importStmtsList = ((MetaInfo.Flow)obj).importStatements;
        if (logger.isDebugEnabled()) {
          logger.debug(importStmtsList);
        }
        for (String imp : importStmtsList) {
          Compiler.compile(imp, ctx, new Compiler.ExecutionCallback()
          {
            public void execute(Stmt stmt, Compiler compiler)
              throws MetaDataRepositoryException
            {
              try
              {
                compiler.compileStmt(stmt);
              }
              catch (Warning e)
              {
                ServerUpgradeUtility.logger.warn(">>>>" + e.getMessage());
              }
            }
          });
        }
      }
    }
  }
  
  public static String convertNewQueries(String selectQuery)
  {
    Pattern stringParamMatch = Pattern.compile("(\")(:)(\\w+)(\")");
    Matcher matcher = stringParamMatch.matcher(selectQuery);
    
    String tempSelectQuery = selectQuery;
    while (matcher.find()) {
      tempSelectQuery = tempSelectQuery.replaceAll(matcher.group(), matcher.group(2) + matcher.group(3));
    }
    stringParamMatch = Pattern.compile("(')(:)(\\w+)(')");
    matcher = stringParamMatch.matcher(selectQuery);
    while (matcher.find()) {
      tempSelectQuery = tempSelectQuery.replaceAll(matcher.group(), matcher.group(2) + matcher.group(3));
    }
    return tempSelectQuery;
  }
  
  private void fixQueries()
  {
    Iterator<MetaInfo.MetaObject> it = this.moSet.iterator();
    while (it.hasNext())
    {
      MetaInfo.MetaObject mo = (MetaInfo.MetaObject)it.next();
      try
      {
        if (mo.type == EntityType.QUERY)
        {
          final MetaInfo.Query query = (MetaInfo.Query)mo;
          
          ctx.useNamespace(mo.nsName);
          if (this.metaDataRepository.getMetaObjectByName(EntityType.QUERY, ctx.getCurNamespace().getName(), query.name, null, WASecurityManager.TOKEN) != null) {
            continue;
          }
          Compiler.compile("CREATE NAMEDQUERY " + query.name + " " + convertNewQueries(query.queryDefinition), ctx, new Compiler.ExecutionCallback()
          {
            public void execute(Stmt stmt, Compiler compiler)
              throws MetaDataRepositoryException
            {
              try
              {
                compiler.compileStmt(stmt);
                ServerUpgradeUtility.this.flows.add(Compiler.NAMED_QUERY_PREFIX + query.name);
              }
              catch (Warning e) {}
            }
          });
        }
      }
      catch (Exception e)
      {
        logger.warn(e.getMessage());
      }
    }
  }
  
  private void fixSubscription(Set<MetaInfo.MetaObject> moSet)
  {
    Iterator<MetaInfo.MetaObject> it = moSet.iterator();
    while (it.hasNext())
    {
      MetaInfo.MetaObject mo = (MetaInfo.MetaObject)it.next();
      try
      {
        if ((mo.type == EntityType.TARGET) && (((MetaInfo.Target)mo).isSubscription()))
        {
          Class<?> adapterFactory = WALoader.get().loadClass(((MetaInfo.Target)mo).adapterClassName);
          
          PropertyTemplate pt = (PropertyTemplate)adapterFactory.getAnnotation(PropertyTemplate.class);
          MetaInfo.MetaObject input = this.metaDataRepository.getMetaObjectByUUID(((MetaInfo.Target)mo).getInputStream(), WASecurityManager.TOKEN);
          mo.setSourceText("CREATE SUBSCRIPTION " + mo.getName() + " USING " + pt.name() + " ( " + Utility.prettyPrintMap(makePropList(new HashMap[] { (HashMap)((MetaInfo.Target)mo).properties })) + " ) INPUT FROM " + input.getName() + "");
          this.metaDataRepository.updateMetaObject(mo, WASecurityManager.TOKEN);
        }
      }
      catch (Exception e)
      {
        logger.warn(e.getMessage());
      }
    }
  }
  
  private void deleteMetadata()
  {
    this.metaDataRepository.clear(true);
  }
  
  public static String getJsonString(String filename)
  {
    String json = null;
    try
    {
      BufferedReader br = new BufferedReader(new FileReader(filename));
      json = "";
      String sCurrentLine;
      while ((sCurrentLine = br.readLine()) != null) {
        json = json + sCurrentLine;
      }
      br.close();
    }
    catch (IOException iox)
    {
      logger.error("error reading file : " + filename, iox);
      errors.put("error reading file : " + filename, iox);
      printf("error reading file : " + filename);
    }
    return json;
  }
  
  private List<Property> makePropList(Map<String, String>[] props)
  {
    if (props == null) {
      return null;
    }
    List<Property> plist = new ArrayList();
    for (Map<String, String> prop : props)
    {
      assert (prop.size() == 1);
      for (Map.Entry<String, String> e : prop.entrySet())
      {
        Property p = new Property((String)e.getKey(), e.getValue());
        plist.add(p);
      }
    }
    return plist;
  }
  
  public static class DashboardConverter
    extends MetaInfo.MetaObject
  {
    static ObjectMapper jsonMapper;
    static HashMap<String, LegacyQueryVisualization> oldQVs;
    
    private static class LegacyQueryVisualization
      extends MetaInfo.MetaObject
    {
      String title;
      String query;
      List<String> dataVisualizations;
      String gridJSON;
      boolean composite;
      
      public void setComposite(boolean composite)
      {
        this.composite = composite;
      }
      
      public void setTitle(String title)
      {
        this.title = title;
      }
      
      public void setQuery(String query)
      {
        this.query = query;
      }
      
      public void setDataVisualizations(List<String> dataVisualizations)
      {
        this.dataVisualizations = dataVisualizations;
      }
      
      public void setGridJSON(String gridJSON)
      {
        this.gridJSON = gridJSON;
      }
    }
    
    public static List<MetaInfo.MetaObject> convert(List<JsonNode> qvObjects, List<JsonNode> pageObjects)
      throws IOException
    {
      jsonMapper = ObjectMapperFactory.newInstance();
      jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      oldQVs = new HashMap();
      
      List<MetaInfo.MetaObject> dashboardMetaObjects = new ArrayList();
      for (JsonNode qvJson : qvObjects)
      {
        List<MetaInfo.QueryVisualization> newQVs = convertCompositeQV(qvJson);
        dashboardMetaObjects.addAll(newQVs);
      }
      for (JsonNode pageJson : pageObjects)
      {
        MetaInfo.Page newPage = convertPage(pageJson);
        dashboardMetaObjects.add(newPage);
      }
      return dashboardMetaObjects;
    }
    
    private static List<MetaInfo.QueryVisualization> convertCompositeQV(JsonNode jsonNode)
      throws IOException
    {
      List<MetaInfo.QueryVisualization> newQVs = new ArrayList();
      
      LegacyQueryVisualization oldQV = (LegacyQueryVisualization)jsonMapper.readValue(jsonNode.toString(), LegacyQueryVisualization.class);
      
      oldQVs.put(oldQV.getName(), oldQV);
      if ((oldQV != null) && (oldQV.dataVisualizations != null) && (oldQV.dataVisualizations.size() > 0))
      {
        ServerUpgradeUtility.logger.debug("Creating " + oldQV.dataVisualizations.size() + " single-panel Visualizations out of " + oldQV.name + "");
        for (String dvJsonString : oldQV.dataVisualizations)
        {
          ObjectNode newQVJSON = (ObjectNode)jsonMapper.readTree(jsonNode.toString());
          JsonNode dvJson = jsonMapper.readTree(dvJsonString);
          
          newQVJSON.put("name", dvJson.get("id").asText());
          newQVJSON.put("uri", jsonNode.get("uri").asText());
          newQVJSON.put("visualizationType", dvJson.get("type").asText());
          newQVJSON.put("config", dvJson.get("config").toString());
          
          MetaInfo.QueryVisualization newQV = jsonToQV(newQVJSON);
          newQVs.add(newQV);
        }
      }
      else
      {
        ServerUpgradeUtility.logger.warn("Warning: " + oldQV.name + " had no visualizations. Cannot determine Visualization type.");
      }
      return newQVs;
    }
    
    private static MetaInfo.QueryVisualization jsonToQV(JsonNode newQvJson)
      throws IOException
    {
      MetaInfo.QueryVisualization qv = (MetaInfo.QueryVisualization)jsonMapper.readValue(newQvJson.toString(), MetaInfo.QueryVisualization.class);
      
      MetaInfo.QueryVisualization newQV = new MetaInfo.QueryVisualization(qv.getName(), null, qv.getNsName(), qv.getNamespaceId());
      
      newQV.setVisualizationType(qv.getVisualizationType());
      newQV.setUri(qv.getUri());
      newQV.setConfig(qv.getConfig());
      newQV.setQuery(qv.getQuery());
      newQV.setTitle(qv.getTitle());
      return newQV;
    }
    
    private static MetaInfo.Page convertPage(JsonNode pageJson)
      throws IOException
    {
      MetaInfo.Page page = (MetaInfo.Page)jsonMapper.readValue(pageJson.toString(), MetaInfo.Page.class);
      ServerUpgradeUtility.logger.debug("\nConverting Page " + page.getTitle() + " with " + page.getQueryVisualizations().size() + " original QVs...");
      
      convertPageQVArray(page);
      convertPageGrid(page);
      
      return page;
    }
    
    private static void convertPageQVArray(MetaInfo.Page metaObject)
      throws IOException
    {
      List<String> oldPageQVNames = metaObject.getQueryVisualizations();
      List<String> newPageQVNames = new ArrayList();
      for (String oldQVName : oldPageQVNames)
      {
        LegacyQueryVisualization oldQV = (LegacyQueryVisualization)oldQVs.get(oldQVName);
        if ((oldQV != null) && (oldQV.dataVisualizations != null)) {
          for (String dvJson : oldQV.dataVisualizations)
          {
            ObjectNode dv = (ObjectNode)jsonMapper.readTree(dvJson);
            String dvName = dv.get("id").asText();
            newPageQVNames.add(dvName);
          }
        }
        metaObject.setQueryVisualizations(newPageQVNames);
      }
    }
    
    private static void convertPageGrid(MetaInfo.Page metaObject)
      throws IOException
    {
      ObjectNode gridJson = (ObjectNode)jsonMapper.readTree(metaObject.getGridJSON());
      
      ServerUpgradeUtility.logger.debug("Flattening page grid with " + gridJson.get("components").size() + " components...");
      
      flattenGrid(gridJson, Integer.valueOf(0), Integer.valueOf(0), Integer.valueOf(12));
      
      ArrayNode newComponents = jsonMapper.createArrayNode();
      for (JsonNode oldQVComponent : gridJson.get("components"))
      {
        ArrayNode unwrappedDVComponents = convertQVGrid((ObjectNode)oldQVComponent);
        newComponents.addAll(unwrappedDVComponents);
      }
      gridJson.set("components", newComponents);
      metaObject.setGridJSON(gridJson.toString());
      
      ServerUpgradeUtility.logger.debug("   Done flattening page grid. Page now has " + gridJson.get("components").size() + " components (all QVs).");
    }
    
    private static ArrayNode convertQVGrid(ObjectNode oldComponent)
      throws IOException
    {
      String oldQVNameInComponent = oldComponent.get("content_id").asText();
      LegacyQueryVisualization oldQV = (LegacyQueryVisualization)oldQVs.get(oldQVNameInComponent);
      ObjectNode oldQVGridJson = (ObjectNode)jsonMapper.readTree(oldQV.gridJSON);
      flattenGrid(oldQVGridJson, Integer.valueOf(oldComponent.get("x").asInt()), Integer.valueOf(oldComponent.get("y").asInt()), Integer.valueOf(oldComponent.get("width").asInt()));
      return (ArrayNode)oldQVGridJson.get("components");
    }
    
    private static void flattenGrid(ObjectNode gridJson, Integer parentX, Integer parentY, Integer parentWidth)
      throws IOException
    {
      ArrayNode components = (ArrayNode)gridJson.get("components");
      
      ArrayNode newComponents = jsonMapper.createArrayNode();
      for (JsonNode jsonComponent : components)
      {
        ObjectNode component = (ObjectNode)jsonComponent;
        
        String componentType = component.get("grid") != null ? "LAYOUT" : component.get("content_id") != null ? "CONTENT" : null;
        if (componentType == null) {
          throw new IOException("Bad component type: " + component);
        }
        if ("LAYOUT".equals(componentType))
        {
          ServerUpgradeUtility.logger.debug("  -- Nested Layout with " + component.get("grid").get("components").size() + " components");
          flattenGrid((ObjectNode)component.get("grid"), Integer.valueOf(component.get("x").asInt()), Integer.valueOf(component.get("y").asInt()), Integer.valueOf(component.get("width").asInt()));
          ServerUpgradeUtility.logger.debug("     Translating components inside nested grid at " + component.get("width") + "x" + component.get("height") + " (" + component.get("x") + "," + component.get("y") + ")");
          
          newComponents.addAll((ArrayNode)component.get("grid").get("components"));
        }
        else
        {
          newComponents.add(component);
          ServerUpgradeUtility.logger.debug("  -- " + newComponents.size() + " Adding new component " + component.get("content_id") + " " + component.get("width") + "x" + component.get("height") + " " + component.get("x") + "," + component.get("y"));
        }
      }
      for (JsonNode nestedJsonComponent : newComponents) {
        translateLayout((ObjectNode)nestedJsonComponent, parentX, parentY, parentWidth);
      }
      gridJson.set("components", newComponents);
    }
    
    private static void translateLayout(ObjectNode nestedComponent, Integer parentX, Integer parentY, Integer parentWidth)
    {
      Double widthRatio = Double.valueOf(parentWidth.intValue() / 12.0D);
      
      Double origWidth = Double.valueOf(nestedComponent.get("width").asDouble());
      Double origHeight = Double.valueOf(nestedComponent.get("height").asDouble());
      Double origX = Double.valueOf(nestedComponent.get("x").asDouble());
      Double origY = Double.valueOf(nestedComponent.get("y").asDouble());
      
      Double newWidth = Double.valueOf(origWidth.doubleValue() * widthRatio.doubleValue());
      
      Double newX = Double.valueOf(parentX.intValue() + origX.doubleValue() * widthRatio.doubleValue());
      
      Double newY = Double.valueOf(parentY.intValue() + origY.doubleValue());
      
      nestedComponent.put("width", newWidth);
      nestedComponent.put("x", newX);
      nestedComponent.put("y", newY);
      
      ServerUpgradeUtility.logger.info("Scaling " + nestedComponent.get("content_id") + " out from grid at (" + parentX + "," + parentY + ") with width ratio " + widthRatio + " : " + origWidth + "x" + origHeight + " (" + origX + "," + origY + ")" + " --> " + nestedComponent.get("width") + "x" + origHeight + " (" + newX + "," + newY + ")");
    }
  }
  
  private static Set<MetaInfo.MetaObject> getMetaObjectsFromJson(String json)
  {
    Set<MetaInfo.MetaObject> moSet = null;
    List<JsonNode> qvObjects = new ArrayList();
    List<JsonNode> pageObjects = new ArrayList();
    String className = "";
    int nodecntr = 1;
    errors.clear();
    try
    {
      ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
      jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      JsonNode listNode = jsonMapper.readTree(json);
      if (!listNode.isContainerNode())
      {
        logger.warn("JSON MetaData is not in list format");
        return null;
      }
      if (logger.isInfoEnabled()) {
        logger.info("Number of objects : " + listNode.size());
      }
      moSet = new HashSet();
      Iterator<JsonNode> it = listNode.elements();
      while (it.hasNext())
      {
        boolean addFlag = false;
        MetaInfo.MetaObject mo = null;
        JsonNode moNode = (JsonNode)it.next();
        className = moNode.get("metaObjectClass").asText();
        nodecntr++;
        if (logger.isDebugEnabled()) {
          logger.debug(nodecntr + " : Building MetaInfo object : " + className);
        }
        try
        {
          if (className.equalsIgnoreCase(MetaInfo.Query.class.getName()))
          {
            mo = MetaInfo.Query.deserialize(moNode);
            addFlag = true;
          }
          else if (className.equalsIgnoreCase(MetaInfo.Role.class.getName()))
          {
            mo = MetaInfo.Role.deserialize(moNode);
            addFlag = true;
          }
          else if (className.equalsIgnoreCase(MetaInfo.User.class.getName()))
          {
            mo = MetaInfo.User.deserialize(moNode);
            addFlag = true;
          }
          else
          {
            String moNodeText = moNode.toString();
            Class<?> moClass = Class.forName(className);
            mo = (MetaInfo.MetaObject)jsonMapper.readValue(moNodeText, moClass);
            addFlag = true;
          }
          if (addFlag) {
            if (mo == null) {
              errors.put(Integer.toString(nodecntr), new Exception("Building " + className + " from below JSON failed:\n" + moNode.asText()));
            } else {
              moSet.add(mo);
            }
          }
        }
        catch (Exception e)
        {
          logger.warn(nodecntr + " : Error building MetaInfo object : " + className, e);
          errors.put(Integer.toString(nodecntr), e);
        }
      }
    }
    catch (Exception e)
    {
      logger.error("error creating metadata objects from json string.", e);
    }
    return moSet;
  }
  
  public static String exportMetadataAsJson(MetaInfo.MetaObject metaObject)
  {
    try
    {
      ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
      jsonMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
      return jsonMapper.writeValueAsString(metaObject);
    }
    catch (JsonGenerationException e)
    {
      logger.error(e);
    }
    catch (JsonMappingException e)
    {
      logger.error(e);
    }
    catch (IOException e)
    {
      logger.error(e);
    }
    return null;
  }
  
  private void importMetadata(Set<MetaInfo.MetaObject> moSet)
  {
    if (logger.isInfoEnabled()) {
      logger.info("Started inserting MetaData to database.");
    }
    for (MetaInfo.MetaObject mo : moSet) {
      if (((!mo.getName().startsWith(Compiler.NAMED_QUERY_PREFIX)) && (mo.type != EntityType.QUERY)) || (!needsQueryCompile)) {
        try
        {
          this.metaDataRepository.putMetaObject(mo, WASecurityManager.TOKEN);
          if ((mo.type == EntityType.TYPE) && (needsQueryCompile)) {
            ((MetaInfo.Type)mo).generateClass();
          }
        }
        catch (Exception e)
        {
          logger.error("error inserting metadata object: " + mo.metaObjectClass, e);
        }
      }
    }
    System.out.println("Done inserting all metadata objects.");
  }
  
  private void exportMetaData()
    throws MetaDataRepositoryException
  {
    String json = this.metaDataRepository.exportMetadataAsJson();
    if ((json == null) || (json.isEmpty()))
    {
      logger.warn("Exporting metadata to json failed.");
      return;
    }
    File f = new File(jsonFileName);
    try
    {
      FileWriter fw = new FileWriter(f);
      fw.write(json);
      fw.close();
    }
    catch (IOException e)
    {
      logger.error("error writing json string to file", e);
    }
  }
  
  private static void printf(String toPrint)
  {
    try
    {
      if (System.console() != null) {
        System.console().printf(toPrint, new Object[0]);
      } else {
        System.out.printf(toPrint, new Object[0]);
      }
    }
    catch (Exception ex) {}
    System.out.flush();
  }
  
  private static void EXTRACTINPUTS(String[] args)
  {
    if ((args == null) || (args.length < 2))
    {
      printf("\n>>> missing input parameters '<import / export>   <filename>'\n");
      System.exit(0);
    }
    if (args[0].equalsIgnoreCase("import"))
    {
      action = MODE.IMPORT;
    }
    else if (args[0].equalsIgnoreCase("export"))
    {
      action = MODE.EXPORT;
    }
    else
    {
      printf("\n>>> missing input parameters '<import / export>   <filename>'\n");
      System.exit(0);
    }
    jsonFileName = args[1];
    if ((args.length > 2) && 
      (args[2].equalsIgnoreCase("yes"))) {
      needsRecompile = true;
    }
    if ((args.length > 3) && 
      (args[3].equalsIgnoreCase("yes"))) {
      needsQueryCompile = true;
    }
  }
  
  public static boolean testCompatibility(String json)
    throws Exception
  {
    if ((json == null) || (json.isEmpty())) {
      json = MetadataRepository.getINSTANCE().exportMetadataAsJson();
    }
    Set<MetaInfo.MetaObject> moSet = getMetaObjectsFromJson(json);
    if (errors.size() > 0)
    {
      UPGRADE_ERROR = "Error importing json. Aborting upgrade process. Total errors :" + errors.size() + "\n";
      printf(UPGRADE_ERROR);
      for (String key : errors.keySet()) {
        printf("node[" + key + "] : " + ((Exception)errors.get(key)).getMessage() + "\n");
      }
      logger.warn("UPGRADE_ERROR");
      return false;
    }
    printf("no of meta obejcts imported :" + moSet.size() + "\n");
    logger.warn("basic compatibility test passed.");
    printf("basic compatibility test passed. \n");
    
    return true;
  }
  
  public static void main(String[] args)
    throws Exception
  {
    System.out.println("Server upgrade utility started....");
    EXTRACTINPUTS(args);
    if ((action.equals(MODE.IMPORT)) && 
      (getJsonString(jsonFileName) == null)) {
      System.exit(-1);
    }
    ctx = Context.createContext(WASecurityManager.TOKEN);
    ServerUpgradeUtility suu = new ServerUpgradeUtility();
    suu.initialize();
    if (action.equals(MODE.EXPORT)) {
      suu.exportMetaData();
    } else if (action.equals(MODE.IMPORT)) {
      suu.upgrade();
    }
    if (UPGRADE_ERROR != null)
    {
      printf(">>>> " + UPGRADE_ERROR);
      System.exit(-1);
    }
    logger.warn("Done " + action + "ing. Now setting recompile flag");
    try
    {
      if (needsRecompile)
      {
        Set<MetaInfo.Source> sources = suu.metaDataRepository.getByEntityType(EntityType.SOURCE, WASecurityManager.TOKEN);
        for (MetaInfo.Source source : sources)
        {
          Class<?> parserCls = null;
          PropertyTemplate adapterAnno = null;
          PropertyTemplate parserAnno = null;
          try
          {
            Class<?> adapterCls = Class.forName(source.adapterClassName);
            if ((source != null) && (source.parserProperties != null) && (source.parserProperties.get("handler") != null)) {
              parserCls = Class.forName((String)source.parserProperties.get("handler"));
            }
            adapterAnno = (PropertyTemplate)adapterCls.getAnnotation(PropertyTemplate.class);
            if (parserCls != null) {
              parserAnno = (PropertyTemplate)parserCls.getAnnotation(PropertyTemplate.class);
            }
          }
          catch (ClassNotFoundException e1)
          {
            e1.printStackTrace();
          }
          MetaInfo.Stream stream = (MetaInfo.Stream)suu.metaDataRepository.getMetaObjectByUUID(source.outputStream, WASecurityManager.TOKEN);
          source.setSourceText(Utility.createSourceStatementText(source.getName(), Boolean.valueOf(false), adapterAnno.name(), suu.makePropList(new HashMap[] { (HashMap)source.properties }), parserAnno == null ? null : parserAnno.name(), source.parserProperties == null ? null : suu.makePropList(new HashMap[] { (HashMap)source.parserProperties }), stream.getName(), null));
          if (logger.isDebugEnabled()) {
            logger.debug(source.getSourceText());
          }
          suu.metaDataRepository.updateMetaObject(source, WASecurityManager.TOKEN);
        }
        Set<MetaInfo.Flow> flows = suu.metaDataRepository.getByEntityType(EntityType.APPLICATION, WASecurityManager.TOKEN);
        for (MetaInfo.Flow flow : flows) {
          if (!suu.flows.contains(flow.getName())) {
            if (!flow.getMetaInfoStatus().isDropped())
            {
              flow.getMetaInfoStatus().setValid(false);
              suu.metaDataRepository.updateMetaObject(flow, WASecurityManager.TOKEN);
              for (UUID uuid : flow.getAllObjects())
              {
                MetaInfo.MetaObject obj = suu.metaDataRepository.getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
                if (obj != null)
                {
                  obj.getMetaInfoStatus().setValid(false);
                  suu.metaDataRepository.updateMetaObject(obj, WASecurityManager.TOKEN);
                }
              }
              ctx.useNamespace(flow.nsName);
              Set<String> tempList = flow.importStatements;
              ctx.alterAppOrFlow(EntityType.APPLICATION, flow.getFullName(), true);
              MetaInfo.Flow currApp = ctx.getFlowInCurSchema(flow.getFullName(), EntityType.APPLICATION);
              currApp.importStatements.addAll(tempList);
              ctx.put(currApp);
            }
          }
        }
      }
      if (needsQueryCompile) {
        suu.fixQueries();
      }
    }
    catch (Exception ex)
    {
      logger.error("error setting recompile flag", ex);
      System.exit(errors.size());
    }
    System.exit(0);
  }
}
