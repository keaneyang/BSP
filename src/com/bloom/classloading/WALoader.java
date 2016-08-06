package com.bloom.classloading;

import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.runtime.compiler.CompilerUtils;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.uuid.UUID;
import com.bloom.waction.Waction;
import com.bloom.waction.WactionContext;
import com.esotericsoftware.kryo.KryoSerializable;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.bloom.event.SimpleEvent;
import com.bloom.event.WactionConvertible;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewConstructor;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import javassist.SerialVersionUID;
import org.apache.log4j.Logger;

public class WALoader
  extends DistributedClassLoader
{
  public static final String ALL_IDS = "#allIds";
  public static final String CLASS_IDS = "#classIds";
  static Logger logger = Logger.getLogger(WALoader.class);
  private static volatile WALoader instance;
  Map<String, BundleLoader> bundleLoaders;
  
  public static WALoader get(ClassLoader parent, boolean isClient)
  {
    if (instance == null) {
      synchronized (WALoader.class)
      {
        if (instance == null) {
          instance = new WALoader(parent == null ? DistributedClassLoader.class.getClassLoader() : parent, isClient);
        }
      }
    }
    return instance;
  }
  
  public static void shutdown()
  {
    instance = null;
  }
  
  public static WALoader get(ClassLoader parent)
  {
    return get(parent, false);
  }
  
  public static WALoader get(boolean isClient)
  {
    return get(DistributedClassLoader.class.getClassLoader(), isClient);
  }
  
  public static WALoader get()
  {
    return get(null, false);
  }
  
  public static WALoaderDelegate getDelegate()
  {
    return new WALoaderDelegate();
  }
  
  protected static class WALoaderDelegate
    extends ClassLoader
  {
    protected Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException
    {
      return WALoader.instance.loadClass(name, resolve);
    }
    
    public Class<?> loadClass(String name)
      throws ClassNotFoundException
    {
      if (WALoader.logger.isTraceEnabled()) {
        WALoader.logger.trace("loading class " + name);
      }
      return WALoader.instance.loadClass(name, false);
    }
    
    public InputStream getResourceAsStream(String name)
    {
      if (WALoader.logger.isTraceEnabled()) {
        WALoader.logger.trace("getting resource " + name + " as stream.");
      }
      return WALoader.instance.getResourceAsStream(name);
    }
  }
  
  class BundleMonitor
    implements EntryListener<String, BundleDefinition>
  {
    BundleMonitor() {}
    
    public void entryAdded(EntryEvent<String, BundleDefinition> event) {}
    
    public void entryRemoved(EntryEvent<String, BundleDefinition> event)
    {
      String uri = (String)event.getKey();
      BundleDefinition def = (BundleDefinition)event.getOldValue();
      
      IMap<String, ClassDefinition> classes = DistributedClassLoader.getClasses();
      IMap<String, String> classToBundleResolver = DistributedClassLoader.getClassToBundleResolver();
      synchronized (WALoader.this.bundleLoaders)
      {
        for (String className : def.getClassNames())
        {
          ((WALoader.BridgePool)WALoader.this.pool).removeCachedClass(className);
          classes.remove(className);
          classToBundleResolver.remove(className);
          if (WALoader.logger.isTraceEnabled()) {
            WALoader.logger.trace("Removed class " + className);
          }
        }
        BundleLoader loader = (BundleLoader)WALoader.this.bundleLoaders.get(uri);
        if (loader != null)
        {
          loader.removeClasses();
          WALoader.this.bundleLoaders.remove(uri);
        }
        if (WALoader.logger.isTraceEnabled()) {
          WALoader.logger.trace("Removed bundle " + (String)event.getKey());
        }
      }
    }
    
    public void entryUpdated(EntryEvent<String, BundleDefinition> event) {}
    
    public void entryEvicted(EntryEvent<String, BundleDefinition> event) {}
    
    public void mapCleared(MapEvent arg0) {}
    
    public void mapEvicted(MapEvent arg0) {}
  }
  
  class BridgePool
    extends ClassPool
  {
    WALoader peerLoader;
    
    public BridgePool(WALoader bLoader)
    {
      super();
      this.peerLoader = bLoader;
    }
    
    public CtClass get(String name)
      throws NotFoundException
    {
      if (WALoader.logger.isTraceEnabled()) {
        WALoader.logger.trace(this.peerLoader.getName() + " get CtClass " + name);
      }
      BundleLoader bl = this.peerLoader.getBundleLoaderForClass(name);
      if (bl != null)
      {
        if (WALoader.logger.isTraceEnabled()) {
          WALoader.logger.trace(this.peerLoader.getName() + " using BundleLoader " + bl.getName() + " for " + name);
        }
        return bl.getPool().get(name);
      }
      if (WALoader.logger.isTraceEnabled()) {
        WALoader.logger.trace(this.peerLoader.getName() + " using default pool for " + name);
      }
      return super.get(name);
    }
    
    public void removeCachedClass(String classname)
    {
      removeCached(classname);
    }
  }
  
  private WALoader(ClassLoader parent, boolean isClient)
  {
    super(parent, isClient);
    this.pool = new BridgePool(this);
    this.pool.childFirstLookup = true;
    this.bundleLoaders = new HashMap();
    setName("WALoader");
  }
  
  public void init()
  {
    IMap<String, BundleDefinition> bundles = getBundles();
    bundles.addEntryListener(new BundleMonitor(), true);
  }
  
  public void lockClass(String name)
  {
    IMap<String, ClassDefinition> classes = getClasses();
    classes.lock(name);
  }
  
  public void unlockClass(String name)
  {
    IMap<String, ClassDefinition> classes = getClasses();
    classes.unlock(name);
  }
  
  public void lockBundle(String name)
  {
    IMap<String, BundleDefinition> bundles = getBundles();
    bundles.lock(name);
  }
  
  public void unlockBundle(String name)
  {
    IMap<String, BundleDefinition> bundles = getBundles();
    bundles.unlock(name);
  }
  
  Map<String, Class<?>> primitiveLookup = null;
  Object primitiveLookupLock = new Object();
  
  void initializePrimitiveLookup()
  {
    synchronized (this.primitiveLookupLock)
    {
      if (this.primitiveLookup == null)
      {
        this.primitiveLookup = new HashMap();
        
        this.primitiveLookup.put(Boolean.TYPE.getName(), Boolean.TYPE);
        this.primitiveLookup.put(Byte.TYPE.getName(), Byte.TYPE);
        this.primitiveLookup.put(Character.TYPE.getName(), Character.TYPE);
        this.primitiveLookup.put(Double.TYPE.getName(), Double.TYPE);
        this.primitiveLookup.put(Float.TYPE.getName(), Float.TYPE);
        this.primitiveLookup.put(Integer.TYPE.getName(), Integer.TYPE);
        this.primitiveLookup.put(Long.TYPE.getName(), Long.TYPE);
        this.primitiveLookup.put(Short.TYPE.getName(), Short.TYPE);
        
        this.primitiveLookup.put("Z", Boolean.TYPE);
        this.primitiveLookup.put("B", Byte.TYPE);
        this.primitiveLookup.put("C", Character.TYPE);
        this.primitiveLookup.put("D", Double.TYPE);
        this.primitiveLookup.put("F", Float.TYPE);
        this.primitiveLookup.put("I", Integer.TYPE);
        this.primitiveLookup.put("J", Long.TYPE);
        this.primitiveLookup.put("S", Short.TYPE);
      }
    }
  }
  
  Class<?> checkForPrimitive(String name)
  {
    if (this.primitiveLookup == null) {
      initializePrimitiveLookup();
    }
    return (Class)this.primitiveLookup.get(name);
  }
  
  protected Class<?> loadClass(String name, boolean resolve)
    throws ClassNotFoundException
  {
    if (logger.isTraceEnabled()) {
      logger.trace(getName() + ": load class " + name);
    }
    boolean isArray = false;
    int arrayDimensions = 0;
    if (name.startsWith("["))
    {
      while (name.startsWith("["))
      {
        name = name.substring(1);
        arrayDimensions++;
      }
      isArray = true;
    }
    else if (name.endsWith("[]"))
    {
      while (name.endsWith("[]"))
      {
        name = name.substring(0, name.length() - 2);
        arrayDimensions++;
      }
      isArray = true;
    }
    Class<?> clazz = checkForPrimitive(name);
    if (clazz == null)
    {
      if ((name.startsWith("L")) && (name.endsWith(";"))) {
        name = name.substring(1, name.length() - 1);
      }
      try
      {
        clazz = getParent().loadClass(name);
      }
      catch (ClassNotFoundException e) {}
      if (clazz == null)
      {
        BundleLoader bl = getBundleLoaderForClass(name);
        if (bl != null) {
          clazz = bl.loadClass(name);
        } else {
          throw new ClassNotFoundException("Could not load class " + name);
        }
      }
    }
    if (isArray) {
      while (arrayDimensions-- > 0) {
        clazz = Array.newInstance(clazz, 0).getClass();
      }
    }
    return clazz;
  }
  
  public final Class<?> defineClass(String name, String loaderName, byte[] b, int off, int len)
    throws ClassFormatError
  {
    Class<?> clazz = null;
    BundleLoader bl = getBundleLoaderForClass(name);
    if (bl != null) {
      clazz = bl.defineClass(name, loaderName, b, off, len);
    } else {
      throw new RuntimeException(getName() + ": Could not define class " + name);
    }
    return clazz;
  }
  
  public InputStream getResourceAsStream(String name)
  {
    if (logger.isTraceEnabled()) {
      logger.trace(getName() + ": get resource " + name);
    }
    String className = name.replaceAll("[/]", ".");
    if (className.endsWith(".class")) {
      className = className.substring(0, className.lastIndexOf('.'));
    }
    if (logger.isTraceEnabled()) {
      logger.trace(getName() + ": get resource checking for class " + className);
    }
    byte[] bytecode = getClassBytes(className);
    if (bytecode != null)
    {
      if (logger.isTraceEnabled()) {
        logger.trace(getName() + ": get resource returning bytes for " + name);
      }
      ByteArrayInputStream bais = new ByteArrayInputStream(bytecode);
      return bais;
    }
    if (logger.isTraceEnabled()) {
      logger.trace(getName() + ": get resource delegating to parent for for " + name);
    }
    return getParent().getResourceAsStream(name);
  }
  
  public boolean isManagedClass(String name)
  {
    IMap<String, ClassDefinition> classes = getClasses();
    return classes.containsKey(name);
  }
  
  public boolean isSystemClass(String name)
  {
    IMap<String, ClassDefinition> classes = getClasses();
    return !classes.containsKey(name);
  }
  
  public boolean isGeneratedClass(String name)
  {
    IMap<String, BundleDefinition> bundles = getBundles();
    IMap<String, String> classToBundleResolver = getClassToBundleResolver();
    if (isManagedClass(name))
    {
      String bundleName = (String)classToBundleResolver.get(name);
      if (bundleName != null)
      {
        BundleDefinition def = (BundleDefinition)bundles.get(bundleName);
        if (def != null) {
          return def.getType() != BundleDefinition.Type.jar;
        }
      }
    }
    return false;
  }
  
  public boolean isExistingClass(String name)
  {
    try
    {
      Class<?> clazz = loadClass(name);
      if (logger.isTraceEnabled()) {
        logger.trace("Loaded class " + clazz + " from name " + name + " using loader " + clazz.getClassLoader());
      }
      return true;
    }
    catch (ClassNotFoundException e)
    {
      if (logger.isTraceEnabled()) {
        logger.trace("Could not load class " + name + " using loader " + this);
      }
    }
    return false;
  }
  
  public byte[] getClassBytes(String name)
  {
    if (logger.isTraceEnabled()) {
      logger.trace(getName() + ": get class bytes " + name);
    }
    IMap<String, ClassDefinition> classes = getClasses();
    ClassDefinition def = (ClassDefinition)classes.get(name);
    if (def != null)
    {
      if (logger.isTraceEnabled()) {
        logger.trace(getName() + ": get class bytes returning bytes for " + name);
      }
      return def.getByteCode();
    }
    if (logger.isTraceEnabled()) {
      logger.trace(getName() + ": get class bytes not found for " + name);
    }
    return null;
  }
  
  private byte[] inputStreamToByteArray(InputStream is)
    throws Exception
  {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    
    byte[] data = new byte[16384];
    int nRead;
    while ((nRead = is.read(data, 0, data.length)) != -1) {
      buffer.write(data, 0, nRead);
    }
    buffer.flush();
    
    return buffer.toByteArray();
  }
  
  private String getClassName(String path, byte[] bytecode)
  {
    try
    {
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytecode));
      dis.readLong();
      int cpcnt = (dis.readShort() & 0xFFFF) - 1;
      int[] classArray = new int[cpcnt];
      String[] strings = new String[cpcnt];
      for (int i = 0; i < cpcnt; i++)
      {
        int t = dis.read();
        if (t == 7)
        {
          classArray[i] = (dis.readShort() & 0xFFFF);
        }
        else if (t == 1)
        {
          strings[i] = dis.readUTF();
        }
        else if ((t == 5) || (t == 6))
        {
          dis.readLong();i++;
        }
        else if (t == 8)
        {
          dis.readShort();
        }
        else
        {
          dis.readInt();
        }
      }
      dis.readShort();
      return strings[(classArray[((dis.readShort() & 0xFFFF) - 1)] - 1)].replace('/', '.');
    }
    catch (Exception e)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("Problem reading classname from: " + path);
      }
    }
    return null;
  }
  
  public int getClassId(String className)
  {
    IMap<String, Integer> idMap = HazelcastSingleton.get().getMap("#classIds");
    idMap.lock("#allIds");
    
    ILock lock;

    Integer id = (Integer)idMap.get(className);
    if (id == null)
    {
      id = (Integer)idMap.get("#allIds");
      if (id == null)
      {
        lock = HazelcastSingleton.get().getLock("classIdsLock");
        lock.lock();
        try
        {
          id = (Integer)idMap.get("#allIds");
          if (id == null)
          {
            id = Integer.valueOf(350);
            idMap.put("#allIds", id);
          }
        }
        finally
        {
          lock.unlock();
        }
      }
      Integer localInteger1 = id = Integer.valueOf(id.intValue() + 1);
      idMap.put("#allIds", id);
      
      idMap.put(className, id);
    }
    idMap.unlock("#allIds");
    return id.intValue();
  }
  
  public void setClassId(String className, int classId)
  {
    IMap<String, Integer> idMap = HazelcastSingleton.get().getMap("#classIds");
    idMap.lock("#allIds");
    idMap.put(className, Integer.valueOf(classId));
    
    Integer curMaxId = (Integer)idMap.get("#allIds");
    if (curMaxId == null) {
      curMaxId = Integer.valueOf(350);
    }
    if (curMaxId.intValue() <= classId) {
      idMap.put("#allIds", Integer.valueOf(classId));
    }
    idMap.unlock("#allIds");
  }
  
  public void setMaxClassId(Integer maxId)
  {
    IMap<String, Integer> idMap = HazelcastSingleton.get().getMap("#classIds");
    idMap.lock("#allIds");
    Integer currentMaxId = (Integer)idMap.get("#allIds");
    if (currentMaxId == null) {
      idMap.put("#allIds", maxId);
    } else {
      logger.warn("Unexpected warn statement while setting max class id, current max id: " + currentMaxId + ", max id: " + maxId);
    }
    idMap.unlock("#allIds");
  }
  
  public void addJar(String appName, String path, String name)
    throws Exception
  {
    IMap<String, BundleDefinition> bundles = getBundles();
    IMap<String, ClassDefinition> classes = getClasses();
    IMap<String, String> classToBundleResolver = getClassToBundleResolver();
    String uri = getBundleUri(appName, BundleDefinition.Type.jar, name);
    BundleDefinition def = (BundleDefinition)bundles.get(uri);
    if (def != null) {
      throw new IllegalArgumentException("Can't add " + uri + " it already exists");
    }
    def = new BundleDefinition(BundleDefinition.Type.jar, appName, path, name);
    uri = def.getUri();
    JarFile jar = new JarFile(path);Throwable localThrowable2 = null;
    try
    {
      Enumeration<JarEntry> entries = jar.entries();
      while (entries.hasMoreElements())
      {
        JarEntry entry = (JarEntry)entries.nextElement();
        if (!entry.isDirectory())
        {
          InputStream is = jar.getInputStream(entry);
          
          byte[] bytecode = inputStreamToByteArray(is);
          String className = getClassName(entry.getName(), bytecode);
          if (className != null)
          {
            ClassDefinition classDef = getClassDefinition(className);
            if (classDef != null) {
              throw new Exception("Class " + className + " is already defined for bundle " + classDef.getBundleName());
            }
            classDef = new ClassDefinition(uri, className, -1, bytecode);
            classes.put(className, classDef);
            classToBundleResolver.put(className, uri);
            def.addClassName(className);
          }
        }
      }
    }
    catch (Throwable localThrowable1)
    {
      localThrowable2 = localThrowable1;throw localThrowable1;
    }
    finally
    {
      if (jar != null) {
        if (localThrowable2 != null) {
          try
          {
            jar.close();
          }
          catch (Throwable x2)
          {
            localThrowable2.addSuppressed(x2);
          }
        } else {
          jar.close();
        }
      }
    }
    bundles.put(uri, def);
  }
  
  public String createIfNotExistsBundleDefinition(String appName, BundleDefinition.Type type, String name)
  {
    IMap<String, BundleDefinition> bundles = getBundles();
    String uri = BundleDefinition.getUri(appName, type, name);
    BundleDefinition def = (BundleDefinition)bundles.get(uri);
    if (def == null)
    {
      def = new BundleDefinition(type, appName, name);
      bundles.put(uri, def);
    }
    return def.getUri();
  }
  
  public String getBundleUri(String appName, BundleDefinition.Type type, String name)
  {
    return BundleDefinition.getUri(appName, type, name);
  }
  
  public boolean isExistingBundle(String uri)
  {
    IMap<String, BundleDefinition> bundles = getBundles();
    return bundles.get(uri) != null;
  }
  
  public String addBundleDefinition(String appName, BundleDefinition.Type type, String name)
  {
    IMap<String, BundleDefinition> bundles = getBundles();
    String uri = getBundleUri(appName, type, name);
    BundleDefinition def = (BundleDefinition)bundles.get(uri);
    if (def != null) {
      throw new IllegalArgumentException("Can't add " + uri + " it already exists");
    }
    def = new BundleDefinition(type, appName, name);
    
    bundles.put(uri, def);
    if (logger.isTraceEnabled()) {
      logger.trace("Bundle created: " + uri);
    }
    return def.getUri();
  }
  
  public void addBundleClass(String bundleUri, String className, byte[] bytecode, boolean register)
  {
    IMap<String, BundleDefinition> bundles = getBundles();
    IMap<String, ClassDefinition> classes = getClasses();
    IMap<String, String> classToBundleResolver = getClassToBundleResolver();
    BundleDefinition def = (BundleDefinition)bundles.get(bundleUri);
    if (def == null) {
      throw new IllegalArgumentException("Can't add class " + className + " to non existant " + bundleUri);
    }
    ClassDefinition classDef = getClassDefinition(className);
    if (classDef != null) {
      throw new IllegalArgumentException("Can't add class " + className + " to " + bundleUri + " it already exists in " + classDef.getBundleName());
    }
    classDef = new ClassDefinition(bundleUri, className, register ? getClassId(className) : -1, bytecode);
    classes.put(className, classDef);
    classToBundleResolver.put(className, bundleUri);
    def.addClassName(className);
    if (logger.isTraceEnabled()) {
      logger.trace("Bundle " + bundleUri + " class added: " + className);
    }
    bundles.put(bundleUri, def);
  }
  
  private BundleLoader getBundleLoader(String uri)
  {
    BundleLoader loader;
    synchronized (this.bundleLoaders)
    {
      IMap<String, BundleDefinition> bundles = getBundles();
      BundleDefinition def = (BundleDefinition)bundles.get(uri);
      if (def == null) {
        throw new IllegalArgumentException("Can't get loader for " + uri + " it does not exists");
      }
      loader = (BundleLoader)this.bundleLoaders.get(uri);
      if (loader == null)
      {
        loader = new BundleLoader(this, this.isClient);
        loader.setName(uri);
        this.bundleLoaders.put(uri, loader);
      }
    }
    return loader;
  }
  
  public ClassPool getBundlePool(String uri)
  {
    BundleLoader loader = getBundleLoader(uri);
    return loader.getPool();
  }
  
  public BundleLoader getBundleLoaderForClass(String name)
  {
    IMap<String, String> classToBundleResolver = getClassToBundleResolver();
    if (classToBundleResolver.containsKey(name))
    {
      String uri = (String)classToBundleResolver.get(name);
      return getBundleLoader(uri);
    }
    return null;
  }
  
  public void removeJar(String appName, String name)
    throws Exception
  {
    String uri = getBundleUri(appName, BundleDefinition.Type.jar, name);
    removeBundle(uri);
  }
  
  public void removeBundle(String uri)
  {
    synchronized (this.bundleLoaders)
    {
      IMap<String, BundleDefinition> bundles = getBundles();
      BundleDefinition def = (BundleDefinition)bundles.get(uri);
      if (def != null) {
        bundles.remove(uri);
      }
    }
  }
  
  public void removeBundle(String appName, BundleDefinition.Type type, String name)
  {
    String uri = getBundleUri(appName, type, name);
    removeBundle(uri);
  }
  
  private String getObject(CtClass type, String value)
  {
    if (CtClass.booleanType.equals(type)) {
      return " new Boolean(" + value + ")";
    }
    if (CtClass.byteType.equals(type)) {
      return " new Byte(" + value + ")";
    }
    if (CtClass.charType.equals(type)) {
      return " new Character(" + value + ")";
    }
    if (CtClass.shortType.equals(type)) {
      return " new Short(" + value + ")";
    }
    if (CtClass.intType.equals(type)) {
      return " new Integer(" + value + ")";
    }
    if (CtClass.longType.equals(type)) {
      return " new Long(" + value + ")";
    }
    if (CtClass.floatType.equals(type)) {
      return " new Float(" + value + ")";
    }
    if (CtClass.doubleType.equals(type)) {
      return " new Double(" + value + ")";
    }
    return value;
  }
  
  private String getCast(CtClass type, String value)
  {
    if (CtClass.booleanType.equals(type)) {
      return "((Boolean) " + value + ").booleanValue()";
    }
    if (CtClass.byteType.equals(type)) {
      return "((Byte) " + value + ").byteValue()";
    }
    if (CtClass.charType.equals(type)) {
      return "((Character) " + value + ").charValue()";
    }
    if (CtClass.shortType.equals(type)) {
      return "((Short) " + value + ").shortValue()";
    }
    if (CtClass.intType.equals(type)) {
      return "((Integer) " + value + ").intValue()";
    }
    if (CtClass.longType.equals(type)) {
      return "((Long) " + value + ").longValue()";
    }
    if (CtClass.floatType.equals(type)) {
      return "((Float) " + value + ").floatValue()";
    }
    if (CtClass.doubleType.equals(type)) {
      return "((Double) " + value + ").doubleValue()";
    }
    if ("org.joda.time.DateTime".equals(type.getName())) {
      return "(" + value + " instanceof org.joda.time.DateTime ?" + " (org.joda.time.DateTime) " + value + " :" + " new org.joda.time.DateTime(" + value + "))";
    }
    return "(" + type.getName() + ") " + value;
  }
  
  private String getMethodName(String type)
  {
    if ("java.lang.String".equals(type)) {
      return "val.toString()";
    }
    if ("char".equals(type)) {
      return "((Character)val).charValue()";
    }
    if ("short".equals(type)) {
      return "((Short)val).shortValue()";
    }
    if ("int".equals(type)) {
      return "((Integer)val).intValue()";
    }
    if ("double".equals(type)) {
      return "((Double)val).doubleValue()";
    }
    if ("float".equals(type)) {
      return "((Float)val).floatValue()";
    }
    if ("long".equals(type)) {
      return "((Long)val).longValue()";
    }
    if ("byte".equals(type)) {
      return "((Byte)val).byteValue()";
    }
    if ("boolean".equals(type)) {
      return "((Boolean)val).booleanValue()";
    }
    return "(" + type + ") val";
  }
  
  private String getMethodName1(String type, String fieldName)
  {
    if ("java.lang.String".equals(type)) {
      return "(" + type + ")ctx.get(\"" + fieldName + "\")";
    }
    if ("char".equals(type)) {
      return "(" + type + ")ctx.get(\"" + fieldName + "\")";
    }
    if ("short".equals(type)) {
      return "((Short)val).shortValue()";
    }
    if ("int".equals(type)) {
      return "((Integer)val).intValue()";
    }
    if ("double".equals(type)) {
      return "((Double)val).doubleValue()";
    }
    if ("float".equals(type)) {
      return "((Float)val).floatValue()";
    }
    if ("long".equals(type)) {
      return "((Long)val).longValue()";
    }
    if ("byte".equals(type)) {
      return "((Byte)val).byteValue()";
    }
    if ("boolean".equals(type)) {
      return "((Boolean)val).booleanValue()";
    }
    return "(" + type + ") val";
  }
  
  private static String getDataType(String type)
  {
    if ("java.lang.String".equals(type)) {
      return "String";
    }
    if ("java.lang.Boolean".equals(type)) {
      return "Boolean";
    }
    return type.substring(0, 1).toUpperCase() + type.substring(1);
  }
  
  public void removeTypeClass(String appName, String className)
  {
    String uri = getBundleUri(appName, BundleDefinition.Type.type, className);
    removeBundle(uri);
  }
  
  protected String getKryoWrite(String indent, CtClass type, String value)
    throws Exception
  {
    if (CtClass.booleanType.equals(type)) {
      return indent + "output.writeBoolean(" + value + ");\n";
    }
    if (CtClass.byteType.equals(type)) {
      return indent + "output.writeByte(" + value + ");\n";
    }
    if (CtClass.charType.equals(type)) {
      return indent + "output.writeChar(" + value + ");\n";
    }
    if (CtClass.shortType.equals(type)) {
      return indent + "output.writeShort(" + value + ");\n";
    }
    if (CtClass.intType.equals(type)) {
      return indent + "output.writeInt(" + value + ");\n";
    }
    if (CtClass.longType.equals(type)) {
      return indent + "output.writeLong(" + value + ");\n";
    }
    if (CtClass.floatType.equals(type)) {
      return indent + "output.writeFloat(" + value + ");\n";
    }
    if (CtClass.doubleType.equals(type)) {
      return indent + "output.writeDouble(" + value + ");\n";
    }
    if ("java.lang.String".equals(type.getName())) {
      return indent + "output.writeString(" + value + ");\n";
    }
    return indent + "kryo.writeClassAndObject(output, " + value + ");\n";
  }
  
  protected String getKryoRead(String indent, CtClass type, String value)
    throws Exception
  {
    if (CtClass.booleanType.equals(type)) {
      return indent + value + " = input.readBoolean();\n";
    }
    if (CtClass.byteType.equals(type)) {
      return indent + value + " = input.readByte();\n";
    }
    if (CtClass.charType.equals(type)) {
      return indent + value + " = input.readChar();\n";
    }
    if (CtClass.shortType.equals(type)) {
      return indent + value + " = input.readShort();\n";
    }
    if (CtClass.intType.equals(type)) {
      return indent + value + " = input.readInt();\n";
    }
    if (CtClass.longType.equals(type)) {
      return indent + value + " = input.readLong();\n";
    }
    if (CtClass.floatType.equals(type)) {
      return indent + value + " = input.readFloat();\n";
    }
    if (CtClass.doubleType.equals(type)) {
      return indent + value + " = input.readDouble();\n";
    }
    if ("java.lang.String".equals(type.getName())) {
      return indent + value + " = input.readString();\n";
    }
    return indent + value + " = (" + type.getName() + ") kryo.readClassAndObject(input);\n";
  }
  
  public BundleDefinition getTypeBundleDef(String appName, String className)
  {
    IMap<String, BundleDefinition> bundles = getBundles();
    String bundleUri = getBundleUri(appName, BundleDefinition.Type.type, className);
    return (BundleDefinition)bundles.get(bundleUri);
  }
  
  private String makeSetFromWActionMethod(Map<String, String> fields, ClassPool pool)
    throws NotFoundException
  {
    StringBuilder sb = new StringBuilder();
    sb.append("public void convertFromWactionToEvent(long timestamp," + UUID.class.getCanonicalName() + " id, String key,java.util.Map context) {\n");
    
    sb.append("setTimeStamp(timestamp); \n");
    sb.append("setID(id); \n");
    sb.append("setKey(key); \n");
    for (Map.Entry<String, String> field : fields.entrySet())
    {
      String name = (String)field.getKey();
      String type = (String)field.getValue();
      CtClass cl = pool.getCtClass(type);
      sb.append("this." + name + " = " + getCast(cl, new StringBuilder().append("context.get(\"").append(name).append("\")").toString()) + ";\n");
    }
    sb.append("}\n");
    return sb.toString();
  }
  
  public void addTypeClass(String appName, String className, Map<String, String> fields)
    throws Exception
  {
    try
    {
      BundleDefinition def = getTypeBundleDef(appName, className);
      if (def != null) {
        throw new IllegalArgumentException("Class for type " + className + " is already defined");
      }
      String bundleUri = addBundleDefinition(appName, BundleDefinition.Type.type, className);
      BundleLoader bl = getBundleLoader(bundleUri);
      
      ClassPool pool = bl.getPool();
      
      CtClass bclass = pool.makeClass(className);
      bclass.addConstructor(CtNewConstructor.defaultConstructor(bclass));
      CtClass sclass = pool.get(SimpleEvent.class.getCanonicalName());
      CtClass wactionConvertibleInterface = pool.get(WactionConvertible.class.getCanonicalName());
      CtClass[] interfaceList = { wactionConvertibleInterface };
      
      bclass.setSuperclass(sclass);
      bclass.setInterfaces(interfaceList);
      String eventConstructorSource = "public " + bclass.getSimpleName() + "(long timestamp) {\n" + "  super(timestamp);\n" + "  fieldIsSet = new byte[" + (fields.size() / 7 + 1) + "];\n" + "}\n";
      if (logger.isTraceEnabled()) {
        logger.trace("Event constructor is " + eventConstructorSource);
      }
      CtConstructor eventConstructor = CtNewConstructor.make(eventConstructorSource, bclass);
      bclass.addConstructor(eventConstructor);
      CtClass[] fclasses = new CtClass[fields.size()];
      CtClass serClass = pool.get(Serializable.class.getName());
      bclass.addInterface(serClass);
      CtClass kserClass = pool.get(KryoSerializable.class.getName());
      bclass.addInterface(kserClass);
      
      String getPayloadMethod = "public Object[] getPayload() {\n  return new Object[] {\n";
      
      String setPayloadMethod = "public void setPayload(Object[] payload) {\n";
      
      String kserWriteMethod = "public void write(com.esotericsoftware.kryo.Kryo kryo, com.esotericsoftware.kryo.io.Output output) {\n  super.write(kryo, output);\n";
      
      String kserReadMethod = "public void read(com.esotericsoftware.kryo.Kryo kryo, com.esotericsoftware.kryo.io.Input input) {\n  super.read(kryo, input);\n";
      
      String setFromContextMapMethod = "public boolean setFromContextMap(java.util.Map map) {\n";
      
      setFromContextMapMethod = setFromContextMapMethod + "  timeStamp = ((java.lang.Long)map.get(\"timestamp\")).longValue();\n";
      setFromContextMapMethod = setFromContextMapMethod + "  _wa_SimpleEvent_ID =  new com.bloom.uuid.UUID((java.lang.String)map.get(\"uuid\"));\n";
      setFromContextMapMethod = setFromContextMapMethod + "  key = (map.get(\"key\") == null ? null : map.get(\"key\").toString());\n";
      int i = 0;
      for (Map.Entry<String, String> field : fields.entrySet())
      {
        String name = (String)field.getKey();
        String type = (String)field.getValue();
        fclasses[i] = pool.getCtClass(type);
        CtField bfield = new CtField(fclasses[i], name, bclass);
        bfield.setModifiers(1);
        
        bclass.addField(bfield);
        bclass.addMethod(CtNewMethod.getter("get" + name.substring(0, 1).toUpperCase() + name.substring(1), bfield));
        bclass.addMethod(CtNewMethod.setter("set" + name.substring(0, 1).toUpperCase() + name.substring(1), bfield));
        getPayloadMethod = getPayloadMethod + "  " + getObject(fclasses[i], name) + (i == fields.size() - 1 ? "" : ",") + "\n";
        setPayloadMethod = setPayloadMethod + "  " + name + " = " + getCast(fclasses[i], new StringBuilder().append("payload[").append(i).append("]").toString()) + ";\n";
        kserWriteMethod = kserWriteMethod + getKryoWrite("  ", fclasses[i], name);
        kserReadMethod = kserReadMethod + getKryoRead("  ", fclasses[i], name);
        setFromContextMapMethod = setFromContextMapMethod + "  " + name + " = " + getCast(fclasses[i], new StringBuilder().append("map.get(\"context-").append(name).append("\")").toString()) + ";\n";
        i++;
      }
      getPayloadMethod = getPayloadMethod + "  };\n}\n";
      setPayloadMethod = setPayloadMethod + "}\n";
      kserWriteMethod = kserWriteMethod + "}\n";
      kserReadMethod = kserReadMethod + "}\n";
      setFromContextMapMethod = setFromContextMapMethod + "  return true;\n}\n";
      if (logger.isTraceEnabled()) {
        logger.trace("getPayloadMethod: " + getPayloadMethod);
      }
      CtMethod m = CtMethod.make(getPayloadMethod, bclass);
      bclass.addMethod(m);
      if (logger.isTraceEnabled()) {
        logger.trace(setPayloadMethod);
      }
      CtMethod m2 = CtMethod.make(setPayloadMethod, bclass);
      bclass.addMethod(m2);
      if (logger.isTraceEnabled()) {
        logger.trace(kserWriteMethod);
      }
      CtMethod m3 = CtMethod.make(kserWriteMethod, bclass);
      bclass.addMethod(m3);
      if (logger.isTraceEnabled()) {
        logger.trace(kserReadMethod);
      }
      CtMethod m4 = CtMethod.make(kserReadMethod, bclass);
      bclass.addMethod(m4);
      if (logger.isTraceEnabled()) {
        logger.trace(setFromContextMapMethod);
      }
      CtMethod m5 = CtMethod.make(setFromContextMapMethod, bclass);
      bclass.addMethod(m5);
      
      String setFromWActionMethod = makeSetFromWActionMethod(fields, pool);
      if (logger.isTraceEnabled()) {
        logger.trace(setFromWActionMethod);
      }
      CtMethod m6 = CtMethod.make(setFromWActionMethod, bclass);
      bclass.addMethod(m6);
      
      CtField mapper = CtField.make("public static com.fasterxml.jackson.databind.ObjectMapper  mapper = com.bloom.event.ObjectMapperFactory.newInstance();", bclass);
      
      bclass.addField(mapper);
      
      String fromJSONsource = "public Object fromJSON(String json) {  return mapper.readValue(json, this.getClass());}";
      
      CtMethod fromJSON = CtMethod.make(fromJSONsource, bclass);
      bclass.addMethod(fromJSON);
      
      String toJSONsource = "public String toJSON() {  return mapper.writeValueAsString(this);}";
      
      CtMethod toJSON = CtMethod.make(toJSONsource, bclass);
      bclass.addMethod(toJSON);
      
      String toStringsource = "public String toString() {  return toJSON();}";
      
      CtMethod toString = CtMethod.make(toStringsource, bclass);
      bclass.addMethod(toString);
      if (logger.isDebugEnabled()) {
        logger.debug("bean class generated. name = " + bclass.getName() + ", toString = " + bclass.toString());
      }
      byte[] bytecode = bclass.toBytecode();
      
      String report = CompilerUtils.verifyBytecode(bytecode, this);
      if (report != null) {
        throw new Error("Internal error: invalid bytecode\n" + report);
      }
      addBundleClass(bundleUri, className, bytecode, true);
      if (logger.isDebugEnabled()) {
        logger.debug("Added to bundle className :" + className);
      }
    }
    catch (Exception e)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("Problem creating class for type " + className, e);
      }
      throw new Exception("Could not instantiate class for type " + className + ": " + e, e);
    }
  }
  
  public void addWactionClass(String className, MetaInfo.Type metaInfo)
    throws Exception
  {
    IMap<String, BundleDefinition> bundles = getBundles();
    if (metaInfo == null) {
      throw new ClassNotFoundException("metaInfo is null");
    }
    if (logger.isTraceEnabled()) {
      logger.trace("trying to create a run time Waction sub class : " + metaInfo.className);
    }
    String appName = metaInfo.nsName;
    
    String bundleUri = getBundleUri(appName, BundleDefinition.Type.waction, className);
    BundleDefinition def = (BundleDefinition)bundles.get(bundleUri);
    if (def != null) {
      throw new IllegalArgumentException("Class for type " + className + " is already defined");
    }
    bundleUri = addBundleDefinition(appName, BundleDefinition.Type.waction, className);
    
    BundleLoader bl = getBundleLoader(bundleUri);
    
    ClassPool pool = bl.getPool();
    
    CtClass bclass = pool.makeClass(className);
    
    CtClass sclass = pool.get(Waction.class.getName());
    SerialVersionUID.setSerialVersionUID(bclass);
    bclass.setSuperclass(sclass);
    
    int noOfFields = metaInfo.fields.size();
    CtClass[] fclasses = new CtClass[noOfFields];
    
    String setContextSrc = "public void setContext(java.util.Map ctx) {\n";
    if (logger.isTraceEnabled()) {
      setContextSrc = setContextSrc + "\tSystem.out.println(\"setcontext of runtime is called.\" + ctx.toString() );\n";
    }
    setContextSrc = setContextSrc + "\tObject val = null;\n";
    
    Map<String, String> fields = metaInfo.fields;
    int i = 0;
    for (Map.Entry<String, String> field : fields.entrySet())
    {
      String fieldName = (String)field.getKey();
      String fieldType = (String)field.getValue();
      
      fclasses[i] = pool.getCtClass(fieldType);
      CtField bfield = new CtField(fclasses[i], fieldName, bclass);
      bfield.setModifiers(1);
      bclass.addField(bfield);
      if (logger.isTraceEnabled()) {
        logger.trace("field name : " + fieldName + ", field.type : " + fieldType);
      }
      bclass.addMethod(CtNewMethod.getter("get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1), bfield));
      
      String setterMethodSrc = "public void set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "(" + fieldType + " " + fieldName + ") {\n";
      setterMethodSrc = setterMethodSrc + "\tthis." + fieldName + " = " + fieldName + ";\n";
      setterMethodSrc = setterMethodSrc + "\tObject val = getWrappedValue(" + fieldName + ");\n";
      setterMethodSrc = setterMethodSrc + "\tcontext.map.put(\"" + fieldName + "\", val);\n";
      if (logger.isTraceEnabled()) {
        setterMethodSrc = setterMethodSrc + "\tSystem.out.println(\"added field '" + fieldName + "' with value = \" + val  );\n";
      }
      setterMethodSrc = setterMethodSrc + "}\n";
      if (logger.isDebugEnabled()) {
        logger.debug("setterMethodSrc for field : " + fieldName + " \n" + setterMethodSrc);
      }
      CtMethod m = CtMethod.make(setterMethodSrc, bclass);
      bclass.addMethod(m);
      
      setContextSrc = setContextSrc + "\tval  = ctx.get(\"" + fieldName + "\");\n";
      setContextSrc = setContextSrc + "\tif(val != null) {\n";
      setContextSrc = setContextSrc + "\t\tset" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "(" + getMethodName(fieldType) + ");\n";
      setContextSrc = setContextSrc + "\t}\n";
      setContextSrc = setContextSrc + "\tval = null;\n";
      if (fieldType.equalsIgnoreCase("org.joda.time.DateTime"))
      {
        String jodaHibGetMth = "public long get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "AsLong( ) {\n";
        jodaHibGetMth = jodaHibGetMth + "\tif(" + fieldName + " == null) return 0L;\n";
        jodaHibGetMth = jodaHibGetMth + "\treturn this." + fieldName + ".getMillis();\n";
        jodaHibGetMth = jodaHibGetMth + "}";
        CtMethod jmget = CtMethod.make(jodaHibGetMth, bclass);
        bclass.addMethod(jmget);
        
        String jodaHibSetMth = "public void set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "AsLong(long val) {\n";
        jodaHibSetMth = jodaHibSetMth + "\tthis.set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "(new org.joda.time.DateTime(val));\n";
        jodaHibSetMth = jodaHibSetMth + "}";
        CtMethod jmset = CtMethod.make(jodaHibSetMth, bclass);
        bclass.addMethod(jmset);
      }
      i++;
    }
    setContextSrc = setContextSrc + "}";
    CtMethod setCntxMeth = CtMethod.make(setContextSrc, bclass);
    bclass.addMethod(setCntxMeth);
    if (logger.isDebugEnabled()) {
      logger.debug("bean class generated. name = " + bclass.getName() + ", toString = " + bclass.toString());
    }
    byte[] bytecode = bclass.toBytecode();
    
    String report = CompilerUtils.verifyBytecode(bytecode, this);
    if (report != null) {
      throw new Error("Internal error: invalid bytecode\n" + report);
    }
    addBundleClass(bundleUri, className, bytecode, true);
  }
  
  public void addWactionContextClassNoEventType(MetaInfo.Type metaInfo)
    throws Exception
  {
    IMap<String, BundleDefinition> bundles = getBundles();
    if (metaInfo == null) {
      throw new ClassNotFoundException("metaInfo is null");
    }
    if (logger.isTraceEnabled()) {
      logger.trace("inside instantiateWactionContextClass : " + metaInfo.className);
    }
    String className = metaInfo.className + "_wactionContext";
    String appName = metaInfo.nsName;
    
    String bundleUri = getBundleUri(appName, BundleDefinition.Type.context, className);
    BundleDefinition def = (BundleDefinition)bundles.get(bundleUri);
    if (def != null) {
      throw new IllegalArgumentException("Class for type " + className + " is already defined");
    }
    bundleUri = addBundleDefinition(appName, BundleDefinition.Type.context, className);
    BundleLoader bl = getBundleLoader(bundleUri);
    
    ClassPool pool = bl.getPool();
    
    CtClass bclass = pool.makeClass(className);
    bclass.addConstructor(CtNewConstructor.defaultConstructor(bclass));
    CtClass sclass = pool.get(WactionContext.class.getName());
    SerialVersionUID.setSerialVersionUID(bclass);
    bclass.setSuperclass(sclass);
    int noOfFields = metaInfo.fields.size();
    CtClass[] fclasses = new CtClass[noOfFields];
    
    Map<String, String> fields = metaInfo.fields;
    
    String putMethodSrc = "public Object put(String key, Object val) {\n";
    int i = 0;
    for (Map.Entry<String, String> field : fields.entrySet())
    {
      String fieldName = (String)field.getKey();
      String fieldType = (String)field.getValue();
      
      fclasses[i] = pool.getCtClass(fieldType);
      CtField bfield = new CtField(fclasses[i], fieldName, bclass);
      bfield.setModifiers(1);
      bclass.addField(bfield);
      if (logger.isTraceEnabled()) {
        logger.trace("field name : " + fieldName + ", field.type : " + fieldType);
      }
      bclass.addMethod(CtNewMethod.getter("get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1), bfield));
      
      String setterMethodSrc = "public void set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "(" + fieldType + " " + fieldName + ") {\n";
      setterMethodSrc = setterMethodSrc + "\tthis." + fieldName + " = " + fieldName + ";\n";
      setterMethodSrc = setterMethodSrc + "\tObject val = getWrappedValue(" + fieldName + ");\n";
      setterMethodSrc = setterMethodSrc + "\tmap.put(\"" + fieldName + "\", val);\n";
      setterMethodSrc = setterMethodSrc + "}\n";
      if (logger.isTraceEnabled()) {
        logger.trace("setterMethodSrc for field : " + fieldName + " \n" + setterMethodSrc);
      }
      CtMethod m = CtMethod.make(setterMethodSrc, bclass);
      bclass.addMethod(m);
      
      putMethodSrc = putMethodSrc + "\tif(key.equals(\"" + fieldName + "\")){\n";
      putMethodSrc = putMethodSrc + "\t\tset" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "(" + getMethodName(fieldType) + ");\n";
      putMethodSrc = putMethodSrc + "\t\treturn map.put(\"" + fieldName + "\",val);\n";
      putMethodSrc = putMethodSrc + "\t}\n";
      
      i++;
    }
    String fieldName = "jsonData";
    String fieldType = "java.lang.String";
    CtClass jsondataClass = pool.getCtClass(fieldType);
    CtField jsondataField = new CtField(jsondataClass, fieldName, bclass);
    jsondataField.setModifiers(1);
    bclass.addField(jsondataField);
    
    bclass.addMethod(CtNewMethod.getter("get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1), jsondataField));
    
    String setterMethodSrc = "public void set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "(" + fieldType + " " + fieldName + ") {\n";
    setterMethodSrc = setterMethodSrc + "\tthis." + fieldName + " = " + fieldName + ";\n";
    setterMethodSrc = setterMethodSrc + "\tObject val = getWrappedValue(" + fieldName + ");\n";
    setterMethodSrc = setterMethodSrc + "\tmap.put(\"" + fieldName + "\", val);\n";
    setterMethodSrc = setterMethodSrc + "}\n";
    putMethodSrc = putMethodSrc + "\tif(key.equals(\"" + fieldName + "\")){\n";
    putMethodSrc = putMethodSrc + "\t\tset" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "(" + getMethodName(fieldType) + ");\n";
    putMethodSrc = putMethodSrc + "\t\treturn map.put(\"" + fieldName + "\",val);\n";
    putMethodSrc = putMethodSrc + "\t}\n";
    
    CtMethod m = CtMethod.make(setterMethodSrc, bclass);
    bclass.addMethod(m);
    
    putMethodSrc = putMethodSrc + "\treturn null;\n";
    putMethodSrc = putMethodSrc + "}\n";
    if (logger.isTraceEnabled()) {
      logger.trace("putMethodSrc : \n" + putMethodSrc);
    }
    CtMethod putm = CtMethod.make(putMethodSrc, bclass);
    bclass.addMethod(putm);
    if (logger.isTraceEnabled()) {
      logger.trace("bean class generated. name = " + bclass.getName() + ", toString = " + bclass.toString());
    }
    byte[] bytecode = bclass.toBytecode();
    addBundleClass(bundleUri, className, bytecode, true);
  }
  
  public void addWactionContextClass(MetaInfo.Type metaInfo)
    throws Exception
  {
    IMap<String, BundleDefinition> bundles = getBundles();
    if (metaInfo == null) {
      throw new ClassNotFoundException("metaInfo is null");
    }
    if (logger.isTraceEnabled()) {
      logger.trace("inside instantiateWactionContextClass : " + metaInfo.className);
    }
    String className = metaInfo.className + "_wactionContext";
    String appName = metaInfo.nsName;
    
    String bundleUri = getBundleUri(appName, BundleDefinition.Type.context, className);
    BundleDefinition def = (BundleDefinition)bundles.get(bundleUri);
    if (def != null) {
      throw new IllegalArgumentException("Class for type " + className + " is already defined");
    }
    bundleUri = addBundleDefinition(appName, BundleDefinition.Type.context, className);
    BundleLoader bl = getBundleLoader(bundleUri);
    
    ClassPool pool = bl.getPool();
    
    CtClass bclass = pool.makeClass(className);
    bclass.addConstructor(CtNewConstructor.defaultConstructor(bclass));
    CtClass sclass = pool.get(WactionContext.class.getName());
    SerialVersionUID.setSerialVersionUID(bclass);
    bclass.setSuperclass(sclass);
    int noOfFields = metaInfo.fields.size();
    CtClass[] fclasses = new CtClass[noOfFields];
    
    Map<String, String> fields = metaInfo.fields;
    
    String putMethodSrc = "public Object put(String key, Object val) {\n";
    int i = 0;
    for (Map.Entry<String, String> field : fields.entrySet())
    {
      String fieldName = (String)field.getKey();
      String fieldType = (String)field.getValue();
      
      fclasses[i] = pool.getCtClass(fieldType);
      CtField bfield = new CtField(fclasses[i], fieldName, bclass);
      bfield.setModifiers(1);
      bclass.addField(bfield);
      if (logger.isTraceEnabled()) {
        logger.trace("field name : " + fieldName + ", field.type : " + fieldType);
      }
      bclass.addMethod(CtNewMethod.getter("get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1), bfield));
      
      String setterMethodSrc = "public void set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "(" + fieldType + " " + fieldName + ") {\n";
      setterMethodSrc = setterMethodSrc + "\tthis." + fieldName + " = " + fieldName + ";\n";
      setterMethodSrc = setterMethodSrc + "\tObject val = getWrappedValue(" + fieldName + ");\n";
      setterMethodSrc = setterMethodSrc + "\tmap.put(\"" + fieldName + "\", val);\n";
      setterMethodSrc = setterMethodSrc + "}\n";
      if (logger.isTraceEnabled()) {
        logger.trace("setterMethodSrc for field : " + fieldName + " \n" + setterMethodSrc);
      }
      CtMethod m = CtMethod.make(setterMethodSrc, bclass);
      bclass.addMethod(m);
      
      putMethodSrc = putMethodSrc + "\tif(key.equals(\"" + fieldName + "\")){\n";
      putMethodSrc = putMethodSrc + "\t\tset" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "(" + getMethodName(fieldType) + ");\n";
      putMethodSrc = putMethodSrc + "\t\treturn map.put(\"" + fieldName + "\",val);\n";
      putMethodSrc = putMethodSrc + "\t}\n";
      
      i++;
    }
    putMethodSrc = putMethodSrc + "\treturn null;\n";
    putMethodSrc = putMethodSrc + "}\n";
    if (logger.isTraceEnabled()) {
      logger.trace("putMethodSrc : \n" + putMethodSrc);
    }
    CtMethod putm = CtMethod.make(putMethodSrc, bclass);
    bclass.addMethod(putm);
    if (logger.isTraceEnabled()) {
      logger.trace("bean class generated. name = " + bclass.getName() + ", toString = " + bclass.toString());
    }
    byte[] bytecode = bclass.toBytecode();
    addBundleClass(bundleUri, className, bytecode, true);
  }
  
  public void removeAll(String appName)
    throws Exception
  {
    IMap<String, BundleDefinition> bundles = getBundles();
    Iterator<String> it = bundles.keySet().iterator();
    while (it.hasNext())
    {
      String key = (String)it.next();
      if (key.startsWith(appName + ":")) {
        it.remove();
      }
    }
  }
  
  public void listAllBundles()
  {
    IMap<String, BundleDefinition> bundles = getBundles();
    IMap<String, ClassDefinition> classes = getClasses();
    for (String bundleName : bundles.keySet())
    {
      System.out.println("Bundle: " + bundleName);
      BundleDefinition def = (BundleDefinition)bundles.get(bundleName);
      for (String className : def.getClassNames())
      {
        System.out.println("  Class: " + className);
        ClassDefinition cdef = (ClassDefinition)classes.get(className);
        System.out.println("    Code Size: " + cdef.getByteCode().length);
      }
    }
  }
  
  public void listClassAndId()
  {
    IMap<String, Integer> idMap = HazelcastSingleton.get().getMap("#classIds");
    for (String className : idMap.keySet()) {
      System.out.println(className + ": " + idMap.get(className));
    }
  }
  
  public static final void main(String[] args)
  {
    System.out.println(Double.TYPE.getCanonicalName() + " " + Double.TYPE.getName() + " " + Double.TYPE.getSimpleName());
    
    String[] names = { "org.joda.time.DateTime", "[Lorg.joda.time.DateTime;", "org.joda.time.DateTime[]", "[[Lorg.joda.time.DateTime;", "org.joda.time.DateTime[][]", "double", "[D", "double[]", "[[D", "double[][]" };
    for (String name : names)
    {
      try
      {
        Class<?> c = get().loadClass(name);
        System.out.println("Classloader loaded class: " + c.getName() + " from " + name);
      }
      catch (ClassNotFoundException e)
      {
        System.err.println("Classloader could not load class: " + name);
      }
      try
      {
        Class<?> c = Class.forName(name);
        System.out.println("Forname loaded class: " + c.getName() + " from " + name);
      }
      catch (ClassNotFoundException e)
      {
        System.err.println("Forname could not load class: " + name);
      }
    }
  }
}
