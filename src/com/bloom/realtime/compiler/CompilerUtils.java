package com.bloom.runtime.compiler;

import com.bloom.classloading.WALoader;
import com.bloom.runtime.exceptions.AmbiguousSignature;
import com.bloom.runtime.exceptions.SignatureNotFound;
import com.bloom.runtime.utils.Factory;
import com.bloom.runtime.utils.NamePolicy;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import javassist.CtMethod;
import javassist.Modifier;
import javassist.bytecode.InstructionPrinter;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileManager;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.TraceClassVisitor;

public class CompilerUtils
{
  private static final String[] javaKeywords;
  private static Logger logger;
  private static final Map<Class<?>, Integer> primitiveTypeMap;
  private static final Map<Class<?>, Integer> boxingTypeMap;
  public static final int VOID = 1;
  public static final int BOOLEAN = 2;
  public static final int BYTE = 3;
  public static final int CHAR = 4;
  public static final int SHORT = 5;
  public static final int INTEGER = 6;
  public static final int LONG = 7;
  public static final int FLOAT = 8;
  public static final int DOUBLE = 9;
  public static final int MAXTYPE = 10;
  private static final boolean[][] wideningMap;
  
  public static boolean isJavaKeyword(String s)
  {
    Arrays.binarySearch(javaKeywords, s, new Comparator()
    {
      public int compare(String o1, String o2)
      {
        return o1.compareTo(o2);
      }
    }) >= 0;
  }
  
  public static String capitalize(String s)
  {
    return s.substring(0, 1).toUpperCase() + s.substring(1);
  }
  
  public static class NullType
    implements Comparable<NullType>
  {
    public int compareTo(NullType o)
    {
      return 0;
    }
  }
  
  public static class ParamTypeUndefined
    implements Comparable<ParamTypeUndefined>
  {
    public int compareTo(ParamTypeUndefined o)
    {
      return 0;
    }
  }
  
  private static Class<?>[] getPrimitiveTypes()
  {
    Class<?>[] primitiveTypes = { Void.TYPE, Boolean.TYPE, Character.TYPE, Byte.TYPE, Short.TYPE, Integer.TYPE, Long.TYPE, Float.TYPE, Double.TYPE };
    
    return primitiveTypes;
  }
  
  private static Class<?>[] getBoxingTypes()
  {
    Class<?>[] boxingTypes = { Void.class, Boolean.class, Character.class, Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class };
    
    return boxingTypes;
  }
  
  static
  {
    javaKeywords = new String[] { "abstract", "assert", "boolean", "break", "byte", "case", "catch", "char", "class", "const", "continue", "default", "do", "double", "else", "enum", "extends", "final", "finally", "float", "for", "goto", "if", "implements", "import", "instanceof", "int", "interface", "long", "native", "new", "package", "private", "protected", "public", "return", "short", "static", "strictfp", "super", "switch", "synchronized", "this", "throw", "throws", "transient", "try", "void", "volatile", "while" };
    
    logger = Logger.getLogger(CompilerUtils.class);
    
    primitiveTypeMap = Factory.makeMap();
    boxingTypeMap = Factory.makeMap();
    
    int index = 0;
    for (Class<?> c : getPrimitiveTypes()) {
      primitiveTypeMap.put(c, Integer.valueOf(++index));
    }
    index = 0;
    for (Class<?> c : getBoxingTypes()) {
      boxingTypeMap.put(c, Integer.valueOf(++index));
    }
    wideningMap = new boolean[10][10];
    
    wideningMap[2][2] = 1;
    wideningMap[3][5] = 1;
    for (int t = 3; t < 10; t++)
    {
      wideningMap[t][t] = 1;
      for (int t2 = (t == 3) || (t == 4) ? 6 : t + 1; t2 < 10; t2++) {
        wideningMap[t][t2] = 1;
      }
    }
  }
  
  public static boolean canBeWidened(int fromTypeID, int toTypeID)
  {
    return wideningMap[fromTypeID][toTypeID];
  }
  
  public static int getPrimitiveOrBoxingTypeID(Class<?> type)
  {
    int t = getPrimitiveTypeID(type);
    return t != 0 ? t : getBoxingTypeID(type);
  }
  
  public static int getPrimitiveTypeID(Class<?> type)
  {
    Integer i = (Integer)primitiveTypeMap.get(type);
    return i == null ? 0 : i.intValue();
  }
  
  public static int getBoxingTypeID(Class<?> type)
  {
    Integer i = (Integer)boxingTypeMap.get(type);
    return i == null ? 0 : i.intValue();
  }
  
  public static int getBaseTypeID(Class<?> type)
  {
    Integer i = type.isPrimitive() ? (Integer)primitiveTypeMap.get(type) : (Integer)boxingTypeMap.get(type);
    
    return i == null ? 0 : i.intValue();
  }
  
  public static boolean compatible(Class<?> t1, Class<?> t2)
  {
    return (t1.isAssignableFrom(t2)) || (t2.isAssignableFrom(t1));
  }
  
  public static boolean compatiblePrimitive(Class<?> t1, Class<?> t2)
  {
    int pt1 = getPrimitiveTypeID(t1);
    int pt2 = getPrimitiveTypeID(t2);
    return (pt1 == pt2) || ((pt1 >= 3) && (pt2 >= 3));
  }
  
  public static boolean isCastable(Class<?> toType, Class<?> fromType)
  {
    if (isParam(fromType)) {
      return true;
    }
    if (isNull(fromType)) {
      return !toType.isPrimitive();
    }
    if (compatible(toType, fromType)) {
      return true;
    }
    if ((toType.isPrimitive()) && (fromType.isPrimitive())) {
      return compatiblePrimitive(toType, fromType);
    }
    if (fromType.isPrimitive())
    {
      Class<?> t = getUnboxingType(toType);
      if ((t != null) && (compatiblePrimitive(fromType, t))) {
        return true;
      }
      return compatible(toType, getBoxingType(fromType));
    }
    if (toType.isPrimitive())
    {
      Class<?> t = getUnboxingType(fromType);
      if ((t != null) && (compatiblePrimitive(toType, t))) {
        return true;
      }
      return compatible(fromType, getBoxingType(toType));
    }
    Class<?> t1 = getUnboxingType(toType);
    Class<?> t2 = getUnboxingType(fromType);
    if ((t1 != null) && (t2 != null) && (compatiblePrimitive(t1, t2))) {
      return true;
    }
    return false;
  }
  
  public static boolean isConvertibleByWidening(Class<?> formal, Class<?> actual)
  {
    if (isNull(actual)) {
      return !formal.isPrimitive();
    }
    if (formal.isAssignableFrom(actual)) {
      return true;
    }
    if ((formal.isPrimitive()) && (actual.isPrimitive()))
    {
      int a = getPrimitiveTypeID(actual);
      int f = getPrimitiveTypeID(formal);
      return canBeWidened(a, f);
    }
    return false;
  }
  
  public static boolean isConvertibleWithBoxing(Class<?> formal, Class<?> actual)
  {
    if (isParam(actual)) {
      return true;
    }
    if (isNull(actual)) {
      return !formal.isPrimitive();
    }
    if (formal.isAssignableFrom(actual)) {
      return true;
    }
    if (actual.isAssignableFrom(formal)) {
      return true;
    }
    if (formal.isPrimitive())
    {
      int f = getPrimitiveTypeID(formal);
      if (actual.isPrimitive())
      {
        int a = getPrimitiveTypeID(actual);
        return canBeWidened(a, f);
      }
      int a = getBoxingTypeID(actual);
      return canBeWidened(a, f);
    }
    if (actual.isPrimitive()) {
      return formal.isAssignableFrom(getBoxingType(actual));
    }
    return false;
  }
  
  public static Class<?> getPrimitiveType(int typeID)
  {
    switch (typeID)
    {
    case 2: 
      return Boolean.TYPE;
    case 4: 
      return Character.TYPE;
    case 3: 
      return Byte.TYPE;
    case 9: 
      return Double.TYPE;
    case 8: 
      return Float.TYPE;
    case 6: 
      return Integer.TYPE;
    case 7: 
      return Long.TYPE;
    case 5: 
      return Short.TYPE;
    }
    return null;
  }
  
  public static Class<?> getBoxingType(int typeID)
  {
    switch (typeID)
    {
    case 3: 
      return Byte.class;
    case 9: 
      return Double.class;
    case 8: 
      return Float.class;
    case 6: 
      return Integer.class;
    case 7: 
      return Long.class;
    case 5: 
      return Short.class;
    case 2: 
      return Boolean.class;
    case 4: 
      return Character.class;
    }
    return null;
  }
  
  public static Class<?> getBoxingType(Class<?> c)
  {
    int typeID = getPrimitiveTypeID(c);
    return getBoxingType(typeID);
  }
  
  public static Class<?> getUnboxingType(Class<?> c)
  {
    int typeID = getBoxingTypeID(c);
    return getPrimitiveType(typeID);
  }
  
  public static boolean isBoxingType(Class<?> c)
  {
    int typeID = getBoxingTypeID(c);
    return typeID > 1;
  }
  
  private static Class<?> getCommonPrimitiveType(int t1, int t2)
  {
    if (t1 == t2) {
      return getPrimitiveType(t1);
    }
    if ((t1 < 3) || (t2 < 3)) {
      return null;
    }
    if (t1 == 4) {
      t1 = 6;
    } else if (t2 == 4) {
      t2 = 6;
    }
    return getPrimitiveType(t1 > t2 ? t1 : t2);
  }
  
  public static boolean isParam(Class<?> t)
  {
    return ParamTypeUndefined.class.isAssignableFrom(t);
  }
  
  public static boolean isNull(Class<?> t)
  {
    return NullType.class.isAssignableFrom(t);
  }
  
  public static Class<?> getDefaultTypeIfCannotInfer(Class<?> t, Class<?> defaulttype)
  {
    return isParam(t) ? defaulttype : t;
  }
  
  public static Class<?> makeReferenceType(Class<?> c)
  {
    return c.isPrimitive() ? getBoxingType(c) : c;
  }
  
  public static Class<?> getCommonSuperType(Class<?> c1, Class<?> c2)
  {
    if (isParam(c1)) {
      return c2;
    }
    if (isParam(c2)) {
      return c1;
    }
    if (isNull(c1)) {
      return makeReferenceType(c2);
    }
    if (isNull(c2)) {
      return makeReferenceType(c1);
    }
    if (c1.isAssignableFrom(c2)) {
      return c1;
    }
    if (c2.isAssignableFrom(c1)) {
      return c2;
    }
    int t1 = getPrimitiveOrBoxingTypeID(c1);
    if (t1 == 0) {
      return null;
    }
    int t2 = getPrimitiveOrBoxingTypeID(c2);
    if (t1 == 0) {
      return null;
    }
    return getCommonPrimitiveType(t1, t2);
  }
  
  public static boolean isComparable(Class<?> c)
  {
    return (c.isPrimitive()) || (Comparable.class.isAssignableFrom(c));
  }
  
  private static final Map<String, Class<?>> shortcuts = loadJavaLangShortcuts();
  private static final Map<String, Class<?>> primitives = loadJavaLangPrimitiveTypes();
  
  private static Map<String, Class<?>> loadJavaLangShortcuts()
  {
    Class<?> klass = String.class;
    URL location = klass.getResource('/' + klass.getName().replace('.', '/') + ".class");
    String path = location.getPath();
    String file = path.substring(0, path.indexOf("!"));
    URI syslibUri = null;
    try
    {
      syslibUri = new URI(file);
    }
    catch (URISyntaxException e2)
    {
      throw new RuntimeException(e2);
    }
    Pattern pattern = Pattern.compile("^java/lang/\\w+\\.class");
    
    Map<String, Class<?>> retval = Factory.makeNameMap();
    for (Class<?> c : getPrimitiveTypes()) {
      retval.put(c.getName(), c);
    }
    try
    {
      ZipFile zf = new ZipFile(new File(syslibUri));??? = null;
      try
      {
        Enumeration<?> e = zf.entries();
        while (e.hasMoreElements())
        {
          ZipEntry ze = (ZipEntry)e.nextElement();
          String fileName = ze.getName();
          if (pattern.matcher(fileName).matches())
          {
            String classname = fileName.replace('/', '.').replace(".class", "");
            String key = classname.substring(classname.lastIndexOf(".") + 1);
            try
            {
              Class<?> c = Class.forName(classname);
              assert (key.equals(c.getSimpleName()));
              retval.put(key, c);
            }
            catch (ClassNotFoundException e1)
            {
              logger.error("Could not find class loading shortcuts", e1);
            }
          }
        }
      }
      catch (Throwable localThrowable1)
      {
        ??? = localThrowable1;throw localThrowable1;
      }
      finally
      {
        if (zf != null) {
          if (??? != null) {
            try
            {
              zf.close();
            }
            catch (Throwable x2)
            {
              ???.addSuppressed(x2);
            }
          } else {
            zf.close();
          }
        }
      }
    }
    catch (ZipException e)
    {
      throw new RuntimeException(e);
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }
    Class<?> dtClass = DateTime.class;
    retval.put(dtClass.getSimpleName(), dtClass);
    return retval;
  }
  
  private static Map<String, Class<?>> loadJavaLangPrimitiveTypes()
  {
    Map<String, Class<?>> retval = Factory.makeNameMap();
    for (Class<?> c : getPrimitiveTypes()) {
      retval.put(c.getName(), c);
    }
    return retval;
  }
  
  public static Class<?> getClassNameShortcut(String name)
  {
    return (Class)shortcuts.get(name);
  }
  
  public static Class<?> getPrimitiveClassByName(String name)
  {
    return (Class)primitives.get(name);
  }
  
  public static Class<?> toArrayType(Class<?> c)
  {
    return Array.newInstance(c, 0).getClass();
  }
  
  public static boolean isSubclassOf(Class<?> subclass, Class<?> superclass)
  {
    if (superclass.isAssignableFrom(subclass)) {
      return true;
    }
    if ((subclass.isPrimitive()) && (superclass.isPrimitive()))
    {
      int psubclass = getPrimitiveTypeID(subclass);
      int psuperclass = getPrimitiveTypeID(superclass);
      return (psubclass < psuperclass) && (psubclass >= 3);
    }
    return false;
  }
  
  public static Class<?>[] moreSpecificSignature(Class<?>[] a, Class<?>[] b)
  {
    if (a.length != b.length) {
      return null;
    }
    Class<?>[] r = null;
    for (int i = 0; i < a.length; i++) {
      if (a[i] != b[i]) {
        if (r == a)
        {
          if (!isSubclassOf(a[i], b[i])) {
            return null;
          }
        }
        else if (r == b)
        {
          if (!isSubclassOf(b[i], a[i])) {
            return null;
          }
        }
        else if (isSubclassOf(a[i], b[i])) {
          r = a;
        } else if (isSubclassOf(b[i], a[i])) {
          r = b;
        } else {
          return null;
        }
      }
    }
    return r;
  }
  
  public static Class<?>[] moreSpecificSignatureVarargs(Class<?>[] a, Class<?>[] b)
  {
    Class<?>[] shorter = null;Class<?>[] longer = null;
    if (a.length < b.length)
    {
      shorter = (Class[])a.clone();
      longer = (Class[])b.clone();
    }
    else
    {
      shorter = (Class[])b.clone();
      longer = (Class[])a.clone();
    }
    Class<?>[] r = null;
    for (int i = 0; i < shorter.length - 1; i++) {
      if (shorter[i] != longer[i]) {
        if (r == shorter)
        {
          if (!isSubclassOf(shorter[i], longer[i])) {
            return null;
          }
        }
        else if (r == longer)
        {
          if (!isSubclassOf(longer[i], shorter[i])) {
            return null;
          }
        }
        else if (isSubclassOf(shorter[i], longer[i])) {
          r = shorter;
        } else if (isSubclassOf(longer[i], shorter[i])) {
          r = longer;
        } else {
          return null;
        }
      }
    }
    Class<?> varArgsType = shorter[(shorter.length - 1)];
    for (int i = shorter.length - 1; i < longer.length; i++) {
      if (varArgsType != longer[i]) {
        if (r == shorter)
        {
          if (!isSubclassOf(varArgsType, longer[i])) {
            return null;
          }
        }
        else if (r == longer)
        {
          if (!isSubclassOf(longer[i], varArgsType)) {
            return null;
          }
        }
        else if (isSubclassOf(varArgsType, longer[i])) {
          r = shorter;
        } else if (isSubclassOf(longer[i], varArgsType)) {
          r = longer;
        } else {
          return null;
        }
      }
    }
    return r;
  }
  
  public static boolean compareSignature(Class<?>[] formal, Class<?>[] actual)
  {
    if (formal.length != actual.length) {
      return false;
    }
    for (int i = 0; i < formal.length; i++) {
      if (!isConvertibleByWidening(formal[i], actual[i])) {
        return false;
      }
    }
    return true;
  }
  
  public static boolean compareSignatureWithBoxing(Class<?>[] formal, Class<?>[] actual)
  {
    if (formal.length != actual.length) {
      return false;
    }
    for (int i = 0; i < formal.length; i++) {
      if (!isConvertibleWithBoxing(formal[i], actual[i])) {
        return false;
      }
    }
    return true;
  }
  
  public static boolean compareSignatureWithBoxingAndVarargs(Class<?>[] formal, Class<?>[] actual)
  {
    if (actual.length < formal.length - 1) {
      return false;
    }
    for (int i = 0; i < formal.length - 1; i++) {
      if (!isConvertibleWithBoxing(formal[i], actual[i])) {
        return false;
      }
    }
    Class<?> varArgsType = formal[(formal.length - 1)].getComponentType();
    for (int i = formal.length - 1; i < actual.length; i++) {
      if (!isConvertibleWithBoxing(varArgsType, actual[i])) {
        return false;
      }
    }
    return true;
  }
  
  public static Constructor<?> moreSpecificConstructor(Constructor<?> a, Constructor<?> b)
  {
    Class<?>[] pa = a.getParameterTypes();
    Class<?>[] pb = b.getParameterTypes();
    Class<?>[] p = (a.isVarArgs()) && (b.isVarArgs()) ? moreSpecificSignatureVarargs(pa, pb) : moreSpecificSignature(pa, pb);
    if (p == null) {
      return null;
    }
    return p == pa ? a : b;
  }
  
  public static Constructor<?> findConstructor(Class<?> klass, Class<?>[] signature)
    throws AmbiguousSignature, SignatureNotFound
  {
    try
    {
      return klass.getConstructor(signature);
    }
    catch (NoSuchMethodException|SecurityException e)
    {
      Constructor<?> r = null;
      for (Constructor<?> co : klass.getConstructors()) {
        if (compareSignature(co.getParameterTypes(), signature)) {
          if (r == null)
          {
            r = co;
          }
          else
          {
            r = moreSpecificConstructor(co, r);
            if (r == null) {
              throw new AmbiguousSignature();
            }
          }
        }
      }
      if (r != null) {
        return r;
      }
      for (Constructor<?> co : klass.getConstructors()) {
        if (compareSignatureWithBoxing(co.getParameterTypes(), signature)) {
          if (r == null)
          {
            r = co;
          }
          else
          {
            r = moreSpecificConstructor(co, r);
            if (r == null) {
              throw new AmbiguousSignature();
            }
          }
        }
      }
      if (r != null) {
        return r;
      }
      for (Constructor<?> co : klass.getConstructors()) {
        if ((co.isVarArgs()) && (compareSignatureWithBoxingAndVarargs(co.getParameterTypes(), signature))) {
          if (r == null)
          {
            r = co;
          }
          else
          {
            r = moreSpecificConstructor(co, r);
            if (r == null) {
              throw new AmbiguousSignature();
            }
          }
        }
      }
      if (r != null) {
        return r;
      }
      throw new SignatureNotFound();
    }
  }
  
  public static Method moreSpecificMethod(Method a, Method b)
  {
    Class<?>[] pa = a.getParameterTypes();
    Class<?>[] pb = b.getParameterTypes();
    Class<?>[] p = (a.isVarArgs()) && (b.isVarArgs()) ? moreSpecificSignatureVarargs(pa, pb) : moreSpecificSignature(pa, pb);
    if (p == null) {
      return null;
    }
    return p == pa ? a : b;
  }
  
  public static Method findMethod(Class<?> klass, String name, Class<?>[] signature)
    throws AmbiguousSignature, SignatureNotFound, NoSuchMethodException
  {
    try
    {
      return klass.getMethod(name, signature);
    }
    catch (NoSuchMethodException|SecurityException e)
    {
      Method r = null;
      boolean hasMethodWithSuchName = false;
      for (Method me : klass.getMethods()) {
        if (NamePolicy.isEqual(me.getName(), name))
        {
          hasMethodWithSuchName = true;
          if (compareSignature(me.getParameterTypes(), signature)) {
            if (r == null)
            {
              r = me;
            }
            else
            {
              r = moreSpecificMethod(me, r);
              if (r == null) {
                throw new AmbiguousSignature();
              }
            }
          }
        }
      }
      if (!hasMethodWithSuchName) {
        throw new NoSuchMethodException();
      }
      if (r != null) {
        return r;
      }
      for (Method me : klass.getMethods()) {
        if ((NamePolicy.isEqual(me.getName(), name)) && (compareSignatureWithBoxing(me.getParameterTypes(), signature))) {
          if (r == null)
          {
            r = me;
          }
          else
          {
            r = moreSpecificMethod(me, r);
            if (r == null) {
              throw new AmbiguousSignature();
            }
          }
        }
      }
      if (r != null) {
        return r;
      }
      for (Method me : klass.getMethods()) {
        if ((NamePolicy.isEqual(me.getName(), name)) && (me.isVarArgs()) && (compareSignatureWithBoxingAndVarargs(me.getParameterTypes(), signature))) {
          if (r == null)
          {
            r = me;
          }
          else
          {
            r = moreSpecificMethod(me, r);
            if (r == null) {
              throw new AmbiguousSignature();
            }
          }
        }
      }
      if (r != null) {
        return r;
      }
      throw new SignatureNotFound();
    }
  }
  
  public static void printBytecode(byte[] code, PrintStream out)
  {
    ClassReader cr = new ClassReader(code);
    cr.accept(new TraceClassVisitor(new PrintWriter(out)), 0);
  }
  
  public static String verifyBytecode(byte[] code, ClassLoader cl)
  {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    ClassReader cr = new ClassReader(code);
    CheckClassAdapter.verify(cr, cl, false, pw);
    String report = sw.toString();
    return report.length() > 0 ? report : null;
  }
  
  public static void writeBytecodeToFile(byte[] code, String className, String dirname)
    throws IOException
  {
    if (dirname == null) {
      dirname = System.getProperty("user.dir");
    }
    if (className == null) {
      className = getClassName(code);
    }
    File dir = new File(dirname);
    File file = new File(dir, className.replace('.', '/') + ".class");
    File parent = file.getParentFile();
    if ((!parent.exists()) && (!parent.mkdirs())) {
      throw new IllegalStateException("Couldn't create dir: " + parent);
    }
    FileOutputStream out = new FileOutputStream(file);
    out.write(code);
    out.close();
  }
  
  public static String getClassName(byte[] code)
  {
    try
    {
      DataInputStream s = new DataInputStream(new ByteArrayInputStream(code));
      s.skipBytes(8);
      int const_pool_count = (s.readShort() & 0xFFFF) - 1;
      int[] classes = new int[const_pool_count];
      String[] strings = new String[const_pool_count];
      for (int i = 0; i < const_pool_count; i++)
      {
        int tag = s.read();
        switch (tag)
        {
        case 1: 
          strings[i] = s.readUTF(); break;
        case 7: 
          classes[i] = (s.readShort() & 0xFFFF); break;
        case 8: 
          s.skipBytes(2); break;
        case 5: 
        case 6: 
          s.skipBytes(8);i++; break;
        case 2: 
        case 3: 
        case 4: 
        default: 
          s.skipBytes(4);
        }
      }
      s.skipBytes(2);
      int this_class_index = s.readShort() & 0xFFFF;
      String className = strings[(classes[(this_class_index - 1)] - 1)];
      return className.replace('/', '.');
    }
    catch (IOException e) {}
    return null;
  }
  
  public static void printMethodBytecode(CtMethod meth, PrintStream out)
  {
    InstructionPrinter.print(meth, out);
  }
  
  public static List<Field> getNonStaticFields(Class<?> klass)
  {
    List<Field> list = new ArrayList();
    for (Field field : klass.getFields()) {
      if (!Modifier.isStatic(field.getModifiers())) {
        list.add(field);
      }
    }
    return list;
  }
  
  public static List<Field> getNotSpecial(List<Field> fields)
  {
    List<Field> ret = new ArrayList();
    Class<?> anno;
    try
    {
      anno = WALoader.get().loadClass("com.bloom.anno.SpecialEventAttribute");
      for (Field f : fields)
      {
        boolean isSpecial = false;
        for (Annotation a : f.getAnnotations())
        {
          Class<?> atype = a.annotationType();
          if (atype.equals(anno))
          {
            isSpecial = true;
            break;
          }
        }
        if (!isSpecial) {
          ret.add(f);
        }
      }
    }
    catch (ClassNotFoundException e)
    {
      
     e.printStackTrace();
    }
    return ret;
  }
  
  public static byte[] compileJavaFile(String fileName)
    throws IOException
  {
    File sourceFile = new File(fileName);
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    StandardJavaFileManager stdFileManager = compiler.getStandardFileManager(null, null, null);
    
    stdFileManager.setLocation(StandardLocation.CLASS_OUTPUT, Arrays.asList(new File[] { sourceFile.getParentFile() }));
    
    JavaFileManager fileManager = new XJavaFileManager(stdFileManager);
    String cp = System.getProperty("java.class.path");
    JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, null, Arrays.asList(new String[] { "-classpath", cp }), null, stdFileManager.getJavaFileObjectsFromFiles(Arrays.asList(new File[] { sourceFile })));
    
    boolean success = task.call().booleanValue();
    fileManager.close();
    if (success) {
      return Files.readAllBytes(Paths.get(fileName.replace(".java", ".class"), new String[0]));
    }
    return null;
  }
}
