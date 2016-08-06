package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.proc.events.ObjectArrayEvent;
import com.bloom.proc.events.StringArrayEvent;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.security.WASecurityManager;
import com.bloom.event.Event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

@PropertyTemplate(name="StreamReader", type=AdapterType.internal, properties={@com.bloom.anno.PropertyTemplateProperty(name="OutputType", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="noLimit", type=String.class, required=true, defaultValue="false"), @com.bloom.anno.PropertyTemplateProperty(name="isSeeded", type=String.class, required=false, defaultValue="false"), @com.bloom.anno.PropertyTemplateProperty(name="maxRows", type=Integer.class, required=false, defaultValue="0"), @com.bloom.anno.PropertyTemplateProperty(name="increment", type=Double.class, required=false, defaultValue="1.0"), @com.bloom.anno.PropertyTemplateProperty(name="iterations", type=Integer.class, required=false, defaultValue="0"), @com.bloom.anno.PropertyTemplateProperty(name="iterationDelay", type=Integer.class, required=false, defaultValue="0"), @com.bloom.anno.PropertyTemplateProperty(name="StringSet", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="NumberSet", type=String.class, required=false, defaultValue="")}, inputType=ObjectArrayEvent.class)
public class StreamReader
  extends SourceProcess
{
  private Logger logger;
  int maxRows;
  int iterations;
  long iterationDelay;
  String dataRange;
  public StringArrayEvent out;
  public ObjectArrayEvent out2;
  int objectIDcount;
  int irc;
  int count;
  boolean infinite;
  String objects;
  int objectListCount;
  String typeName;
  String noLimit;
  String strSet;
  DateTime dt;
  int strCounter;
  String dataType;
  int listSize;
  double Lmin;
  double increment;
  RandomGen generator;
  String isSeeded;
  int m_r;
  long r_t;
  List<String> nameList;
  List<Object> dataTypeList;
  ArrayList<String> setElements;
  Map<String, boundaries> dataRanges;
  Map<String, ArrayList<String>> strSetMap;
  MDRepository cache;
  public String[] eachTypeBound;
  public String[] nameMinMaxDis;
  public String[] strList;
  public String[] nameArray;
  String namespace;
  
  public StreamReader()
  {
    this.logger = Logger.getLogger(StreamReader.class);
    
    this.cache = MetadataRepository.getINSTANCE();
    
    this.namespace = "";
  }
  
  public void setNamespace(String namespace)
  {
    this.namespace = namespace;
  }
  
  public static class RandomGen
    extends Random
  {
    public RandomGen(int seed)
    {
      super();
    }
    
    public RandomGen() {}
    
    public int nextIntInRange(int min, int max)
    {
      return nextInt(max - min + 1) + min;
    }
  }
  
  public synchronized void init(Map<String, Object> properties)
    throws Exception
  {
    this.dt = new DateTime();
    this.noLimit = "";
    this.maxRows = 0;
    this.iterations = 0;
    this.iterationDelay = 0L;
    this.dataRange = "";
    this.objectIDcount = 1;
    this.irc = 0;
    this.count = 0;
    this.infinite = false;
    this.objects = "";
    this.objectListCount = 0;
    this.typeName = "";
    this.strSet = "";
    this.strCounter = 0;
    this.dataType = "";
    this.Lmin = 0.0D;
    this.increment = 1.0D;
    this.listSize = 0;
    
    this.m_r = 0;
    
    this.r_t = 0L;
    
    this.nameList = new ArrayList();
    this.dataTypeList = new ArrayList();
    this.dataRanges = new HashMap();
    this.strSetMap = new HashMap();
    
    MDRepository cache = MetadataRepository.getINSTANCE();
    this.eachTypeBound = new String[0];
    this.nameMinMaxDis = new String[0];
    this.strList = new String[0];
    this.nameArray = new String[0];
    
    super.init(properties);
    Map<String, Object> localCopyOfProperty = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    localCopyOfProperty.putAll(properties);
    this.maxRows = getIntValue(localCopyOfProperty.get("maxRows"));
    this.noLimit = ((String)localCopyOfProperty.get("noLimit"));
    this.iterations = getIntValue(localCopyOfProperty.get("iterations"));
    this.iterationDelay = getLongValue(localCopyOfProperty.get("iterationDelay"));
    this.typeName = ((String)localCopyOfProperty.get("OutputType"));
    this.dataRange = ((String)localCopyOfProperty.get("NumberSet"));
    this.strSet = ((String)localCopyOfProperty.get("StringSet"));
    this.isSeeded = ((String)localCopyOfProperty.get("isSeeded"));
    this.increment = ((Double)localCopyOfProperty.get("increment")).doubleValue();
    if (this.isSeeded.equalsIgnoreCase("true")) {
      this.generator = new RandomGen(1);
    } else {
      this.generator = new RandomGen();
    }
    if (this.strSet != null)
    {
      this.strList = this.strSet.split(",");
      if (this.strList.length > 0)
      {
        this.nameArray = this.strList[0].replaceAll("\\s+", "").split("\\[|\\-|\\]");
        this.listSize = (this.nameArray.length - 1);
      }
      for (int i = 0; i < this.strList.length; i++)
      {
        this.nameArray = this.strList[i].replaceAll("\\s+", "").split("\\[|\\-|\\]");
        this.setElements = new ArrayList();
        for (int j = 1; j < this.nameArray.length; j++) {
          this.setElements.add(this.nameArray[j]);
        }
        this.strSetMap.put(this.nameArray[0], this.setElements);
      }
    }
    try
    {
      MetaInfo.Type t;
      if (this.typeName.contains("."))
      {
        String[] namespaceAndName = this.typeName.split("\\.");
        t = (MetaInfo.Type)cache.getMetaObjectByName(EntityType.TYPE, namespaceAndName[0], namespaceAndName[1], null, WASecurityManager.TOKEN);
      }
      else
      {
        t = (MetaInfo.Type)cache.getMetaObjectByName(EntityType.TYPE, this.namespace, this.typeName.trim(), null, WASecurityManager.TOKEN);
      }
      for (Map.Entry<String, String> attr : t.fields.entrySet())
      {
        this.nameList.add(attr.getKey());
        this.dataTypeList.add(attr.getValue());
      }
    }
    catch (NullPointerException e)
    {
      throw new NullPointerException("Incorrect namespace for TYPE: " + this.typeName + "\nCheck format in Source");
    }
    if (this.dataRange != null)
    {
      this.eachTypeBound = this.dataRange.split(",");
      for (int i = 0; i < this.eachTypeBound.length; i++)
      {
        this.nameMinMaxDis = this.eachTypeBound[i].replaceAll("\\s+", "").split("\\[|\\-|\\]");
        boundaries a;
        if (this.nameMinMaxDis.length == 3)
        {
           a = new boundaries(Double.parseDouble(this.nameMinMaxDis[1]), Double.parseDouble(this.nameMinMaxDis[1]), this.nameMinMaxDis[2]);
          if (this.nameMinMaxDis[2].equalsIgnoreCase("Linc")) {
            a.point -= 1.0D;
          } else if (this.nameMinMaxDis[2].equalsIgnoreCase("Ldec")) {
            a.point += 1.0D;
          }
        }
        else
        {
          a = new boundaries(Double.parseDouble(this.nameMinMaxDis[1]), Double.parseDouble(this.nameMinMaxDis[2]), this.nameMinMaxDis[3]);
          if (this.nameMinMaxDis[3].equalsIgnoreCase("Linc")) {
            a.point -= 1.0D;
          } else if (this.nameMinMaxDis[3].equalsIgnoreCase("Ldec")) {
            a.point += 1.0D;
          }
        }
        this.dataRanges.put(this.nameMinMaxDis[0], a);
        if (this.strSet == null) {
          this.listSize = this.eachTypeBound.length;
        }
      }
    }
  }
  
  public int getIntValue(Object data)
  {
    if ((data instanceof Integer)) {
      return ((Integer)data).intValue();
    }
    return ((Long)data).intValue();
  }
  
  public long getLongValue(Object data)
  {
    if ((data instanceof Integer)) {
      return ((Integer)data).longValue();
    }
    return ((Long)data).longValue();
  }
  
  public synchronized void close()
    throws Exception
  {
    super.close();
  }
  
  public synchronized void receiveImpl(int channel, Event event)
    throws Exception
  {
    this.out2 = new ObjectArrayEvent(System.currentTimeMillis());
    this.out2.data = new Object[this.nameList.size()];
    if (this.noLimit.equalsIgnoreCase("true"))
    {
      setFields();
      if (this.iterationDelay > 0L) {
        Thread.sleep(this.iterationDelay);
      }
      send(this.out2, 0);
      if (this.objectListCount == this.listSize - 1) {
        this.objectListCount = 0;
      } else {
        this.objectListCount += 1;
      }
    }
    if ((this.irc < this.iterations) && (this.noLimit.equalsIgnoreCase("false")))
    {
      setFields();
      send(this.out2, 0);
      if ((this.iterationDelay > 0L) && (this.objectListCount + 1 == this.listSize)) {
        Thread.sleep(this.iterationDelay);
      }
      if (this.objectListCount == this.listSize - 1)
      {
        this.objectListCount = 0;
        this.irc += 1;
      }
      else
      {
        this.objectListCount += 1;
      }
    }
    if ((this.m_r < this.maxRows) && (this.noLimit.equalsIgnoreCase("false")))
    {
      setFields();
      if (this.objectListCount == this.listSize - 1) {
        this.objectListCount = 0;
      } else {
        this.objectListCount += 1;
      }
      this.m_r += 1;
      if (this.iterationDelay > 0L) {
        Thread.sleep(this.iterationDelay);
      }
      send(this.out2, 0);
    }
  }
  
  public void setFields()
    throws Exception
  {
    for (int i = 0; i < this.nameList.size(); i++)
    {
      this.dataType = String.valueOf(this.dataTypeList.get(i));
      try
      {
        if (this.dataType.equalsIgnoreCase("java.lang.String"))
        {
          ArrayList<String> strArr = (ArrayList)this.strSetMap.get(this.nameList.get(i));
          if (this.objectListCount >= strArr.size()) {
            continue;
          }
          this.out2.data[i] = strArr.get(this.objectListCount);
        }
        else if ((this.dataType.equalsIgnoreCase("java.lang.Long")) || (this.dataType.equalsIgnoreCase("int")) || (this.dataType.equalsIgnoreCase("java.lang.Double")) || (this.dataType.equalsIgnoreCase("integer")) || (this.dataType.equalsIgnoreCase("java.lang.Integer")))
        {
          boundaries newb = (boundaries)this.dataRanges.get(this.nameList.get(i));
          if (newb.dis.equalsIgnoreCase("G")) {
            this.out2.data[i] = GDistribution(newb, this.dataType);
          } else if (newb.dis.equalsIgnoreCase("R")) {
            this.out2.data[i] = RDistribution(newb, this.dataType);
          } else if (newb.dis.equalsIgnoreCase("L")) {
            this.out2.data[i] = Double.valueOf(LDistribution(newb, this.dataType));
          } else if (newb.dis.equalsIgnoreCase("LInc")) {
            this.out2.data[i] = Double.valueOf(LIncDistribution(newb, this.dataType));
          } else if (newb.dis.equalsIgnoreCase("LDec")) {
            this.out2.data[i] = Double.valueOf(LDecDistribution(newb, this.dataType));
          }
        }
        else if (this.dataType.equalsIgnoreCase("org.joda.time.DateTime"))
        {
          this.dt = DateTime.now();
          this.out2.data[i] = this.dt;
        }
        else
        {
          String e = "Unsupported data type";
          throw new Exception(e);
        }
      }
      catch (NullPointerException xx)
      {
        throw new NullPointerException("Incorrect StringSet Name");
      }
    }
  }
  
  public static boolean isNumeric(String str)
  {
	double d;
    try
    {
      d = Double.parseDouble(str);
    }
    catch (NumberFormatException nfe)
    {
      
      return false;
    }
    return true;
  }
  
  public double LDistribution(boundaries x, String dT)
  {
    int Rnumber = this.generator.nextIntInRange((int)x.min, (int)x.max);
    if (x.point < Rnumber) {
      x.point += this.increment;
    } else if (x.point >= Rnumber) {
      x.point -= this.increment;
    }
    if ((dT.equalsIgnoreCase("int")) || (dT.equalsIgnoreCase("integer"))) {
      return (int)Math.round(x.point);
    }
    if (dT.equalsIgnoreCase("long")) {
      return Math.round(x.point);
    }
    return x.point;
  }
  
  public double LIncDistribution(boundaries x, String dT)
  {
    x.point += this.increment;
    if ((dT.equalsIgnoreCase("int")) || (dT.equalsIgnoreCase("integer"))) {
      return (int)Math.round(x.point);
    }
    if (dT.equalsIgnoreCase("long")) {
      return Math.round(x.point);
    }
    return x.point;
  }
  
  public double LDecDistribution(boundaries x, String dT)
  {
    x.point -= this.increment;
    if ((dT.equalsIgnoreCase("int")) || (dT.equalsIgnoreCase("integer"))) {
      return (int)Math.round(x.point);
    }
    if (dT.equalsIgnoreCase("long")) {
      return Math.round(x.point);
    }
    return x.point;
  }
  
  public Object GDistribution(boundaries x, String dT)
  {
    double mean = (x.min + x.max) / 2.0D;
    double SD = (x.max - mean) / 2.0D;
    double Gnumber;
    int delay;
    do
    {
      Gnumber = mean + this.generator.nextGaussian() * SD;
      delay = (int)Math.round(Gnumber);
    } while ((delay <= 0) || (delay > x.max));
    if ((dT.equalsIgnoreCase("int")) || (dT.equalsIgnoreCase("integer"))) {
      return Integer.valueOf((int)Math.round(Gnumber));
    }
    if (dT.equalsIgnoreCase("long")) {
      return Long.valueOf(Math.round(Gnumber));
    }
    return Double.valueOf(Gnumber);
  }
  
  public Object RDistribution(boundaries x, String dT)
  {
    if ((dT.equalsIgnoreCase("int")) || (dT.equalsIgnoreCase("integer")))
    {
      int Rnumber = this.generator.nextIntInRange((int)x.min, (int)x.max);
      return Integer.valueOf(Rnumber);
    }
    if (dT.equalsIgnoreCase("long"))
    {
      double range = x.max - x.min + 1.0D;
      double frac = range * this.generator.nextDouble();
      
      return Long.valueOf((frac + x.min));
    }
    double range = x.max - x.min + 1.0D;
    double frac = range * this.generator.nextDouble();
    return Double.valueOf(frac + x.min);
  }
  
  public void main(String[] args) {}
  
  class boundaries
  {
    double min = 0.0D;
    double point = 0.0D;
    double max = 0.0D;
    String dis = "";
    
    boundaries(double min, double max, String distribution)
    {
      this.min = min;
      this.point = min;
      this.max = max;
      this.dis = distribution;
    }
    
    boundaries()
    {
      this.min = 0.0D;
      this.point = this.min;
      this.max = 0.0D;
      this.dis = "R";
    }
    
    public String toString()
    {
      return "min: " + this.min + " " + "max: " + this.max + "dis: " + this.dis;
    }
  }
  
  static class Util
  {
    public static final Random nRandom = new Random();
    
    public String ranString(Random rng, String characters, int length)
    {
      char[] ranStr = new char[length];
      for (int i = 0; i < length; i++) {
        ranStr[i] = characters.charAt(rng.nextInt(characters.length()));
      }
      return new String(ranStr);
    }
    
    public static int ranInt(Random a, double start, double end)
    {
      double range = end - start + 1.0D;
      double frac = range * a.nextDouble();
      
      int ranNumber = (int)(frac + start);
      return ranNumber;
    }
  }
}
