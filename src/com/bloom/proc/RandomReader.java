package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetadataRepository;
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
import org.apache.log4j.Logger;

@PropertyTemplate(name="ranReader", type=AdapterType.internal, properties={@com.bloom.anno.PropertyTemplateProperty(name="OutputType", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="NoLimit", type=String.class, required=true, defaultValue="false"), @com.bloom.anno.PropertyTemplateProperty(name="SampleSize", type=Integer.class, required=true, defaultValue="-1"), @com.bloom.anno.PropertyTemplateProperty(name="DataKey", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="NumberOfUniqueKeys", type=Integer.class, required=true, defaultValue="0"), @com.bloom.anno.PropertyTemplateProperty(name="TimeInterval", type=Integer.class, required=false, defaultValue="0")}, inputType=StringArrayEvent.class)
public class RandomReader
  extends SourceProcess
{
  private static Logger logger = Logger.getLogger(RandomReader.class);
  boolean infinite;
  int sampleSize;
  static String keyInput;
  int uniqueKeys;
  int ss;
  public static StringArrayEvent out;
  static String[] sampArray;
  int tempSize;
  int counter;
  int ranNumber;
  int numOfUK;
  static String useKey;
  private long interval_in_microsec;
  static List<String> keyList;
  static List<Object> valList;
  MDRepository cache = MetadataRepository.getINSTANCE();
  String[] dummy = null;
  int recordsForEachKey = 1;
  
  public synchronized void init(Map<String, Object> properties)
    throws Exception
  {
    this.infinite = false;
    this.sampleSize = 0;
    keyInput = "";
    this.uniqueKeys = 0;
    this.ss = 0;
    this.tempSize = 0;
    this.counter = 0;
    this.ranNumber = 0;
    this.numOfUK = 0;
    useKey = "";
    this.interval_in_microsec = 0L;
    
    keyList = new ArrayList();
    valList = new ArrayList();
    
    this.cache = MetadataRepository.getINSTANCE();
    this.dummy = null;
    this.recordsForEachKey = 1;
    
    super.init(properties);
    this.infinite = ((properties.containsKey("NoLimit")) && (properties.get("NoLimit").toString().equalsIgnoreCase("true")));
    Object ssize = properties.get("SampleSize");
    if (ssize != null) {
      this.sampleSize = Integer.parseInt(ssize.toString());
    }
    keyInput = (String)properties.get("DataKey");
    
    Object interval = properties.get("TimeInterval");
    if (interval != null) {
      this.interval_in_microsec = Long.parseLong(interval.toString());
    }
    String typeName = (String)properties.get("OutputType");
    
    String[] namespaceAndName = typeName.split("\\.");
    MetaInfo.Type t = (MetaInfo.Type)this.cache.getMetaObjectByName(EntityType.TYPE, namespaceAndName[0], namespaceAndName[1], null, WASecurityManager.TOKEN);
    for (Map.Entry<String, String> attr : t.fields.entrySet())
    {
      keyList.add(attr.getKey());
      valList.add(attr.getValue());
    }
    sampArray = new String[valList.size()];
    if (!this.infinite)
    {
      if (!keyList.contains(keyInput)) {
        throw new Exception("Key does not exist");
      }
      if (keyInput.isEmpty()) {
        this.uniqueKeys = this.sampleSize;
      } else {
        this.uniqueKeys = ((Integer)properties.get("NumberOfUniqueKeys")).intValue();
      }
      if (this.uniqueKeys > this.sampleSize) {
        throw new Exception("NumberOfUniqueKeys is out of bounds (must be less than sampleSize)");
      }
      if (this.uniqueKeys <= 0) {
        this.uniqueKeys = this.sampleSize;
      }
      this.tempSize = (this.sampleSize - this.uniqueKeys);
    }
    int stringArraySize = keyList.size();
    this.dummy = new String[stringArraySize];
  }
  
  public synchronized void close()
    throws Exception
  {
    super.close();
  }
  
  public synchronized void receiveImpl(int channel, Event event)
    throws Exception
  {
    out = new StringArrayEvent(System.currentTimeMillis());
    out.data = this.dummy;
    if ((this.ss < this.sampleSize) || (this.infinite == true))
    {
      if (this.infinite == true)
      {
        InfFields();
      }
      else if ((this.counter == 0) && (this.numOfUK != this.uniqueKeys))
      {
        RanFields();
        
        this.ranNumber = Util.ranInt(Util.nRandom, 0, this.tempSize);
        this.tempSize -= this.ranNumber;
        this.counter = this.ranNumber;
        
        this.ss += 1;
        this.numOfUK += 1;
      }
      else
      {
        keySpecificFields();
        
        this.ss += 1;
        this.counter -= 1;
      }
      if (this.interval_in_microsec > 0L)
      {
        long delay = this.interval_in_microsec * 1000L;
        long start;
        while (System.nanoTime() < start + delay) {}
      }
      int myC = 0;
      send(out, 0);
    }
    else
    {
      Thread.sleep(1000L);
    }
  }
  
  public static void InfFields()
  {
    for (int i = 0; i < valList.size(); i++) {
      if (valList.get(i).equals("java.lang.String"))
      {
        out.data[i] = Util.ranString(Util.nRandom, "aeiou bcdfghj", 9);
      }
      else if (valList.get(i).equals("int"))
      {
        int temp = Util.ranInt(Util.nRandom, 20000, 20100);
        out.data[i] = Integer.toString(temp);
      }
      else if ((valList.get(i).equals("double")) || (valList.get(i).equals("java.lang.Double")))
      {
        double temp = Util.ranInt(Util.nRandom, 10000, 999999) / 1000.0D;
        out.data[i] = String.valueOf(temp);
      }
    }
  }
  
  public static void RanFields()
  {
    for (int i = 0; i < valList.size(); i++) {
      if (valList.get(i).equals("java.lang.String"))
      {
        out.data[i] = Util.ranString(Util.nRandom, "aeiou bcdfghj", 9);
        sampArray[i] = out.data[i];
        if (((String)keyList.get(i)).equalsIgnoreCase(keyInput)) {
          useKey = out.data[i];
        }
      }
      else if (valList.get(i).equals("int"))
      {
        int temp = Util.ranInt(Util.nRandom, 20000, 20100);
        out.data[i] = Integer.toString(temp);
        sampArray[i] = out.data[i];
        if (((String)keyList.get(i)).equalsIgnoreCase(keyInput)) {
          useKey = out.data[i];
        }
      }
      else if ((valList.get(i).equals("double")) || (valList.get(i).equals("java.lang.Double")))
      {
        double temp = Util.ranInt(Util.nRandom, 10000, 999999) / 1000.0D;
        out.data[i] = String.valueOf(temp);
        sampArray[i] = out.data[i];
        if (((String)keyList.get(i)).equalsIgnoreCase(keyInput)) {
          useKey = out.data[i];
        }
      }
    }
  }
  
  public static void keySpecificFields()
  {
    for (int i = 0; i < valList.size(); i++) {
      if (!((String)keyList.get(i)).equalsIgnoreCase(keyInput)) {
        out.data[i] = sampArray[i];
      } else {
        out.data[i] = useKey;
      }
    }
  }
  
  public static void main(String[] args)
  {
    Map<String, Object> props = new HashMap();
    
    props.put("NoLimit", "false");
    props.put("SampleSize", "100");
    props.put("DataKey", "2");
    props.put("NumberOfUniqueKeys", "10");
    
    RandomReader mrr = new RandomReader();
    try
    {
      mrr.init(props);
      
      mrr.addEventSink(new AbstractEventSink()
      {
        public void receive(int channel, Event event)
        {
          int eventLength = ((Object[])event.getPayload()[0]).length;
          if (RandomReader.logger.isDebugEnabled()) {
            RandomReader.logger.debug("EventLength : " + eventLength);
          }
          String data = "";
          for (int i = 0; i < eventLength; i++) {
            data = data + (String)((Object[])(Object[])event.getPayload()[0])[i] + ',';
          }
          if (RandomReader.logger.isDebugEnabled()) {
            RandomReader.logger.debug(data);
          }
        }
      });
      for (int i = 0; i < 10000; i++) {
        mrr.receive(0, null);
      }
    }
    catch (Exception e)
    {
      logger.error(e);
    }
  }
}
