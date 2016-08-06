package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.proc.events.ClusterTestEvent;
import com.bloom.event.Event;

import java.util.Map;
import java.util.Random;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

@PropertyTemplate(name="ClusterTester", type=AdapterType.internal, properties={@com.bloom.anno.PropertyTemplateProperty(name="keyPrefix", type=String.class, required=false, defaultValue="KEY-"), @com.bloom.anno.PropertyTemplateProperty(name="numKeys", type=int.class, required=false, defaultValue="10"), @com.bloom.anno.PropertyTemplateProperty(name="numEvents", type=int.class, required=false, defaultValue="100"), @com.bloom.anno.PropertyTemplateProperty(name="intMin", type=int.class, required=false, defaultValue="1"), @com.bloom.anno.PropertyTemplateProperty(name="intMax", type=int.class, required=false, defaultValue="1"), @com.bloom.anno.PropertyTemplateProperty(name="doubleMin", type=double.class, required=false, defaultValue="1"), @com.bloom.anno.PropertyTemplateProperty(name="doubleMax", type=double.class, required=false, defaultValue="1"), @com.bloom.anno.PropertyTemplateProperty(name="tsDelta", type=long.class, required=false, defaultValue="1000"), @com.bloom.anno.PropertyTemplateProperty(name="sequential", type=boolean.class, required=false, defaultValue="false"), @com.bloom.anno.PropertyTemplateProperty(name="realtime", type=boolean.class, required=false, defaultValue="false")}, inputType=ClusterTestEvent.class)
public class ClusterTester_1_0
  extends SourceProcess
{
  private static Logger logger = Logger.getLogger(ClusterTester_1_0.class);
  String keyPrefix;
  int numKeys;
  int numEvents;
  int intMin;
  int intMax;
  int intRange;
  double doubleMin;
  double doubleMax;
  double doubleRange;
  long tsDelta;
  boolean sequential;
  boolean realtime;
  boolean reporteos;
  int count = 0;
  int currKey = 1;
  int sample;
  long currTs = 0L;
  Random rand = new Random(1627235253L);
  
  private String getProp(Map<String, Object> properties, String name)
  {
    Object o = properties.get(name);
    if (o == null) {
      o = properties.get(name.toLowerCase());
    }
    return o == null ? "" : o.toString();
  }
  
  public void init(Map<String, Object> properties)
    throws Exception
  {
    super.init(properties);
    this.keyPrefix = getProp(properties, "keyPrefix");
    
    this.numKeys = Integer.parseInt(getProp(properties, "numKeys"));
    this.numEvents = Integer.parseInt(getProp(properties, "numEvents"));
    
    this.intMin = Integer.parseInt(getProp(properties, "intMin"));
    this.intMax = Integer.parseInt(getProp(properties, "intMax"));
    this.intRange = (this.intMax - this.intMin);
    
    this.doubleMin = Double.parseDouble(getProp(properties, "doubleMin"));
    this.doubleMax = Double.parseDouble(getProp(properties, "doubleMax"));
    this.doubleRange = (this.doubleMax - this.doubleMin);
    
    this.tsDelta = Long.parseLong(getProp(properties, "tsDelta"));
    this.sequential = Boolean.parseBoolean(getProp(properties, "sequential"));
    this.realtime = Boolean.parseBoolean(getProp(properties, "realtime"));
    this.reporteos = Boolean.parseBoolean(getProp(properties, "reporteos"));
    
    this.sample = (this.numEvents / 100);
  }
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    if ((this.numEvents == -1) || (this.count < this.numEvents))
    {
      if (this.realtime)
      {
        Thread.sleep(this.tsDelta);
        this.currTs = System.currentTimeMillis();
      }
      else if (this.currTs == 0L)
      {
        this.currTs = System.currentTimeMillis();
      }
      else
      {
        this.currTs += this.tsDelta;
      }
      ClusterTestEvent t = new ClusterTestEvent(this.currTs);
      if (this.sequential)
      {
        t.keyValue = (this.keyPrefix + this.currKey++);
        if (this.currKey > this.numKeys) {
          this.currKey = 1;
        }
        int val;
        if (this.intRange >= 0) {
          val = this.intMin + this.count % (this.intRange + 1);
        } else {
          val = this.intMin + this.count;
        }
        t.intValue = val;
        t.doubleValue = val;
      }
      else
      {
        t.keyValue = (this.keyPrefix + (this.rand.nextInt(this.numKeys) + 1));
        t.intValue = ((this.intRange > 0 ? this.rand.nextInt() * this.intRange : 0) + this.intMin);
        t.doubleValue = ((this.doubleRange > 0.0D ? this.rand.nextDouble() * this.doubleRange : 0.0D) + this.doubleMin);
      }
      t.ts = new DateTime(this.currTs);
      
      send(t, 0);
    }
    else
    {
      Thread.sleep(1000L);
    }
    this.count += 1;
    if ((this.count == this.numEvents) && (this.reporteos) && 
      (logger.isDebugEnabled())) {
      logger.debug("ClusterTester finished output of " + this.numEvents + "  events");
    }
  }
  
  public static void main(String[] args) {}
}
