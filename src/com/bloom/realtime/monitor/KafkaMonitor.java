package com.bloom.runtime.monitor;

import com.bloom.runtime.Pair;
import com.yammer.metrics.reporting.JmxReporter;
import com.yammer.metrics.reporting.JmxReporter.MeterMBean;
import java.io.IOException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.management.Descriptor;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.JMX;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.log4j.Logger;

public class KafkaMonitor
  implements Runnable
{
  private static Logger logger = Logger.getLogger(MonitorModel.class);
  private final Set<Pair<String, Integer>> serversPorts = new HashSet();
  
  public void add(String server, int port)
  {
    if (logger.isDebugEnabled()) {
      logger.debug("Adding kafka broker to listener: " + server + ":" + port);
    }
    this.serversPorts.add(new Pair(server, Integer.valueOf(port)));
  }
  
  Long prevTimestamp = null;
  Long prevMsgs = null;
  Long prevBytes = null;
  
  public void run()
  {
    Iterator i$ = this.serversPorts.iterator();
    for (;;)
    {
      if (i$.hasNext())
      {
        Pair<String, Integer> serverPort = (Pair)i$.next();
        String server = (String)serverPort.first;
        Integer port = (Integer)serverPort.second;
        if (logger.isDebugEnabled()) {
          logger.debug("Running kafka monitor for broker: " + server + ":" + port);
        }
        JMXConnector jmxc = null;
        try
        {
          JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + server + ":" + port + "/jmxrmi");
          jmxc = JMXConnectorFactory.connect(url, null);
          MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
          if (logger.isDebugEnabled())
          {
            System.out.println("\nDomains:");
            String[] domains = mbsc.getDomains();
            Arrays.sort(domains);
            for (String domain : domains) {
              System.out.println("\tDomain = " + domain);
            }
            System.out.println("\nMBeanServer default domain = " + mbsc.getDefaultDomain());
            System.out.println("\nMBean count = " + mbsc.getMBeanCount());
            System.out.println("\nQuery MBeanServer MBeans:");
            Set<ObjectName> names = new TreeSet(mbsc.queryNames(null, null));
            for (ObjectName name : names)
            {
              System.out.println("\tObjectName = " + name);
              MBeanInfo info = mbsc.getMBeanInfo(name);
              MBeanAttributeInfo[] attrs = info.getAttributes();
              for (MBeanAttributeInfo attr : attrs)
              {
                System.out.println("\t\tAttr: " + attr);
                String[] fieldNames = attr.getDescriptor().getFieldNames();
                for (String fieldName : fieldNames)
                {
                  Object fieldValue = attr.getDescriptor().getFieldValue(fieldName);
                  System.out.println("\t\t\tField name=" + fieldName);
                  System.out.println("\t\t\t     value=" + fieldValue);
                }
              }
            }
          }
          List<MonitorEvent> eventList = new ArrayList();
          long timestamp = System.currentTimeMillis();
          
          ObjectName mbeanName = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec");
          JmxReporter.MeterMBean mbeanProxy = (JmxReporter.MeterMBean)JMX.newMBeanProxy(mbsc, mbeanName, JmxReporter.MeterMBean.class, true);
          
          long kafkaMsgs = mbeanProxy.getCount();
          MonitorEvent event = new MonitorEvent(MonitorModel.KAFKA_SERVER_UUID, MonitorModel.KAFKA_ENTITY_UUID, MonitorEvent.Type.KAFKA_MSGS, Long.valueOf(kafkaMsgs), Long.valueOf(timestamp));
          eventList.add(event);
          if (this.prevMsgs != null)
          {
            long delta = Math.abs(kafkaMsgs - this.prevMsgs.longValue());
            Long rate = Long.valueOf(1000L * delta / (timestamp - this.prevTimestamp.longValue()));
            event = new MonitorEvent(MonitorModel.KAFKA_SERVER_UUID, MonitorModel.KAFKA_ENTITY_UUID, MonitorEvent.Type.KAFKA_MSGS_RATE_LONG, rate, Long.valueOf(timestamp));
            eventList.add(event);
            
            String value = rate + " msgs/sec";
            event = new MonitorEvent(MonitorModel.KAFKA_SERVER_UUID, MonitorModel.KAFKA_ENTITY_UUID, MonitorEvent.Type.KAFKA_MSGS_RATE, value, Long.valueOf(timestamp));
            eventList.add(event);
          }
          this.prevMsgs = Long.valueOf(kafkaMsgs);
          
          mbeanName = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec");
          mbeanProxy = (JmxReporter.MeterMBean)JMX.newMBeanProxy(mbsc, mbeanName, JmxReporter.MeterMBean.class, true);
          long kafkaBytes = mbeanProxy.getCount();
          event = new MonitorEvent(MonitorModel.KAFKA_SERVER_UUID, MonitorModel.KAFKA_ENTITY_UUID, MonitorEvent.Type.KAFKA_BYTES, Long.valueOf(kafkaBytes), Long.valueOf(timestamp));
          eventList.add(event);
          if (this.prevBytes != null)
          {
            long delta = Math.abs(kafkaBytes - this.prevBytes.longValue());
            Long rate = Long.valueOf(1000L * delta / (timestamp - this.prevTimestamp.longValue()));
            event = new MonitorEvent(MonitorModel.KAFKA_SERVER_UUID, MonitorModel.KAFKA_ENTITY_UUID, MonitorEvent.Type.KAFKA_BYTES_RATE_LONG, rate, Long.valueOf(timestamp));
            eventList.add(event);
            
            String value = rate + " bytes/sec";
            event = new MonitorEvent(MonitorModel.KAFKA_SERVER_UUID, MonitorModel.KAFKA_ENTITY_UUID, MonitorEvent.Type.KAFKA_BYTES_RATE, value, Long.valueOf(timestamp));
            eventList.add(event);
          }
          this.prevBytes = Long.valueOf(kafkaBytes);
          this.prevTimestamp = Long.valueOf(timestamp);
          
          MonitorBatchEvent batchEvent = new MonitorBatchEvent(timestamp, eventList);
          MonitorModel.processBatch(batchEvent);
          if (jmxc != null) {
            try
            {
              jmxc.close();
            }
            catch (IOException e)
            {
              logger.error("Failed to close JMX Connection", e);
            }
          }
        }
        catch (MalformedURLException e)
        {
          logger.error("Cannot monitor Kafka because the connection URL is invalid", e);
        }
        catch (IOException e)
        {
          logger.error("Cannot monitor Kafka because the connection failed", e);
        }
        catch (MalformedObjectNameException e)
        {
          logger.error("Cannot monitor Kafka because of malformed object name", e);
        }
        catch (InstanceNotFoundException e)
        {
          logger.error("Cannot monitor Kafka because of the notification listener subscription failed", e);
        }
        catch (IntrospectionException e)
        {
          logger.error("Cannot monitor Kafka because of an introspection error", e);
        }
        catch (ReflectionException e)
        {
          logger.error("Cannot monitor Kafka because of a reflection error", e);
        }
        catch (Exception e)
        {
          logger.error("Cannot monitor Kafka because of an unknown error", e);
        }
        finally
        {
          if (jmxc != null) {
            try
            {
              jmxc.close();
            }
            catch (IOException e)
            {
              logger.error("Failed to close JMX Connection", e);
            }
          }
        }
      }
    }
  }
  
  public void addKafkaBrokers(String kafkaBrokers)
  {
    if (logger.isDebugEnabled()) {
      logger.debug("Adding kafka brokers to monitor: " + kafkaBrokers);
    }
    String[] brokers = kafkaBrokers.split(",");
    for (String broker : brokers)
    {
      String[] brokerParts = broker.split(":");
      if (brokerParts.length == 2)
      {
        int port = tryParseInt(brokerParts[1], -1);
        if (port != -1) {
          add(brokerParts[0], port);
        }
      }
    }
  }
  
  public static int tryParseInt(String value, int defaultValue)
  {
    try
    {
      return Integer.parseInt(value);
    }
    catch (NumberFormatException e) {}
    return defaultValue;
  }
}
