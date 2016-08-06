package com.bloom.kafkamessaging;

import com.bloom.runtime.components.Stream;
import com.bloom.ser.KryoSingleton;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.log4j.Logger;

public class KafkaDecoder
  implements Deserializer
{
  private static Logger logger = Logger.getLogger(Stream.class);
  
  public KafkaDecoder()
  {
    logger.debug("Kafka Decode creation");
  }
  
  public void close()
  {
    logger.debug("Kafka Encode closing");
  }
  
  public void configure(Map arg0, boolean arg1)
  {
    logger.debug("Kafka Encode configure arg0=" + arg0 + " arg1=" + arg1);
  }
  
  public Object deserialize(String arg0, byte[] arg1)
  {
    return KryoSingleton.read(arg1, false);
  }
}
