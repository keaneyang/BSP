package com.bloom.kafkamessaging;

import com.bloom.runtime.components.Stream;
import com.bloom.ser.KryoSingleton;

import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.Logger;

public class KafkaEncoder
  implements Serializer
{
  private static Logger logger = Logger.getLogger(Stream.class);
  
  public KafkaEncoder()
  {
    logger.debug("Kafka Encode creation");
  }
  
  public void close()
  {
    logger.debug("Kafka Encode closing");
  }
  
  public void configure(Map arg0, boolean arg1)
  {
    logger.debug("Kafka Encode configure arg0=" + arg0 + " arg1=" + arg1);
  }
  
  public byte[] serialize(String arg0, Object arg1)
  {
    return KryoSingleton.write(arg1, false);
  }
}
