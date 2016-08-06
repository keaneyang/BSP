package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.proc.events.ObjectArrayEvent;
import com.bloom.runtime.components.Flow;
import com.bloom.uuid.UUID;
import com.bloom.event.Event;
import com.bloom.recovery.NumberSourcePosition;
import com.bloom.recovery.Position;
import com.bloom.recovery.SourcePosition;

import java.util.Map;
import org.apache.log4j.Logger;

@PropertyTemplate(name="NumberSource", type=AdapterType.internal, properties={@com.bloom.anno.PropertyTemplateProperty(name="lowValue", type=Long.class, required=true, defaultValue="0"), @com.bloom.anno.PropertyTemplateProperty(name="highValue", type=Long.class, required=true, defaultValue="100000"), @com.bloom.anno.PropertyTemplateProperty(name="repeat", type=Boolean.class, required=false, defaultValue="false"), @com.bloom.anno.PropertyTemplateProperty(name="delayMillis", type=Long.class, required=false, defaultValue="1")}, inputType=ObjectArrayEvent.class)
public class NumberSource
  extends SourceProcess
{
  private Logger logger = Logger.getLogger(NumberSource.class);
  boolean stopped = false;
  long value;
  private long lowValue;
  private long highValue;
  private Boolean repeat;
  private long delayMillis;
  private boolean sendPositions;
  
  public synchronized void init(Map<String, Object> properties, Map<String, Object> parserProperties, UUID uuid, String distributionID, SourcePosition restartPosition, boolean sendPositions, Flow flow)
    throws Exception
  {
    super.init(properties, parserProperties, uuid, distributionID, restartPosition, sendPositions, flow);
    
    this.lowValue = getLongValue(properties.get("lowValue"));
    this.highValue = getLongValue(properties.get("highValue"));
    this.repeat = getBooleanValue(properties.get("repeat"));
    this.delayMillis = getLongValue(properties.get("delayMillis"));
    this.sendPositions = sendPositions;
    if ((restartPosition instanceof NumberSourcePosition))
    {
      this.value = ((NumberSourcePosition)restartPosition).value;
      if (this.logger.isDebugEnabled()) {
        this.logger.debug("Set restart position from restartPosition to " + this.value);
      }
    }
    else
    {
      this.value = this.lowValue;
      if (this.logger.isDebugEnabled()) {
        this.logger.debug("Null restart position therefore " + this.value);
      }
    }
    this.stopped = false;
  }
  
  public synchronized void close()
    throws Exception
  {
    super.close();
  }
  
  public synchronized void receiveImpl(int channel, Event event)
    throws Exception
  {
    if (this.stopped) {
      return;
    }
    if ((!this.repeat.booleanValue()) && (this.value > this.highValue))
    {
      this.stopped = true;
      if (this.logger.isDebugEnabled()) {
        this.logger.debug("Number Source has reached the max value " + this.highValue);
      }
      return;
    }
    ObjectArrayEvent outEvent = new ObjectArrayEvent(System.currentTimeMillis());
    outEvent.data = new Object[2];
    outEvent.data[0] = Long.valueOf(System.currentTimeMillis());
    outEvent.data[1] = Long.valueOf(this.value);
    
    Position outPos = this.sendPositions ? Position.from(this.sourceUUID, this.distributionID, new NumberSourcePosition(this.value)) : null;
    
    send(outEvent, 0, outPos);
    
    this.value += 1L;
    if (this.delayMillis > 0L) {
      Thread.sleep(this.delayMillis);
    }
  }
  
  public void setSourcePosition(SourcePosition sourcePosition)
  {
    if ((sourcePosition instanceof NumberSourcePosition))
    {
      NumberSourcePosition lsp = (NumberSourcePosition)sourcePosition;
      this.value = lsp.value;
    }
    else
    {
      this.logger.warn("LongSource.setSourcePosition() called with wrong kind of SourcePosition: " + sourcePosition.getClass());
    }
  }
  
  public Position getCheckpoint()
  {
    SourcePosition sourcePosition = new NumberSourcePosition(this.value);
    
    Position result = Position.from(this.sourceUUID, this.distributionID, sourcePosition);
    if (this.logger.isDebugEnabled()) {
      this.logger.debug("NumberSource checkpoint is " + result);
    }
    return result;
  }
  
  private Boolean getBooleanValue(Object data)
  {
    if ((data instanceof Boolean)) {
      return (Boolean)data;
    }
    return Boolean.valueOf(data.toString().equals("TRUE"));
  }
  
  private long getLongValue(Object data)
  {
    if ((data instanceof Integer)) {
      return ((Integer)data).longValue();
    }
    if ((data instanceof Integer)) {
      return ((Long)data).longValue();
    }
    try
    {
      return Long.valueOf(data.toString()).longValue();
    }
    catch (NumberFormatException e)
    {
      this.logger.warn("LongSource cannot turn \"" + data + "\" into a Long value", e);
    }
    return 0L;
  }
}
