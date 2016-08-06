package com.bloom.proc.events;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.anno.EventType;
import com.bloom.anno.EventTypeData;
import com.bloom.event.SimpleEvent;
import org.apache.mahout.math.Arrays;
import org.joda.time.DateTime;

@EventType(schema="Internal", classification="All", uri="com.bloom.proc.events:WindowsLogEvent:1.0")
public class WindowsLogEvent
  extends SimpleEvent
{
  private static final long serialVersionUID = -8077354738580620696L;
  @EventTypeData
  public WindowsLog data;
  
  public WindowsLog getData()
  {
    return this.data;
  }
  
  public void setData(WindowsLog data)
  {
    this.data = data;
  }
  
  public void setPayload(Object[] payload)
  {
    this.data = ((WindowsLog)payload[0]);
  }
  
  public Object[] getPayload()
  {
    return new Object[] { this.data };
  }
  
  public String toString()
  {
    return this.data.toString();
  }
  
  public void write(Kryo kryo, Output output)
  {
    super.write(kryo, output);
    kryo.writeClassAndObject(output, this.data);
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    this.data = ((WindowsLog)kryo.readClassAndObject(input));
  }
  
  public static class WindowsLog
  {
    public String sourceName;
    public String computerName;
    public String userSid;
    public long recordNumber;
    public DateTime timeGenerated;
    public DateTime timeWritten;
    public int eventID;
    public short eventType;
    public int eventCategory;
    public String[] stringPayload;
    
    public String toString()
    {
      StringBuffer buffer = new StringBuffer();
      if (this.sourceName != null) {
        buffer.append("SourceName : " + this.sourceName + " \n");
      }
      if (this.computerName != null) {
        buffer.append("ComputerName : " + this.computerName + " \n");
      }
      if (this.userSid != null) {
        buffer.append("UserSid : " + this.userSid + " \n");
      }
      buffer.append("RecordNumber : " + this.recordNumber + " \n");
      if (this.timeGenerated != null) {
        buffer.append("TimeGenerated : " + this.timeGenerated.toString() + " \n");
      }
      if (this.timeWritten != null) {
        buffer.append("TimeWritten : " + this.timeWritten.toString() + " \n");
      }
      buffer.append("EventID : " + this.eventID + " \n");
      buffer.append("EventType : " + this.eventType + " \n");
      buffer.append("EventCategory : " + this.eventCategory + " \n");
      if ((this.stringPayload != null) && (this.stringPayload.length > 0)) {
        buffer.append("String : " + Arrays.toString(this.stringPayload) + " \n");
      }
      return buffer.toString();
    }
  }
}
