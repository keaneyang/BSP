package com.bloom.proc.events;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.event.SimpleEvent;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AlertEvent
  extends SimpleEvent
{
  private static final long serialVersionUID = -1211231231L;
  public String name;
  public String keyVal;
  public String severity;
  public String flag;
  public String message;
  private String channel;
  private Severity sevVal;
  private alertFlag flagVal;
  public AlertEvent() {}
  
  public static enum Severity
  {
    info,  warning,  error;
    
    private Severity() {}
  }
  
  public static enum alertFlag
  {
    raise,  cancel;
    
    private alertFlag() {}
  }
  
  public AlertEvent(long timestamp)
  {
    super(timestamp);
  }
  
  public void validate()
  {
    this.sevVal = Severity.valueOf(this.severity);
    this.flagVal = alertFlag.valueOf(this.flag);
  }
  
  public Severity severity()
  {
    return this.sevVal;
  }
  
  public alertFlag flag()
  {
    return this.flagVal;
  }
  
  public AlertEvent(String name, String keyVal, String severity, String flag, String message)
  {
    super(System.currentTimeMillis());
    this.name = name;
    this.keyVal = keyVal;
    this.severity = severity;
    this.flag = flag;
    this.message = message;
    validate();
  }
  
  public void setChannel(String channel)
  {
    this.channel = channel;
  }
  
  public String getChannel()
  {
    return this.channel;
  }
  
  public String getTimeStampInDateFormat()
  {
    Date date = new Date(this.timeStamp);
    Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    return format.format(date).toString();
  }
  
  public String toString()
  {
    return getTimeStampInDateFormat() + " - " + this.message;
  }
  
  public void write(Kryo kryo, Output output)
  {
    super.write(kryo, output);
    output.writeString(this.name);
    output.writeString(this.keyVal);
    output.writeString(this.severity);
    output.writeString(this.flag);
    output.writeString(this.message);
    output.writeString(this.channel);
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    this.name = input.readString();
    this.keyVal = input.readString();
    this.severity = input.readString();
    this.flag = input.readString();
    this.message = input.readString();
    this.channel = input.readString();
    validate();
  }
  
  public Object[] getPayload()
  {
    Object[] result = { this.name, this.keyVal, this.severity, this.flag, this.message };
    
    return result;
  }
}
