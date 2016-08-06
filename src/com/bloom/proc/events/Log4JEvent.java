package com.bloom.proc.events;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.anno.EventType;
import com.bloom.anno.EventTypeData;
import com.bloom.event.SimpleEvent;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.joda.time.DateTime;

@EventType(schema="Internal", classification="All", uri="com.bloom.proc.events:Log4JEvent:1.0")
public class Log4JEvent
  extends SimpleEvent
{
  private static final long serialVersionUID = -861041374336281417L;
  @EventTypeData
  public String logger = null;
  public DateTime ts = null;
  public String level = null;
  public String thread = null;
  public String message = null;
  public String throwable = null;
  public String clazz = null;
  public String method = null;
  public String file = null;
  public String line = null;
  public String ndc = null;
  
  public Log4JEvent() {}
  
  public Log4JEvent(LoggingEvent loggingEvent)
  {
    LocationInfo loggingEventLocation = loggingEvent.getLocationInformation();
    
    this.logger = loggingEvent.getLoggerName();
    this.ts = new DateTime(loggingEvent.getTimeStamp());
    this.level = loggingEvent.getLevel().toString();
    this.thread = loggingEvent.getThreadName();
    this.message = loggingEvent.getRenderedMessage();
    this.throwable = join(loggingEvent.getThrowableStrRep());
    this.clazz = loggingEventLocation.getClassName();
    this.method = loggingEventLocation.getMethodName();
    this.file = loggingEventLocation.getFileName();
    this.line = loggingEventLocation.getLineNumber();
    this.ndc = loggingEvent.getNDC();
  }
  
  private static String join(String[] strings)
  {
    if (strings == null) {
      return null;
    }
    StringBuilder out = new StringBuilder();
    out.append(strings[0]);
    for (int index = 1; index < strings.length; index++)
    {
      out.append('\n');
      out.append(strings[index]);
    }
    return out.toString();
  }
  
  public void setPayload(Object[] payload) {}
  
  public Object[] getPayload()
  {
    return new Object[] { this.logger, this.ts, this.level, this.thread, this.message, this.throwable, this.clazz, this.method, this.file, this.line, this.ndc };
  }
  
  public void write(Kryo kryo, Output output)
  {
    super.write(kryo, output);
    output.writeString(this.logger);
    kryo.writeClassAndObject(output, this.ts);
    output.writeString(this.level);
    output.writeString(this.thread);
    output.writeString(this.message);
    output.writeString(this.throwable);
    output.writeString(this.clazz);
    output.writeString(this.method);
    output.writeString(this.file);
    output.writeString(this.line);
    output.writeString(this.ndc);
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    this.logger = input.readString();
    this.ts = ((DateTime)kryo.readClassAndObject(input));
    this.level = input.readString();
    this.thread = input.readString();
    this.message = input.readString();
    this.throwable = input.readString();
    this.clazz = input.readString();
    this.method = input.readString();
    this.file = input.readString();
    this.line = input.readString();
    this.ndc = input.readString();
  }
  
  public int length()
  {
    int eventBytes = 0;
    
    eventBytes += (this.logger == null ? 0 : this.logger.length());
    eventBytes += (this.ts == null ? 0 : this.ts.toString().length());
    eventBytes += (this.level == null ? 0 : this.level.length());
    eventBytes += (this.thread == null ? 0 : this.thread.length());
    eventBytes += (this.message == null ? 0 : this.message.length());
    eventBytes += (this.throwable == null ? 0 : this.throwable.length());
    eventBytes += (this.clazz == null ? 0 : this.clazz.length());
    eventBytes += (this.method == null ? 0 : this.method.length());
    eventBytes += (this.file == null ? 0 : this.file.length());
    eventBytes += (this.line == null ? 0 : this.line.length());
    eventBytes += (this.ndc == null ? 0 : this.ndc.length());
    
    return eventBytes;
  }
}
