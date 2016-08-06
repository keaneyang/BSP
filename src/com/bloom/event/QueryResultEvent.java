package com.bloom.event;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.event.SimpleEvent;

public class QueryResultEvent
  extends SimpleEvent
{
  private static final long serialVersionUID = -7225126644053717038L;
  public String[] fieldsInfo;
  
  public QueryResultEvent() {}
  
  public QueryResultEvent(long timestamp)
  {
    super(timestamp);
  }
  
  public void setFieldsInfo(String[] fieldsInfo)
  {
    this.fieldsInfo = ((String[])fieldsInfo.clone());
  }
  
  public String[] getFieldsInfo()
  {
    return this.fieldsInfo;
  }
  
  public void write(Kryo kryo, Output output)
  {
    super.write(kryo, output);
    if (this.fieldsInfo == null)
    {
      output.writeByte((byte)0);
    }
    else
    {
      output.writeByte((byte)1);
      output.writeInt(this.fieldsInfo.length);
      for (String str : this.fieldsInfo) {
        output.writeString(str);
      }
    }
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    if (input.readByte() == 0)
    {
      this.fieldsInfo = null;
    }
    else
    {
      int size = input.readInt();
      this.fieldsInfo = new String[size];
      for (int i = 0; i < size; i++) {
        this.fieldsInfo[i] = input.readString();
      }
    }
  }
  
  public void setDataPoints(Object[] dataPoints) {}
  
  public Object[] getDataPoints()
  {
    return this.payload;
  }
}
