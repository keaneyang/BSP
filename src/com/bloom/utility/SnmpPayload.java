package com.bloom.utility;

public class SnmpPayload
{
  byte[] snmpBytes;
  String snmpString;
  
  public SnmpPayload(byte[] b, String str)
  {
    this.snmpBytes = (b != null ? (byte[])b.clone() : null);
    this.snmpString = str;
  }
  
  public String getSnmpStringValue()
  {
    return this.snmpString;
  }
  
  public byte[] getSnmpByteValue()
  {
    return this.snmpBytes;
  }
}
