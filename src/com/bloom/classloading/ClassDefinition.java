package com.bloom.classloading;

import java.io.Serializable;

public class ClassDefinition
  implements Serializable
{
  private static final long serialVersionUID = 4175403813634486465L;
  private String bundleName;
  private String className;
  private int classId;
  private byte[] byteCode;
  
  public ClassDefinition() {}
  
  public ClassDefinition(String bundleName, String className, int classId, byte[] byteCode)
  {
    this.bundleName = bundleName;
    this.className = className;
    this.classId = classId;
    this.byteCode = ((byte[])byteCode.clone());
  }
  
  public String getBundleName()
  {
    return this.bundleName;
  }
  
  public void setBundleName(String bundleName)
  {
    this.bundleName = bundleName;
  }
  
  public String getClassName()
  {
    return this.className;
  }
  
  public void setClassName(String className)
  {
    this.className = className;
  }
  
  public int getClassId()
  {
    return this.classId;
  }
  
  public void setClassId(int classId)
  {
    this.classId = classId;
  }
  
  public byte[] getByteCode()
  {
    return this.byteCode;
  }
  
  public void setByteCode(byte[] byteCode)
  {
    this.byteCode = ((byte[])byteCode.clone());
  }
}
