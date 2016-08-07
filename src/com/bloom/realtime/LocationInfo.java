package com.bloom.runtime;

import org.apache.commons.lang.builder.HashCodeBuilder;

public class LocationInfo
{
  public double latVal = 0.0D;
  public double longVal = 0.0D;
  public String city = null;
  public String zip = null;
  public String companyName = null;
  
  public LocationInfo() {}
  
  LocationInfo(double latVal, double longVal, String city, String zip, String companyName)
  {
    this.latVal = latVal;
    this.longVal = longVal;
    this.city = city;
    this.zip = zip;
    this.companyName = companyName;
  }
  
  public boolean equals(Object other)
  {
    if ((other instanceof LocationInfo))
    {
      LocationInfo li = (LocationInfo)other;
      if ((this.latVal == li.latVal) && (this.longVal == li.longVal)) {
        return true;
      }
    }
    return false;
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(this.latVal).append(this.longVal).toHashCode();
  }
  
  public String toString()
  {
    return super.toString();
  }
}
