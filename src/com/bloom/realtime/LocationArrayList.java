package com.bloom.runtime;

import java.util.ArrayList;

public class LocationArrayList
{
  public ArrayList<LocationInfo> value;
  
  public void add(LocationInfo locationInfo)
  {
    synchronized (this.value)
    {
      this.value.add(locationInfo);
    }
  }
  
  public void remove(LocationInfo locationInfo)
  {
    synchronized (this.value)
    {
      this.value.remove(locationInfo);
    }
  }
  
  public LocationArrayList(LocationArrayList other)
  {
    this.value = new ArrayList(other.value);
  }
  
  public LocationArrayList()
  {
    this.value = new ArrayList();
  }
}
