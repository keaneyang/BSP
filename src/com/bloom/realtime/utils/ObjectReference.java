package com.bloom.runtime.utils;

public class ObjectReference<T>
{
  private T value;
  
  public void set(T val)
  {
    this.value = val;
  }
  
  public T get()
  {
    return (T)this.value;
  }
}
