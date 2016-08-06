package com.bloom.intf;

import java.util.Set;

import com.bloom.waction.Waction;

public abstract interface PersistencePolicy
{
  public abstract boolean addWaction(Waction paramWaction)
    throws Exception;
  
  public abstract Set<Waction> getUnpersistedWactions();
  
  public abstract void flush();
  
  public abstract void close();
}

