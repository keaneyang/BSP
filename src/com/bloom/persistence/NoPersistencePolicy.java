package com.bloom.persistence;

import com.bloom.intf.PersistencePolicy;
import com.bloom.waction.Waction;

import java.util.Set;
import org.apache.log4j.Logger;

public class NoPersistencePolicy
  implements PersistencePolicy
{
  private static Logger logger = Logger.getLogger(NoPersistencePolicy.class);
  
  public boolean addWaction(Waction w)
  {
    if (logger.isDebugEnabled()) {
      logger.debug("NoPersistencePolicy ignoring waction: " + w);
    }
    return false;
  }
  
  public Set<Waction> getUnpersistedWactions()
  {
    return null;
  }
  
  public void flush() {}
  
  public void close() {}
}
