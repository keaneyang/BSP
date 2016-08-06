package com.bloom.persistence;

import com.bloom.intf.PersistenceLayer;
import com.bloom.intf.PersistencePolicy;
import com.bloom.intf.PersistenceLayer.Range;
import com.bloom.runtime.components.EntityType;
import com.bloom.waction.Waction;

import java.util.Set;
import org.apache.log4j.Logger;

public class ImmediatePersistencePolicy
  implements PersistencePolicy
{
  private static Logger logger = Logger.getLogger(ImmediatePersistencePolicy.class);
  private final PersistenceLayer persistenceLayer;
  private final WactionStore ws;
  
  public ImmediatePersistencePolicy(PersistenceLayer persistenceLayer, WactionStore ws)
  {
    this.persistenceLayer = persistenceLayer;
    this.ws = ws;
  }
  
  public boolean addWaction(Waction w)
    throws Exception
  {
    if (this.persistenceLayer != null)
    {
      PersistenceLayer.Range[] ranges = this.persistenceLayer.persist(w);
      if ((ranges == null) || (ranges.length == 0) || (!ranges[0].isSuccessful())) {
        this.ws.notifyAppMgr(EntityType.WACTIONSTORE, this.ws.getMetaName(), this.ws.getMetaID(), new Exception("Failed to persist waction: " + w), "Persist waction", new Object[] { w });
      }
      return true;
    }
    logger.error("Unable to persist waction immediately because there is no persistence layer");
    return false;
  }
  
  public Set<Waction> getUnpersistedWactions()
  {
    return null;
  }
  
  public void flush() {}
  
  public void close() {}
}
