package com.bloom.persistence;

import com.bloom.intf.PersistenceLayer;
import com.bloom.intf.PersistencePolicy;
import com.bloom.runtime.BaseServer;

import java.util.Map;
import org.apache.log4j.Logger;

public class PersistenceFactory
{
  private static Logger logger = Logger.getLogger(PersistenceFactory.class);
  
  public static enum PersistingPurpose
  {
    WACTIONSTORE,  MONITORING,  RECOVERY;
    
    private PersistingPurpose() {}
  }
  
  public static PersistenceLayer createPersistenceLayer(String target_database, PersistingPurpose purpose, String puname, Map<String, Object> props)
  {
    if (target_database == null) {
      target_database = "Auto";
    }
    if (target_database.equalsIgnoreCase("sqlmx")) {
      return new HibernatePersistenceLayerImpl(puname, props);
    }
    if (target_database.equalsIgnoreCase("ONDB")) {
      return new ONDBPersistenceLayerImpl(puname, props);
    }
    if (purpose == PersistingPurpose.MONITORING) {
      return new DefaultJPAPersistenceLayerImpl(puname, props);
    }
    if (purpose == PersistingPurpose.RECOVERY) {
      return new AppCheckpointPersistenceLayer(puname, props);
    }
    if (purpose == PersistingPurpose.WACTIONSTORE) {
      return new WactionStorePersistenceLayerImpl(puname, props);
    }
    return null;
  }
  
  public static PersistenceLayer createPersistenceLayerWithRetry(String target_database, PersistingPurpose purpose, String puname, Map<String, Object> props)
  {
    return new RetryPersistenceLayer(createPersistenceLayer(target_database, purpose, puname, props));
  }
  
  public static PersistencePolicy createPersistencePolicy(Long intervalMicroseconds, PersistenceLayer persistenceLayer, BaseServer srv, Map<String, Object> props, WactionStore ws)
  {
    if ((intervalMicroseconds == null) || (intervalMicroseconds.longValue() < 0L)) {
      return new NoPersistencePolicy();
    }
    if (intervalMicroseconds.longValue() == 0L) {
      return new ImmediatePersistencePolicy(persistenceLayer, ws);
    }
    long intervalMilliseconds = intervalMicroseconds.longValue() / 1000L;
    return new PeriodicPersistencePolicy(intervalMilliseconds, persistenceLayer, srv, props, ws);
  }
  
  public static LRUList createExpungeList(String expungePolicyDescription)
  {
    return new LRUList();
  }
}
