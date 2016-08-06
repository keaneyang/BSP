package com.bloom.metaRepository;

import com.bloom.intf.PersistenceLayer;
import com.bloom.persistence.PersistenceFactory;
import com.bloom.persistence.PersistenceFactory.PersistingPurpose;
import com.bloom.recovery.AppCheckpoint;
import com.bloom.runtime.Server;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Initializer;
import com.bloom.utility.Utility;
import com.bloom.uuid.UUID;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.bloom.recovery.Path;
import com.bloom.recovery.Position;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.PersistenceException;
import javax.persistence.RollbackException;
import org.apache.log4j.Logger;
import org.eclipse.persistence.exceptions.DatabaseException;
import org.eclipse.persistence.exceptions.PersistenceUnitLoadingException;

public class StatusDataStore
{
  private static Logger logger = Logger.getLogger(StatusDataStore.class);
  private static StatusDataStore instance;
  
  public static StatusDataStore getInstance()
  {
    if (instance == null) {
      synchronized (StatusDataStore.class)
      {
        if (instance == null) {
          instance = new StatusDataStore();
        }
      }
    }
    return instance;
  }
  
  private PersistenceLayer theDb = null;
  
  private PersistenceLayer getDb()
  {
    if (this.theDb == null) {
      synchronized (this)
      {
        if (this.theDb == null) {
          if (Server.persistenceIsEnabled()) {
            try
            {
              if (Logger.getLogger("Recovery").isDebugEnabled()) {
                Logger.getLogger("Recovery").debug("Attempting to get DB connection for ...");
              }
              IMap<String, MetaInfo.Initializer> startUpMap = HazelcastSingleton.get().getMap("#startUpMap");
              String clusterName = HazelcastSingleton.getClusterName();
              MetaInfo.Initializer initializer = (MetaInfo.Initializer)startUpMap.get(clusterName);
              
              String DBUname = initializer.MetaDataRepositoryUname;
              String DBPassword = initializer.MetaDataRepositoryPass;
              String DBName = initializer.MetaDataRepositoryDBname;
              String DBLocation = initializer.MetaDataRepositoryLocation;
              MetaDataDbProvider metaDataDbProvider = Server.getMetaDataDBProviderDetails();
              Map<String, Object> props = new HashMap();
              if ((DBUname != null) && (!DBUname.isEmpty())) {
                props.put("javax.persistence.jdbc.user", DBUname);
              }
              if ((DBPassword != null) && (!DBPassword.isEmpty())) {
                props.put("javax.persistence.jdbc.password", DBPassword);
              }
              if ((DBName != null) && (!DBName.isEmpty()) && (DBUname != null) && (!DBUname.isEmpty()) && (DBPassword != null) && (!DBPassword.isEmpty()) && (DBLocation != null) && (!DBLocation.isEmpty())) {
                props.put("javax.persistence.jdbc.url", metaDataDbProvider.getJDBCURL(DBLocation, DBName, DBUname, DBPassword));
              }
              PersistenceLayer pl = PersistenceFactory.createPersistenceLayerWithRetry(null, PersistenceFactory.PersistingPurpose.RECOVERY, "status-data-cache", props);
              pl.init();
              
              this.theDb = pl;
            }
            catch (PersistenceUnitLoadingException e)
            {
              logger.error("Error initializing the Bloom App Checkpoint persistence unit, which indicates a bad configuration", e);
            }
            catch (Exception e)
            {
              logger.error("Unexpected error when building database connection: " + e.getMessage());
            }
          } else if (Logger.getLogger("Recovery").isInfoEnabled()) {
            Logger.getLogger("Recovery").info("Bloom persistence is OFF; turn it on by specifying com.bloom.config.persist=True");
          }
        }
      }
    }
    return this.theDb;
  }
  
  public synchronized boolean putAppCheckpoint(UUID flowUuid, Position checkpointPosition)
  {
    try
    {
      PersistenceLayer db = getDb();
      if (db == null)
      {
        logger.warn("Could not write source restart position because DB connection could not be established");
        return false;
      }
      List<AppCheckpoint> appCheckpoints = new ArrayList();
      for (Path path : checkpointPosition.values()) {
        appCheckpoints.add(new AppCheckpoint(flowUuid, path.getPathItems(), path.getSourcePosition()));
      }
      db.persist(appCheckpoints);
      if (Logger.getLogger("Recovery").isDebugEnabled())
      {
        Logger.getLogger("Recovery").debug("Successfully Wrote App Checkpoint:");
        Utility.prettyPrint(checkpointPosition);
      }
      return true;
    }
    catch (DatabaseException e)
    {
      logger.error("Unable to write status data to table because of a communication failure: " + e.getMessage());
      return false;
    }
    catch (RollbackException e)
    {
      logger.error("Rolling back attempt to write status data to table: " + e.getMessage());
      return false;
    }
    catch (PersistenceException e)
    {
      logger.error("Persistence error during attempt to write status data to table: " + e.getMessage());
      return false;
    }
    catch (Exception e)
    {
      logger.error("Unable to write status data to table: " + e.getMessage());
    }
    return false;
  }
  
  public synchronized boolean trimAppCheckpoint(UUID flowUuid, Position checkpointPosition)
  {
    try
    {
      PersistenceLayer db = getDb();
      if (db == null)
      {
        Logger.getLogger("Recovery").warn("Could not trim source restart position because DB connection could not be established");
        return false;
      }
      List<AppCheckpoint> appCheckpoints = new ArrayList();
      for (Path path : checkpointPosition.values()) {
        appCheckpoints.add(new AppCheckpoint(flowUuid, path.getPathItems(), path.getSourcePosition()));
      }
      PersistenceLayer pl = db;
      pl.delete(appCheckpoints);
      
      return true;
    }
    catch (DatabaseException e)
    {
      logger.error("Unable to trim app checkpoint because of a communication failure: " + e.getMessage());
      return false;
    }
    catch (RollbackException e)
    {
      logger.error("Rolling back attempt to trim app checkpoint: " + e.getMessage());
      return false;
    }
    catch (PersistenceException e)
    {
      logger.error("Persistence error during attempt to trim app checkpoint: " + e.getMessage());
      return false;
    }
    catch (Exception e)
    {
      logger.error("Unable to delete from app checkpoint: " + e.getMessage());
    }
    return false;
  }
  
  public Position getAppCheckpoint(UUID appUuid)
  {
    try
    {
      PersistenceLayer db = getDb();
      if (db == null)
      {
        logger.warn("Could not get App checkpoint because DB connection could not be established");
        return null;
      }
      String query = "SELECT c FROM AppCheckpoint c WHERE c.flowUuid=:flowUuid";
      
      Map<String, Object> params = new HashMap();
      params.put("flowUuid", appUuid.getUUIDString());
      List<?> queryResults = db.runQuery(query, params, null);
      if ((queryResults == null) || (queryResults.size() == 0)) {
        return null;
      }
      Set<Path> paths = new HashSet();
      for (Object ob : queryResults) {
        if (!(ob instanceof AppCheckpoint))
        {
          logger.warn("Unexpected data type when App Checkpoint expected: " + ob.getClass());
        }
        else
        {
          AppCheckpoint c = (AppCheckpoint)ob;
          paths.add(new Path(c.pathItems, c.sourcePosition));
        }
      }
      return new Position(paths);
    }
    catch (DatabaseException e)
    {
      logger.error("Unable to write status data to table because of a communication failure: " + e.getMessage());
      return null;
    }
    catch (RollbackException e)
    {
      logger.error("Rolling back attempt to write status data to table: " + e.getMessage());
      return null;
    }
    catch (PersistenceException e)
    {
      logger.error("Persistence error during attempt to write status data to table: " + e.getMessage());
      return null;
    }
    catch (Exception e)
    {
      logger.error("Unable to write status data to table: " + e.getMessage());
    }
    return null;
  }
  
  public synchronized void clearAppCheckpoint(UUID appUuid)
  {
    try
    {
      PersistenceLayer db = getDb();
      if (db == null) {
        Logger.getLogger("Recovery").warn("Could not clear App checkpoint because DB connection could not be established");
      }
      String query = "DELETE FROM AppCheckpoint WHERE flowUuid=:flowUuid";
      
      Map<String, Object> params = new HashMap();
      params.put("flowUuid", appUuid.getUUIDString());
      int rowsDeleted = db.executeUpdate(query, params);
      if (Logger.getLogger("Recovery").isInfoEnabled()) {
        Logger.getLogger("Recovery").info("Cleared the " + rowsDeleted + " rows of the App Checkpoint for UUID=" + appUuid);
      }
    }
    catch (DatabaseException e)
    {
      logger.error("Unable to write status data to table because of a communication failure: " + e.getMessage());
    }
    catch (RollbackException e)
    {
      logger.error("Rolling back attempt to write status data to table: " + e.getMessage());
    }
    catch (PersistenceException e)
    {
      logger.error("Persistence error during attempt to write status data to table: " + e.getMessage());
    }
    catch (Exception e)
    {
      logger.error("Unable to write status data to table: " + e.getMessage());
    }
  }
  
  public boolean databaseIsAvailable()
  {
    boolean result = getDb() != null;
    return result;
  }
}
