package com.bloom.runtime;

import com.bloom.Wizard.ICDCWizard;
import com.bloom.anno.NotSet;
import com.bloom.anno.PropertyTemplate;
import com.bloom.anno.PropertyTemplateProperty;
import com.bloom.appmanager.AppManager;
import com.bloom.appmanager.AppManagerServerLocation;
import com.bloom.classloading.WALoader;
import com.bloom.discovery.UdpDiscoveryServer;
import com.bloom.distribution.WAQueue;
import com.bloom.distribution.WAQueue.Listener;
import com.bloom.drop.DropMetaObject;
import com.bloom.exception.FatalException;
import com.bloom.exception.ServerException;
import com.bloom.fileSystem.FileSystemBrowserServlet;
import com.bloom.fileSystem.FileUploaderServlet;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.metaRepository.RemoteCall;
import com.bloom.metaRepository.StatusDataStore;
import com.bloom.persistence.WactionStore;
import com.bloom.preview.DataPreviewServlet;
import com.bloom.proc.BaseProcess;
import com.bloom.proc.SourceProcess;
import com.bloom.proc.StreamReader;
import com.bloom.rest.HealthServlet;
import com.bloom.rest.MetadataRepositoryServlet;
import com.bloom.rest.SecurityManagerServlet;
import com.bloom.rest.StreamInfoServlet;
import com.bloom.runtime.channels.Channel;
import com.bloom.runtime.channels.ZMQChannel;
import com.bloom.runtime.compiler.select.RSFieldDesc;
import com.bloom.runtime.compiler.stmts.DeploymentRule;
import com.bloom.runtime.compiler.stmts.RecoveryDescription;
import com.bloom.runtime.components.CQTask;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.components.Flow;
import com.bloom.runtime.components.FlowComponent;
import com.bloom.runtime.components.IStreamGenerator;
import com.bloom.runtime.components.IWindow;
import com.bloom.runtime.components.Publisher;
import com.bloom.runtime.components.Restartable;
import com.bloom.runtime.components.Sorter;
import com.bloom.runtime.components.Source;
import com.bloom.runtime.components.Stream;
import com.bloom.runtime.components.Subscriber;
import com.bloom.runtime.components.Target;
import com.bloom.runtime.components.WAStoreView;
import com.bloom.runtime.deployment.Constant;
import com.bloom.runtime.meta.CQExecutionPlan;
import com.bloom.runtime.meta.ImplicitApplicationBuilder;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.MetaInfo.CQ;
import com.bloom.runtime.meta.MetaInfo.Cache;
import com.bloom.runtime.meta.MetaInfo.DeploymentGroup;
import com.bloom.runtime.meta.MetaInfo.Initializer;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.MetaObjectInfo;
import com.bloom.runtime.meta.MetaInfo.Namespace;
import com.bloom.runtime.meta.MetaInfo.PropertyDef;
import com.bloom.runtime.meta.MetaInfo.PropertySet;
import com.bloom.runtime.meta.MetaInfo.PropertyTemplateInfo;
import com.bloom.runtime.meta.MetaInfo.Query;
import com.bloom.runtime.meta.MetaInfo.Role;
import com.bloom.runtime.meta.MetaInfo.Server;
import com.bloom.runtime.meta.MetaInfo.ShowStream;
import com.bloom.runtime.meta.MetaInfo.StatusInfo;
import com.bloom.runtime.meta.MetaInfo.StreamGenerator;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.meta.MetaInfo.User;
import com.bloom.runtime.meta.MetaInfo.WActionStore;
import com.bloom.runtime.meta.MetaInfo.Window;
import com.bloom.runtime.meta.MetaInfo.Flow.Detail;
import com.bloom.runtime.meta.MetaInfo.StatusInfo.Status;
import com.bloom.runtime.monitor.LiveObjectViews;
import com.bloom.runtime.monitor.MonitorModel;
import com.bloom.runtime.monitor.Monitorable;
import com.bloom.runtime.utils.Factory;
import com.bloom.runtime.utils.NamePolicy;
import com.bloom.runtime.utils.NetLogger;
import com.bloom.security.ObjectPermission;
import com.bloom.security.SessionInfo;
import com.bloom.security.WASecurityManager;
import com.bloom.security.ObjectPermission.Action;
import com.bloom.security.ObjectPermission.ObjectType;
import com.bloom.usagemetrics.api.Usage;
import com.bloom.utility.DeployUtility;
import com.bloom.utility.GraphUtility;
import com.bloom.utility.Utility;
import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;
import com.bloom.waction.WactionApi;
import com.bloom.waction.WactionApiServlet;
import com.bloom.wactionstore.WActionStores;
import com.bloom.web.RMIWebSocket;
import com.bloom.web.WebServer;
import com.bloom.web.RMIWebSocket.Handler;
import com.google.common.collect.Sets;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.ClientService;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Endpoint;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.bloom.license.LicenseManager;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import javassist.Modifier;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;

public class Server
  extends BaseServer
  implements ShowStreamExecutor, LiveObjectViews, Monitorable, EntryListener<String, Object>, WAQueue.Listener, ClientListener
{
  public static final String defaultDeploymentGroupName = "default";
  public static final String SERVER_LIST_LOCK = "ServerList";
  private static Logger logger = Logger.getLogger(Server.class);
  private final ExecutorService pool;
  private final ScheduledThreadPoolExecutor servicesScheduler;
  private final Map<UUID, HashMap<UUID, Pair<Publisher, Subscriber>>> userQueryRecords = new HashMap();
  private static IMap<UUID, UUID> nodeIDToAuthToken;
  private final AuthToken sessionID;
  private final UUID serverID;
  public static final int shutdownTimeout = 5;
  private AppManager appManager;
  public MetaInfo.Server ServerInfo;
  public WASecurityManager security_manager;
  private IMap<UUID, List<UUID>> serverToDeploymentGroup;
  private final MetaInfo.Initializer initializer;
  
  public static Server getServer()
  {
    return server;
  }
  
  public AppManager getAppManager()
  {
    return this.appManager;
  }
  
  public void startAppManager()
    throws Exception
  {
    this.appManager = new AppManager(this);
  }
  
  private native void initSimbaServer(String paramString);
  
  private native void setClassLoader(Object paramObject);
  
  public Server()
  {
    BaseServer.setMetaDataDbProviderDetails();
    this.initializer = startUptheNode();
    this.pool = Executors.newCachedThreadPool(new CustomThreadFactory("BaseServer_WorkingThread"));
    
    this.servicesScheduler = new ScheduledThreadPoolExecutor(2, new CustomThreadFactory("BaseServer_serviceScheduler", true));
    
    scheduleStatsReporting(statsReporter("main", getScheduler()), 5, true);
    
    this.sessionID = WASecurityManager.TOKEN;
    this.serverID = HazelcastSingleton.getNodeId();
    initializeMetaData();
    try
    {
      this.metadataRepository.initialize();
    }
    catch (MetaDataRepositoryException e)
    {
      logger.error(e.getMessage());
      System.exit(0);
    }
    baseServer = this;
  }
  
  public void initializeHazelcast()
  {
    HazelcastInstance hz = HazelcastSingleton.get();
    hz.getClientService().addClientListener(this);
    InetSocketAddress adr = hz.getCluster().getLocalMember().getInetSocketAddress();
    logger.info("Hazelcast cluster has " + hz.getCluster().getMembers().size() + " members");
    long serverId = hz.getIdGenerator("#serverids").newId();
    logger.info("Generated server Id for this server is: " + serverId);
    String version = Version.getPOMVersionString();
    int cpuNum = Runtime.getRuntime().availableProcessors();
    String macAddress = HazelcastSingleton.getMacID();
    this.serverToDeploymentGroup = HazelcastSingleton.get().getMap("#serverToDeploymentGroup");
    nodeIDToAuthToken = HazelcastSingleton.get().getMap("#nodeIDToAuthToken");
    this.ServerInfo = new MetaInfo.Server();
    this.ServerInfo.construct(this.serverID, getServerName(), cpuNum, version, this.initializer, serverId, false);
    this.ServerInfo.setMacAdd(macAddress);
    this.ServerInfo.setUri(NamePolicy.makeKey("Global:" + EntityType.SERVER + ":" + getServerName() + ":" + "1"));
    
    MDC.put("ServerToken", this.ServerInfo.name);
    
    IMap<String, Object> clusterSettings = HazelcastSingleton.get().getMap("#ClusterSettings");
    
    clusterSettings.addEntryListener(this, true);
    initializeMessaging();
  }
  
  public static Runnable statsReporter(final String name, ThreadPoolExecutor pool)
  {
    new Runnable()
    {
      public void run()
      {
        long active = this.val$pool.getActiveCount();
        long waiting = this.val$pool.getTaskCount();
        long completed = this.val$pool.getCompletedTaskCount();
        int queueSize = this.val$pool.getQueue().size();
        NetLogger.out().println(name + " scheduler tasks: active=" + active + " waiting=" + waiting + " completed=" + completed + " queueSize=" + queueSize);
      }
    };
  }
  
  private void checkVersion()
  {
    String curNodeVersion = Version.getPOMVersionString();
    if (logger.isInfoEnabled()) {
      logger.info("Current node version (read from file " + Version.getBuildPropFileName() + ") : " + curNodeVersion);
    }
    if (persistenceIsEnabled()) {
      if (Version.isConnectedToCompatibleMetaData())
      {
        if (logger.isInfoEnabled()) {
          logger.info("Connected to metadata version : " + Version.getMetaDataVersion());
        }
      }
      else
      {
        logger.warn(">>> Server with version:'" + curNodeVersion + "' is connecting to a incompatible metadata with version:'" + Version.getMetaDataVersion() + "'.");
        System.out.println(">>> Server with version:'" + curNodeVersion + "' is connecting to a incompatible metadata with version:'" + Version.getMetaDataVersion() + "'.");
        System.out.println(">>> initiating shutdown....");
        
        initiateShutdown(" because, current node  version is:" + curNodeVersion + ", and metadata you trying to connect is of different version:" + Version.getMetaDataVersion());
      }
    }
    IMap<UUID, MetaInfo.Server> servers = HazelcastSingleton.get().getMap("#servers");
    if ((servers != null) && (servers.size() > 0))
    {
      if (logger.isInfoEnabled()) {
        logger.info("Checking version before connecting to an existing node/cluster. Number of servers+agents in cluster: " + servers.size());
      }
      Set<Map.Entry<UUID, MetaInfo.Server>> set = servers.entrySet();
      Iterator<Map.Entry<UUID, MetaInfo.Server>> iter = set.iterator();
      List<MetaInfo.Server> serversList = new ArrayList();
      while (iter.hasNext())
      {
        MetaInfo.Server temp = (MetaInfo.Server)((Map.Entry)iter.next()).getValue();
        if (!temp.isAgent) {
          serversList.add(temp);
        }
      }
      if (serversList.isEmpty()) {
        return;
      }
      if (logger.isInfoEnabled()) {
        logger.info("Total number of servers in cluster are: " + serversList.size());
      }
      MetaInfo.Server firstServer = (MetaInfo.Server)serversList.get(0);
      String clusterVersion = firstServer.version;
      if (logger.isInfoEnabled()) {
        logger.info("Matching with the version of first server from cluster running on URI :" + firstServer.webBaseUri);
      }
      if (curNodeVersion.equalsIgnoreCase(clusterVersion))
      {
        if (logger.isInfoEnabled()) {
          logger.info("Joining cluster with version:" + clusterVersion);
        }
      }
      else
      {
        logger.warn(">>> Cluster version:'" + clusterVersion + "'  and current node version:'" + curNodeVersion + "'  are NOT compatible.");
        System.out.println(">>> Cluster version '" + clusterVersion + "'  and current node version '" + curNodeVersion + "'  are NOT compatible.");
        System.out.println(">>> initiating shutdown...");
        
        initiateShutdown(" because, current node version is:" + curNodeVersion + ", and cluster you trying to connect is of different version:" + clusterVersion);
      }
    }
    else if (logger.isInfoEnabled())
    {
      logger.info("First node. No version compatability check necessary.");
    }
  }
  
  private static void initiateShutdown(String reason)
  {
    String finalReason = reason;
    try
    {
      Thread t = new Thread(new Runnable()
      {
        public void run()
        {
          try
          {
            Server.shutDown(this.val$finalReason);
          }
          catch (Exception e)
          {
            Server.logger.warn("Problem shutting down", e);
          }
        }
      });
      Runtime.getRuntime().addShutdownHook(t);
      System.exit(0);
    }
    catch (Exception e)
    {
      logger.error("error shutting down server", e);
    }
  }
  
  private void setListeners()
  {
    final Server srv = this;
    
    EntryListener<String, MetaInfo.MetaObject> listener = new EntryListener()
    {
      public void entryAdded(EntryEvent<String, MetaInfo.MetaObject> e)
      {
        try
        {
          srv.addedOrUpdated((MetaInfo.MetaObject)e.getValue(), (MetaInfo.MetaObject)e.getOldValue(), false);
        }
        catch (MetaDataRepositoryException e1) {}
      }
      
      public void entryEvicted(EntryEvent<String, MetaInfo.MetaObject> e) {}
      
      public void entryRemoved(EntryEvent<String, MetaInfo.MetaObject> e)
      {
        try
        {
          srv.removed((MetaInfo.MetaObject)e.getOldValue());
        }
        catch (MetaDataRepositoryException e1) {}
      }
      
      public void entryUpdated(EntryEvent<String, MetaInfo.MetaObject> e)
      {
        try
        {
          srv.addedOrUpdated((MetaInfo.MetaObject)e.getValue(), (MetaInfo.MetaObject)e.getOldValue(), true);
        }
        catch (MetaDataRepositoryException e1) {}
      }
      
      public void mapEvicted(MapEvent event) {}
      
      public void mapCleared(MapEvent event) {}
    };
    this.metadataRepository.registerListenerForMetaObject(listener);
    
    EntryListener<UUID, MetaInfo.StatusInfo> statusListener = new StatusListener();
    this.metadataRepository.registerListenerForStatusInfo(statusListener);
    
    MessageListener<MetaInfo.ShowStream> showstreamlistener = new MessageListener()
    {
      public void onMessage(Message<MetaInfo.ShowStream> a)
      {
        try
        {
          srv.showStreamStmt((MetaInfo.ShowStream)a.getMessageObject());
        }
        catch (Exception e) {}
      }
    };
    this.metadataRepository.registerListenerForShowStream(showstreamlistener);
  }
  
  class ClusterMemberListener
    implements MembershipListener
  {
    ClusterMemberListener() {}
    
    public void memberAdded(MembershipEvent membershipEvent) {}
    
    public void memberRemoved(MembershipEvent membershipEvent)
    {
      ILock lock = HazelcastSingleton.get().getLock("ServerList");
      lock.lock();
      try
      {
        Member removedMember = membershipEvent.getMember();
        AppManagerServerLocation.removeAppManagerServerId(new UUID(removedMember.getUuid()));
        MetaInfo.Server removedServer = (MetaInfo.Server)Server.this.metadataRepository.getMetaObjectByUUID(new UUID(removedMember.getUuid()), Server.this.sessionID);
        Set<Member> members = HazelcastSingleton.get().getCluster().getMembers();
        
        List<UUID> valid = new ArrayList();
        for (Member member : members) {
          valid.add(new UUID(member.getUuid()));
        }
        List<UUID> toRemove = new ArrayList();
        
        Set<MetaInfo.MetaObject> servers = Server.this.metadataRepository.getByEntityType(EntityType.SERVER, Server.this.sessionID);
        for (MetaInfo.MetaObject obj : servers)
        {
          MetaInfo.Server server = (MetaInfo.Server)obj;
          if ((!valid.contains(server.uuid)) && (!server.isAgent)) {
            toRemove.add(server.uuid);
          }
        }
        for (UUID uuid : toRemove)
        {
          if (Server.logger.isDebugEnabled()) {
            Server.logger.debug("Trying to remove : " + uuid + " from \n" + valid);
          }
          Server.this.metadataRepository.removeMetaObjectByUUID(uuid, Server.this.sessionID);
        }
        Server.this.removeServerFromDeploymentGroup(removedMember);
        Server.this.nodeFailover();
        RMIWebSocket.cleanupLeftUsers(membershipEvent.getMember().getUuid());
      }
      catch (MetaDataRepositoryException e) {}catch (Throwable e)
      {
        Server.logger.error("Failed to remove member with exception " + e);
      }
      finally
      {
        lock.unlock();
      }
    }
    
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {}
  }
  
  private void removeServerFromDeploymentGroup(Endpoint removedMember)
    throws MetaDataRepositoryException
  {
    List<UUID> deploymentGroups = (List)this.serverToDeploymentGroup.get(new UUID(removedMember.getUuid()));
    if (deploymentGroups == null) {
      return;
    }
    MetaInfo.DeploymentGroup removedServerDeploymentGroup = null;
    for (UUID deploymentGroup : deploymentGroups)
    {
      removedServerDeploymentGroup = (MetaInfo.DeploymentGroup)this.metadataRepository.getMetaObjectByUUID(deploymentGroup, this.sessionID);
      removedServerDeploymentGroup.removeMember(new UUID(removedMember.getUuid()));
      if (logger.isInfoEnabled()) {
        logger.info("Node " + removedMember.getSocketAddress() + " is removed from deployment group " + removedServerDeploymentGroup);
      }
      this.metadataRepository.putMetaObject(removedServerDeploymentGroup, this.sessionID);
    }
    this.serverToDeploymentGroup.remove(new UUID(removedMember.getUuid()));
  }
  
  private void nodeFailover()
    throws Exception
  {
    Set<MetaInfo.Flow> applications = this.metadataRepository.getByEntityType(EntityType.APPLICATION, this.sessionID);
    for (MetaInfo.Flow flowMetaInfo : applications) {
      if (flowMetaInfo.deploymentPlan != null)
      {
        MetaInfo.StatusInfo statusInfo = MetadataRepository.getINSTANCE().getStatusInfo(flowMetaInfo.getUuid(), WASecurityManager.TOKEN);
        if ((statusInfo != null) && (statusInfo.status != null) && (!statusInfo.status.equals(MetaInfo.StatusInfo.Status.CREATED)) && (!statusInfo.status.equals(MetaInfo.StatusInfo.Status.UNKNOWN)))
        {
          if (logger.isDebugEnabled()) {
            logger.debug(flowMetaInfo.name + " before deploy");
          }
          try
          {
            deployFlow(flowMetaInfo, true);
          }
          catch (Exception e)
          {
            if (logger.isInfoEnabled()) {
              logger.info(flowMetaInfo.type + " already deployed on this server  " + flowMetaInfo.name + ":" + flowMetaInfo.type + ":" + flowMetaInfo.uuid);
            }
          }
          continue;
          if (logger.isDebugEnabled()) {
            logger.debug(flowMetaInfo.name + " after deploy");
          }
        }
      }
    }
  }
  
  public static class AdhocCleanup
    implements RemoteCall<Object>
  {
    private static final long serialVersionUID = -44213816144550014L;
    String consoleQueueName;
    
    public AdhocCleanup(String consoleQueueName)
    {
      this.consoleQueueName = consoleQueueName;
    }
    
    public Object call()
      throws Exception
    {
      return Server.server.adhocCleanup(this.consoleQueueName, null);
    }
  }
  
  public Object adhocCleanup(String authToken, UUID queryApplicationUUID)
    throws MetaDataRepositoryException
  {
    LocalLockProvider.Key keyForLock = new LocalLockProvider.Key(authToken, null);
    Lock lock = LocalLockProvider.getLock(keyForLock);
    lock.lock();
    if (logger.isDebugEnabled()) {
      logger.debug(Thread.currentThread().getName() + " : Clearing adhocs for auth :" + authToken);
    }
    try
    {
      Set<UUID> set = getAnyMoreLeftOverAuthTokens();
      set.add(new UUID(authToken));
      if (logger.isDebugEnabled()) {
        logger.debug(Thread.currentThread().getName() + " : Clearing adhocs for auth :" + authToken);
      }
      for (UUID clientSessionID : set) {
        if (queryApplicationUUID == null)
        {
          Context ctx = createContext(new AuthToken(clientSessionID));
          
          Set<UUID> userQueryApplicationUUIDs = new HashSet();
          HashMap<UUID, Pair<Publisher, Subscriber>> uuidPairHashMap = (HashMap)this.userQueryRecords.get(clientSessionID);
          if (uuidPairHashMap != null) {
            for (UUID key : uuidPairHashMap.keySet()) {
              userQueryApplicationUUIDs.add(key);
            }
          }
          Set<UUID> userQueryUUIDs = new HashSet();
          for (UUID uuid : userQueryApplicationUUIDs)
          {
            MetaInfo.Flow flowMetaObject = (MetaInfo.Flow)getMetaObject(uuid);
            if (flowMetaObject != null)
            {
              if (logger.isDebugEnabled()) {
                logger.debug(Thread.currentThread().getName() + " : Calling stop & undeploy on :" + flowMetaObject.name);
              }
              UUID queryMetaObjectUUID = (UUID)flowMetaObject.getReverseIndexObjectDependencies().iterator().next();
              userQueryUUIDs.add(queryMetaObjectUUID);
              try
              {
                stopUndeployQuery(flowMetaObject, clientSessionID, null);
              }
              catch (Exception e)
              {
                logger.warn(e.getMessage());
              }
            }
          }
          for (UUID queryMetaObjectUUID : userQueryUUIDs)
          {
            MetaInfo.Query query = (MetaInfo.Query)getObject(queryMetaObjectUUID);
            ctx.deleteQuery(query, this.sessionID);
          }
          this.userQueryRecords.remove(clientSessionID);
        }
      }
    }
    finally
    {
      lock.unlock();
      LocalLockProvider.removeLock(keyForLock);
    }
    if (logger.isDebugEnabled()) {
      logger.debug(Thread.currentThread().getName() + " : Clearing adhocs for auth :" + authToken + " done.");
    }
    return null;
  }
  
  private Set<UUID> getAnyMoreLeftOverAuthTokens()
  {
    Set<UUID> set = new HashSet();
    for (UUID uuid : this.userQueryRecords.keySet()) {
      if ((!nodeIDToAuthToken.containsValue(uuid)) && (!WASecurityManager.get().isAuthenticated(new AuthToken(uuid)))) {
        set.add(uuid);
      }
    }
    return set;
  }
  
  private int countCaches(MetaInfo.Flow flow)
    throws MetaDataRepositoryException
  {
    int count = flow.getObjects(EntityType.CACHE).size();
    for (UUID id : flow.getObjects(EntityType.FLOW))
    {
      MetaInfo.Flow subflow = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByUUID(id, this.sessionID);
      count += countCaches(subflow);
    }
    return count;
  }
  
  protected void setSecMgrAndOther()
    throws Exception
  {
    this.security_manager = WASecurityManager.get();
    if (!persistenceIsEnabled())
    {
      MetaInfo.Namespace global = MetaInfo.GlobalNamespace;
      putObject(global);
    }
    loadPropertyTemplates();
    initializeDeploymentGroups();
    setListeners();
    
    ILock lock = HazelcastSingleton.get().getLock("ServerList");
    lock.lock();
    
    int totalCpus = 0;
    int allowedCpus = LicenseManager.get().getClusterSize();
    try
    {
      createAdminUserAndRoleSetup();
      
      Set<MetaInfo.MetaObject> servers = this.metadataRepository.getByEntityType(EntityType.SERVER, this.sessionID);
      if (servers == null) {
        servers = new HashSet();
      }
      if (!servers.contains(this.ServerInfo)) {
        servers.add(this.ServerInfo);
      }
      int numberOfNodesAllowed = LicenseManager.get().getNumberOfNodes();
      if ((numberOfNodesAllowed != -1) && (servers.size() > numberOfNodesAllowed))
      {
        String message = String.format("\nCannot add this bloom server to the cluster because it would bring the total number of servers to %d.\nThe current license allows up to %d server(s) in the cluster.\n", new Object[] { Integer.valueOf(servers.size()), Integer.valueOf(numberOfNodesAllowed) });
        
        printf(message);
        lock.unlock();
        System.exit(88);
      }
      printf("Servers in cluster: \n");
      totalCpus = checkForNumCpusAllowed(servers);
      if (totalCpus > allowedCpus)
      {
        String message = String.format("\nCannot add this bloom server to the cluster because it would bring the total number of CPUs to %d.\nThe current license allows up to %d CPU(s) in the cluster.\n", new Object[] { Integer.valueOf(totalCpus), Integer.valueOf(allowedCpus) });
        
        printf(message);
        lock.unlock();
        System.exit(88);
      }
      else if (logger.isInfoEnabled())
      {
        logger.info("Total capacity used " + totalCpus + " of " + allowedCpus + " cpus - " + (allowedCpus - totalCpus) + " remaining\n");
      }
      this.metadataRepository.putServer(this.ServerInfo, WASecurityManager.TOKEN);
    }
    finally
    {
      lock.unlock();
    }
    loadExistingObjects();
    insertDefaultKafkaPropertySet();
    HazelcastSingleton.get().getCluster().addMembershipListener(new ClusterMemberListener());
  }
  
  private void insertDefaultKafkaPropertySet()
    throws MetaDataRepositoryException
  {
    MetaInfo.PropertySet kpset = (MetaInfo.PropertySet)getObject(EntityType.PROPERTYSET, "Global", "DefaultKafkaProperties");
    if (kpset == null)
    {
      kpset = new MetaInfo.PropertySet();
      Map default_properties = new HashMap(2);
      String zk_address = System.getProperty("com.bloom.config.zkAddress", "localhost:2181");
      String broker_address = System.getProperty("com.bloom.config.brokerAddress", "localhost:9092");
      
      default_properties.put("zk.address", zk_address);
      default_properties.put("bootstrap.brokers", broker_address);
      default_properties.put("jmx.broker", "localhost:9998");
      
      kpset.construct("DefaultKafkaProperties", MetaInfo.GlobalNamespace, default_properties);
      putObject(kpset);
    }
  }
  
  public int checkForNumCpusAllowed(Set<MetaInfo.MetaObject> servers)
  {
    int totalCpus = 0;
    TreeMap<String, Integer> map = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    for (MetaInfo.MetaObject obj : servers)
    {
      MetaInfo.Server s = (MetaInfo.Server)obj;
      if (!s.isAgent)
      {
        if (s.uuid.equals(this.ServerInfo.uuid)) {
          printf("  [this] ");
        } else {
          printf("         ");
        }
        printf(s.name + " [" + s.uuid + "] \n");
        if (logger.isInfoEnabled()) {
          logger.info(" capacity" + s.numCpus + "\n");
        }
        String key = s.macAdd;
        Integer val = Integer.valueOf(s.numCpus);
        if (!map.containsKey(key)) {
          map.put(key, val);
        } else if ((map.containsKey(key)) && (val.intValue() > ((Integer)map.get(key)).intValue())) {
          map.put(key, val);
        }
      }
    }
    for (String key : map.keySet()) {
      totalCpus += ((Integer)map.get(key)).intValue();
    }
    return totalCpus;
  }
  
  private MetaInfo.Initializer startUptheNode()
  {
    NodeStartUp nsu = null;
    MetaInfo.Initializer ini = null;
    boolean persist = persistenceIsEnabled();
    if (logger.isDebugEnabled()) {
      logger.debug("persist : " + persist);
    }
    nsu = new NodeStartUp(persist);
    if (nsu.isNodeStarted())
    {
      ini = nsu.getInitializer();
      if (persist)
      {
        if (logger.isDebugEnabled()) {
          logger.debug("Startup details : " + ini.MetaDataRepositoryLocation + " , " + ini.MetaDataRepositoryDBname + " , " + ini.MetaDataRepositoryUname + " , " + ini.MetaDataRepositoryPass);
        }
        HazelcastSingleton.setDBDetailsForMetaDataRepository(ini.MetaDataRepositoryLocation, ini.MetaDataRepositoryDBname, ini.MetaDataRepositoryUname, ini.MetaDataRepositoryPass);
      }
      printf("Current node started in cluster : " + ini.WAClusterName + ", " + (persist ? "with" : "without") + " Metadata Repository \n");
      
      printf("Registered to: " + ini.CompanyName + "\nProductKey: " + ini.ProductKey + "\n");
      
      printf("License Key: " + ini.LicenseKey + "\n");
      
      LicenseManager lm = LicenseManager.get();
      try
      {
        lm.setProductKey(ini.CompanyName, ini.ProductKey);
      }
      catch (RuntimeException e)
      {
        lm.setProductKey(ini.WAClusterName, ini.ProductKey);
      }
      lm.setLicenseKey(ini.LicenseKey);
      if (lm.isExpired())
      {
        printf("License " + lm.getExpiry() + "! Please obtain a new license.\n");
        
        System.exit(99);
      }
      printf("License " + lm.getExpiry() + "\n");
    }
    return ini;
  }
  
  private static Collection<String> getDeploymentGroupsProperty()
  {
    Set<String> ret = new HashSet();
    String dg = System.getProperty("com.bloom.deploymentGroups");
    if (dg != null) {
      ret.addAll(Arrays.asList(dg.split(",")));
    }
    if (!ret.contains("default")) {
      ret.add("default");
    }
    return ret;
  }
  
  private void initializeDeploymentGroups()
    throws MetaDataRepositoryException
  {
    ILock lock = HazelcastSingleton.get().getLock("initializeDGLock");
    lock.lock();
    try
    {
      List<UUID> dgList = new ArrayList();
      for (String dgname : getDeploymentGroupsProperty())
      {
        MetaInfo.DeploymentGroup dg = getDeploymentGroupByName(dgname);
        if (dg == null)
        {
          dg = new MetaInfo.DeploymentGroup();
          dg.construct(dgname);
          putObject(dg);
        }
        dg.addMembers(Collections.singletonList(this.serverID), this.ServerInfo.getId());
        this.ServerInfo.deploymentGroupsIDs.add(dg.uuid);
        updateObject(dg);
        dgList.add(dg.uuid);
      }
      this.serverToDeploymentGroup.put(this.serverID, dgList);
      if (logger.isInfoEnabled()) {
        logger.info("Server " + this.serverID + " is in deployment groups " + getDeploymentGroups());
      }
    }
    finally
    {
      lock.unlock();
    }
  }
  
  public List<String> getDeploymentGroups()
  {
    List<String> ret = new ArrayList();
    for (UUID id : this.ServerInfo.deploymentGroupsIDs) {
      try
      {
        MetaInfo.DeploymentGroup dg = getDeploymentGroupByID(id);
        ret.add(dg.name);
      }
      catch (ServerException e)
      {
        logger.error(e, e);
      }
      catch (MetaDataRepositoryException e)
      {
        logger.error(e.getMessage());
      }
    }
    return ret;
  }
  
  private void createAdminUserAndRoleSetup()
    throws Exception
  {
    if (this.security_manager.getUser("admin") == null)
    {
      String adminPassword = System.getProperty("com.bloom.config.adminPassword", System.getenv("WA_ADMIN_PASSWORD"));
      
      String password = null;
      boolean verified = false;
      if ((adminPassword != null) && (!adminPassword.isEmpty()))
      {
        if (logger.isInfoEnabled()) {
          logger.info("Using password sent through system properties : " + adminPassword);
        }
        password = adminPassword;
        verified = true;
      }
      else
      {
        printf("Required property \"Admin Password\" is undefined\n");
        password = "";
        while (password.equals("")) {
          password = ConsoleReader.readPassword("Enter Admin Password: ");
        }
        int count = 0;
        while (count < 3)
        {
          String passToString2 = ConsoleReader.readPassword("Re-enter the password : ");
          if (password.equals(passToString2))
          {
            if (logger.isInfoEnabled()) {
              logger.info("Matched Password");
            }
            verified = true;
            break;
          }
          logger.error("Password did not match");
          verified = false;
          
          count++;
        }
      }
      if (verified)
      {
        createDefaultRoles();
        createAdmin(password);
        if (logger.isInfoEnabled()) {
          logger.info("done creating default roles - operator, appadmin, appdev, appuser");
        }
      }
      else
      {
        System.exit(1);
      }
    }
  }
  
  private void createAdmin(String password)
    throws Exception
  {
    if (this.metadataRepository.getMetaObjectByName(EntityType.ROLE, "Global", "admin", null, this.sessionID) == null)
    {
      MetaInfo.Role newRole = new MetaInfo.Role();
      newRole.construct(MetaInfo.GlobalNamespace, "admin");
      newRole.grantPermission(new ObjectPermission("*"));
      putObject(newRole);
    }
    if (this.security_manager.getUser("admin") == null)
    {
      MetaInfo.User newuser = null;
      
      MetaInfo.Role r = (MetaInfo.Role)getObject(EntityType.ROLE, "Global", "admin");
      List<String> lrole = new ArrayList();
      lrole.add(r.getRole());
      
      newuser = new MetaInfo.User();
      newuser.construct("admin", password);
      
      newuser.setDefaultNamespace("admin");
      this.security_manager.addUser(newuser, this.sessionID);
      MetaInfo.Namespace newnamespace = (MetaInfo.Namespace)getObject(EntityType.NAMESPACE, "Global", "admin");
      if (newnamespace == null)
      {
        AuthToken temptoken = WASecurityManager.TOKEN;
        Context ctx = createContext(temptoken);
        IMap<AuthToken, SessionInfo> authTokens = HazelcastSingleton.get().getMap("#authTokens");
        SessionInfo value = new SessionInfo("admin", System.currentTimeMillis());
        authTokens.put(temptoken, value);
        ctx.putNamespace(false, "admin");
        authTokens.remove(temptoken);
        this.security_manager.refreshInternalMaps(null);
        ctx = null;
      }
      newuser = this.security_manager.getUser("admin");
      this.security_manager.grantUserRoles(newuser.getUserId(), lrole, this.sessionID);
      newuser = this.security_manager.getUser("admin");
    }
  }
  
  private void createDefaultRoles()
    throws Exception
  {
    if (this.security_manager.getRole("Global:appadmin") == null)
    {
      MetaInfo.Role role = new MetaInfo.Role();
      role.construct(MetaInfo.GlobalNamespace, "appadmin");
      role.grantPermissions(getDefaultAppAdminPermissions());
      this.security_manager.addRole(role, this.sessionID);
    }
    if (this.security_manager.getRole("Global:appdev") == null)
    {
      MetaInfo.Role role = new MetaInfo.Role();
      role.construct(MetaInfo.GlobalNamespace, "appdev");
      role.grantPermissions(getDefaultDevPermissions());
      this.security_manager.addRole(role, this.sessionID);
    }
    if (this.security_manager.getRole("Global:appuser") == null)
    {
      MetaInfo.Role role = new MetaInfo.Role();
      role.construct(MetaInfo.GlobalNamespace, "appuser");
      role.grantPermissions(getDefaultEndUserPermissions());
      this.security_manager.addRole(role, this.sessionID);
    }
    if (this.security_manager.getRole("Global:systemuser") == null)
    {
      MetaInfo.Role role = new MetaInfo.Role();
      role.construct(MetaInfo.GlobalNamespace, "systemuser");
      role.grantPermissions(getDefaultSystemUserPermissions());
      this.security_manager.addRole(role, this.sessionID);
    }
    if (this.security_manager.getRole("Global:uiuser") == null)
    {
      MetaInfo.Role role = new MetaInfo.Role();
      role.construct(MetaInfo.GlobalNamespace, "uiuser");
      role.grantPermissions(getDefaultUIUserPermissions());
      this.security_manager.addRole(role, this.sessionID);
    }
  }
  
  private static List<ObjectPermission> getDefaultSystemUserPermissions()
    throws Exception
  {
    List<ObjectPermission> defaultUserPermissions = new ArrayList();
    
    Set<String> domains = new HashSet();
    Set<ObjectPermission.Action> actions = new HashSet();
    Set<ObjectPermission.ObjectType> objectTypes = new HashSet();
    Set<String> names = new HashSet();
    
    Set<String> domains1 = new HashSet();
    Set<ObjectPermission.Action> actions1 = new HashSet();
    Set<ObjectPermission.ObjectType> objectTypes1 = new HashSet();
    domains1.add("Global");
    actions1.add(ObjectPermission.Action.select);
    actions1.add(ObjectPermission.Action.read);
    objectTypes1.add(ObjectPermission.ObjectType.propertytemplate);
    objectTypes1.add(ObjectPermission.ObjectType.deploymentgroup);
    objectTypes1.add(ObjectPermission.ObjectType.type);
    
    ObjectPermission readPropTemplatesPerm = new ObjectPermission(domains1, actions1, objectTypes1, null);
    defaultUserPermissions.add(readPropTemplatesPerm);
    return defaultUserPermissions;
  }
  
  private static List<ObjectPermission> getDefaultUIUserPermissions()
    throws Exception
  {
    List<ObjectPermission> permissions = new ArrayList();
    ObjectPermission p1 = new ObjectPermission("*:*:dashboard_ui,apps_ui,sourcepreview_ui,monitor_ui:*");
    permissions.add(p1);
    return permissions;
  }
  
  private static List<ObjectPermission> getDefaultAppAdminPermissions()
    throws Exception
  {
    List<ObjectPermission> permissions = new ArrayList();
    ObjectPermission p1 = new ObjectPermission("Global:create:namespace:*");
    
    permissions.add(p1);
    ObjectPermission p2 = new ObjectPermission("Global:read,select:*:*");
    
    permissions.add(p2);
    return permissions;
  }
  
  private static List<ObjectPermission> getDefaultDevPermissions()
    throws Exception
  {
    List<ObjectPermission> allPermissions = getDefaultEndUserPermissions();
    
    ObjectPermission p1 = new ObjectPermission("*:create:namespace:*");
    allPermissions.add(p1);
    return allPermissions;
  }
  
  private static List<ObjectPermission> getDefaultEndUserPermissions()
    throws Exception
  {
    List<ObjectPermission> allPermissions = new ArrayList();
    Set<String> domains = new HashSet();
    Set<ObjectPermission.Action> actions = new HashSet();
    Set<ObjectPermission.ObjectType> objectTypes = new HashSet();
    
    domains.add("Global");
    actions.add(ObjectPermission.Action.select);
    actions.add(ObjectPermission.Action.read);
    ObjectPermission.ObjectType[] allObjectTypes = ObjectPermission.ObjectType.values();
    for (ObjectPermission.ObjectType type : allObjectTypes) {
      if (!type.equals(ObjectPermission.ObjectType.user)) {
        objectTypes.add(type);
      }
    }
    ObjectPermission globalPermissions = new ObjectPermission(domains, actions, objectTypes, null);
    allPermissions.add(globalPermissions);
    return allPermissions;
  }
  
  public void loadExistingObjects()
    throws Exception
  {
    initMonitoringApp();
    MDRepository md = this.metadataRepository;
    if (md == null) {
      return;
    }
    Integer maxId = md.getMaxClassId();
    assert (maxId != null);
    if (maxId.intValue() > -1)
    {
      WALoader waLoader = WALoader.get();
      waLoader.setMaxClassId(maxId);
    }
    Set<MetaInfo.Namespace> namespaces = md.getByEntityType(EntityType.NAMESPACE, this.sessionID);
    Set<UUID> allApplications = new LinkedHashSet();
    for (Iterator i$ = namespaces.iterator(); i$.hasNext();)
    {
      namespace = (MetaInfo.Namespace)i$.next();
      if (logger.isInfoEnabled()) {
        logger.info("Loading objects for namespace: " + namespace.name);
      }
      Set<MetaInfo.MetaObject> objs = MetadataRepository.getINSTANCE().getByNameSpace(namespace.name, this.sessionID);
      if (objs != null)
      {
        for (MetaInfo.MetaObject obj : objs) {
          if (obj.type == EntityType.TYPE)
          {
            String metaObjectData;
            if (logger.isInfoEnabled()) {
              metaObjectData = obj.type + " --> " + obj.uuid + " ; " + namespace.name + ":" + obj.type + ":" + obj.name + " ; version number " + obj.version;
            }
            added(obj);
          }
        }
        for (MetaInfo.MetaObject obj : objs) {
          if (obj.type == EntityType.STREAM)
          {
            String metaObjectData;
            if (logger.isInfoEnabled()) {
              metaObjectData = obj.type + " --> " + obj.uuid + " ; " + namespace.name + ":" + obj.type + ":" + obj.name + " ; version number " + obj.version;
            }
            if (((MetaInfo.Stream)obj).pset != null)
            {
              KafkaStreamUtils.getPropertySet((MetaInfo.Stream)obj);
              this.metadataRepository.updateMetaObject(obj, WASecurityManager.TOKEN);
            }
          }
        }
        for (MetaInfo.MetaObject obj : objs) {
          if (obj.type == EntityType.WACTIONSTORE)
          {
            String metaObjectData;
            if (logger.isInfoEnabled()) {
              metaObjectData = obj.type + " --> " + obj.uuid + " ; " + namespace.name + ":" + obj.type + ":" + obj.name + " ; version number " + obj.version;
            }
            added(obj);
          }
        }
        for (MetaInfo.MetaObject obj : objs) {
          if ((obj.type != EntityType.TYPE) && (obj.type != EntityType.STREAM) && (obj.type != EntityType.APPLICATION) && (obj.type != EntityType.WACTIONSTORE))
          {
            String metaObjectData;
            if (logger.isInfoEnabled()) {
              metaObjectData = obj.type + " --> " + obj.uuid + " ; " + namespace.name + ":" + obj.type + ":" + obj.name + " ; version number " + obj.version;
            }
            added(obj);
          }
        }
        for (MetaInfo.MetaObject obj : objs)
        {
          DropMetaObject.addToRecentVersion(obj.nsName, obj.uuid);
          if (obj.type == EntityType.APPLICATION)
          {
            if (!MonitorModel.monitorIsEnabled())
            {
              MetaInfo.Flow flow = (MetaInfo.Flow)obj;
              if ((flow.getFullName().equals("Global.MonitoringSourceApp")) || (flow.getFullName().equals("Global.MonitoringProcessApp")))
              {
                if (!logger.isInfoEnabled()) {
                  continue;
                }
                logger.info("bloom Monitor is not enabled"); continue;
              }
            }
            String metaObjectData;
            if (logger.isInfoEnabled()) {
              metaObjectData = obj.type + " --> " + obj.uuid + " ; " + namespace.name + ":" + obj.type + ":" + obj.name + " ; version number " + obj.version;
            }
            allApplications.add(obj.getUuid());
            added(obj);
          }
        }
      }
    }
    MetaInfo.Namespace namespace;
    Set<UUID> orderOfDeployment = null;
    try
    {
      orderOfDeployment = calculateApplicationOrdering(allApplications);
    }
    catch (Exception e)
    {
      if (logger.isInfoEnabled())
      {
        logger.info("Calculating order of deployment failed with exception " + e.getMessage(), e);
        
        StringBuilder errorMessage = new StringBuilder();
        errorMessage.append("Applications that failed: \n");
        for (UUID applicationUUID : allApplications) {
          try
          {
            MetaInfo.MetaObject appObj = this.metadataRepository.getMetaObjectByUUID(applicationUUID, this.sessionID);
            if (appObj != null) {
              errorMessage.append("APP => " + appObj.getFullName() + " \n");
            } else {
              errorMessage.append("APPUUID => " + applicationUUID + " \n");
            }
          }
          catch (MetaDataRepositoryException metaDataException)
          {
            errorMessage.append("APPUUID => " + applicationUUID + " \n");
          }
        }
        logger.info(errorMessage);
      }
      return;
    }
    if (orderOfDeployment.size() != allApplications.size()) {
      throw new RuntimeException("Application dependency ordering is missing some applications, will use non ordered version");
    }
    for (UUID uuid : orderOfDeployment) {
      if (!allApplications.contains(uuid)) {
        throw new RuntimeException("Application dependency ordering is missing some applications.");
      }
    }
    for (UUID appUUID : orderOfDeployment)
    {
      MetaInfo.MetaObject appMetaObject = getMetaObject(appUUID);
      if ((appMetaObject != null) && (appMetaObject.type == EntityType.APPLICATION)) {
        try
        {
          if (logger.isDebugEnabled()) {
            logger.debug("Trying to deploy application " + appMetaObject.getFullName());
          }
          deployFlow((MetaInfo.Flow)appMetaObject, true);
        }
        catch (Exception e)
        {
          logger.warn("Failed to deploy application " + appMetaObject.getFullName() + " on this node  with exception " + e.getMessage());
          
          undeployFlow((MetaInfo.Flow)appMetaObject, null);
        }
      }
    }
  }
  
  private LinkedHashSet<UUID> calculateApplicationOrdering(Set<UUID> allApplications)
    throws Exception
  {
    Map<UUID, Set<UUID>> dependentApps = new HashMap();
    
    Map<UUID, Set<UUID>> objectDependentOfApps = new HashMap();
    for (UUID applicationUUID : allApplications)
    {
      flowMetaObject = (MetaInfo.Flow)getMetaObject(applicationUUID);
      if (flowMetaObject == null)
      {
        logger.warn("Failed to calcualte application ordering for appId " + applicationUUID + " because flowMetaObject does not exist for this flow");
      }
      else
      {
        Set<UUID> appDeepDependencies = null;
        try
        {
          appDeepDependencies = flowMetaObject.getDeepDependencies();
        }
        catch (FatalException e)
        {
          logger.info("Problem occured in finding application dependencies, skipping application ordering.", e);
          throw e;
        }
        for (UUID uuid : appDeepDependencies) {
          if (objectDependentOfApps.get(uuid) == null)
          {
            Set<UUID> appSet = new HashSet();
            appSet.add(flowMetaObject.uuid);
            objectDependentOfApps.put(uuid, appSet);
          }
          else
          {
            ((Set)objectDependentOfApps.get(uuid)).add(flowMetaObject.uuid);
          }
        }
      }
    }
    MetaInfo.Flow flowMetaObject;
    for (Iterator i$ = objectDependentOfApps.entrySet().iterator(); i$.hasNext();)
    {
      objectDependencyMapEntry = (Map.Entry)i$.next();
      if (((Set)objectDependencyMapEntry.getValue()).size() > 1) {
        for (UUID application : (Set)objectDependencyMapEntry.getValue())
        {
          flowMetaObject = (MetaInfo.Flow)getObject(application);
          Set<UUID> flowObjects = flowMetaObject.getAllObjects();
          if (flowObjects.contains(objectDependencyMapEntry.getKey()))
          {
            List<UUID> adjacencyList = new ArrayList();
            adjacencyList.addAll((Collection)objectDependencyMapEntry.getValue());
            adjacencyList.remove(flowMetaObject.getUuid());
            for (UUID adjacentVertexUUID : adjacencyList)
            {
              UUID vertexFirst = flowMetaObject.getUuid();
              UUID vertexSecond = adjacentVertexUUID;
              
              MetaInfo.Flow secondVertexApplication = (MetaInfo.Flow)getObject(vertexSecond);
              Set<UUID> secondVertexApplicationObjects = secondVertexApplication.getAllObjects();
              if (!secondVertexApplicationObjects.contains(objectDependencyMapEntry.getKey()))
              {
                if (!dependentApps.containsKey(vertexFirst)) {
                  dependentApps.put(vertexFirst, new LinkedHashSet());
                }
                if (!dependentApps.containsKey(vertexSecond)) {
                  dependentApps.put(vertexSecond, new LinkedHashSet());
                }
                ((Set)dependentApps.get(vertexFirst)).add(vertexSecond);
              }
            }
          }
        }
      }
    }
    Map.Entry<UUID, Set<UUID>> objectDependencyMapEntry;
    MetaInfo.Flow flowMetaObject;
    LinkedHashSet<UUID> newOrder = new LinkedHashSet();
    
    newOrder.addAll(GraphUtility.topologicalSort(dependentApps));
    for (UUID uuid : allApplications) {
      if (!newOrder.contains(uuid)) {
        newOrder.add(uuid);
      }
    }
    return newOrder;
  }
  
  private Set<URL> getListOfPropTempURLs()
  {
    String homeList = System.getProperty("com.bloom.platform.home");
    String[] platformHome = new String[2];
    if ((homeList != null) && (homeList.contains(";"))) {
      platformHome = homeList.split(";");
    } else {
      platformHome[0] = homeList;
    }
    Set<URL> result = Sets.newHashSet();
    for (int i = 0; i < platformHome.length; i++)
    {
      File platformHomeFile;
      File platformHomeFile;
      if (platformHome[i] == null) {
        platformHomeFile = new File(".");
      } else {
        platformHomeFile = new File(platformHome[i]);
      }
      String platformHomeAbs;
      try
      {
        platformHomeAbs = platformHomeFile.getCanonicalPath();
      }
      catch (IOException e)
      {
        platformHomeAbs = platformHomeFile.getAbsolutePath();
      }
      ClassLoader[] loaders = ClasspathHelper.classLoaders(new ClassLoader[0]);
      for (ClassLoader classLoader : loaders) {
        while (classLoader != null)
        {
          if ((classLoader instanceof URLClassLoader))
          {
            URL[] urls = ((URLClassLoader)classLoader).getURLs();
            if (urls != null) {
              for (URL url : urls) {
                if (url.getPath().contains(platformHomeAbs)) {
                  result.add(url);
                }
              }
            }
          }
          classLoader = classLoader.getParent();
        }
      }
    }
    return result;
  }
  
  private void loadPropTemps(Collection<Class<?>> annotatedClasses)
  {
    for (Class<?> c : annotatedClasses)
    {
      PropertyTemplate pt = (PropertyTemplate)c.getAnnotation(PropertyTemplate.class);
      
      PropertyTemplateProperty[] ptp = pt.properties();
      Map<String, MetaInfo.PropertyDef> props = Factory.makeMap();
      for (PropertyTemplateProperty val : ptp)
      {
        MetaInfo.PropertyDef def = new MetaInfo.PropertyDef();
        def.construct(val.required(), val.type(), val.defaultValue(), val.label(), val.description());
        props.put(val.name(), def);
      }
      Lock propTemplateLock = HazelcastSingleton.get().getLock("propTemplateLock");
      
      propTemplateLock.lock();
      try
      {
        genDefaultClasses(pt);
        MetaInfo.PropertyTemplateInfo pti = (MetaInfo.PropertyTemplateInfo)getObject(EntityType.PROPERTYTEMPLATE, "Global", pt.name());
        
        boolean isUnique = false;
        Map<String, MetaInfo.PropertyDef> defCurrent;
        if (pti != null)
        {
          defCurrent = pti.propertyMap;
          for (Map.Entry<String, MetaInfo.PropertyDef> entry : props.entrySet()) {
            if (!((MetaInfo.PropertyDef)entry.getValue()).equals((MetaInfo.PropertyDef)defCurrent.get(entry.getKey())))
            {
              isUnique = true;
              break;
            }
          }
        }
        else
        {
          isUnique = true;
        }
        if (isUnique)
        {
          pti = new MetaInfo.PropertyTemplateInfo();
          pti.construct(pt.name(), pt.type(), props, pt.inputType().getName(), pt.outputType().getName(), c.getName(), pt.requiresParser(), pt.requiresFormatter());
          
          putObject(pti);
        }
      }
      catch (Exception e)
      {
        logger.error(e.getLocalizedMessage());
      }
      finally
      {
        propTemplateLock.unlock();
      }
    }
  }
  
  public void loadPropertyTemplates()
  {
    Set<URL> propTemps = getListOfPropTempURLs();
    Reflections refs = new Reflections(propTemps.toArray());
    Set<Class<?>> annotatedClasses = refs.getTypesAnnotatedWith(PropertyTemplate.class);
    
    loadPropTemps(annotatedClasses);
  }
  
  private void genDefaultClasses(PropertyTemplate pt)
    throws MetaDataRepositoryException
  {
    Map<String, String> fields = new LinkedHashMap();
    Class<?> inputType = pt.inputType();
    Class<?> outputType = pt.outputType();
    MetaInfo.Type type = null;
    if (!inputType.equals(NotSet.class))
    {
      Field[] cFields = inputType.getDeclaredFields();
      for (Field f : cFields) {
        if (Modifier.isPublic(f.getModifiers())) {
          fields.put(f.getName(), f.getType().getCanonicalName());
        }
      }
      if (getObject(EntityType.TYPE, "Global", inputType.getSimpleName()) == null)
      {
        type = new MetaInfo.Type();
        type.construct("Global." + inputType.getSimpleName(), MetaInfo.GlobalNamespace, inputType.getName(), fields, null, false);
      }
    }
    else
    {
      Field[] cFields = outputType.getDeclaredFields();
      for (Field f : cFields) {
        if (Modifier.isPublic(f.getModifiers())) {
          fields.put(f.getName(), f.getType().getCanonicalName());
        }
      }
      if (getObject(EntityType.TYPE, "Global", outputType.getSimpleName()) == null)
      {
        type = new MetaInfo.Type();
        type.construct("Global." + outputType.getSimpleName(), MetaInfo.GlobalNamespace, outputType.getName(), fields, null, false);
      }
    }
    if (type != null) {
      putObject(type);
    }
  }
  
  public <T extends MetaInfo.MetaObject> T getObjectInfo(UUID uuid, EntityType type)
    throws ServerException, MetaDataRepositoryException
  {
    MetaInfo.MetaObject o = getObject(uuid);
    if ((o != null) && (o.type == type)) {
      return o;
    }
    throw new ServerException("cannot find metadata for " + type.name() + " " + uuid);
  }
  
  public ExecutorService getThreadPool()
  {
    return this.pool;
  }
  
  public ScheduledFuture<?> scheduleStatsReporting(Runnable task, int period, boolean optional)
  {
    return optional ? null : this.servicesScheduler.scheduleAtFixedRate(task, period, period, TimeUnit.SECONDS);
  }
  
  public boolean inDeploymentGroup(UUID deploymentGroupID)
  {
    return this.ServerInfo.deploymentGroupsIDs.contains(deploymentGroupID);
  }
  
  boolean checkIfMetaObjectExists(UUID uuid)
    throws MetaDataRepositoryException
  {
    return this.metadataRepository.getMetaObjectByUUID(uuid, this.sessionID) != null;
  }
  
  public void added(MetaInfo.MetaObject obj)
    throws MetaDataRepositoryException
  {
    DropMetaObject.addToRecentVersion(obj.nsName, obj.uuid);
    addedOrUpdated(obj, null, false);
  }
  
  public void addedOrUpdated(MetaInfo.MetaObject obj, MetaInfo.MetaObject oldObj, boolean updated)
    throws MetaDataRepositoryException
  {
    switch (obj.type)
    {
    case TYPE: 
      MetaInfo.Type type = (MetaInfo.Type)obj;
      if (!type.getMetaInfoStatus().isDropped()) {
        try
        {
          type.generateClass();
        }
        catch (Exception e) {}
      }
      break;
    case DG: 
      MetaInfo.DeploymentGroup dg = (MetaInfo.DeploymentGroup)obj;
      if (logger.isInfoEnabled()) {
        logger.info("Got update to deploymentgroup: " + dg.uri + " " + dg);
      }
      if (!dg.isAutomaticDG())
      {
        ILock lock = HazelcastSingleton.get().getLock("initializeDGLock");
        lock.lock();
        try
        {
          List<UUID> dgs = (List)this.serverToDeploymentGroup.get(this.serverID);
          assert (dgs != null);
          
          String serverName = getServerName();
          if ((serverName != null) && (dg.configuredMembers.contains(serverName)))
          {
            if (logger.isInfoEnabled()) {
              logger.info("Adding server " + serverName + " to dg " + dg.name);
            }
            if (this.ServerInfo.deploymentGroupsIDs.add(dg.uuid))
            {
              dgs.add(dg.uuid);
              
              MetaInfo.DeploymentGroup dgLatest = getDeploymentGroupByName(dg.getName());
              dgLatest.addMembers(Collections.singletonList(this.serverID), this.ServerInfo.getId());
              updateObject(dgLatest);
              this.serverToDeploymentGroup.put(this.serverID, dgs);
              this.metadataRepository.putServer(this.ServerInfo, WASecurityManager.TOKEN);
              if (logger.isInfoEnabled()) {
                logger.info("Added DG " + dg.name + "\nCurrent deployment groups: " + getDeploymentGroups());
              }
            }
          }
          else if (this.ServerInfo.deploymentGroupsIDs.remove(dg.uuid))
          {
            dgs.remove(dg.uuid);
            this.serverToDeploymentGroup.put(this.serverID, dgs);
            this.metadataRepository.putServer(this.ServerInfo, WASecurityManager.TOKEN);
            if (logger.isInfoEnabled()) {
              logger.info("Removed DG " + dg.name + "\nCurrent deployment groups: " + getDeploymentGroups());
            }
          }
        }
        finally
        {
          lock.unlock();
        }
      }
      break;
    case ROLE: 
      this.security_manager.refreshInternalMaps(null);
      break;
    case USER: 
      this.security_manager.refreshInternalMaps(((MetaInfo.User)obj).name);
      break;
    case WACTIONSTORE: 
      MetaInfo.WActionStore wActionStore = (MetaInfo.WActionStore)obj;
      if (!wActionStore.getMetaInfoStatus().isDropped()) {
        try
        {
          wActionStore.generateClasses();
        }
        catch (Exception e)
        {
          logger.error("Failed to generate classes for WAction Store: " + wActionStore.getFullName(), e);
        }
      }
      break;
    }
  }
  
  public void removed(MetaInfo.MetaObject obj)
    throws MetaDataRepositoryException
  {
    switch (obj.type)
    {
    case DG: 
      MetaInfo.DeploymentGroup dg = (MetaInfo.DeploymentGroup)obj;
      if (logger.isInfoEnabled()) {
        logger.info("Got removal request for deploymentgroup: " + dg.uri + " " + dg);
      }
      if (!dg.isAutomaticDG())
      {
        ILock lock = HazelcastSingleton.get().getLock("initializeDGLock");
        lock.lock();
        try
        {
          List<UUID> dgs = (List)this.serverToDeploymentGroup.get(this.serverID);
          assert (dgs != null);
          if (this.ServerInfo.deploymentGroupsIDs.remove(dg.uuid))
          {
            dgs.remove(dg.uuid);
            this.serverToDeploymentGroup.put(this.serverID, dgs);
            this.metadataRepository.putServer(this.ServerInfo, WASecurityManager.TOKEN);
            logger.info("Removed DG " + dg.name + "\nCurrent deployment groups: " + getDeploymentGroups());
          }
        }
        finally
        {
          lock.unlock();
        }
      }
      break;
    }
  }
  
  private void addAlertQueue(String channelName)
  {
    WAQueue.getQueue(channelName);
  }
  
  public WactionStore createWActionStore(MetaInfo.WActionStore obj)
    throws Exception
  {
    WactionStore was = WactionStore.get(obj.uuid, this);
    putOpenObject(was);
    return was;
  }
  
  public WAStoreView createWAStoreView(MetaInfo.WAStoreView obj)
    throws Exception
  {
    WactionStore wactionStore = (WactionStore)getOpenObject(obj.wastoreID);
    WAStoreView wasview = new WAStoreView(obj, wactionStore, this);
    putOpenObject(wasview);
    return wasview;
  }
  
  public void removeDeployedObject(FlowComponent obj)
    throws Exception
  {
    if (obj.getMetaType() == EntityType.SOURCE) {
      try
      {
        FlowComponent so = getOpenObject(obj.getMetaID());
        if (so != null)
        {
          Source src = (Source)so;
          src.getAdapter().onUndeploy();
        }
      }
      catch (Exception e)
      {
        logger.error("Failed to call unDeploy on Source process for " + obj.getMetaName(), e);
      }
    }
    FlowComponent o = removeDeployedObjectById(obj.getMetaID());
    if (o == null) {
      closeOpenObject(obj);
    } else if (o != obj) {
      throw new RuntimeException("system error: duplicate object " + obj.metaToString());
    }
  }
  
  public FlowComponent removeDeployedObjectById(UUID id)
    throws Exception
  {
    synchronized (this.openObjects)
    {
      FlowComponent o = (FlowComponent)this.openObjects.remove(id);
      if (o != null) {
        closeOpenObject(o);
      }
      return o;
    }
  }
  
  public Stream createStream(UUID uuid, final Flow flow)
    throws Exception
  {
    final MetaInfo.Stream si = getStreamInfo(uuid);
    if (si != null)
    {
      BaseServer.StreamObjectFac fac = new BaseServer.StreamObjectFac()
      {
        public FlowComponent create()
          throws Exception
        {
          Stream s = new Stream(si, Server.this, flow);
          return s;
        }
      };
      FlowComponent stream_component = putOpenObjectIfNotExists(uuid, fac);
      return (Stream)stream_component;
    }
    return null;
  }
  
  public Stream getStream(final UUID streamId, final Flow flow)
    throws Exception
  {
    BaseServer.StreamObjectFac fac = new BaseServer.StreamObjectFac()
    {
      public FlowComponent create()
        throws Exception
      {
        MetaInfo.Stream si = Server.this.getStreamInfo(streamId);
        Stream s = new Stream(si, Server.this, flow);
        return s;
      }
    };
    return (Stream)putOpenObjectIfNotExists(streamId, fac);
  }
  
  public IWindow createWindow(MetaInfo.Window info)
    throws Exception
  {
    IWindow w = new Window(info, this);
    putOpenObject(w);
    return w;
  }
  
  public CQTask createCQ(MetaInfo.CQ info)
    throws Exception
  {
    CQTask ct = new CQTask(info, this);
    putOpenObject(ct);
    return ct;
  }
  
  public Sorter createSorter(MetaInfo.Sorter info)
    throws Exception
  {
    Sorter s = new Sorter(info, this);
    putOpenObject(s);
    return s;
  }
  
  public Target createTarget(MetaInfo.Target targetInfo)
    throws Exception
  {
    try
    {
      Class<?> adapterFactory = WALoader.get().loadClass(targetInfo.adapterClassName);
      
      BaseProcess bp = (BaseProcess)adapterFactory.newInstance();
      Target t = new Target(bp, targetInfo, this);
      
      t.onDeploy();
      putOpenObject(t);
      return t;
    }
    catch (ClassNotFoundException e)
    {
      throw new ServerException("cannot create adapter " + targetInfo.adapterClassName, e);
    }
  }
  
  /* Error */
  public com.bloom.historicalcache.Cache createCache(MetaInfo.Cache targetInfo)
    throws Exception
  {
    // Byte code:
    //   0: aload_0
    //   1: getfield 621	com/bloom/runtime/Server:openObjects	Ljava/util/Map;
    //   4: dup
    //   5: astore_2
    //   6: monitorenter
    //   7: aconst_null
    //   8: astore_3
    //   9: aload_0
    //   10: aload_1
    //   11: getfield 646	com/bloom/runtime/meta/MetaInfo$Cache:typename	Lcom/bloom/uuid/UUID;
    //   14: invokevirtual 216	com/bloom/runtime/Server:getObject	(Lcom/bloom/uuid/UUID;)Lcom/bloom/runtime/meta/MetaInfo$MetaObject;
    //   17: checkcast 565	com/bloom/runtime/meta/MetaInfo$Type
    //   20: astore 4
    //   22: aload_1
    //   23: getfield 647	com/bloom/runtime/meta/MetaInfo$Cache:reader_properties	Ljava/util/Map;
    //   26: ldc_w 648
    //   29: aload 4
    //   31: getfield 649	com/bloom/runtime/meta/MetaInfo$Type:className	Ljava/lang/String;
    //   34: invokeinterface 277 3 0
    //   39: pop
    //   40: aload_1
    //   41: getfield 650	com/bloom/runtime/meta/MetaInfo$Cache:adapterClassName	Ljava/lang/String;
    //   44: ifnull +41 -> 85
    //   47: invokestatic 440	com/bloom/classloading/WALoader:get	()Lcom/bloom/classloading/WALoader;
    //   50: aload_1
    //   51: getfield 650	com/bloom/runtime/meta/MetaInfo$Cache:adapterClassName	Ljava/lang/String;
    //   54: invokevirtual 637	com/bloom/classloading/WALoader:loadClass	(Ljava/lang/String;)Ljava/lang/Class;
    //   57: astore 5
    //   59: aload_1
    //   60: getfield 647	com/bloom/runtime/meta/MetaInfo$Cache:reader_properties	Ljava/util/Map;
    //   63: ldc_w 651
    //   66: iconst_1
    //   67: invokestatic 652	java/lang/Boolean:valueOf	(Z)Ljava/lang/Boolean;
    //   70: invokeinterface 277 3 0
    //   75: pop
    //   76: aload 5
    //   78: invokevirtual 638	java/lang/Class:newInstance	()Ljava/lang/Object;
    //   81: checkcast 639	com/bloom/proc/BaseProcess
    //   84: astore_3
    //   85: new 653	com/bloom/historicalcache/Cache
    //   88: dup
    //   89: aload_1
    //   90: aload_0
    //   91: aload_3
    //   92: invokespecial 654	com/bloom/historicalcache/Cache:<init>	(Lcom/bloom/runtime/meta/MetaInfo$Cache;Lcom/bloom/runtime/BaseServer;Lcom/bloom/proc/BaseProcess;)V
    //   95: astore 5
    //   97: aload_0
    //   98: aload 5
    //   100: invokevirtual 603	com/bloom/runtime/Server:putOpenObject	(Lcom/bloom/runtime/components/FlowComponent;)V
    //   103: aload 5
    //   105: aload_2
    //   106: monitorexit
    //   107: areturn
    //   108: astore 6
    //   110: aload_2
    //   111: monitorexit
    //   112: aload 6
    //   114: athrow
    //   115: astore_2
    //   116: new 360	com/bloom/exception/ServerException
    //   119: dup
    //   120: new 49	java/lang/StringBuilder
    //   123: dup
    //   124: invokespecial 50	java/lang/StringBuilder:<init>	()V
    //   127: ldc_w 644
    //   130: invokevirtual 52	java/lang/StringBuilder:append	(Ljava/lang/String;)Ljava/lang/StringBuilder;
    //   133: aload_1
    //   134: getfield 650	com/bloom/runtime/meta/MetaInfo$Cache:adapterClassName	Ljava/lang/String;
    //   137: invokevirtual 52	java/lang/StringBuilder:append	(Ljava/lang/String;)Ljava/lang/StringBuilder;
    //   140: invokevirtual 57	java/lang/StringBuilder:toString	()Ljava/lang/String;
    //   143: aload_2
    //   144: invokespecial 645	com/bloom/exception/ServerException:<init>	(Ljava/lang/String;Ljava/lang/Throwable;)V
    //   147: athrow
    // Line number table:
    //   Java source line #1874	-> byte code offset #0
    //   Java source line #1876	-> byte code offset #7
    //   Java source line #1882	-> byte code offset #9
    //   Java source line #1883	-> byte code offset #22
    //   Java source line #1884	-> byte code offset #40
    //   Java source line #1885	-> byte code offset #47
    //   Java source line #1887	-> byte code offset #59
    //   Java source line #1888	-> byte code offset #76
    //   Java source line #1890	-> byte code offset #85
    //   Java source line #1891	-> byte code offset #97
    //   Java source line #1893	-> byte code offset #103
    //   Java source line #1894	-> byte code offset #108
    //   Java source line #1895	-> byte code offset #115
    //   Java source line #1896	-> byte code offset #116
    // Local variable table:
    //   start	length	slot	name	signature
    //   0	148	0	this	Server
    //   0	148	1	targetInfo	MetaInfo.Cache
    //   115	29	2	e	ClassNotFoundException
    //   8	84	3	bp	BaseProcess
    //   20	10	4	t	MetaInfo.Type
    //   57	20	5	adapterFactory	Class<?>
    //   95	9	5	cache	com.bloom.historicalcache.Cache
    //   108	5	6	localObject1	Object
    // Exception table:
    //   from	to	target	type
    //   7	107	108	finally
    //   108	112	108	finally
    //   0	107	115	java/lang/ClassNotFoundException
    //   108	115	115	java/lang/ClassNotFoundException
  }
  
  public IStreamGenerator createStreamGen(MetaInfo.StreamGenerator g)
    throws Exception
  {
    synchronized (this.openObjects)
    {
      try
      {
        Class<?> klass = WALoader.get().loadClass(g.className);
        Constructor<?> ctor = klass.getConstructor(new Class[] { MetaInfo.StreamGenerator.class, BaseServer.class });
        IStreamGenerator gen = (IStreamGenerator)ctor.newInstance(new Object[] { g, this });
        putOpenObject(gen);
        return gen;
      }
      catch (ClassNotFoundException e)
      {
        throw new ServerException("cannot find generator class " + g.className, e);
      }
      catch (ClassCastException e)
      {
        throw new ServerException("class " + g.className + " does not support interface IStreamGenerator", e);
      }
    }
  }
  
  private void getStreamsInObject(UUID id, Map<UUID, MetaInfo.Stream> result)
    throws Exception
  {
    MetaInfo.MetaObject obj = getObject(id);
    if (obj != null) {
      if ((obj instanceof MetaInfo.Stream)) {
        result.put(id, (MetaInfo.Stream)obj);
      } else {
        for (UUID dep : obj.getDependencies()) {
          getStreamsInObject(dep, result);
        }
      }
    }
  }
  
  public void shutdown()
  {
    logger.warn("Shutting down bloom Server\n");
    synchronized (this.openObjects)
    {
      for (FlowComponent o : this.openObjects.values()) {
        if ((o instanceof Flow)) {
          try
          {
            logger.info("checkpointing: " + o.getMetaFullName());
            ((Flow)o).takeAppCheckpoint();
          }
          catch (Exception e)
          {
            logger.error(e, e);
          }
        }
      }
    }
    if (this.discoveryTask != null) {
      this.discoveryTask.cancel(true);
    }
    WActionStores.shutdown();
    this.servicesScheduler.shutdownNow();
    this.pool.shutdown();
    try
    {
      if (!this.pool.awaitTermination(5L, TimeUnit.SECONDS))
      {
        this.pool.shutdownNow();
        if (!this.pool.awaitTermination(60L, TimeUnit.SECONDS)) {
          System.err.println("Pool did not terminate");
        }
      }
    }
    catch (InterruptedException ie)
    {
      this.pool.shutdownNow();
      
      Thread.currentThread().interrupt();
    }
    super.shutdown();
    if (logger.isInfoEnabled()) {
      logger.info("shutdown finished");
    }
  }
  
  public ZMQChannel getDistributedChannel(Stream owner)
  {
    return new ZMQChannel(this, owner);
  }
  
  public Flow createFlow(MetaInfo.Flow flowInfo, Flow parent)
    throws Exception
  {
    Flow flow = new Flow(flowInfo, this, parent);
    if (flowInfo.type == EntityType.APPLICATION)
    {
      MetaInfo.Flow.Detail d = (MetaInfo.Flow.Detail)flowInfo.deploymentPlan.get(0);
      MetaInfo.DeploymentGroup dg = getDeploymentGroupByID(d.deploymentGroup);
      if ((inDeploymentGroup(d.deploymentGroup)) && 
        (!flow.checkStrategy(d, dg))) {
        return null;
      }
    }
    putOpenObject(flow);
    return flow;
  }
  
  public Source createSource(MetaInfo.Source sourceInfo)
    throws Exception
  {
    try
    {
      Class<?> adapterFactory = WALoader.get().loadClass(sourceInfo.adapterClassName);
      
      String namespace = sourceInfo.getNsName();
      SourceProcess bp = (SourceProcess)adapterFactory.newInstance();
      if ((bp instanceof StreamReader))
      {
        StreamReader x = (StreamReader)bp;
        x.setNamespace(namespace);
      }
      Source src = new Source(bp, sourceInfo, this);
      putOpenObject(src);
      return src;
    }
    catch (ClassNotFoundException e)
    {
      throw new ServerException("cannot create adapter " + sourceInfo.adapterClassName, e);
    }
  }
  
  protected MetaInfo.PropertyTemplateInfo getAdapterProperties(String name)
    throws MetaDataRepositoryException
  {
    return (MetaInfo.PropertyTemplateInfo)getObject(EntityType.PROPERTYTEMPLATE, "Global", name);
  }
  
  QueryValidator qv = null;
  public static volatile Server server;
  private static volatile WebServer webServer;
  
  public void remoteCallOnObject(ActionType what, MetaInfo.MetaObject obj, Object[] params, UUID clientSessionID)
    throws Exception
  {
    if (this.qv == null)
    {
      this.qv = new QueryValidator(this);
      this.qv.setUpdateMode(true);
    }
    if (obj == null) {
      throw new RuntimeException(what.toString() + " on not existing object");
    }
    if (clientSessionID == null) {
      throw new RuntimeException("Authentication token not passed to load/unload function");
    }
    if (logger.isInfoEnabled()) {
      logger.info(String.format(Constant.REMOTE_CALL_MSG, new Object[] { what.toString(), obj.getFullName(), Integer.valueOf(params == null ? 0 : params.length), clientSessionID.toString() }));
    }
    MetaInfo.Flow autoBuiltFlow = null;
    FlowComponent flowComponent = getOpenObject(obj.getUuid());
    Flow flow = null;
    if (flowComponent != null) {
      flow = flowComponent.getFlow();
    }
    try
    {
      assert (obj != null);
      switch (what)
      {
      case LOAD: 
        if (flow != null) {
          throw new RuntimeException(String.format(Constant.ALREAD_LOADED_MSG, new Object[] { what.toString(), obj.getFullName() }));
        }
        String appname = obj.name + "_app";
        if (null != this.metadataRepository.getMetaObjectByName(EntityType.APPLICATION, obj.nsName, appname, null, this.sessionID)) {
          throw new RuntimeException("Cannot LOAD " + obj.name + " because application already exists: " + appname);
        }
        autoBuiltFlow = new ImplicitApplicationBuilder().init((MetaInfo.Namespace)getMetaObject(obj.getNamespaceId()), appname, this.sessionID).addObject(obj).build();
        assert (autoBuiltFlow != null);
        if (autoBuiltFlow == null) {
          throw new RuntimeException("Missing " + DeploymentRule.class.getSimpleName() + " as parameter");
        }
        DeploymentRule dr = (DeploymentRule)params[0];
        assert (dr != null);
        if (dr == null) {
          throw new RuntimeException("Missing " + DeploymentRule.class.getSimpleName() + " as parameter");
        }
        dr.flowName = autoBuiltFlow.getFullName();
        if (logger.isInfoEnabled()) {
          logger.info(String.format(Constant.DEPLOY_START_MSG, new Object[] { dr, dr.flowName }));
        }
        this.qv.LoadDataComponent(new AuthToken(clientSessionID), autoBuiltFlow, dr);
        break;
      case UNLOAD: 
        if (flow == null) {
          throw new RuntimeException(String.format(Constant.ALREAD_UNLOADED_MSG, new Object[] { what.toString(), obj.getFullName() }));
        }
        this.qv.UnloadDataComponent(new AuthToken(clientSessionID), flow, obj);
        
        break;
      default: 
        throw new RuntimeException(String.format(Constant.OPERATION_NOT_SUPPORTED, new Object[] { what == null ? "N/A" : what.toString() }));
      }
    }
    catch (Throwable e)
    {
      if (what == ActionType.LOAD) {
        if ((autoBuiltFlow != null) && (autoBuiltFlow.getUuid() != null)) {
          this.metadataRepository.removeMetaObjectByUUID(autoBuiltFlow.getUuid(), new AuthToken(clientSessionID));
        } else if (logger.isInfoEnabled()) {
          logger.info(String.format(Constant.LOAD_FAILED_WITH_NO_IMPLICIT_APP, new Object[] { obj == null ? "N/A" : obj.getFullName() }));
        }
      }
      throw e;
    }
  }
  
  public void changeFlowState(ActionType what, MetaInfo.Flow info, List<UUID> servers, UUID clientSessionID, List<Property> params, Long epochNumber)
    throws Exception
  {
    try
    {
      assert (info != null);
      switch (what)
      {
      case START: 
        startFlow(info, params, servers, epochNumber);
        break;
      case VERIFY_START: 
        verifyStartFlow(info, servers);
        break;
      case START_SOURCES: 
        startSources(info);
        break;
      case STOP: 
        stopFlow(info, epochNumber);
        break;
      case DEPLOY: 
        deployFlow(info, true);
        break;
      case INIT_CACHES: 
        initCaches(info);
        break;
      case START_CACHES: 
        startCaches(info, servers, epochNumber.longValue());
        break;
      case STOP_CACHES: 
        stopCaches(info, epochNumber.longValue());
        break;
      case UNDEPLOY: 
        undeployFlow(info, clientSessionID);
        break;
      case STATUS: 
        break;
      case START_ADHOC: 
        deployStartQuery(info, clientSessionID, what, params);
        break;
      case STOP_ADHOC: 
        stopUndeployQuery(info, clientSessionID, what);
        break;
      default: 
        if (!$assertionsDisabled) {
          throw new AssertionError();
        }
        break;
      }
    }
    catch (Throwable e)
    {
      throw e;
    }
  }
  
  public void checkNotDeployed(MetaInfo.MetaObject obj)
    throws ServerException
  {
    FlowComponent o = getOpenObject(obj.uuid);
    if (o != null) {
      throw new ServerException(obj.type + " already deployed on this server  " + obj.name + ":" + obj.type + ":" + obj.uuid);
    }
  }
  
  private String[] getAdHocQueryResultSetDesc(MetaInfo.Flow info)
    throws ServerException, MetaDataRepositoryException
  {
    for (UUID cqid : info.getObjects(EntityType.CQ))
    {
      MetaInfo.CQ cq = (MetaInfo.CQ)getObjectInfo(cqid, EntityType.CQ);
      if (cq.stream != null)
      {
        List<RSFieldDesc> desc = cq.plan.resultSetDesc;
        String[] fieldsInfo = new String[desc.size()];
        int i = 0;
        for (RSFieldDesc f : desc)
        {
          fieldsInfo[i] = f.name;
          i++;
        }
        return fieldsInfo;
      }
    }
    throw new RuntimeException("query has no result set description");
  }
  
  public void deployStartQuery(MetaInfo.Flow info, UUID clientSessionID, ActionType what, List<Property> params)
    throws Exception
  {
    LocalLockProvider.Key keyForLock = new LocalLockProvider.Key(clientSessionID.toString(), info.getUuid().toString());
    Lock lock = LocalLockProvider.getLock(keyForLock);
    lock.lock();
    if (logger.isDebugEnabled()) {
      logger.debug(Thread.currentThread().getName() + " : " + what + " : " + info.getFullName());
    }
    try
    {
      Flow f = deployFlow(info, false);
      if (f == null) {
        return;
      }
      f.bindParams(params);
      String[] fieldsInfo = getAdHocQueryResultSetDesc(info);
      UUID id = (UUID)info.getObjects(EntityType.STREAM).iterator().next();
      Stream outputStream = (Stream)getOpenObject(id);
      assert (outputStream != null);
      String queueName = "consoleQueue" + clientSessionID.getUUIDString();
      
      WAQueue consoleQueue = WAQueue.getQueue(queueName);
      UUID queryMetaObjectUUID = (UUID)info.getReverseIndexObjectDependencies().iterator().next();
      if (queryMetaObjectUUID == null) {
        return;
      }
      AdhocStreamSubscriber adhocStreamSubscriber = new AdhocStreamSubscriber(fieldsInfo, queryMetaObjectUUID, queueName, consoleQueue);
      
      subscribe(outputStream, adhocStreamSubscriber);
      if (this.userQueryRecords.containsKey(clientSessionID))
      {
        ((HashMap)this.userQueryRecords.get(clientSessionID)).put(info.uuid, Pair.make(outputStream, adhocStreamSubscriber));
      }
      else
      {
        HashMap<UUID, Pair<Publisher, Subscriber>> newUserRecord = new HashMap();
        newUserRecord.put(info.uuid, Pair.make(outputStream, adhocStreamSubscriber));
        this.userQueryRecords.put(clientSessionID, newUserRecord);
      }
      List<UUID> servers = new ArrayList();
      servers.add(getServer().getServerID());
      startFlow(info, null, servers, null);
      startSources(info);
    }
    finally
    {
      lock.unlock();
    }
    if (logger.isDebugEnabled()) {
      logger.debug(Thread.currentThread().getName() + " : " + what + " : " + info.getFullName() + " DONE.");
    }
  }
  
  public void stopUndeployQuery(MetaInfo.Flow info, UUID clientSessionID, ActionType what)
    throws Exception
  {
    LocalLockProvider.Key keyForLock = new LocalLockProvider.Key(clientSessionID.toString(), info.getUuid().toString());
    Lock lock = LocalLockProvider.getLock(keyForLock);
    lock.lock();
    if (logger.isDebugEnabled()) {
      logger.debug(Thread.currentThread().getName() + " : " + what + " : " + info.getFullName());
    }
    try
    {
      if (getOpenObject(info.getUuid()) == null)
      {
        if (logger.isDebugEnabled()) {
          logger.debug("No such adhoc " + info.getFullName() + " is deployed in server " + getDebugId());
        }
        return;
      }
      if (!this.userQueryRecords.containsKey(clientSessionID)) {
        return;
      }
      Pair<Publisher, Subscriber> pubsub = (Pair)((HashMap)this.userQueryRecords.get(clientSessionID)).remove(info.uuid);
      if (((HashMap)this.userQueryRecords.get(clientSessionID)).size() == 0) {
        this.userQueryRecords.remove(clientSessionID);
      }
      stopFlow(info, null);
      try
      {
        unsubscribe((Publisher)pubsub.first, (Subscriber)pubsub.second);
      }
      catch (Exception e)
      {
        logger.warn(e.getMessage());
      }
      undeployFlow(info, null);
      if (logger.isDebugEnabled()) {
        logger.debug(Thread.currentThread().getName() + " : undeploying app : " + info.getFullName() + " done.");
      }
      if (WAQueue.getQueue("consoleQueue" + clientSessionID.getUUIDString()).listeners.size() == 0)
      {
        WAQueue.getQueue("consoleQueue" + clientSessionID.getUUIDString()).isActive = false;
        if (logger.isDebugEnabled()) {
          logger.debug("consoleQueue" + clientSessionID.getUUIDString() + " removed, due to no listeners");
        }
        WAQueue.removeQueue("consoleQueue" + clientSessionID.getUUIDString());
      }
    }
    finally
    {
      lock.unlock();
      LocalLockProvider.removeLock(keyForLock);
    }
    if (logger.isDebugEnabled()) {
      logger.debug(Thread.currentThread().getName() + " : " + what + " : " + info.getFullName() + " DONE.");
    }
  }
  
  public Flow deployFlow(MetaInfo.Flow info, boolean doStartNodeManager)
    throws Exception
  {
    if (!DeployUtility.canFlowBeDeployed(info, getServerID())) {
      return null;
    }
    checkNotDeployed(info);
    
    Flow f = createFlow(info, null);
    if (f == null)
    {
      logger.warn("Flow " + info.uuid + " cannot be deployed because create flow failed");
      return null;
    }
    boolean startNodeManager = false;
    try
    {
      startNodeManager = f.deploy(null);
    }
    catch (Throwable e)
    {
      logger.error("Deploy for flow: " + info.name + "(uuid: " + info.uuid + ") failed with exception " + e.getMessage(), e);
      removeDeployedObject(f);
      throw e;
    }
    List<MetaInfo.MetaObjectInfo> deployed = null;
    if (getOpenObject(info.uuid) != null) {
      deployed = Flow.getObjectsMeta(f.getDeployedObjects());
    }
    if (deployed != null)
    {
      this.ServerInfo.addCurrentObjectsInServer(f.getMetaName(), deployed);
      this.metadataRepository.putServer(this.ServerInfo, this.sessionID);
    }
    if (countCaches(info) > 0) {
      initCaches(info);
    }
    if ((startNodeManager) && (doStartNodeManager)) {
      f.startNodeManager();
    }
    return f;
  }
  
  public void initCaches(MetaInfo.Flow info)
    throws Exception
  {
    Flow f = (Flow)getOpenObject(info.uuid);
    if (f != null) {
      f.initCaches();
    }
  }
  
  public void startCaches(MetaInfo.Flow info, List<UUID> servers, long epochNumber)
    throws Exception
  {
    if ((servers == null) || (servers.isEmpty())) {
      throw new Exception("Passed in list of servers is empty");
    }
    Flow f = (Flow)getOpenObject(info.uuid);
    if (f != null) {
      f.startCaches(servers, Long.valueOf(epochNumber));
    }
  }
  
  public void stopCaches(MetaInfo.Flow info, long epochNumber)
    throws Exception
  {
    Flow f = (Flow)getOpenObject(info.uuid);
    if (f != null) {
      f.stopCaches(Long.valueOf(epochNumber));
    }
  }
  
  public void startFlow(MetaInfo.Flow info, List<Property> params, List<UUID> servers, Long epochNumber)
    throws Exception
  {
    Flow flow = setupRecovery(info, params);
    try
    {
      if (servers != null) {
        flow.startFlow(servers, epochNumber);
      } else {
        flow.startFlow(epochNumber);
      }
    }
    catch (Exception e)
    {
      throw e;
    }
  }
  
  public Flow setupRecovery(MetaInfo.Flow info, List<Property> params)
    throws Exception
  {
    assert (info != null);
    Flow flow = (Flow)getOpenObject(info.uuid);
    if (flow == null)
    {
      logger.warn("Cannot find application to start: " + info.name);
      return flow;
    }
    ILock appCheckpointLock = null;
    ILock wsCheckpointLock = null;
    RecoveryDescription recov = null;
    String signalsSetName = "default";
    if (params != null) {
      for (Property param : params)
      {
        if (param.name.equals("RecoveryDescription")) {
          recov = (RecoveryDescription)param.value;
        }
        if (param.name.equals("StartupSignalsSetName")) {
          signalsSetName = (String)param.value;
        }
      }
    }
    if (recov != null)
    {
      if (recov.type == 3)
      {
        ISet signalsSet = HazelcastSingleton.get().getSet(signalsSetName);
        try
        {
          appCheckpointLock = HazelcastSingleton.get().getLock(info.uuid.toString());
          appCheckpointLock.lock();
          if (!signalsSet.contains(info.uuid.toString()))
          {
            StatusDataStore.getInstance().clearAppCheckpoint(flow.getMetaID());
            signalsSet.add(info.uuid.toString());
          }
          flow.setAppCheckpoint(null);
        }
        finally
        {
          if (appCheckpointLock != null) {
            appCheckpointLock.unlock();
          }
        }
        List<WactionStore> allWactionStores = flow.getAllWactionStores();
        Object failWactionStores = new Stack();
        for (WactionStore ws : allWactionStores) {
          try
          {
            wsCheckpointLock = HazelcastSingleton.get().getLock(ws.getMetaID().toString());
            wsCheckpointLock.lock();
            if (!signalsSet.contains(ws.getMetaID().toString()))
            {
              if (Logger.getLogger("Recovery").isDebugEnabled()) {
                Logger.getLogger("Recovery").debug("Resetting the WactionStore Checkpoint for WS " + ws.getMetaName() + " on node " + getServerName());
              }
              if (!ws.clearWactionStoreCheckpoint()) {
                ((Stack)failWactionStores).push(ws);
              }
              signalsSet.add(ws.getMetaID().toString());
            }
          }
          finally
          {
            if (wsCheckpointLock != null) {
              wsCheckpointLock.unlock();
            }
          }
        }
        if (!((Stack)failWactionStores).isEmpty())
        {
          StringBuilder names = new StringBuilder("Failed to clear WactionStore checkpoint for: ");
          for (WactionStore ws : (Stack)failWactionStores) {
            names.append(ws.getMetaName()).append(" ");
          }
          throw new Exception(names.toString());
        }
      }
      flow.setRecoveryType(Integer.valueOf(2));
      flow.setRecoveryPeriod(Long.valueOf(recov.interval));
    }
    else
    {
      flow.setRecoveryType(Integer.valueOf(flow.flowInfo.recoveryType));
      flow.setRecoveryPeriod(Long.valueOf(flow.flowInfo.recoveryPeriod));
    }
    return flow;
  }
  
  public void verifyStartFlow(MetaInfo.Flow info, List<UUID> servers)
    throws Exception
  {
    if ((servers == null) || (servers.isEmpty())) {
      throw new Exception("Passed in list of servers is empty");
    }
    if (servers != null)
    {
      Flow fl = (Flow)getOpenObject(info.uuid);
      if (fl == null) {
        throw new Exception("Flow is not deployed on this node. Failed to verify flow start");
      }
      if (!fl.verifyAppStart(servers)) {
        throw new Exception("Start flow failed this time. Failed to verify flow start");
      }
    }
  }
  
  public void startSources(MetaInfo.Flow info)
    throws Exception
  {
    assert (info != null);
    Flow fl = (Flow)getOpenObject(info.uuid);
    if (fl != null) {
      try
      {
        fl.startSources();
      }
      catch (Exception e)
      {
        logger.warn("Failure in Starting Sources.", e);
        throw e;
      }
    }
  }
  
  public void stopFlow(MetaInfo.Flow info, Long epochNumber)
    throws Exception
  {
    assert (info != null);
    Flow fl = (Flow)getOpenObject(info.uuid);
    if (fl != null) {
      fl.stopFlow(epochNumber);
    }
  }
  
  private void closeNotUsedStreams(List<Flow> flows)
    throws Exception
  {
    for (Flow f : flows)
    {
      Map<UUID, Stream> used_streams = f.getUsedStreams();
      for (Stream stream_runtime : used_streams.values()) {
        if (stream_runtime != null)
        {
          for (Flow flow : flows) {
            stream_runtime.disconnectFlow(flow);
          }
          if (!stream_runtime.isConnected()) {
            removeDeployedObject(stream_runtime);
          }
        }
      }
    }
  }
  
  public void undeployFlow(MetaInfo.Flow info, UUID clientSessionID)
    throws Exception
  {
    assert (info != null);
    verifyQueries(info, clientSessionID);
    Flow f = (Flow)getOpenObject(info.uuid);
    if (f != null)
    {
      List<Flow> flows = f.getAllFlows();
      List<MetaInfo.MetaObjectInfo> undeployed = Flow.getObjectsMeta(f.getDeployedObjects());
      removeDeployedObject(f);
      closeNotUsedStreams(flows);
      this.ServerInfo.removeCurrentObjectsInServer(f.getMetaName(), undeployed);
      this.metadataRepository.putServer(this.ServerInfo, WASecurityManager.TOKEN);
    }
  }
  
  private Set<UUID> findDataSourcesUUID(MetaInfo.Flow application)
  {
    EntityType[] dataSources = { EntityType.STREAM, EntityType.CACHE, EntityType.WACTIONSTORE };
    Set<UUID> objectsBelongToApplication = new LinkedHashSet();
    for (EntityType entityType : dataSources) {
      objectsBelongToApplication.addAll(application.getObjects(entityType));
    }
    return objectsBelongToApplication;
  }
  
  private Set<MetaInfo.MetaObject> findDataSources(Set<UUID> allQueriableDataSources)
    throws MetaDataRepositoryException
  {
    Set<MetaInfo.MetaObject> objectsBelongToApplication = new LinkedHashSet();
    for (UUID uuid : allQueriableDataSources) {
      objectsBelongToApplication.add(getObject(uuid));
    }
    return objectsBelongToApplication;
  }
  
  private void verifyQueries(MetaInfo.Flow info, UUID clientUUID)
    throws MetaDataRepositoryException
  {
    if (clientUUID == null) {
      return;
    }
    Set<MetaInfo.MetaObject> allQueriableDataSources = findDataSources(findDataSourcesUUID(info));
    
    Set<UUID> reverseObjectsFromQueriableDataSources = new LinkedHashSet();
    for (MetaInfo.MetaObject queriableObject : allQueriableDataSources) {
      reverseObjectsFromQueriableDataSources.addAll(queriableObject.getReverseIndexObjectDependencies());
    }
    Set<MetaInfo.MetaObject> adhocQueryRelatedObjects = new LinkedHashSet();
    for (UUID reverseIndex : reverseObjectsFromQueriableDataSources)
    {
      MetaInfo.MetaObject reverseIndexedMetaObject = getObject(reverseIndex);
      if ((reverseIndexedMetaObject != null) && (reverseIndexedMetaObject.getMetaInfoStatus().isAdhoc())) {
        adhocQueryRelatedObjects.add(reverseIndexedMetaObject);
      }
    }
    Set<MetaInfo.Flow> generatedAdhocApplications = new LinkedHashSet();
    for (MetaInfo.MetaObject metaObject : adhocQueryRelatedObjects)
    {
      MetaInfo.Flow application = metaObject.getCurrentApp();
      if ((application != null) && (application.getMetaInfoStatus().isAdhoc())) {
        generatedAdhocApplications.add(application);
      }
    }
    for (MetaInfo.Flow application : generatedAdhocApplications) {
      if ((application != null) && (application.getReverseIndexObjectDependencies() != null))
      {
        Iterator<UUID> iterator = application.getReverseIndexObjectDependencies().iterator();
        if (iterator.hasNext())
        {
          UUID queryUUID = (UUID)application.getReverseIndexObjectDependencies().iterator().next();
          if (queryUUID != null)
          {
            QueryValidator queryValidator = new QueryValidator(this);
            queryValidator.createNewContext(new AuthToken(clientUUID));
            queryValidator.stopAdhocQuery(new AuthToken(clientUUID), queryUUID);
          }
        }
      }
    }
  }
  
  public void showStreamStmt(MetaInfo.ShowStream show_stream)
    throws Exception
  {
    Stream stream = (Stream)getOpenObject(show_stream.stream_name);
    if (stream != null) {
      stream.initQueue(show_stream);
    }
  }
  
  public static void shutDown(String who)
    throws Exception
  {
    if (logger.isInfoEnabled()) {
      logger.info("shutdown " + who);
    }
  }
  
  private static WebServer startWebServer(Server srv)
  {
    Lock webServerLock = HazelcastSingleton.get().getLock("startingWebServer");
    
    webServerLock.lock();
    try
    {
      QueryValidator qv = new QueryValidator(srv);
      Context ctx = srv.createContext();
      qv.addContext(ctx.getAuthToken(), ctx);
      webServer = new WebServer(HazelcastSingleton.getBindingInterface());
      Class<?> oracleWizClass = WALoader.get().loadClass("com.bloom.source.oraclecommon.OracleCDCWizard");
      Class<?> mssqlWizClass = WALoader.get().loadClass("com.bloom.source.mssql.MSSqlWizard");
      Class<?> mysqlWizClass = WALoader.get().loadClass("com.bloom.source.mysql.MySQLWizard");
      ICDCWizard msWiz = (ICDCWizard)mssqlWizClass.newInstance();
      ICDCWizard OraWiz = (ICDCWizard)oracleWizClass.newInstance();
      ICDCWizard mysqlWiz = (ICDCWizard)mysqlWizClass.newInstance();
      try
      {
        RMIWebSocket.register(WactionApi.get());
        RMIWebSocket.register(WASecurityManager.get());
        RMIWebSocket.register(MetadataRepository.getINSTANCE());
        RMIWebSocket.register(qv);
        RMIWebSocket.register(Usage.getInstance());
        RMIWebSocket.register(OraWiz);
        RMIWebSocket.register(msWiz);
        RMIWebSocket.register(mysqlWiz);
      }
      catch (Exception e)
      {
        logger.error("Problem registering web server WS classes", e);
      }
      try
      {
        webServer.addContextHandler("/rmiws", new RMIWebSocket.Handler());
        
        webServer.addServletContextHandler("/wactions/*", new WactionApiServlet());
        
        webServer.addServletContextHandler("/security/*", new SecurityManagerServlet());
        
        webServer.addServletContextHandler("/metadata/*", new MetadataRepositoryServlet());
        
        webServer.addServletContextHandler("/api/metadata/*", new MetadataRepositoryServlet());
        
        webServer.addResourceHandler("/web", "./Platform/web", true, new String[] { "index.html" });
        
        webServer.addResourceHandler("/login", "./ui", true, new String[] { "login/index.html" });
        
        webServer.addResourceHandler("/dashboards", "./ui", true, new String[] { "dashboards/index.html" });
        
        webServer.addResourceHandler("/dashboard", "./ui", true, new String[] { "dashboard_index.html" });
        
        webServer.addResourceHandler("/dashboard/edit", "./ui", true, new String[] { "edit-visualizations.html" });
        
        webServer.addResourceHandler("/monitor", "./ui", true, new String[] { "monitor_index.html" });
        
        webServer.addResourceHandler("/admin", "./ui/admin", true, new String[] { "index.html" });
        
        webServer.addResourceHandler("/docs", "./webui/onlinedocs/", true, new String[] { "bloom_documentation.html" });
        
        webServer.addResourceHandler("/flow", "./ui", true, new String[] { "flow/flow.html" });
        
        webServer.addResourceHandler("/flow/edit", "./ui", true, new String[] { "flow/flow.html" });
        
        webServer.addResourceHandler("/root", "./ui", false, null);
        webServer.addResourceHandler("/webui", "./webui", true, new String[] { "index.html" });
        
        webServer.addResourceHandler("/", "./ui", true, new String[] { "index.html" });
        
        webServer.addServletContextHandler("/wactions/*", new WactionApiServlet());
        
        webServer.addServletContextHandler("/security/*", new SecurityManagerServlet());
        
        webServer.addServletContextHandler("/upload/*", new FileUploaderServlet());
        
        webServer.addServletContextHandler("/file_browser/*", new FileSystemBrowserServlet());
        
        webServer.addServletContextHandler("/preview/*", new DataPreviewServlet());
        
        webServer.addServletContextHandler("/streams/*", new StreamInfoServlet());
        
        webServer.addServletContextHandler("/health/*", new HealthServlet());
        RMIWebSocket.initCleanupRoutine();
      }
      catch (Exception e)
      {
        logger.error("Problem creating web server contexts", e);
      }
      try
      {
        webServer.start();
      }
      catch (Exception e)
      {
        logger.error("Problem starting web server", e);
        throw new RuntimeException("Problem starting web server: " + e.getMessage());
      }
    }
    catch (ClassNotFoundException e)
    {
      logger.error("Could not find specified class", e);
      e.printStackTrace();
    }
    catch (InstantiationException e)
    {
      e.printStackTrace();
    }
    catch (IllegalAccessException e)
    {
      e.printStackTrace();
    }
    finally
    {
      webServerLock.unlock();
    }
    return webServer;
  }
  
  private Future<?> discoveryTask = null;
  
  public void startDiscoveryServer()
  {
    UdpDiscoveryServer discoveryServer = new UdpDiscoveryServer(webServer.getHttpPort(), WASecurityManager.ENCRYPTION_SALT);
    this.discoveryTask = this.pool.submit(discoveryServer);
  }
  
  public void updateWebUri()
  {
    String baseHttpUri = "http://" + HazelcastSingleton.getBindingInterface() + ":" + webServer.getHttpPort();
    try
    {
      this.ServerInfo.setWebBaseUri(baseHttpUri);
      this.metadataRepository.putServer(this.ServerInfo, WASecurityManager.TOKEN);
    }
    catch (MetaDataRepositoryException e)
    {
      logger.warn("Failed to add base URI to ServerInfo " + baseHttpUri, e);
    }
  }
  
  public int printOpenObjs()
  {
    logger.info("Open Objs");
    if (this.openObjects != null)
    {
      logger.info("Size: " + this.openObjects.size());
      return this.openObjects.size();
    }
    return 0;
  }
  
  public static void main(String[] args)
    throws Exception
  {
    printf("Starting bloom Server - " + Version.getVersionString() + "\n");
    if (logger.isInfoEnabled()) {
      logger.info("Starting bloom Server - " + Version.getVersionString() + "\n");
    }
    server = new Server();
    
    ILock lock = HazelcastSingleton.get().getLock("#ServerStartupLock");
    lock.lock();
    try
    {
      server.initializeHazelcast();
      
      WActionStores.startup();
      server.checkVersion();
      server.setSecMgrAndOther();
      server.addAlertQueue("systemAlerts");
      server.initExceptionHandler();
      server.initShowStream();
      server.startAppManager();
      
      WebServer webServer = null;
      try
      {
        webServer = startWebServer(server);
        server.updateWebUri();
        server.startDiscoveryServer();
      }
      catch (Exception e)
      {
        logger.error(e.getMessage() + " - Shutting down!");
        shutDown("Web Server Failure");
        System.exit(77);
      }
      WASecurityManager.setFirstTime(false);
      if (System.getProperty("com.bloom.config.odbcenabled", "false").equalsIgnoreCase("true")) {
        try
        {
          System.loadLibrary("bloomODBCJNI");
          
          server.setClassLoader(WALoader.get());
          
          server.initSimbaServer(HazelcastSingleton.getBindingInterface());
        }
        catch (UnsatisfiedLinkError e)
        {
          logger.error("Unable to initialize ODBC driver. Reason - " + e.getLocalizedMessage());
        }
      }
      printf("\nstarted.");
      
      String srvInterface = HazelcastSingleton.getBindingInterface();
      int wsHttpPort = webServer.getHttpPort();
      int wsHttpsPort = webServer.getHttpsPort();
      if ((wsHttpPort != -1) && (wsHttpsPort != -1)) {
        printf("\nPlease go to http://" + (srvInterface != null ? srvInterface : "localhost") + ":" + wsHttpPort + " or https://" + (srvInterface != null ? srvInterface : "localhost") + ":" + wsHttpsPort + " to administer, or use console\n");
      } else if (wsHttpPort != -1) {
        printf("\nPlease go to http://" + (srvInterface != null ? srvInterface : "localhost") + ":" + wsHttpPort + " to administer, or use console\n");
      } else if (wsHttpsPort != -1) {
        printf("\nPlease go to https://" + (srvInterface != null ? srvInterface : "localhost") + ":" + wsHttpsPort + " to administer, or use console\n");
      } else {
        printf("\nNo WebServer configured, please use the console to administer\n");
      }
      if (System.getProperty("com.bloom.integration.test.util.DistributedTestHandlerLoader", "false").equalsIgnoreCase("true"))
      {
        Class<?> clazz = null;
        try
        {
          clazz = Thread.currentThread().getContextClassLoader().loadClass("com.bloom.integration.test.util.DistributedTestHandlerLoader");
          if (clazz != null)
          {
            Object chorusHandlers = clazz.newInstance();
            Method loadH = clazz.getMethod("loadHandlers", new Class[0]);
            if (loadH != null) {
              loadH.invoke(chorusHandlers, new Object[0]);
            }
          }
        }
        catch (Exception cfne) {}
      }
      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
      {
        public void run()
        {
          try
          {
            Server.shutDown("server");
          }
          catch (Exception e)
          {
            Server.logger.warn("Problem shutting down", e);
          }
        }
      }));
    }
    finally
    {
      lock.unlock();
    }
  }
  
  public Collection<FlowComponent> getAllObjectsView()
  {
    return this.openObjects.values();
  }
  
  public Collection<Channel> getAllChannelsView()
  {
    return Collections.emptyList();
  }
  
  public UUID getServerID()
  {
    return this.serverID;
  }
  
  private String s2n(UUID sid)
  {
    return sid.toString();
  }
  
  private String getDebugId()
  {
    return s2n(this.serverID);
  }
  
  private static void addLibraryPath(File file)
  {
    try
    {
      Method method = URLClassLoader.class.getDeclaredMethod("addURL", new Class[] { URL.class });
      
      method.setAccessible(true);
      method.invoke(ClassLoader.getSystemClassLoader(), new Object[] { file.toURI().toURL() });
    }
    catch (NoSuchMethodException|SecurityException|IllegalAccessException|IllegalArgumentException|InvocationTargetException|MalformedURLException e)
    {
      e.printStackTrace();
    }
  }
  
  public Context createContext()
  {
    return new Context(this.sessionID);
  }
  
  public Context createContext(AuthToken token)
  {
    return new Context(token);
  }
  
  public static double evictionThresholdPercent()
  {
    double defaultVal = 66.0D;
    if (HazelcastSingleton.isAvailable())
    {
      if (HazelcastSingleton.isClientMember()) {
        return defaultVal;
      }
      Object val = HazelcastSingleton.get().getMap("#ClusterSettings").get("com.bloom.config.waction-eviction-threshold");
      if (val != null) {
        return ((Double)val).doubleValue();
      }
    }
    String localProp = System.getProperty("com.bloom.config.waction-eviction-threshold");
    Double dVal = null;
    if (localProp != null) {
      try
      {
        dVal = Double.valueOf(Double.parseDouble(localProp));
      }
      catch (NumberFormatException e)
      {
        dVal = new Double(defaultVal);
      }
    } else {
      dVal = new Double(defaultVal);
    }
    if (HazelcastSingleton.isAvailable()) {
      HazelcastSingleton.get().getMap("#ClusterSettings").put("com.bloom.config.waction-eviction-threshold", dVal);
    }
    return dVal.doubleValue();
  }
  
  public static boolean persistenceIsEnabled()
  {
    if (HazelcastSingleton.isAvailable())
    {
      if (HazelcastSingleton.isClientMember()) {
        return false;
      }
      Object enablePersist = HazelcastSingleton.get().getMap("#ClusterSettings").get("com.bloom.config.persist");
      if (enablePersist != null) {
        return enablePersist.toString().equalsIgnoreCase("TRUE");
      }
    }
    String localProp = System.getProperty("com.bloom.config.persist");
    Boolean b = new Boolean((localProp == null) || (!localProp.equalsIgnoreCase("FALSE")));
    if (HazelcastSingleton.isAvailable()) {
      HazelcastSingleton.get().getMap("#ClusterSettings").put("com.bloom.config.persist", b);
    }
    return b.booleanValue();
  }
  
  private static void printf(String toPrint)
  {
    if (System.console() != null) {
      System.console().printf(toPrint, new Object[0]);
    } else {
      System.out.printf(toPrint, new Object[0]);
    }
    System.out.flush();
  }
  
  public void entryRemoved(EntryEvent<String, Object> event) {}
  
  public void entryEvicted(EntryEvent<String, Object> event) {}
  
  public void entryAdded(EntryEvent<String, Object> event)
  {
    entryUpdated(event);
  }
  
  public void entryUpdated(EntryEvent<String, Object> event)
  {
    if (((String)event.getKey()).equalsIgnoreCase("com.bloom.config.enable-monitor-persistence"))
    {
      MonitorModel monitorModel = MonitorModel.getInstance();
      if ((monitorModel != null) && (event.getValue() != null))
      {
        boolean turnOn = event.getValue().toString().equalsIgnoreCase("TRUE");
        
        monitorModel.setPersistence(turnOn);
      }
    }
  }
  
  public MetaInfo.StatusInfo.Status getFlowStatus(MetaInfo.Flow flow)
    throws Exception
  {
    Flow f = (Flow)getOpenObject(flow.uuid);
    if (f == null) {
      return MetaInfo.StatusInfo.Status.CREATED;
    }
    flow = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByUUID(flow.getUuid(), WASecurityManager.TOKEN);
    if (flow != null) {
      return flow.getFlowStatus();
    }
    return MetaInfo.StatusInfo.Status.UNKNOWN;
  }
  
  public Map<UUID, Boolean> whatIsRunning(UUID flowId)
    throws Exception
  {
    Map<UUID, Boolean> map = new HashMap();
    Flow f = (Flow)getOpenObject(flowId);
    if (f != null) {
      for (FlowComponent o : f.getDeployedObjects()) {
        if ((o instanceof Restartable))
        {
          Restartable r = (Restartable)o;
          boolean running = r.isRunning();
          map.put(o.getMetaID(), Boolean.valueOf(running));
        }
      }
    }
    return map;
  }
  
  public Collection<MetaInfo.MetaObjectInfo> getDeployedObjects(UUID flowId)
    throws Exception
  {
    Flow f = (Flow)getOpenObject(flowId);
    if (f != null) {
      return Flow.getObjectsMeta(f.getDeployedObjects());
    }
    return Collections.emptyList();
  }
  
  public void onItem(Object item)
  {
    if ((item != null) && ((item instanceof ExceptionEvent)))
    {
      ExceptionEvent ee = (ExceptionEvent)item;
      if (ee.action.ordinal() == ActionType.STOP.ordinal()) {
        try
        {
          UUID id = ee.getAppid();
          if (id != null)
          {
            MetaInfo.Flow flow = (MetaInfo.Flow)getObject(id);
            stopFlow(flow, null);
          }
          else if (logger.isInfoEnabled())
          {
            logger.info("exception even sent without app uuid info");
          }
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
      }
    }
  }
  
  public boolean isServer()
  {
    return true;
  }
  
  public void mapEvicted(MapEvent event) {}
  
  public void mapCleared(MapEvent event) {}
  
  public void clientConnected(Client arg0) {}
  
  public void clientDisconnected(Client arg0)
  {
    ILock lock = HazelcastSingleton.get().getLock("ServerList");
    lock.lock();
    try
    {
      MetaInfo.Server removedAgent = (MetaInfo.Server)this.metadataRepository.getMetaObjectByUUID(new UUID(arg0.getUuid()), this.sessionID);
      if (removedAgent == null)
      {
        getShowStream().cleanChannelSubscriber(arg0.getUuid());
      }
      else if ((removedAgent != null) && (removedAgent.isAgent))
      {
        Collection<Client> members = HazelcastSingleton.get().getClientService().getConnectedClients();
        List<UUID> valid = new ArrayList();
        for (Client cl : members) {
          valid.add(new UUID(cl.getUuid()));
        }
        List<UUID> toRemove = new ArrayList();
        
        Set<MetaInfo.MetaObject> agents = this.metadataRepository.getByEntityType(EntityType.SERVER, this.sessionID);
        for (MetaInfo.MetaObject obj : agents)
        {
          MetaInfo.Server agent = (MetaInfo.Server)obj;
          if ((!valid.contains(agent.uuid)) && (agent.isAgent)) {
            toRemove.add(agent.uuid);
          }
        }
        for (UUID uuid : toRemove)
        {
          if (logger.isDebugEnabled()) {
            logger.debug("Trying to remove Agent : " + uuid + " from \n" + valid);
          }
          this.metadataRepository.removeMetaObjectByUUID(uuid, this.sessionID);
        }
        removeServerFromDeploymentGroup(arg0);
      }
      trimAuthTokens(arg0.getUuid());
    }
    catch (MetaDataRepositoryException e) {}catch (Throwable e)
    {
      logger.error("Failed to remove member with exception " + e);
    }
    finally
    {
      lock.unlock();
    }
    UUID consoleQueueAuthToken = (UUID)nodeIDToAuthToken.get(new UUID(arg0.getUuid()));
    if (consoleQueueAuthToken != null) {
      DistributedExecutionManager.exec(HazelcastSingleton.get(), new AdhocCleanup(consoleQueueAuthToken.getUUIDString()), HazelcastSingleton.get().getCluster().getMembers());
    }
  }
  
  public static void trimAuthTokens(String clientId)
  {
    if ((clientId == null) || (clientId.isEmpty())) {
      return;
    }
    try
    {
      IMap<AuthToken, SessionInfo> authTokens = HazelcastSingleton.get().getMap("#authTokens");
      boolean found = false;
      for (AuthToken authToken : authTokens.keySet())
      {
        SessionInfo sessionInfo = (SessionInfo)authTokens.get(authToken);
        if ((sessionInfo.clientId != null) && (sessionInfo.clientId.equals(clientId)))
        {
          if (logger.isInfoEnabled()) {
            logger.info("Removed " + sessionInfo.type + ": " + clientId);
          }
          authTokens.remove(authToken);
          found = true;
          break;
        }
      }
      if (!found) {
        logger.warn("Could not find session \"" + clientId + "\" for token removal");
      }
    }
    catch (Exception e)
    {
      logger.error("Error when trimming " + clientId + " from auth tokens", e);
    }
  }
  
  public FlowComponent createComponent(MetaInfo.MetaObject o)
    throws Exception
  {
    switch (o.getType())
    {
    case CACHE: 
      return createCache((MetaInfo.Cache)o);
    case CQ: 
      return createCQ((MetaInfo.CQ)o);
    case SORTER: 
      return createSorter((MetaInfo.Sorter)o);
    case SOURCE: 
      return createSource((MetaInfo.Source)o);
    case STREAM_GENERATOR: 
      return createStreamGen((MetaInfo.StreamGenerator)o);
    case TARGET: 
      return createTarget((MetaInfo.Target)o);
    case WACTIONSTORE: 
      return createWActionStore((MetaInfo.WActionStore)o);
    case WASTOREVIEW: 
      return createWAStoreView((MetaInfo.WAStoreView)o);
    case WINDOW: 
      return createWindow((MetaInfo.Window)o);
    }
    return null;
  }
  
  public void changeOptions(String paramname, Object paramvalue)
  {
    if (paramname.equalsIgnoreCase("loglevel")) {
      Utility.changeLogLevel(paramvalue, false);
    }
  }
}
