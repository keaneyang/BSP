package com.bloom.runtime.components;

import com.bloom.appmanager.NodeManager;
import com.bloom.exception.ServerException;
import com.bloom.historicalcache.Cache;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.metaRepository.StatusDataStore;
import com.bloom.persistence.WactionStore;
import com.bloom.proc.SourceProcess;
import com.bloom.runtime.BaseServer;
import com.bloom.runtime.DeploymentStrategy;
import com.bloom.runtime.Pair;
import com.bloom.runtime.Property;
import com.bloom.runtime.Server;
import com.bloom.runtime.channels.KafkaChannel;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.MetaInfo.DeploymentGroup;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.MetaObjectInfo;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.meta.MetaInfo.WActionStore;
import com.bloom.runtime.meta.MetaInfo.Flow.Detail;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.runtime.window.Window;
import com.bloom.security.WASecurityManager;
import com.bloom.utility.DeployUtility;
import com.bloom.utility.Utility;
import com.bloom.uuid.UUID;
import com.bloom.recovery.PartitionedSourcePosition;
import com.bloom.recovery.Path;
import com.bloom.recovery.Path.Item;
import com.bloom.recovery.Position;
import com.bloom.recovery.SourcePosition;

import java.io.PrintStream;
import java.util.ArrayList;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class Flow
  extends FlowComponent
  implements Restartable, FlowStateApi
{
  private static Logger logger = Logger.getLogger(Flow.class);
  private final Flow parent;
  private volatile boolean running = false;
  private final Server srv;
  public final MetaInfo.Flow flowInfo;
  private final List<IStreamGenerator> gens = new ArrayList();
  private final List<Source> sources = new ArrayList();
  private final List<Target> targets = new ArrayList();
  private final List<CQTask> cqs = new ArrayList();
  private final List<IWindow> windows = new ArrayList();
  private final List<Sorter> sorters = new ArrayList();
  private final List<Cache> caches = new ArrayList();
  private final List<WactionStore> wastores = new ArrayList();
  private final List<WAStoreView> wastoreviews = new ArrayList();
  private final List<Flow> flows = new ArrayList();
  private final Map<UUID, Stream> usedStreams = new HashMap();
  private final List<Compound> compoundComponents = new ArrayList();
  private Long recoveryPeriod = null;
  private Integer recoveryType = null;
  private Position appCheckpoint = null;
  private MDRepository mdRepository = MetadataRepository.getINSTANCE();
  private NodeManager nodeManager;
  private ScheduledExecutorService appCheckpointExecutor = null;
  
  public Flow(MetaInfo.Flow flowInfo, BaseServer srv, Flow parent)
    throws Exception
  {
    super(srv, flowInfo);
    this.parent = parent;
    this.srv = ((Server)srv);
    this.flowInfo = flowInfo;
    for (UUID streamID : getListOfReferredStreams()) {
      this.usedStreams.put(streamID, null);
    }
  }
  
  public Flow getTopLevelFlow()
  {
    if (getMetaType() == EntityType.APPLICATION) {
      return this;
    }
    return super.getTopLevelFlow();
  }
  
  public Flow getFlow()
  {
    if (getMetaType() == EntityType.APPLICATION) {
      return this;
    }
    return super.getFlow();
  }
  
  public void startNodeManager()
    throws Exception
  {
    this.nodeManager = new NodeManager(this, this.srv);
  }
  
  public NodeManager getNodeManager()
  {
    return this.nodeManager;
  }
  
  private void getStreamsInObject(UUID id, Set<UUID> result)
    throws Exception
  {
    MetaInfo.MetaObject obj = this.srv.getObject(id);
    if (obj != null) {
      if ((obj instanceof MetaInfo.Stream)) {
        result.add(obj.uuid);
      } else {
        for (UUID dep : obj.getDependencies()) {
          getStreamsInObject(dep, result);
        }
      }
    }
  }
  
  private Collection<UUID> getListOfReferredStreams()
    throws Exception
  {
    Set<UUID> result = new HashSet();
    for (UUID dep : getMetaDependencies()) {
      getStreamsInObject(dep, result);
    }
    return result;
  }
  
  private void stopObjects(MetaInfo.MetaObject mo)
    throws Exception
  {
    FlowComponent o = this.srv.getOpenObject(mo.getUuid());
    if (o == null) {
      return;
    }
    switch (mo.getType())
    {
    case STREAM: 
      UUID streamUUID = mo.getUuid();
      if ((this.flowInfo != null) && (this.flowInfo.objects != null) && (this.flowInfo.objects.containsKey(EntityType.STREAM)) && 
        (((LinkedHashSet)this.flowInfo.objects.get(EntityType.STREAM)).contains(streamUUID))) {
        ((Stream)o).stop();
      }
      break;
    case CQ: 
      assert (mo.getType() == EntityType.CQ);
      ((CQTask)o).stop();
      break;
    case SORTER: 
      ((Sorter)o).stop();
      break;
    case SOURCE: 
      ((Source)o).stop();
      break;
    case STREAM_GENERATOR: 
      ((IStreamGenerator)o).stop();
      break;
    case TARGET: 
      ((Target)o).stop();
      break;
    case WACTIONSTORE: 
      ((WactionStore)o).flush();
      ((WactionStore)o).stopCache();
      break;
    case WASTOREVIEW: 
      ((WAStoreView)o).stop();
      break;
    case WINDOW: 
      ((IWindow)o).stop();
      break;
    }
  }
  
  public synchronized void start()
    throws Exception
  {
    setFlowsAndCheckpoints(null);
    List<Pair<MetaInfo.MetaObject, MetaInfo.Flow>> orderWithFlowInfo = getFlowMeta().showTopology();
    Collections.reverse(orderWithFlowInfo);
    for (Pair<MetaInfo.MetaObject, MetaInfo.Flow> pair : orderWithFlowInfo)
    {
      Flow f = (Flow)this.srv.getOpenObject(((MetaInfo.Flow)pair.second).getUuid());
      if (f != null) {
        f.start((MetaInfo.MetaObject)pair.first);
      }
    }
  }
  
  public synchronized void start(List<UUID> servers)
    throws Exception
  {
    Map<UUID, Set<UUID>> serverToDeployedObjects = filterServerToDeployedList(servers, getMetaName());
    setFlowsAndCheckpoints(serverToDeployedObjects);
    List<Pair<MetaInfo.MetaObject, MetaInfo.Flow>> orderWithFlowInfo = getFlowMeta().showTopology();
    Collections.reverse(orderWithFlowInfo);
    for (Pair<MetaInfo.MetaObject, MetaInfo.Flow> pair : orderWithFlowInfo)
    {
      Flow f = (Flow)this.srv.getOpenObject(((MetaInfo.Flow)pair.second).getUuid());
      if (f != null) {
        f.start((MetaInfo.MetaObject)pair.first, serverToDeployedObjects);
      }
    }
  }
  
  void setFlowsAndCheckpoints(Map<UUID, Set<UUID>> serverToDeployedObjects)
    throws Exception
  {
    if (this.running) {
      return;
    }
    this.running = true;
    try
    {
      for (Stream stream : this.usedStreams.values()) {
        if (stream != null) {
          stream.start(serverToDeployedObjects);
        }
      }
      for (Flow s : this.flows) {
        s.setFlowsAndCheckpoints(serverToDeployedObjects);
      }
    }
    catch (Exception e)
    {
      stopAllComponents();
      this.running = false;
      throw e;
    }
  }
  
  public synchronized void start(MetaInfo.MetaObject obj, Map<UUID, Set<UUID>> serverToDeployedObjects)
    throws Exception
  {
    if (obj.getType().equals(EntityType.STREAM))
    {
      FlowComponent o = this.srv.getOpenObject(obj.getUuid());
      if (o != null) {
        ((Stream)o).start(serverToDeployedObjects);
      }
    }
    else
    {
      start(obj);
    }
  }
  
  public synchronized void start(MetaInfo.MetaObject obj)
    throws Exception
  {
    Position appCheckpoint = null;
    Position wactionStoreCheckpoint = null;
    Position mergedCheckpoint = null;
    try
    {
      FlowComponent o = this.srv.getOpenObject(obj.getUuid());
      switch (obj.getType())
      {
      case STREAM: 
        ((Stream)o).start();
        break;
      case CACHE: 
        ((Cache)o).start();
        break;
      case CQ: 
        ((CQTask)o).start();
        break;
      case SORTER: 
        ((Sorter)o).start();
        break;
      case SOURCE: 
        break;
      case STREAM_GENERATOR: 
        break;
      case TARGET: 
        if (getRecoveryType().intValue() == 2)
        {
          appCheckpoint = getAppCheckpoint();
          if (appCheckpoint != null)
          {
            Position p = appCheckpoint.getLowPositionForComponent(o.getMetaID());
            if (!p.isEmpty())
            {
              if (Logger.getLogger("Recovery").isInfoEnabled())
              {
                Logger.getLogger("Recovery").info("Repositioning target " + o.getMetaName() + " to " + p);
                Utility.prettyPrint(p);
              }
              ((Target)o).setRestartPosition(p);
            }
          }
        }
        ((Target)o).start();
        break;
      case WACTIONSTORE: 
        ((WactionStore)o).start();
        break;
      case WASTOREVIEW: 
        ((WAStoreView)o).start();
        break;
      case WINDOW: 
        if (getRecoveryType().intValue() == 2)
        {
          appCheckpoint = getAppCheckpoint();
          wactionStoreCheckpoint = getWactionStoreCheckpoint();
          mergedCheckpoint = new Position();
          
          mergedCheckpoint.mergeLowerPositions(appCheckpoint);
          mergedCheckpoint.mergeLowerPositions(wactionStoreCheckpoint);
          if (Logger.getLogger("Recovery").isDebugEnabled())
          {
            Logger.getLogger("Recovery").debug("Flow " + this.flowInfo.name + " building the Merged Checkpoint from the AC and the WSC");
            Logger.getLogger("Recovery").debug("    ...the App Checkpoint:");
            if (appCheckpoint != null) {
              Utility.prettyPrint(appCheckpoint);
            }
            Logger.getLogger("Recovery").debug("    ...the WactionStore Checkpoint:");
            if (wactionStoreCheckpoint != null) {
              Utility.prettyPrint(wactionStoreCheckpoint);
            }
            Logger.getLogger("Recovery").debug("    ...the Merged Checkpoint:");
            if (mergedCheckpoint != null) {
              Utility.prettyPrint(mergedCheckpoint);
            }
          }
          if ((wactionStoreCheckpoint != null) && 
            ((o instanceof Window)))
          {
            Window win = (Window)o;
            Position pMC = mergedCheckpoint.getLowPositionForComponent(o.getMetaID());
            if (!pMC.isEmpty()) {
              win.setWaitPosition(pMC);
            }
          }
        }
        ((IWindow)o).start();
      }
    }
    catch (Exception e)
    {
      stopAllComponents();
      this.running = false;
      throw e;
    }
  }
  
  public boolean verifyAppStart(List<UUID> servers)
    throws MetaDataRepositoryException
  {
    Map<UUID, Set<UUID>> serverToDeployedObjects = filterServerToDeployedList(servers, getMetaName());
    return verifyStart(serverToDeployedObjects);
  }
  
  private static Map<UUID, Set<UUID>> filterServerToDeployedList(List<UUID> servers, String flowName)
    throws MetaDataRepositoryException
  {
    Map<UUID, Set<UUID>> serverToDeployedObjects = new HashMap();
    
    Set<MetaInfo.Server> serversMetaInfo = MetadataRepository.getINSTANCE().getByEntityType(EntityType.SERVER, WASecurityManager.TOKEN);
    for (MetaInfo.Server srv : serversMetaInfo) {
      if (servers.contains(srv.getUuid()))
      {
        Map<String, Set<UUID>> srvObjectUUIDs = srv.getCurrentUUIDs();
        Set<UUID> deployedObjects = (Set)srvObjectUUIDs.get(flowName);
        serverToDeployedObjects.put(srv.getUuid(), deployedObjects);
      }
    }
    return serverToDeployedObjects;
  }
  
  public boolean verifyStart(Map<UUID, Set<UUID>> serverToDeployedObjects)
    throws MetaDataRepositoryException
  {
    if (!this.running) {
      return false;
    }
    try
    {
      for (Stream stream : this.usedStreams.values()) {
        if ((stream != null) && 
          (!stream.verifyStart(serverToDeployedObjects))) {
          return false;
        }
      }
      for (Flow s : this.flows) {
        if (!s.verifyStart(serverToDeployedObjects)) {
          return false;
        }
      }
    }
    catch (Exception e)
    {
      throw e;
    }
    return true;
  }
  
  private Position getAppCheckpoint()
    throws Exception
  {
    if (this.appCheckpoint == null)
    {
      Position p = StatusDataStore.getInstance().getAppCheckpoint(this.flowInfo.getCurrentApp().uuid);
      setAppCheckpoint(p);
    }
    return this.appCheckpoint;
  }
  
  public synchronized void startSources()
    throws Exception
  {
    try
    {
      Position appCheckpoint = getAppCheckpoint();
      if (logger.isInfoEnabled())
      {
        logger.info("Flow " + this.flowInfo.name + " on server " + BaseServer.getServerName() + " restarting sources at position " + appCheckpoint);
        if (appCheckpoint != null) {
          Utility.prettyPrint(appCheckpoint);
        }
      }
      if (appCheckpoint != null)
      {
        Set<Path> paths = new HashSet();
        for (Path path : appCheckpoint.values())
        {
          Path.Item lastItem = path.getLastPathItem();
          if (getMetaDependencies().contains(lastItem.getComponentUUID())) {
            paths.add(path);
          }
        }
        this.previousAppCheckpoint = new Position(paths);
        if (logger.isInfoEnabled())
        {
          logger.info("Flow " + this.flowInfo.name + " on server " + BaseServer.getServerName() + " initialized previous App Checkpoint to " + this.previousAppCheckpoint);
          if (this.previousAppCheckpoint != null) {
            Utility.prettyPrint(this.previousAppCheckpoint);
          }
        }
      }
      boolean sendPositions = recoveryIsEnabled();
      for (Source s : this.sources)
      {
        SourcePosition sp;
        SourcePosition sp;
        if (s.requiresPartitionedSourcePosition()) {
          sp = appCheckpoint != null ? appCheckpoint.getPartitionedSourcePositionForComponent(s.getMetaID()) : null;
        } else {
          sp = appCheckpoint != null ? appCheckpoint.getLowSourcePositionForComponent(s.getMetaID()) : null;
        }
        s.setPosition(sp);
        s.setSendPositions(sendPositions);
        s.setOwnerFlow(this);
        if (logger.isInfoEnabled()) {
          logger.info("Starting source " + s.getMetaName() + " at position " + sp);
        }
        s.start();
      }
      for (Stream s : this.usedStreams.values()) {
        if ((s != null) && (s.getMetaInfo().pset != null))
        {
          PartitionedSourcePosition sp = appCheckpoint != null ? appCheckpoint.getPartitionedSourcePositionForComponent(s.getMetaID()) : null;
          if (logger.isInfoEnabled()) {
            logger.info("Starting persistent stream " + s.getMetaName() + " at position " + sp);
          }
          s.setPositionAndStartEmitting(sp);
        }
      }
      for (IStreamGenerator g : this.gens) {
        g.start();
      }
      for (Flow s : this.flows) {
        s.startSources();
      }
      if ((this.flowInfo.recoveryPeriod > 0L) && 
        (this.appCheckpointExecutor == null))
      {
        Runnable appCheckpointRunnable = new Runnable()
        {
          public void run()
          {
            Flow.this.recordAppCheckpoint();
          }
        };
        this.appCheckpointExecutor = new ScheduledThreadPoolExecutor(1);
        this.appCheckpointExecutor.scheduleWithFixedDelay(appCheckpointRunnable, this.flowInfo.recoveryPeriod, this.flowInfo.recoveryPeriod, TimeUnit.SECONDS);
      }
    }
    catch (Exception e)
    {
      stopAllComponents();
      this.running = false;
      throw e;
    }
  }
  
  void stopFlowsAndCheckpoint()
  {
    if (!this.running) {
      return;
    }
    this.running = false;
    if (this.appCheckpointExecutor != null)
    {
      try
      {
        this.appCheckpointExecutor.shutdown();
        boolean terminated = this.appCheckpointExecutor.awaitTermination(10L, TimeUnit.SECONDS);
        if (!terminated) {
          logger.error("While stopping flow, failed to terminate the App Checkpoint thread. No final checkpoint was written.");
        } else if (recoveryIsEnabled()) {
          recordAppCheckpoint();
        }
      }
      catch (InterruptedException e)
      {
        logger.error("While stopping flow, termination the App Checkpoint thread was interrupted. No final checkpoint was written.", e);
      }
      this.appCheckpointExecutor = null;
    }
    for (Stream stream : this.usedStreams.values()) {
      if (stream != null) {
        if (stream.getMetaInfo().pset != null)
        {
          stream.stop();
        }
        else
        {
          UUID streamUUID = stream.getMetaID();
          if ((this.flowInfo != null) && (this.flowInfo.objects != null) && (this.flowInfo.objects.containsKey(EntityType.STREAM)) && 
            (((LinkedHashSet)this.flowInfo.objects.get(EntityType.STREAM)).contains(streamUUID))) {
            stream.stop();
          }
        }
      }
    }
    for (Flow s : this.flows) {
      s.stopFlowsAndCheckpoint();
    }
  }
  
  public synchronized void stop()
    throws Exception
  {
    if (!this.running) {
      return;
    }
    stopFlowsAndCheckpoint();
    stopAllComponents();
    setAppCheckpoint(null);
  }
  
  void stopAllComponents()
    throws Exception
  {
    List<Pair<MetaInfo.MetaObject, MetaInfo.Flow>> orderWithFlowInfo = getFlowMeta().showTopology();
    for (Pair<MetaInfo.MetaObject, MetaInfo.Flow> pair : orderWithFlowInfo)
    {
      Flow f = (Flow)this.srv.getOpenObject(((MetaInfo.Flow)pair.second).getUuid());
      if (f != null) {
        try
        {
          f.stopObjects((MetaInfo.MetaObject)pair.first);
        }
        catch (Exception e)
        {
          logger.warn("Internal error while stopping flow - " + f.getMetaFullName() + " Reason - " + e.getLocalizedMessage());
        }
      }
    }
  }
  
  public boolean isRunning()
  {
    return this.running;
  }
  
  private void undeploy(List<? extends FlowComponent> list)
    throws Exception
  {
    for (FlowComponent o : list)
    {
      this.srv.removeDeployedObject(o);
      o.setFlow(null);
    }
  }
  
  private void undeploy(Collection<UUID> list)
    throws Exception
  {
    for (UUID id : list) {
      this.srv.removeDeployedObjectById(id);
    }
  }
  
  public synchronized void close()
    throws Exception
  {
    stop();
    if (this.nodeManager != null)
    {
      this.nodeManager.close();
      this.nodeManager = null;
    }
    undeploy(this.flows);
    undeploy(this.gens);
    undeploy(this.sources);
    undeploy(this.targets);
    undeploy(this.cqs);
    undeploy(this.windows);
    undeploy(this.sorters);
    undeploy(this.caches);
    undeploy(this.wastores);
    undeploy(this.wastoreviews);
    for (EntityType t : new EntityType[] { EntityType.FLOW, EntityType.STREAM_GENERATOR, EntityType.SOURCE, EntityType.TARGET, EntityType.CQ, EntityType.WINDOW, EntityType.SORTER, EntityType.CACHE, EntityType.WACTIONSTORE, EntityType.WASTOREVIEW }) {
      undeploy(this.flowInfo.getObjects(t));
    }
  }
  
  public void addSpecificMonitorEvents(MonitorEventsCollection monEvs)
  {
    for (Stream stream : this.usedStreams.values()) {
      if ((stream != null) && ((stream.getChannel() instanceof KafkaChannel)))
      {
        if (Logger.getLogger("KafkaStreams").isDebugEnabled())
        {
          Map m = ((KafkaChannel)stream.getChannel()).getStats();
          if (m != null) {
            System.out.println("Kafka Stats for " + stream.getMetaFullName() + " : " + m);
          }
        }
        ((KafkaChannel)stream.getChannel()).addSpecificMonitorEvents(monEvs);
      }
    }
  }
  
  private CopyOnWriteArrayList<UUID> getLatestVersions(Set<UUID> metaObjectOfSomeType, EntityType entityType)
    throws Exception
  {
    Map<String, MetaInfo.MetaObject> latestVersions = Collections.synchronizedMap(new LinkedHashMap());
    for (UUID id : metaObjectOfSomeType)
    {
      MetaInfo.MetaObject o = this.srv.getObjectInfo(id, entityType);
      if ((latestVersions.containsKey(o.name)) && (((MetaInfo.MetaObject)latestVersions.get(o.name)).version < o.version)) {
        latestVersions.put(o.name, o);
      } else if (!latestVersions.containsKey(o.name)) {
        latestVersions.put(o.name, o);
      }
    }
    CopyOnWriteArrayList<UUID> result = new CopyOnWriteArrayList();
    for (MetaInfo.MetaObject latesMetaObjects : latestVersions.values()) {
      result.add(latesMetaObjects.uuid);
    }
    if (logger.isTraceEnabled()) {
      for (Map.Entry<String, MetaInfo.MetaObject> obj : latestVersions.entrySet()) {
        logger.trace(entityType + " " + (String)obj.getKey() + " " + ((MetaInfo.MetaObject)obj.getValue()).version);
      }
    }
    return result;
  }
  
  private void connectParts()
    throws Exception
  {
    for (Compound c : this.compoundComponents) {
      c.connectParts(this);
    }
    for (Flow f : this.flows) {
      f.connectParts();
    }
  }
  
  private boolean deploySubFlows(Flow parent, boolean deployHere, MetaInfo.Flow.Detail parentDetail, MetaInfo.DeploymentGroup parentGroup)
    throws Exception
  {
    assert (parent != null);
    assert (parent.flowInfo.deploymentPlan != null);
    boolean deployed = false;
    for (UUID id : getLatestVersions(this.flowInfo.getObjects(EntityType.FLOW), EntityType.FLOW))
    {
      MetaInfo.Flow o = (MetaInfo.Flow)this.srv.getObjectInfo(id, EntityType.FLOW);
      assert (o.deploymentPlan == null);
      
      boolean dstrategy = true;
      MetaInfo.Flow.Detail d = DeployUtility.haveDeploymentDetail(o, parent.flowInfo);
      MetaInfo.DeploymentGroup dg;
      boolean canBeDeployedHere;
      if (d == null)
      {
        boolean canBeDeployedHere = deployHere;
        d = parentDetail;
        dg = parentGroup;
      }
      else
      {
        MetaInfo.DeploymentGroup dg = this.srv.getDeploymentGroupByID(d.deploymentGroup);
        canBeDeployedHere = (this.srv.inDeploymentGroup(d.deploymentGroup)) && ((dstrategy = checkStrategy(d, dg)));
      }
      if (canBeDeployedHere)
      {
        this.srv.checkNotDeployed(o);
        Flow flow = this.srv.createFlow(o, parent);
        this.flows.add(flow);
        deployed = flow.deploy(parent);
      }
    }
    return deployed;
  }
  
  public boolean checkStrategy(MetaInfo.Flow.Detail d, MetaInfo.DeploymentGroup dg)
    throws Exception
  {
    if (d.strategy == DeploymentStrategy.ON_ALL) {
      return true;
    }
    Long val = (Long)dg.groupMembers.get(this.srv.getServerID());
    if (val == null) {
      throw new RuntimeException("server is not in <" + dg.name + "> deployment group");
    }
    long idInGroup = val.longValue();
    for (Iterator i$ = dg.groupMembers.values().iterator(); i$.hasNext();)
    {
      long id = ((Long)i$.next()).longValue();
      if (idInGroup > id)
      {
        logger.info("Flow " + d.flow + " cannot be deployed because the server is not in lowest id in the deployment group");
        return false;
      }
    }
    return true;
  }
  
  public boolean deploy(Flow parent)
    throws Exception
  {
    boolean startAppManager = false;
    if (parent == null)
    {
      assert (this.flowInfo.deploymentPlan != null);
      MetaInfo.Flow.Detail d = (MetaInfo.Flow.Detail)this.flowInfo.deploymentPlan.get(0);
      MetaInfo.DeploymentGroup dg = this.srv.getDeploymentGroupByID(d.deploymentGroup);
      if (logger.isInfoEnabled()) {
        logger.info("Start deploy " + this.flowInfo.name + " in " + this.srv.getServerID() + " " + this.srv.getDeploymentGroups());
      }
      if (this.srv.inDeploymentGroup(d.deploymentGroup))
      {
        if (checkStrategy(d, dg))
        {
          if (logger.isInfoEnabled()) {
            logger.info("... do deploy everything on THIS node");
          }
          deploySubFlows(this, true, d, dg);
          startAppManager = true;
        }
        else
        {
          if (logger.isInfoEnabled()) {
            logger.info("... DO NOT deploy everything on THIS node  because it's not a FIRST NODE in deployment group " + dg.name);
          }
          this.srv.openObjects.remove(getMetaID());
        }
      }
      else
      {
        if (logger.isDebugEnabled()) {
          logger.debug("... deploy ONLY subflows which should be deployed on THIS node");
        }
        startAppManager = deploySubFlows(this, false, d, dg);
      }
      deployObjects();
      connectParts();
    }
    else
    {
      if (logger.isInfoEnabled()) {
        logger.info("do deploy subflow " + this.flowInfo.name + " of flow " + parent.flowInfo.name + " on THIS node");
      }
      startAppManager = true;
      
      assert (this.flowInfo.getObjects(EntityType.FLOW).isEmpty());
    }
    return startAppManager;
  }
  
  void deployObjects()
    throws Exception
  {
    List<Pair<MetaInfo.MetaObject, MetaInfo.Flow>> orderWithFlowInfo = getFlowMeta().showTopology();
    for (Pair<MetaInfo.MetaObject, MetaInfo.Flow> pair : orderWithFlowInfo)
    {
      Flow flow = (Flow)this.srv.getOpenObject(((MetaInfo.Flow)pair.second).getUuid());
      if (flow != null)
      {
        if (flow.getMetaInfo().getType() == EntityType.FLOW)
        {
          if (logger.isDebugEnabled()) {
            logger.debug("for flow :" + flow.getMetaFullName() + ", setting :" + getMetaFullName() + " as parent flow.");
          }
          flow.setFlow(this);
        }
        flow.deployObject((MetaInfo.MetaObject)pair.first);
      }
    }
  }
  
  void deployObject(MetaInfo.MetaObject o)
    throws Exception
  {
    if (o.getType().equals(EntityType.STREAM))
    {
      createStream(o.getUuid(), this);
      return;
    }
    this.srv.checkNotDeployed(o);
    FlowComponent w = this.srv.createComponent(o);
    if (w == null) {
      return;
    }
    w.setFlow(this);
    if (logger.isDebugEnabled()) {
      logger.debug("for component :" + w.getMetaFullName() + ", setting :" + getMetaFullName() + " as parent flow.");
    }
    switch (o.getType())
    {
    case CACHE: 
      this.caches.add((Cache)w);
      this.compoundComponents.add((Compound)w);
      break;
    case CQ: 
      this.cqs.add((CQTask)w);
      this.compoundComponents.add((Compound)w);
      break;
    case SORTER: 
      this.sorters.add((Sorter)w);
      this.compoundComponents.add((Compound)w);
      break;
    case SOURCE: 
      this.sources.add((Source)w);
      this.compoundComponents.add((Compound)w);
      break;
    case STREAM_GENERATOR: 
      this.gens.add((IStreamGenerator)w);
      break;
    case TARGET: 
      this.targets.add((Target)w);
      this.compoundComponents.add((Compound)w);
      break;
    case WACTIONSTORE: 
      this.wastores.add((WactionStore)w);
      break;
    case WASTOREVIEW: 
      this.wastoreviews.add((WAStoreView)w);
      this.compoundComponents.add((Compound)w);
      break;
    case WINDOW: 
      this.windows.add((IWindow)w);
      this.compoundComponents.add((Compound)w);
      break;
    }
  }
  
  public List<FlowComponent> getDeployedObjects()
  {
    List<FlowComponent> list = new ArrayList();
    list.add(this);
    list.addAll(this.sources);
    list.addAll(this.gens);
    list.addAll(this.targets);
    list.addAll(this.cqs);
    list.addAll(this.windows);
    list.addAll(this.caches);
    list.addAll(this.wastores);
    for (UUID entry : this.usedStreams.keySet())
    {
      Stream s = null;
      try
      {
        s = getStream(entry);
      }
      catch (Exception e) {}
      if (s != null) {
        list.add(s);
      }
    }
    for (Flow f : this.flows) {
      list.addAll(f.getDeployedObjects());
    }
    return list;
  }
  
  public static List<MetaInfo.MetaObjectInfo> getObjectsMeta(List<FlowComponent> objs)
  {
    List<MetaInfo.MetaObjectInfo> list = new ArrayList();
    for (FlowComponent o : objs) {
      list.add(o.getMetaObjectInfo());
    }
    return list;
  }
  
  public List<FlowComponent> initCaches()
    throws Exception
  {
    List<FlowComponent> list = new ArrayList();
    for (Cache c : this.caches) {
      c.registerAndInitCache();
    }
    list.addAll(this.caches);
    for (Flow f : this.flows)
    {
      List<FlowComponent> l = f.initCaches();
      list.addAll(l);
    }
    return list;
  }
  
  public void startAllCaches(List<UUID> servers)
    throws Exception
  {
    List<UUID> relevantServers = findServersWithThisFlow(servers, this.flowInfo.getUuid());
    for (WactionStore ws : this.wastores)
    {
      if (logger.isInfoEnabled()) {
        logger.info("Starting cache for waction store " + ws.getMetaName() + " with servers " + relevantServers);
      }
      ws.startCache(relevantServers);
    }
    for (Cache c : this.caches)
    {
      if (logger.isInfoEnabled()) {
        logger.info("Starting cache " + c.getMetaName() + " with servers " + relevantServers);
      }
      c.startCache(relevantServers);
    }
    for (Flow f : this.flows) {
      f.startAllCaches(servers);
    }
  }
  
  public static List<UUID> findServersWithThisFlow(List<UUID> servers, UUID flowUuid)
    throws MetaDataRepositoryException
  {
    Collection<UUID> wd = MetadataRepository.getINSTANCE().getServersForDeployment(flowUuid, WASecurityManager.TOKEN);
    
    List<UUID> whereDeployed = new ArrayList();
    for (Iterator i$ = wd.iterator(); i$.hasNext();)
    {
      obj = i$.next();
      if ((obj instanceof UUID))
      {
        if (servers.contains(obj)) {
          whereDeployed.add((UUID)obj);
        }
      }
      else if ((obj instanceof Collection))
      {
        Collection<UUID> uuids = (Collection)obj;
        for (UUID uuid : uuids) {
          if (servers.contains(obj)) {
            whereDeployed.add(uuid);
          }
        }
      }
    }
    Object obj;
    return whereDeployed;
  }
  
  public void stopAllCaches()
    throws Exception
  {
    for (WactionStore ws : this.wastores) {
      ws.stopCache();
    }
    for (Cache c : this.caches) {
      c.stopCache();
    }
    for (Flow f : this.flows) {
      f.stopAllCaches();
    }
  }
  
  private Position previousAppCheckpoint = null;
  
  public synchronized void takeAppCheckpoint()
  {
    if (!this.running) {
      return;
    }
    if (this.appCheckpointExecutor != null) {
      recordAppCheckpoint();
    }
  }
  
  private synchronized void recordAppCheckpoint()
  {
    try
    {
      if (Logger.getLogger("Recovery").isDebugEnabled()) {
        Logger.getLogger("Recovery").debug("Recording the App Checkpoint...");
      }
      Position appCheckpoint = new Position();
      String distId = getDistributionId();
      for (Source s : getAllSources())
      {
        Position checkpoint = s.getAdapter().getCheckpoint();
        if ((checkpoint == null) || (checkpoint.isEmpty())) {
          checkpoint = new Position(new Path(s.getMetaID(), distId, null));
        }
        appCheckpoint.mergeLowerPositions(checkpoint);
      }
      for (Stream stream : getAllStreams()) {
        if (stream == null)
        {
          logger.warn("While taking the app checkpoint, found a null stream");
        }
        else
        {
          Position checkpoint = stream.getCheckpoint();
          if ((checkpoint == null) || (checkpoint.isEmpty())) {
            checkpoint = new Position(new Path(stream.getMetaID(), distId, null));
          }
          appCheckpoint.mergeLowerPositions(checkpoint);
        }
      }
      for (IWindow w : getAllWindows()) {
        if ((w instanceof Window))
        {
          Window window = (Window)w;
          Position checkpoint = window.getCheckpoint();
          if ((checkpoint == null) || (checkpoint.isEmpty())) {
            checkpoint = new Position(new Path(w.getMetaID(), distId, null));
          }
          appCheckpoint.mergeLowerPositions(checkpoint);
        }
      }
      for (WactionStore ws : getAllWactionStores())
      {
        Position checkpoint = ws.getCheckpoint();
        if ((checkpoint == null) || (checkpoint.isEmpty())) {
          checkpoint = new Position(new Path(ws.getMetaID(), distId, null));
        }
        appCheckpoint.mergeLowerPositions(checkpoint);
      }
      for (Target target : getAllTargets())
      {
        Position checkpoint = target.getCheckpoint();
        if ((checkpoint == null) || (checkpoint.isEmpty())) {
          checkpoint = new Position(new Path(target.getMetaID(), distId, null));
        }
        appCheckpoint.mergeLowerPositions(checkpoint);
      }
      if (appCheckpoint.isEmpty())
      {
        if (Logger.getLogger("Recovery").isDebugEnabled()) {
          Logger.getLogger("Recovery").debug("...the collected App Checkpoint is empty, so writing nothing.");
        }
        return;
      }
      boolean success = false;
      for (int tries = 0; (tries < 10) && (!success); tries++)
      {
        success = StatusDataStore.getInstance().putAppCheckpoint(getMetaID(), appCheckpoint);
        if (Logger.getLogger("Recovery").isDebugEnabled()) {
          Utility.prettyPrint(appCheckpoint);
        }
      }
      if (!success)
      {
        if (Logger.getLogger("Recovery").isDebugEnabled()) {
          Logger.getLogger("Recovery").debug("...failed to write the App Checkpoint! Another attempt will be made periodically.");
        }
        return;
      }
      if ((this.previousAppCheckpoint == null) || (this.previousAppCheckpoint.isEmpty()))
      {
        if (Logger.getLogger("Recovery").isDebugEnabled()) {
          Logger.getLogger("Recovery").debug("...previous App Checkpoint is empty, so do not trim anything.");
        }
        this.previousAppCheckpoint = appCheckpoint;
        return;
      }
      Position trimThis = this.previousAppCheckpoint.createPositionWithoutPaths(appCheckpoint.keySet());
      if (trimThis.isEmpty())
      {
        if (Logger.getLogger("Recovery").isDebugEnabled()) {
          Logger.getLogger("Recovery").debug("...nothing to trim from the App Checkpoint, so the operation is complete.");
        }
        return;
      }
      success = StatusDataStore.getInstance().trimAppCheckpoint(getMetaID(), trimThis);
      if (!success)
      {
        if (Logger.getLogger("Recovery").isDebugEnabled()) {
          Logger.getLogger("Recovery").debug("...failed to trim the App Checkpoint! Another attempt will be made periodically and the old data will be remembered until then.");
        }
        this.previousAppCheckpoint.mergeHigherPositions(appCheckpoint);
        return;
      }
      if (Logger.getLogger("Recovery").isDebugEnabled())
      {
        Logger.getLogger("Recovery").debug("Trimmed these paths:");
        Utility.prettyPrint(trimThis);
      }
      this.previousAppCheckpoint = appCheckpoint;
      if (Logger.getLogger("Recovery").isDebugEnabled()) {
        Logger.getLogger("Recovery").debug("...done successfully recording the App Checkpoint!");
      }
    }
    catch (Throwable t)
    {
      logger.error("Error getting the App Checkpoint", t);
      return;
    }
  }
  
  private List<Source> getAllSources()
  {
    List<Source> result = new ArrayList();
    
    result.addAll(this.sources);
    for (Flow f : this.flows) {
      result.addAll(f.getAllSources());
    }
    return result;
  }
  
  private Set<Stream> getAllStreams()
  {
    Set<Stream> result = new HashSet();
    
    result.addAll(this.usedStreams.values());
    for (Flow f : this.flows) {
      result.addAll(f.getAllStreams());
    }
    return result;
  }
  
  public List<WactionStore> getAllWactionStores()
  {
    List<WactionStore> result = new ArrayList();
    
    result.addAll(this.wastores);
    for (Flow f : this.flows) {
      result.addAll(f.getAllWactionStores());
    }
    return result;
  }
  
  private List<Target> getAllTargets()
  {
    List<Target> result = new ArrayList();
    
    result.addAll(this.targets);
    for (Flow f : this.flows) {
      result.addAll(f.getAllTargets());
    }
    return result;
  }
  
  private List<IWindow> getAllWindows()
  {
    List<IWindow> result = new ArrayList();
    
    result.addAll(this.windows);
    for (Flow f : this.flows) {
      result.addAll(f.getAllWindows());
    }
    return result;
  }
  
  public Stream getStream(UUID streamID)
    throws Exception
  {
    Stream s = (Stream)this.usedStreams.get(streamID);
    return s;
  }
  
  private Stream createStream(UUID uuid, Flow flow)
    throws Exception
  {
    Stream stream_runtime = (Stream)this.usedStreams.get(uuid);
    if (stream_runtime == null)
    {
      MetaInfo.Stream stream_metaobject = (MetaInfo.Stream)this.mdRepository.getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
      if (stream_metaobject.pset != null)
      {
        stream_runtime = new Stream(stream_metaobject, Server.getServer(), flow);
        if (logger.isDebugEnabled()) {
          logger.debug("Created new kafka stream runtime object for : " + stream_metaobject.getFullName() + " to be used in flow : " + flow.getMetaFullName());
        }
      }
      else
      {
        stream_runtime = this.srv.createStream(uuid, flow);
      }
      this.usedStreams.put(uuid, stream_runtime);
    }
    else
    {
      logger.warn("Unexpected state: Stream was not NULL");
    }
    return stream_runtime;
  }
  
  public WactionStore getWactionStore(UUID wasID)
    throws Exception
  {
    for (WactionStore was : this.wastores) {
      if (was.getMetaID().equals(wasID)) {
        return was;
      }
    }
    WactionStore was = (WactionStore)this.srv.getOpenObject(wasID);
    if (was == null)
    {
      MetaInfo.WActionStore store = (MetaInfo.WActionStore)this.srv.getObjectInfo(wasID, EntityType.WACTIONSTORE);
      if (getMetaInfo().getMetaInfoStatus().isAdhoc()) {
        throw new RuntimeException("WactionStore [" + store.getFullName() + "] is not deployed, so query fails.");
      }
      throw new RuntimeException("cannot connect to not deployed WactionStore [" + store.getFullName() + "]");
    }
    return was;
  }
  
  private FlowComponent getPubSub(UUID id)
    throws Exception
  {
    if (this.usedStreams.containsKey(id))
    {
      Stream s = getStream(id);
      if (s == null)
      {
        s = createStream(id, this);
        if (logger.isInfoEnabled()) {
          logger.info("Getting access to a stream in a different flow : " + s.getMetaFullName());
        }
      }
      else if (logger.isInfoEnabled())
      {
        logger.info("Getting access to a stream in the current flow : " + s.getMetaFullName());
      }
      s.connectedFlow(this);
      return s;
    }
    FlowComponent o = this.srv.getOpenObject(id);
    if (o != null)
    {
      assert (!(o instanceof Stream));
      return o;
    }
    MetaInfo.MetaObject obj = this.srv.getObject(id);
    if (obj == null) {
      throw new ServerException(getMetaType() + " " + getMetaName() + " tries to access unknown object " + id);
    }
    throw new ServerException(getMetaType() + " " + getMetaName() + " tries to access undeployed object " + obj.type + " " + obj.name + " " + obj.uuid);
  }
  
  public Subscriber getSubscriber(UUID subID)
    throws Exception
  {
    return (Subscriber)getPubSub(subID);
  }
  
  public Publisher getPublisher(UUID pubID)
    throws Exception
  {
    return (Publisher)getPubSub(pubID);
  }
  
  public List<Flow> getAllFlows()
  {
    if (this.flows.isEmpty()) {
      return Collections.singletonList(this);
    }
    List<Flow> ret = new ArrayList();
    ret.add(this);
    for (Flow f : this.flows) {
      ret.addAll(f.getAllFlows());
    }
    return ret;
  }
  
  public SourcePosition getRestartSourcePositionForSourceUUID(UUID sourceUUID)
  {
    SourcePosition result = null;
    Position p = StatusDataStore.getInstance().getAppCheckpoint(getMetaID());
    if (p != null) {
      result = p.getLowSourcePositionForComponent(sourceUUID);
    }
    return result;
  }
  
  public boolean recoveryIsEnabled()
  {
    Integer r = getRecoveryType();
    return r.intValue() == 2;
  }
  
  public Long getRecoveryPeriod()
  {
    long period = this.recoveryPeriod != null ? this.recoveryPeriod.longValue() : this.flowInfo.recoveryPeriod;
    return Long.valueOf(period);
  }
  
  public void setRecoveryPeriod(Long recoveryPeriod)
  {
    this.recoveryPeriod = recoveryPeriod;
    for (Flow s : this.flows) {
      s.setRecoveryPeriod(recoveryPeriod);
    }
  }
  
  public Integer getRecoveryType()
  {
    if (this.parent != null) {
      return this.parent.getRecoveryType();
    }
    int type = this.recoveryType != null ? this.recoveryType.intValue() : this.flowInfo.recoveryType;
    return Integer.valueOf(type);
  }
  
  public void setRecoveryType(Integer recoveryType)
  {
    this.recoveryType = recoveryType;
    for (Flow s : this.flows) {
      s.setRecoveryType(recoveryType);
    }
  }
  
  private Position getWactionStoreCheckpoint()
  {
    if (logger.isDebugEnabled()) {
      logger.debug("Trying to build position from WactionStore checkpoints...");
    }
    Position result = new Position();
    
    List<WactionStore> allWactionStores = getAllWactionStores();
    for (WactionStore ws : allWactionStores)
    {
      Position wsCheckpoint = ws.getPersistedCheckpointPosition();
      result.mergeLowerPositions(wsCheckpoint);
    }
    if (result.isEmpty()) {
      return null;
    }
    return result;
  }
  
  public void setAppCheckpoint(Position p)
  {
    if (Logger.getLogger("Recovery").isDebugEnabled())
    {
      Logger.getLogger("Recovery").debug(getMetaName() + " flow setAppCheckpoint to: ");
      Utility.prettyPrint(p);
    }
    this.appCheckpoint = p;
    for (Flow s : this.flows) {
      s.setAppCheckpoint(p);
    }
  }
  
  public void bindParams(List<Property> params)
  {
    for (CQTask cq : this.cqs) {
      if (!cq.bindParameters(params)) {
        throw new RuntimeException("some parameters are missing for app <" + getMetaName() + ">");
      }
    }
  }
  
  public MetaInfo.Flow getFlowMeta()
  {
    return this.flowInfo;
  }
  
  public String getDistributionId()
  {
    String ret = BaseServer.getServerName();
    MetaInfo.Flow.Detail detail = DeployUtility.haveDeploymentDetail(this.flowInfo, this.parent == null ? null : this.parent.flowInfo);
    if (detail.strategy == DeploymentStrategy.ON_ONE) {
      ret = null;
    }
    return ret;
  }
  
  public void startFlow(List<UUID> servers, Long epochNumber)
    throws Exception
  {
    start(servers);
    if (this.nodeManager != null) {
      this.nodeManager.flowStarted(epochNumber);
    }
  }
  
  public void startFlow(Long epochNumber)
    throws Exception
  {
    start();
    if (this.nodeManager != null) {
      this.nodeManager.flowStarted(epochNumber);
    }
  }
  
  public void stopFlow(Long epochNumber)
    throws Exception
  {
    stop();
    if (this.nodeManager != null) {
      this.nodeManager.flowStopped(epochNumber);
    }
  }
  
  public void startCaches(List<UUID> servers, Long epochNumber)
    throws Exception
  {
    startAllCaches(servers);
    if (this.nodeManager != null) {
      this.nodeManager.flowDeployed(epochNumber);
    }
  }
  
  public void stopCaches(Long epochNumber)
    throws Exception
  {
    stopAllCaches();
  }
  
  public Map<UUID, Stream> getUsedStreams()
  {
    return this.usedStreams;
  }
}
