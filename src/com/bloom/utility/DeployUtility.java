package com.bloom.utility;

import com.bloom.exception.ServerException;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.runtime.DeploymentStrategy;
import com.bloom.runtime.Server;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.DeploymentGroup;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Flow.Detail;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.UUID;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

public class DeployUtility
{
  private static Logger logger = Logger.getLogger(DeployUtility.class);
  
  public static boolean canFlowBeDeployed(MetaInfo.Flow flow, UUID serverId)
    throws Exception
  {
    String apps = System.getProperty("com.bloom.config.doNotDeployApps");
    List<String> doNotDeployAppList = new ArrayList();
    if (apps != null) {
      doNotDeployAppList.addAll(Arrays.asList(apps.split(",")));
    }
    if ((doNotDeployAppList.contains(flow.getName())) || (doNotDeployAppList.contains("*"))) {
      return false;
    }
    if (flow.deploymentPlan == null)
    {
      logger.info("Flow " + flow.uuid + " cannot be deployed because deployment plan is null");
      return false;
    }
    for (MetaInfo.Flow.Detail d : flow.deploymentPlan) {
      if (checkStrategy(d, serverId))
      {
        logger.info("Flow " + flow.uuid + " can be deployed on the server " + serverId);
        return true;
      }
    }
    logger.info("Flow " + flow.uuid + " cannot be deployed on the server " + serverId);
    return false;
  }
  
  public static boolean inDeploymentGroup(UUID deploymentGroupID, UUID serverId)
  {
    List<UUID> deploymentGroups = (List)HazelcastSingleton.get().getMap("#serverToDeploymentGroup").get(serverId);
    logger.info("List of DGs server " + serverId + " belongs to " + deploymentGroups + " . DG being looked for " + deploymentGroupID);
    if (deploymentGroups != null) {
      return deploymentGroups.contains(deploymentGroupID);
    }
    return false;
  }
  
  protected static boolean checkStrategy(MetaInfo.Flow.Detail d, UUID serverId)
    throws Exception
  {
    logger.info("Checking strategy if Flow " + d.flow + " can be deployed on the server " + serverId);
    if (!inDeploymentGroup(d.deploymentGroup, serverId))
    {
      logger.info("Flow " + d.flow + " cannot be deployed on the server " + serverId + " is not in the deployment group");
      return false;
    }
    MetaInfo.DeploymentGroup dg = getDeploymentGroupByID(d.deploymentGroup);
    if (d.strategy == DeploymentStrategy.ON_ALL)
    {
      logger.info("Flow " + d.flow + " can be deployed on the server " + serverId + " because deployment strategy for the flow is ON_ALL");
      return true;
    }
    Long val = (Long)dg.groupMembers.get(serverId);
    if (val == null) {
      throw new RuntimeException("Flow " + d.flow + " cannot be deployed because server " + serverId + " is not in <" + dg.name + "> deployment group");
    }
    long idInGroup = val.longValue();
    logger.info("Found server " + serverId + " is in DG " + dg.name + " with id " + idInGroup);
    for (Iterator i$ = dg.groupMembers.values().iterator(); i$.hasNext();)
    {
      long id = ((Long)i$.next()).longValue();
      logger.info("checking " + idInGroup + " against " + id);
      if (idInGroup > id)
      {
        logger.info("Flow " + d.flow + " cannot be deployed because the server " + serverId + " is not in lowest id in the deployment group");
        return false;
      }
    }
    return true;
  }
  
  public static MetaInfo.DeploymentGroup getDeploymentGroupByID(UUID uuid)
    throws ServerException, MetaDataRepositoryException
  {
    MDRepository MDRepository = MetadataRepository.getINSTANCE();
    MetaInfo.MetaObject o = MDRepository.getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
    if ((o != null) && (o.type == EntityType.DG)) {
      return (MetaInfo.DeploymentGroup)o;
    }
    throw new ServerException("cannot find metadata for " + EntityType.DG.name() + " " + uuid);
  }
  
  public static boolean hasRequiredServers(List<MetaInfo.Flow> flows, List<UUID> serverIds, MetaInfo.Flow parent)
    throws Exception
  {
    Set<UUID> deploymentGroups = new HashSet();
    
    MetaInfo.Flow.Detail detail = haveDeploymentDetail(parent, null);
    deploymentGroups.add(detail.deploymentGroup);
    for (MetaInfo.Flow flow : flows)
    {
      detail = haveDeploymentDetail(flow, parent);
      deploymentGroups.add(detail.deploymentGroup);
    }
    for (UUID deploymentGroup : deploymentGroups)
    {
      int count = 0;
      for (UUID serverId : serverIds) {
        if (inDeploymentGroup(deploymentGroup, serverId)) {
          count++;
        }
      }
      MetaInfo.DeploymentGroup dg = Server.getServer().getDeploymentGroupByID(deploymentGroup);
      if (count < dg.getMinimumRequiredServers())
      {
        logger.info("Not enough servers in DG " + dg.name + " Required minimum " + dg.getMinimumRequiredServers() + " Actual: " + count);
        return false;
      }
    }
    logger.info("All DGs have minimum number of servers to continue");
    return true;
  }
  
  public static MetaInfo.Flow.Detail haveDeploymentDetail(MetaInfo.Flow o, MetaInfo.Flow parent)
  {
    if (parent == null) {
      return (MetaInfo.Flow.Detail)o.deploymentPlan.get(0);
    }
    for (MetaInfo.Flow.Detail d : parent.deploymentPlan) {
      if (o.uuid.equals(d.flow)) {
        return d;
      }
    }
    return (MetaInfo.Flow.Detail)parent.deploymentPlan.get(0);
  }
}
