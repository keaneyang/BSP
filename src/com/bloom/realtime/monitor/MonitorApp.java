package com.bloom.runtime.monitor;

import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.runtime.Context;
import com.bloom.runtime.DeploymentStrategy;
import com.bloom.runtime.QueryValidator;
import com.bloom.runtime.Server;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.MetaInfo.DeploymentGroup;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.meta.MetaInfo.Namespace;
import com.bloom.runtime.meta.MetaInfo.Flow.Detail;
import com.bloom.runtime.meta.MetaInfo.Flow.Detail.FailOverRule;
import com.bloom.runtime.meta.MetaInfo.StatusInfo.Status;
import com.bloom.uuid.AuthToken;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

public class MonitorApp
{
  private static Logger logger = Logger.getLogger(MonitorApp.class);
  private static String appStmt = "IMPORT com.bloom.runtime.monitor.MonitorModel;\nIMPORT com.bloom.runtime.monitor.MonitorBatchEvent;\n\nCREATE APPLICATION MonitoringSourceApp;\n\nCREATE OR REPLACE STREAM MonitoringSourceStream OF com.bloom.runtime.monitor.MonitorBatchEvent;\n\nCREATE OR REPLACE STREAM MonitoringStream1 OF com.bloom.runtime.monitor.MonitorBatchEvent;\n\nCREATE FLOW MonitoringSourceFlow;\n\nCREATE OR REPLACE SOURCE MonitoringSource1 USING MonitoringDataSource (\n )\nOUTPUT TO MonitoringSourceStream;\n\nEND FLOW MonitoringSourceFlow;\n\nEND APPLICATION MonitoringSourceApp;\n\nCREATE APPLICATION MonitoringProcessApp;\n\nCREATE OR REPLACE CQ MonitoringCQ\nINSERT INTO MonitoringStream1\nselect processBatch(m) from MonitoringSourceStream m;\n\nEND APPLICATION MonitoringProcessApp;";
  
  public static void getMonitorApp()
    throws MetaDataRepositoryException
  {
    if (!MonitorModel.monitorIsEnabled())
    {
      if (logger.isInfoEnabled()) {
        logger.info("bloom Monitor is not enabled");
      }
      return;
    }
    Server srv = Server.server;
    Context ctx = srv.createContext();
    
    MetadataRepository instance = MetadataRepository.getINSTANCE();
    String name = ctx.getCurNamespace().getName();
    AuthToken authToken = ctx.getAuthToken();
    
    ILock lock = HazelcastSingleton.get().getLock("CreateMonitoringAppLock");
    lock.lock();
    
    boolean needCreate = false;
    boolean needDeploy = false;
    try
    {
      MetaInfo.Flow monitoringApp = (MetaInfo.Flow)instance.getMetaObjectByName(EntityType.APPLICATION, name, "MonitoringSourceApp", null, authToken);
      if (monitoringApp != null)
      {
        if (logger.isInfoEnabled()) {
          logger.info("Monitoring App Exists, Checking Deployment");
        }
        if ((monitoringApp.deploymentPlan == null) || (monitoringApp.deploymentPlan.size() == 0) || (!monitoringApp.flowStatus.equals(MetaInfo.StatusInfo.Status.RUNNING)))
        {
          if (logger.isInfoEnabled()) {
            logger.info("Monitoring App Exists, but Source App not deployed properly");
          }
          needDeploy = true;
        }
        monitoringApp = (MetaInfo.Flow)instance.getMetaObjectByName(EntityType.APPLICATION, name, "MonitoringProcessApp", null, authToken);
        if ((monitoringApp.deploymentPlan == null) || (monitoringApp.deploymentPlan.size() == 0) || (!monitoringApp.flowStatus.equals(MetaInfo.StatusInfo.Status.RUNNING)))
        {
          if (logger.isInfoEnabled()) {
            logger.info("Monitoring App Exists, but Processing App not deployed properly");
          }
          needDeploy = true;
        }
      }
      else
      {
        needCreate = true;
        needDeploy = true;
      }
      if (needCreate)
      {
        QueryValidator qv = new QueryValidator(srv);
        qv.addContext(authToken, ctx);
        qv.setUpdateMode(true);
        try
        {
          qv.compileText(authToken, appStmt);
        }
        catch (Exception e)
        {
          logger.error("Problem creating Monitoring App", e);
        }
      }
      if (needDeploy) {
        try
        {
          deployApplication(ctx, instance, name, authToken, "MonitoringSourceApp", DeploymentStrategy.ON_ALL);
          deployApplication(ctx, instance, name, authToken, "MonitoringProcessApp", DeploymentStrategy.ON_ONE);
        }
        catch (Exception e)
        {
          logger.error("Problem deploying Monitoring App", e);
        }
      }
    }
    finally
    {
      lock.unlock();
    }
  }
  
  public static void deployApplication(Context ctx, MetadataRepository instance, String name, AuthToken authToken, String appName, DeploymentStrategy deploymentStrategy)
    throws MetaDataRepositoryException
  {
    List<MetaInfo.Flow.Detail> deploymentPlan = new ArrayList();
    
    MetaInfo.DeploymentGroup dg = ctx.getDeploymentGroup("default");
    MetaInfo.Flow.Detail sourceAppDetail = new MetaInfo.Flow.Detail();
    MetaInfo.Flow monitoringApp = (MetaInfo.Flow)instance.getMetaObjectByName(EntityType.APPLICATION, name, appName, null, authToken);
    
    sourceAppDetail.construct(deploymentStrategy, dg.uuid, monitoringApp.uuid, MetaInfo.Flow.Detail.FailOverRule.AUTO);
    deploymentPlan.add(sourceAppDetail);
    
    List<MetaInfo.Flow> flows = monitoringApp.getSubFlows();
    for (MetaInfo.Flow flow : flows) {
      if (flow.getName().equals("MonitoringSourceFlow"))
      {
        MetaInfo.Flow.Detail sourceFlowDetail = new MetaInfo.Flow.Detail();
        sourceFlowDetail.construct(DeploymentStrategy.ON_ALL, dg.uuid, flow.uuid, MetaInfo.Flow.Detail.FailOverRule.AUTO);
        deploymentPlan.add(sourceFlowDetail);
      }
    }
    monitoringApp.setDeploymentPlan(deploymentPlan);
    monitoringApp.setFlowStatus(MetaInfo.StatusInfo.Status.RUNNING);
    ctx.put(monitoringApp);
    
    monitoringApp = (MetaInfo.Flow)instance.getMetaObjectByName(EntityType.APPLICATION, name, appName, null, authToken);
    
    monitoringApp.getMetaInfoStatus().setAnonymous(true);
    instance.updateMetaObject(monitoringApp, authToken);
    if (monitoringApp == null)
    {
      logger.warn("Failed to create Monitoring Application");
      return;
    }
    assert (monitoringApp.deploymentPlan.size() > 0);
    assert (monitoringApp.flowStatus.equals(MetaInfo.StatusInfo.Status.RUNNING));
  }
}
