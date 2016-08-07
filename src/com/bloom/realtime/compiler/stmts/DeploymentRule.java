package com.bloom.runtime.compiler.stmts;

import java.io.Serializable;

import com.bloom.runtime.DeploymentStrategy;

public class DeploymentRule
  implements Serializable
{
  private static final long serialVersionUID = 5003499339179392410L;
  public final DeploymentStrategy strategy;
  public String flowName;
  public final String deploymentGroup;
  
  public DeploymentRule(DeploymentStrategy strategy, String flowName, String deploymentGroup)
  {
    this.strategy = strategy;
    this.flowName = flowName;
    this.deploymentGroup = deploymentGroup;
  }
  
  public String toString()
  {
    return this.flowName + (this.strategy == DeploymentStrategy.ON_ONE ? " ON ONE " : " ON ALL ") + "IN " + this.deploymentGroup + " ";
  }
}
