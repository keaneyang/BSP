package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

import java.util.List;

public class CreateDeploymentGroupStmt
  extends CreateStmt
{
  public final List<String> deploymentGroup;
  public final Long minServers;
  
  public CreateDeploymentGroupStmt(String groupname, List<String> deploymentGroup, long minServers)
  {
    super(EntityType.DG, groupname, false);
    this.deploymentGroup = deploymentGroup;
    this.minServers = Long.valueOf(minServers);
  }
  
  public String toString()
  {
    return stmtToString() + " (" + this.deploymentGroup + ")";
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileCreateDeploymentGroupStmt(this);
  }
}
