package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;

import java.util.List;

public class AlterDeploymentGroupStmt
  extends Stmt
{
  public final boolean add;
  public final String groupname;
  public final List<String> deploymentGroup;
  
  public AlterDeploymentGroupStmt(boolean add, String groupname, List<String> deploymentGroup)
  {
    this.add = add;
    this.groupname = groupname;
    this.deploymentGroup = deploymentGroup;
  }
  
  public String toString()
  {
    return " (" + this.deploymentGroup + ")";
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileAlterDeploymentGroupStmt(this);
  }
}
