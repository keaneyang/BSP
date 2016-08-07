package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.Pair;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

import java.util.List;
import java.util.Set;

public class CreateAppOrFlowStatement
  extends CreateStmt
{
  public final List<Pair<EntityType, String>> entities;
  public final RecoveryDescription recoveryDesc;
  public final ExceptionHandler eh;
  public boolean encrypted = false;
  public final Set<String> importStatements;
  public final List<DeploymentRule> deploymentRules;
  
  public CreateAppOrFlowStatement(EntityType type, String name, Boolean doReplace, List<Pair<EntityType, String>> entities, Boolean encrypt, RecoveryDescription recov, ExceptionHandler eh, Set<String> importStatements, List<DeploymentRule> deploymentRules)
  {
    super(type, name, doReplace.booleanValue());
    this.entities = entities;
    this.recoveryDesc = recov;
    if (type.ordinal() == EntityType.APPLICATION.ordinal()) {
      this.encrypted = encrypt.booleanValue();
    }
    this.eh = eh;
    this.importStatements = importStatements;
    this.deploymentRules = deploymentRules;
  }
  
  public String toString()
  {
    return stmtToString() + " (" + this.entities + ")";
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileCreateAppOrFlowStatement(this);
  }
}
