package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.Pair;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

import java.util.List;

public class DeployStmt
  extends Stmt
{
  public final EntityType appOrFlow;
  public final DeploymentRule appRule;
  public final List<DeploymentRule> flowRules;
  public final List<Pair<String, String>> options;
  
  public DeployStmt(EntityType type, DeploymentRule appRule, List<DeploymentRule> flowRules, List<Pair<String, String>> options)
  {
    this.appOrFlow = type;
    this.appRule = appRule;
    this.flowRules = flowRules;
    this.options = options;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileDeployStmt(this);
  }
  
  public String toString()
  {
    if ((this.sourceText != null) && (!this.sourceText.isEmpty())) {
      return this.sourceText;
    }
    StringBuilder sb = new StringBuilder();
    sb.append("DEPLOY APPLICATION ");
    if (this.appRule != null) {
      sb.append(this.appRule.toString());
    }
    if ((this.flowRules != null) && (this.flowRules.size() > 0))
    {
      sb.append(" WITH ");
      String temp = "";
      for (DeploymentRule dr : this.flowRules) {
        temp = temp + dr.toString() + ",";
      }
      temp = temp.substring(0, temp.length() - 1);
      sb.append(temp);
    }
    return sb.toString();
  }
}
