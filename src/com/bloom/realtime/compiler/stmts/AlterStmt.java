package com.bloom.runtime.compiler.stmts;

import com.bloom.kafkamessaging.StreamPersistencePolicy;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;

import java.util.List;

public class AlterStmt
  extends Stmt
{
  private final String objectName;
  private final Boolean enablePartitionBy;
  private final StreamPersistencePolicy persistencePolicy;
  private final Boolean enablePersistence;
  private final List<String> partitionBy;
  
  public AlterStmt(String objectName, Boolean enablePartitionBy, List<String> partitionBy, Boolean enablePersistence, StreamPersistencePolicy persistencePolicy)
  {
    this.objectName = objectName;
    this.enablePartitionBy = enablePartitionBy;
    this.partitionBy = partitionBy;
    this.enablePersistence = enablePersistence;
    this.persistencePolicy = persistencePolicy;
  }
  
  public String getObjectName()
  {
    return this.objectName;
  }
  
  public StreamPersistencePolicy getPersistencePolicy()
  {
    return this.persistencePolicy;
  }
  
  public Boolean getEnablePersistence()
  {
    return this.enablePersistence;
  }
  
  public List<String> getPartitionBy()
  {
    return this.partitionBy;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileAlterStmt(this);
  }
  
  public Boolean getEnablePartitionBy()
  {
    return this.enablePartitionBy;
  }
  
  public String toString()
  {
    return this.objectName + " " + this.enablePartitionBy + " " + this.persistencePolicy + " " + this.enablePersistence + "  " + this.partitionBy;
  }
}
