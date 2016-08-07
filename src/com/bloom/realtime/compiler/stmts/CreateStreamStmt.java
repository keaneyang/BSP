package com.bloom.runtime.compiler.stmts;

import com.bloom.kafkamessaging.StreamPersistencePolicy;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.TypeField;
import com.bloom.runtime.components.EntityType;

import java.util.List;

public class CreateStreamStmt
  extends CreateStmt
{
  public final String typeName;
  public final List<TypeField> fields;
  public final List<String> partitioning_fields;
  public final GracePeriod gracePeriod;
  public final StreamPersistencePolicy spp;
  
  public CreateStreamStmt(String streamName, Boolean doReplace, List<String> partition_fields, String typeName, List<TypeField> fields, GracePeriod gp, StreamPersistencePolicy spp)
  {
    super(EntityType.STREAM, streamName, doReplace.booleanValue());
    this.partitioning_fields = partition_fields;
    this.typeName = typeName;
    this.fields = fields;
    this.gracePeriod = gp;
    this.spp = spp;
  }
  
  public String toString()
  {
    String t = " (" + this.fields + ")";
    
    return stmtToString() + t + (this.gracePeriod == null ? "" : new StringBuilder().append(" ").append(this.gracePeriod).toString());
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileCreateStreamStmt(this);
  }
}
