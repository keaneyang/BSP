package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

public class ConnectStmt
  extends CreateStmt
{
  public final String password;
  public final String clusterid;
  public final String host;
  
  public ConnectStmt(String username, String userpass, String clusterid, String host)
  {
    super(EntityType.USER, username, false);
    this.password = userpass;
    this.clusterid = clusterid;
    this.host = host;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileConnectStmt(this);
  }
}
