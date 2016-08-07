package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo.User.AUTHORIZATION_TYPE;

public class CreateUserStmt
  extends CreateStmt
{
  public final String password;
  public final UserProperty prop;
  
  public CreateUserStmt(String userid, String password, UserProperty prop, String ldap)
  {
    super(EntityType.USER, userid, false);
    this.password = password;
    this.prop = prop;
    if (ldap != null)
    {
      prop.originType = MetaInfo.User.AUTHORIZATION_TYPE.LDAP;
      prop.ldap = ldap;
    }
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileCreateUserStmt(this);
  }
}
