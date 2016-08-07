package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.Property;
import com.bloom.runtime.compiler.Compiler;

import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class UpdateUserInfoStmt
  extends Stmt
{
  public final String username;
  public List<Property> properties;
  
  public UpdateUserInfoStmt(String username, List<Property> props)
  {
    this.username = username;
    this.properties = props;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.UpdateUserInfoStmt(this);
  }
  
  public String getRedactedSourceText()
  {
    for (Property p : this.properties) {
      if (StringUtils.containsIgnoreCase(p.name, "password")) {
        return "alter user " + this.username + " set (REDACTED PASSWORDS)";
      }
    }
    return this.sourceText;
  }
}
