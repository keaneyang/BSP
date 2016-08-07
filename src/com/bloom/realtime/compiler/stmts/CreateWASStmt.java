package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.Interval;
import com.bloom.runtime.Property;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.TypeDefOrName;
import com.bloom.runtime.components.EntityType;
import com.bloom.security.Password;
import com.bloom.utility.Utility;

import java.util.List;
import java.util.Map;

public class CreateWASStmt
  extends CreateStmt
{
  public final TypeDefOrName typeDef;
  public final List<EventType> evenTypes;
  public final Interval howToPersist;
  public final List<Property> properties;
  
  public CreateWASStmt(String name, Boolean doReplace, TypeDefOrName typeDef, List<EventType> ets, Interval how, List<Property> props)
  {
    super(EntityType.WACTIONSTORE, name, doReplace.booleanValue());
    this.typeDef = typeDef;
    this.evenTypes = ets;
    this.howToPersist = how;
    this.properties = props;
    encryptJdbcPassword();
  }
  
  private void encryptJdbcPassword()
  {
    if ((this.properties == null) || (this.properties.isEmpty())) {
      return;
    }
    Map<String, Object> map = Utility.makePropertyMap(this.properties);
    String key = "jdbc_password";
    String passwd = (String)map.get(key);
    if ((passwd == null) || (passwd.isEmpty())) {
      return;
    }
    if (Utility.isValueEncryptionFlagSetToTrue(key, map)) {
      return;
    }
    if (Utility.isValueEncryptionFlagExists(key, map))
    {
      for (int ik = 0; ik < this.properties.size(); ik++)
      {
        if (((Property)this.properties.get(ik)).name.equalsIgnoreCase(key)) {
          this.properties.set(ik, new Property(((Property)this.properties.get(ik)).name, Password.getEncryptedStatic((String)((Property)this.properties.get(ik)).value)));
        }
        if (((Property)this.properties.get(ik)).name.equalsIgnoreCase(key + "_encrypted")) {
          this.properties.set(ik, new Property(((Property)this.properties.get(ik)).name, Boolean.valueOf(true)));
        }
      }
    }
    else
    {
      for (int ik = 0; ik < this.properties.size(); ik++) {
        if (((Property)this.properties.get(ik)).name.equalsIgnoreCase(key)) {
          this.properties.set(ik, new Property(((Property)this.properties.get(ik)).name, Password.getEncryptedStatic((String)((Property)this.properties.get(ik)).value)));
        }
      }
      this.properties.add(new Property(key + "_encrypted", Boolean.valueOf(true)));
    }
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileCreateWASStmt(this);
  }
}
