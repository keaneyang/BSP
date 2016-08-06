package com.bloom.metaRepository.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.bloom.event.ObjectMapperFactory;
import java.io.Serializable;

public abstract class ActionableProperties
  implements Serializable
{
  private ActionableFieldType actionableFieldType;
  private boolean isRequired;
  
  public static enum ActionableFieldType
  {
    TEXT,  NUMBER,  METAOBJECT,  BOOLEAN,  ENUM,  OBJECT;
    
    private ActionableFieldType() {}
  }
  
  public ActionableFieldType getActionablefieldType()
  {
    return this.actionableFieldType;
  }
  
  public void setActionableFieldType(ActionableFieldType actionableFieldType)
  {
    this.actionableFieldType = actionableFieldType;
  }
  
  public boolean isRequired()
  {
    return this.isRequired;
  }
  
  public void setIsRequired(boolean isRequired)
  {
    this.isRequired = isRequired;
  }
  
  public ObjectNode getJsonObject()
  {
    ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
    ObjectNode node = jsonMapper.createObjectNode();
    node.put("actionAbleFieldType", this.actionableFieldType.name());
    node.put("isRequired", this.isRequired);
    return node;
  }
}
