package com.bloom.runtime;

import java.util.List;
import org.apache.log4j.Logger;

public class ExceptionControlAction
{
  private static final Logger logger = Logger.getLogger(ExceptionControlAction.class);
  private String name;
  private ActionType actionType;
  private List<Property> props = null;
  
  public ExceptionControlAction()
  {
    this("exceptionAction", ActionType.CONTINUE, null);
  }
  
  public ExceptionControlAction(String name, ActionType type, List<Property> props)
  {
    this.name = name;
    this.actionType = type;
    this.props = props;
  }
  
  public String getName()
  {
    return this.name;
  }
  
  public void setName(String name)
  {
    this.name = name;
  }
  
  public ActionType getActionType()
  {
    return this.actionType;
  }
  
  public void setActionType(ActionType actionType)
  {
    this.actionType = actionType;
  }
  
  public List<Property> getProps()
  {
    return this.props;
  }
  
  public void setProps(List<Property> props)
  {
    this.props = props;
  }
  
  public static void main(String[] args) {}
}
