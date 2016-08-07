package com.bloom.runtime.components;

public abstract interface IFlowComponent
{
  public abstract void setFlow(Flow paramFlow);
  
  public abstract Flow getFlow();
  
  public abstract Flow getTopLevelFlow();
  
  public abstract boolean recoveryIsEnabled();
}

