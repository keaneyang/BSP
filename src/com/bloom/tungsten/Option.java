package com.bloom.tungsten;

public class Option
{
  String option;
  Object state;
  
  public Option(String optionIn, Object stateIn)
  {
    this.option = optionIn;
    this.state = stateIn;
  }
  
  public String getOption()
  {
    return this.option;
  }
  
  public void setOption(String option)
  {
    this.option = option;
  }
  
  public Object getState()
  {
    return this.state;
  }
  
  public void setState(Object state)
  {
    this.state = state;
  }
}
