package com.bloom.wactionstore;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class WAction
  extends ObjectNode
{
  public WAction()
  {
    super(Utility.nodeFactory);
  }
  
  public WAction(WAction copy)
  {
    this();
    setAll(copy);
  }
}
