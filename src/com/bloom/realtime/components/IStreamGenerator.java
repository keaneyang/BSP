package com.bloom.runtime.components;

import com.bloom.runtime.BaseServer;
import com.bloom.runtime.meta.MetaInfo.MetaObject;

public abstract class IStreamGenerator
  extends FlowComponent
  implements Publisher, Runnable, Restartable
{
  public IStreamGenerator(BaseServer srv, MetaInfo.MetaObject info)
  {
    super(srv, info);
  }
}
