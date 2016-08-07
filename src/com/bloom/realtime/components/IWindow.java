package com.bloom.runtime.components;

import com.bloom.runtime.BaseServer;
import com.bloom.runtime.channels.Channel.NewSubscriberAddedCallback;
import com.bloom.runtime.meta.MetaInfo.MetaObject;

public abstract class IWindow
  extends FlowComponent
  implements PubSub, Channel.NewSubscriberAddedCallback, Restartable, Compound
{
  public IWindow(BaseServer srv, MetaInfo.MetaObject info)
  {
    super(srv, info);
  }
}
