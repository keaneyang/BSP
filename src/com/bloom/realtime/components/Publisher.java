package com.bloom.runtime.components;

import com.bloom.runtime.channels.Channel;

public abstract interface Publisher
{
  public abstract Channel getChannel();
}

