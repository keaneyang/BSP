package com.bloom.runtime.compiler.select;

import com.bloom.runtime.meta.MetaInfo.WAStoreView;
import com.bloom.runtime.meta.MetaInfo.WActionStore;

public abstract class JsonWAStoreInfo
{
  public abstract MetaInfo.WAStoreView getStoreView();
  
  public abstract MetaInfo.WActionStore getStore();
  
  public abstract String toString();
}

