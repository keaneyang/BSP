package com.bloom.runtime;

import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.ShowStream;

public abstract interface ShowStreamExecutor
{
  public abstract void showStreamStmt(MetaInfo.ShowStream paramShowStream)
    throws Exception;
}
