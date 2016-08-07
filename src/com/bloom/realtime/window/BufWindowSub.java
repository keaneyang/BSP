package com.bloom.runtime.window;

import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;
import java.util.Collection;

public abstract interface BufWindowSub
{
  public abstract void receive(RecordKey paramRecordKey, Collection<WAEvent> paramCollection, IBatch paramIBatch1, IBatch paramIBatch2);
}
