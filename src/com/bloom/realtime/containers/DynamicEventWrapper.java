package com.bloom.runtime.containers;

import com.bloom.proc.events.DynamicEvent;
import com.bloom.runtime.components.TranslatedSchema;

public class DynamicEventWrapper
{
  public final DynamicEvent event;
  public final TranslatedSchema schema;
  
  public DynamicEventWrapper(DynamicEvent event, TranslatedSchema schema)
  {
    this.event = event;
    this.schema = schema;
  }
}
