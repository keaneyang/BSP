package com.bloom.metaRepository;

import com.bloom.runtime.components.EntityType;
import com.bloom.uuid.UUID;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.LinkedHashSet;
import java.util.Map;

public class FlowMapConverter
  extends PropertyDefConverter
{
  public static final long serialVersionUID = 2740988897547657681L;
  
  TypeReference getTypeReference()
  {
    return new TypeReference() {};
  }
}
