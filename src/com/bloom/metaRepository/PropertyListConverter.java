package com.bloom.metaRepository;

import com.bloom.uuid.UUID;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.List;

public class PropertyListConverter
  extends PropertyDefConverter
{
  public static final long serialVersionUID = 2740988897547957681L;
  
  TypeReference getTypeReference()
  {
    return new TypeReference() {};
  }
}
