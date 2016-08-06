package com.bloom.metaRepository;

import com.bloom.uuid.UUID;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.Set;

public class UUIDSetConverter
  extends PropertyDefConverter
{
  private static final long serialVersionUID = -9060006939480717113L;
  
  TypeReference getTypeReference()
  {
    return new TypeReference() {};
  }
}
