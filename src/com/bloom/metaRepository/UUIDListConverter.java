package com.bloom.metaRepository;

import com.bloom.uuid.UUID;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.List;

public class UUIDListConverter
  extends PropertyDefConverter
{
  private static final long serialVersionUID = -5546356012011481582L;
  
  TypeReference getTypeReference()
  {
    return new TypeReference() {};
  }
}
