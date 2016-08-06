package com.bloom.metaRepository;

import com.bloom.runtime.meta.MetaInfo.Role;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.List;

public class RolesListConverter
  extends PropertyDefConverter
{
  private static final long serialVersionUID = 1061473287059512670L;
  
  TypeReference getTypeReference()
  {
    return new TypeReference() {};
  }
}
