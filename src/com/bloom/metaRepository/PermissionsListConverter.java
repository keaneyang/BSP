package com.bloom.metaRepository;

import com.bloom.security.ObjectPermission;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.List;

public class PermissionsListConverter
  extends PropertyDefConverter
{
  private static final long serialVersionUID = 4550360623119070790L;
  
  TypeReference getTypeReference()
  {
    return new TypeReference() {};
  }
}
