package com.bloom.metaRepository;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Set;

public class StringSetConverter
  extends PropertyDefConverter
{
  private static final long serialVersionUID = 6551168592266073269L;
  
  TypeReference getTypeReference()
  {
    return new TypeReference() {};
  }
}
