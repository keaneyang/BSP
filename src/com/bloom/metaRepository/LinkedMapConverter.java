package com.bloom.metaRepository;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.LinkedHashMap;

public class LinkedMapConverter
  extends PropertyDefConverter
{
  private static final long serialVersionUID = 1061473287059512670L;
  
  TypeReference getTypeReference()
  {
    return new TypeReference() {};
  }
}
