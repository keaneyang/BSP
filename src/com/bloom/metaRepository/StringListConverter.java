package com.bloom.metaRepository;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;

public class StringListConverter
  extends PropertyDefConverter
{
  public static final long serialVersionUID = 2740988897547657681L;
  
  TypeReference getTypeReference()
  {
    return new TypeReference() {};
  }
}
