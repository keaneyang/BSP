package com.bloom.metaRepository;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;

public class GroupMembersConverter
  extends PropertyDefConverter
{
  private static final long serialVersionUID = -5369700228207899772L;
  
  TypeReference getTypeReference()
  {
    return new TypeReference() {};
  }
}
