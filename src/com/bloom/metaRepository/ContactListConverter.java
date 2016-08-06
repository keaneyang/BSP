package com.bloom.metaRepository;

import com.bloom.runtime.meta.MetaInfo.ContactMechanism;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.List;

public class ContactListConverter
  extends PropertyDefConverter
{
  private static final long serialVersionUID = -5126868144838596329L;
  
  TypeReference getTypeReference()
  {
    return new TypeReference() {};
  }
}
