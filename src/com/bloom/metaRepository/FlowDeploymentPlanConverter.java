package com.bloom.metaRepository;

import com.bloom.runtime.meta.MetaInfo.Flow.Detail;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.List;

public class FlowDeploymentPlanConverter
  extends PropertyDefConverter
{
  public static final long serialVersionUID = 2740988897547657681L;
  
  TypeReference getTypeReference()
  {
    return new TypeReference() {};
  }
}
