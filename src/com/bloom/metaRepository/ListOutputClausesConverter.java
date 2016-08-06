package com.bloom.metaRepository;

import com.bloom.runtime.compiler.stmts.OutputClause;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.List;

public class ListOutputClausesConverter
  extends PropertyDefConverter
{
  TypeReference getTypeReference()
  {
    return new TypeReference() {};
  }
}
