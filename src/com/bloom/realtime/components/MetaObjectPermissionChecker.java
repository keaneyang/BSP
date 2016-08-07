package com.bloom.runtime.components;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.uuid.AuthToken;

public abstract interface MetaObjectPermissionChecker
{
  public abstract boolean checkPermissionForMetaPropertyVariable(AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
}

