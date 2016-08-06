package com.bloom.metaRepository;

import java.io.Serializable;
import java.util.concurrent.Callable;

public abstract interface RemoteCall<T>
  extends Callable<T>, Serializable
{}

