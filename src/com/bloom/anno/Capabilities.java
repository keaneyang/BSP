package com.bloom.anno;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Capabilities
{
  public static final int RECOVERY_NONE = 0;
  public static final int RECOVERY_REALTIME = 1;
  public static final int RECOVERY_APP = 2;
  public static final int RECOVERY_RESET = 3;
  
  int recovery() default 0;
}

