package com.bloom.anno;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface PropertyTemplateProperty
{
  String name();
  
  Class<?> type();
  
  boolean required();
  
  String defaultValue();
  
  String label() default "";
  
  String description() default "";
}

