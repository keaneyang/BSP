package com.bloom.anno;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface PropertyTemplate
{
  String name();
  
  AdapterType type();
  
  String runtimeClass() default "<NOT SET>";
  
  PropertyTemplateProperty[] properties();
  
  Class<?> inputType() default NotSet.class;
  
  Class<?> outputType() default NotSet.class;
  
  boolean requiresParser() default false;
  
  boolean requiresFormatter() default false;
}

