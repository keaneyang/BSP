package com.bloom.runtime.compiler;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;

import com.bloom.classloading.ClassDefinition;
import com.bloom.classloading.WALoader;

import javax.tools.SimpleJavaFileObject;

class XJavaFileObject
  extends SimpleJavaFileObject
{
  public final String name;
  
  protected XJavaFileObject(String name)
  {
    super(URI.create("string:///" + name.replace('.', '/') + JavaFileObject.Kind.CLASS.extension), JavaFileObject.Kind.CLASS);
    
    this.name = name;
  }
  
  public InputStream openInputStream()
    throws IOException
  {
    byte[] bytes = WALoader.get().getClassDefinition(this.name).getByteCode();
    return new ByteArrayInputStream(bytes);
  }
}
