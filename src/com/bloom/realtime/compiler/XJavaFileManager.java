package com.bloom.runtime.compiler;

import com.bloom.classloading.DistributedClassLoader;
import com.bloom.classloading.WALoader;
import com.hazelcast.core.IMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileManager.Location;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.StandardLocation;

class XJavaFileManager
  extends ForwardingJavaFileManager<JavaFileManager>
{
  protected XJavaFileManager(JavaFileManager fileManager)
  {
    super(fileManager);
  }
  
  public String inferBinaryName(JavaFileManager.Location location, JavaFileObject file)
  {
    if ((file instanceof XJavaFileObject)) {
      return ((XJavaFileObject)file).name;
    }
    return super.inferBinaryName(location, file);
  }
  
  public ClassLoader getClassLoader(JavaFileManager.Location location)
  {
    return WALoader.get();
  }
  
  public Iterable<JavaFileObject> list(JavaFileManager.Location location, String packageName, Set<JavaFileObject.Kind> kinds, boolean recurse)
    throws IOException
  {
    Iterable<JavaFileObject> stdResults = this.fileManager.list(location, packageName, kinds, recurse);
    if ((location != StandardLocation.CLASS_PATH) || (!kinds.contains(JavaFileObject.Kind.CLASS)) || (!packageName.startsWith("wa"))) {
      return stdResults;
    }
    List<JavaFileObject> additional = new ArrayList();
    for (String name : DistributedClassLoader.getClasses().keySet()) {
      if (name.startsWith(packageName + ".")) {
        additional.add(new XJavaFileObject(name));
      }
    }
    if (additional.isEmpty()) {
      return stdResults;
    }
    for (JavaFileObject obj : stdResults) {
      additional.add(obj);
    }
    return additional;
  }
}
