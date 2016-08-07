package com.bloom.runtime.meta;

public enum ProtectedNamespaces
{
  Global,  Admin; 
  private static final ProtectedNamespaces VALUES[];

  static {
	  VALUES = (new ProtectedNamespaces[] { Global, Admin }); 
	  }

	  
  private ProtectedNamespaces() {}
  
  public static ProtectedNamespaces getEnum(String value)
  {
    for (ProtectedNamespaces v : VALUES) {
      if (v.name().equalsIgnoreCase(value)) {
        return v;
      }
    }
    return null;
  }
}
