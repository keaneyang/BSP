package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.Property;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.utility.Utility;

import java.util.List;

public abstract class Stmt
{
  public String sourceText;
  public List<Property> compileOptions;
  public List returnText;
  
  public abstract Object compile(Compiler paramCompiler)
    throws MetaDataRepositoryException;
  
  public void setSourceText(String txt, List<Property> compileOptions)
  {
    this.sourceText = txt;
    this.compileOptions = compileOptions;
  }
  
  public boolean haveOption(String opt)
  {
    if (this.compileOptions != null) {
      for (Property s : this.compileOptions) {
        if (s.name.equalsIgnoreCase(opt)) {
          return true;
        }
      }
    }
    return false;
  }
  
  public Object getOption(String opt)
  {
    if (this.compileOptions != null) {
      for (Property s : this.compileOptions) {
        if (s.name.equalsIgnoreCase(opt)) {
          return s.value;
        }
      }
    }
    return null;
  }
  
  public String toString()
  {
    String str = this.sourceText;
    if ((this.sourceText == null) || (this.sourceText.isEmpty())) {
      str = str + Utility.prettyPrintMap(this.compileOptions);
    }
    return str;
  }
  
  public String getRedactedSourceText()
  {
    return this.sourceText;
  }
}
