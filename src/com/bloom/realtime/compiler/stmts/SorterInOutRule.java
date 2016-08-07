package com.bloom.runtime.compiler.stmts;

public class SorterInOutRule
{
  public final String inStream;
  public final String inStreamField;
  public final String outStream;
  
  public SorterInOutRule(String inStream, String inStreamField, String outStream)
  {
    this.inStream = inStream;
    this.inStreamField = inStreamField;
    this.outStream = outStream;
  }
  
  public String toString()
  {
    return this.inStream + " ON " + this.inStreamField + " OUPUT TO " + this.outStream;
  }
}
