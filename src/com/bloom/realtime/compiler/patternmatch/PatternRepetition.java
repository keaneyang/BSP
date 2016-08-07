package com.bloom.runtime.compiler.patternmatch;

public class PatternRepetition
{
  public static final PatternRepetition zeroOrMore = new PatternRepetition(0, Integer.MAX_VALUE, 42);
  public static final PatternRepetition oneOrMore = new PatternRepetition(1, Integer.MAX_VALUE, 43);
  public static final PatternRepetition zeroOrOne = new PatternRepetition(0, 1, 63);
  public final int mintimes;
  public final int maxtimes;
  private final int symbol;
  
  public PatternRepetition(int mintimes, int maxtimes, int symbol)
  {
    this.mintimes = mintimes;
    this.maxtimes = maxtimes;
    this.symbol = symbol;
  }
  
  public String toString()
  {
    if (this.symbol != 0) {
      return String.valueOf((char)this.symbol);
    }
    if (this.maxtimes == this.mintimes) {
      return "{" + this.mintimes + "}";
    }
    return "{" + this.mintimes + ", " + this.maxtimes + "}";
  }
}
