package com.bloom.proc;

import java.util.Random;

class Util
{
  public static Random nRandom = new Random();
  
  public static String ranString(Random rng, String characters, int length)
  {
    char[] ranStr = new char[length];
    for (int i = 0; i < length; i++) {
      ranStr[i] = characters.charAt(rng.nextInt(characters.length()));
    }
    return new String(ranStr);
  }
  
  public static int ranInt(Random a, int start, int end)
  {
    int range = end - start + 1;
    double frac = range * a.nextDouble();
    
    int ranNumber = (int)(frac + start);
    return ranNumber;
  }
}
