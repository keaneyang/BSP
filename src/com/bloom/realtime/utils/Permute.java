package com.bloom.runtime.utils;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

public class Permute
{
  private static  void makePermutations(List<List> res, Object[] buf, List input, BitSet index, int inputSize)
  {
    int pos = index.cardinality();
    if (pos == inputSize)
    {
      res.add(new ArrayList(Arrays.asList(buf)));
      return;
    }
    for (int i = 0; ((i = index.nextClearBit(i)) >= 0) && (i < inputSize); i++)
    {
      buf[pos] = input.get(i);
      index.set(i);
      makePermutations(res, buf, input, index, inputSize);
      index.clear(i);
    }
  }
  
  public static  List<List> makePermutations(List in)
  {
    List<List> res = new ArrayList();
    int size = in.size();
    makePermutations(res, (Object[])new Object[size], in, new BitSet(size), size);
    return res;
  }
  
  @SafeVarargs
  public static <T> void printPermutations(T... in)
  {
    int i = 0;
    for (List<T> res : makePermutations(Arrays.asList(in))) {
      System.out.println(String.format("%3d", new Object[] { Integer.valueOf(++i) }) + " " + res);
    }
    System.out.println("-------------------");
  }
  
  public static void main(String[] args)
  {
    printPermutations(new Integer[] { Integer.valueOf(1), Integer.valueOf(2) });
    printPermutations(new String[] { "1_", "2_", "3_" });
    printPermutations(new Double[] { Double.valueOf(1.0D), Double.valueOf(2.0D), Double.valueOf(3.0D), Double.valueOf(4.1D) });
  }
}
