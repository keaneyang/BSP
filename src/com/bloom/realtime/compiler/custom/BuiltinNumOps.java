package com.bloom.runtime.compiler.custom;

import java.lang.reflect.Method;
import java.util.List;

import com.bloom.runtime.compiler.AST;
import com.bloom.runtime.compiler.exprs.ExprCmd;
import com.bloom.runtime.compiler.exprs.FuncCall;
import com.bloom.runtime.compiler.exprs.ValueExpr;

public class BuiltinNumOps
{
  public static ValueExpr rewriteNumOp(ExprCmd op, Class<?> operandType, List<ValueExpr> args)
  {
    Method m = findMethod(op, operandType, args.size());
    if (args.size() > 1)
    {
      ValueExpr ret = null;
      for (ValueExpr arg : args) {
        if (ret == null) {
          ret = arg;
        } else {
          ret = makeFuncCall(m, AST.NewList(ret, arg));
        }
      }
      return ret;
    }
    assert (args.size() == 1);
    return makeFuncCall(m, args);
  }
  
  private static String type2prefix(Class<?> retType)
  {
    if (retType == Integer.class) {
      return "i";
    }
    if (retType == Long.class) {
      return "l";
    }
    if (retType == Float.class) {
      return "f";
    }
    if (retType == Double.class) {
      return "d";
    }
    if (!$assertionsDisabled) {
      throw new AssertionError();
    }
    return null;
  }
  
  private static Method findMethod(ExprCmd op, Class<?> type, int nparam)
  {
    String opname = type2prefix(type) + op.name().toLowerCase();
    try
    {
      if (nparam == 1) {
        return BuiltinNumOps.class.getMethod(opname, new Class[] { type });
      }
      return BuiltinNumOps.class.getMethod(opname, new Class[] { type, type });
    }
    catch (NoSuchMethodException|SecurityException e)
    {
      e.printStackTrace();
      if (!$assertionsDisabled) {
        throw new AssertionError();
      }
    }
    return null;
  }
  
  private static FuncCall makeFuncCall(Method m, List<ValueExpr> fargs)
  {
    FuncCall fc = new FuncCall(m.getName(), fargs, 0);
    fc.setMethodRef(m);
    return fc;
  }
  
  public static Integer iplus(Integer x, Integer y)
  {
    return (x != null) && (y != null) ? Integer.valueOf(x.intValue() + y.intValue()) : null;
  }
  
  public static Integer iminus(Integer x, Integer y)
  {
    return (x != null) && (y != null) ? Integer.valueOf(x.intValue() - y.intValue()) : null;
  }
  
  public static Integer imul(Integer x, Integer y)
  {
    return (x != null) && (y != null) ? Integer.valueOf(x.intValue() * y.intValue()) : null;
  }
  
  public static Integer idiv(Integer x, Integer y)
  {
    return (x != null) && (y != null) ? Integer.valueOf(x.intValue() / y.intValue()) : null;
  }
  
  public static Integer imod(Integer x, Integer y)
  {
    return (x != null) && (y != null) ? Integer.valueOf(x.intValue() % y.intValue()) : null;
  }
  
  public static Integer ibitand(Integer x, Integer y)
  {
    return (x != null) && (y != null) ? Integer.valueOf(x.intValue() & y.intValue()) : null;
  }
  
  public static Integer ibitor(Integer x, Integer y)
  {
    return (x != null) && (y != null) ? Integer.valueOf(x.intValue() | y.intValue()) : null;
  }
  
  public static Integer ibitxor(Integer x, Integer y)
  {
    return (x != null) && (y != null) ? Integer.valueOf(x.intValue() ^ y.intValue()) : null;
  }
  
  public static Integer ilshift(Integer x, Integer y)
  {
    return (x != null) && (y != null) ? Integer.valueOf(x.intValue() << y.intValue()) : null;
  }
  
  public static Integer irshift(Integer x, Integer y)
  {
    return (x != null) && (y != null) ? Integer.valueOf(x.intValue() >> y.intValue()) : null;
  }
  
  public static Integer iurshift(Integer x, Integer y)
  {
    return (x != null) && (y != null) ? Integer.valueOf(x.intValue() >>> y.intValue()) : null;
  }
  
  public static Integer iuminus(Integer x)
  {
    return x != null ? Integer.valueOf(-x.intValue()) : null;
  }
  
  public static Integer iinvert(Integer x)
  {
    return x != null ? Integer.valueOf(x.intValue() ^ 0xFFFFFFFF) : null;
  }
  
  public static Integer iuplus(Integer x)
  {
    return x;
  }
  
  public static Long lplus(Long x, Long y)
  {
    return (x != null) && (y != null) ? Long.valueOf(x.longValue() + y.longValue()) : null;
  }
  
  public static Long lminus(Long x, Long y)
  {
    return (x != null) && (y != null) ? Long.valueOf(x.longValue() - y.longValue()) : null;
  }
  
  public static Long lmul(Long x, Long y)
  {
    return (x != null) && (y != null) ? Long.valueOf(x.longValue() * y.longValue()) : null;
  }
  
  public static Long ldiv(Long x, Long y)
  {
    return (x != null) && (y != null) ? Long.valueOf(x.longValue() / y.longValue()) : null;
  }
  
  public static Long lmod(Long x, Long y)
  {
    return (x != null) && (y != null) ? Long.valueOf(x.longValue() % y.longValue()) : null;
  }
  
  public static Long lbitand(Long x, Long y)
  {
    return (x != null) && (y != null) ? Long.valueOf(x.longValue() & y.longValue()) : null;
  }
  
  public static Long lbitor(Long x, Long y)
  {
    return (x != null) && (y != null) ? Long.valueOf(x.longValue() | y.longValue()) : null;
  }
  
  public static Long lbitxor(Long x, Long y)
  {
    return (x != null) && (y != null) ? Long.valueOf(x.longValue() ^ y.longValue()) : null;
  }
  
  public static Long llshift(Long x, Long y)
  {
    return (x != null) && (y != null) ? Long.valueOf(x.longValue() << (int)y.longValue()) : null;
  }
  
  public static Long lrshift(Long x, Long y)
  {
    return (x != null) && (y != null) ? Long.valueOf(x.longValue() >> (int)y.longValue()) : null;
  }
  
  public static Long lurshift(Long x, Long y)
  {
    return (x != null) && (y != null) ? Long.valueOf(x.longValue() >>> (int)y.longValue()) : null;
  }
  
  public static Long luminus(Long x)
  {
    return x != null ? Long.valueOf(-x.longValue()) : null;
  }
  
  public static Long linvert(Long x)
  {
    return x != null ? Long.valueOf(x.longValue() ^ 0xFFFFFFFFFFFFFFFF) : null;
  }
  
  public static Long luplus(Long x)
  {
    return x;
  }
  
  public static Float fplus(Float x, Float y)
  {
    return (x != null) && (y != null) ? Float.valueOf(x.floatValue() + y.floatValue()) : null;
  }
  
  public static Float fminus(Float x, Float y)
  {
    return (x != null) && (y != null) ? Float.valueOf(x.floatValue() - y.floatValue()) : null;
  }
  
  public static Float fmul(Float x, Float y)
  {
    return (x != null) && (y != null) ? Float.valueOf(x.floatValue() * y.floatValue()) : null;
  }
  
  public static Float fdiv(Float x, Float y)
  {
    return (x != null) && (y != null) ? Float.valueOf(x.floatValue() / y.floatValue()) : null;
  }
  
  public static Float fmod(Float x, Float y)
  {
    return (x != null) && (y != null) ? Float.valueOf(x.floatValue() % y.floatValue()) : null;
  }
  
  public static Float fuminus(Float x)
  {
    return x != null ? Float.valueOf(-x.floatValue()) : null;
  }
  
  public static Float fuplus(Float x)
  {
    return x;
  }
  
  public static Double dplus(Double x, Double y)
  {
    return (x != null) && (y != null) ? Double.valueOf(x.doubleValue() + y.doubleValue()) : null;
  }
  
  public static Double dminus(Double x, Double y)
  {
    return (x != null) && (y != null) ? Double.valueOf(x.doubleValue() - y.doubleValue()) : null;
  }
  
  public static Double dmul(Double x, Double y)
  {
    return (x != null) && (y != null) ? Double.valueOf(x.doubleValue() * y.doubleValue()) : null;
  }
  
  public static Double ddiv(Double x, Double y)
  {
    return (x != null) && (y != null) ? Double.valueOf(x.doubleValue() / y.doubleValue()) : null;
  }
  
  public static Double dmod(Double x, Double y)
  {
    return (x != null) && (y != null) ? Double.valueOf(x.doubleValue() % y.doubleValue()) : null;
  }
  
  public static Double duminus(Double x)
  {
    return x != null ? Double.valueOf(-x.doubleValue()) : null;
  }
  
  public static Double duplus(Double x)
  {
    return x;
  }
}
