package com.bloom.runtime.compiler.exprs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.bloom.runtime.compiler.custom.AcceptWildcard;
import com.bloom.runtime.compiler.custom.AggHandlerDesc;
import com.bloom.runtime.compiler.custom.CustomFunction;
import com.bloom.runtime.compiler.custom.CustomFunctionTranslator;
import com.bloom.runtime.compiler.custom.Nondeterministic;
import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class FuncCall
  extends Operation
  implements Serializable
{
  protected static Method dummyMethod;
  private String name;
  private int kind;
  protected Method method;
  
  static
  {
    try
    {
      dummyMethod = FuncCall.class.getDeclaredMethod("m", new Class[0]);
    }
    catch (NoSuchMethodException|SecurityException e)
    {
      
    }
  }
  
  private int index = -1;
  
  public FuncCall(String funcName, List<ValueExpr> args, int options)
  {
    super(ExprCmd.FUNC, args);
    this.name = funcName;
    this.kind = options;
    this.method = dummyMethod;
  }
  
  public String toString()
  {
    return exprToString() + this.name + "(" + argsToString() + ")";
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitFuncCall(this, params);
  }
  
  public void setMethodRef(Method meth)
  {
    this.method = meth;
  }
  
  public CustomFunctionTranslator isCustomFunc()
  {
    CustomFunction desc = (CustomFunction)this.method.getAnnotation(CustomFunction.class);
    if (desc == null) {
      return null;
    }
    try
    {
      Object o = desc.translator().newInstance();
      return (CustomFunctionTranslator)o;
    }
    catch (InstantiationException|IllegalAccessException e) {}
    return null;
  }
  
  public AggHandlerDesc getAggrDesc()
  {
    return (AggHandlerDesc)this.method.getAnnotation(AggHandlerDesc.class);
  }
  
  public boolean acceptWildcard()
  {
    return this.method.getAnnotation(AcceptWildcard.class) != null;
  }
  
  public boolean isDistinct()
  {
    return (this.kind & 0x4) != 0;
  }
  
  public Class<?> getAggrClass()
  {
    AggHandlerDesc desc = getAggrDesc();
    if (desc == null) {
      return null;
    }
    if (!isDistinct()) {
      return desc.handler();
    }
    Class<?> c = desc.distinctHandler();
    return c == Object.class ? null : c;
  }
  
  public Method getMethd()
  {
    return this.method;
  }
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof FuncCall)) {
      return false;
    }
    FuncCall o = (FuncCall)other;
    return (super.equals(o)) && (this.kind == o.kind) && (this.method.equals(o.method));
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(this.kind).append(this.method).append(super.hashCode()).toHashCode();
  }
  
  public ValueExpr getThis()
  {
    return null;
  }
  
  public List<ValueExpr> getFuncArgs()
  {
    return this.args;
  }
  
  public boolean isConst()
  {
    if (getArgs().isEmpty()) {
      return false;
    }
    assert (this.method != null);
    if (this.method.getAnnotation(Nondeterministic.class) != null) {
      return false;
    }
    if (this.method.getAnnotation(CustomFunction.class) != null) {
      return false;
    }
    if (getAggrDesc() != null) {
      return false;
    }
    return super.isConst();
  }
  
  public Class<?> getType()
  {
    return this.method.getReturnType();
  }
  
  public int getIndex()
  {
    return this.index;
  }
  
  public void setIndex(int index)
  {
    this.index = index;
  }
  
  public String getName()
  {
    return this.name;
  }
  
  public boolean isValidated()
  {
    return this.method != dummyMethod;
  }
  
  private void writeObject(ObjectOutputStream out)
    throws IOException
  {
    out.writeUTF(this.name);
    out.writeInt(this.kind);
    out.writeInt(this.index);
    out.writeObject(this.method.getDeclaringClass());
    out.writeUTF(this.method.getName());
    out.writeObject(this.method.getParameterTypes());
  }
  
  private void readObject(ObjectInputStream in)
    throws IOException, ClassNotFoundException
  {
    this.name = in.readUTF();
    this.kind = in.readInt();
    this.index = in.readInt();
    Class<?> declaringClass = (Class)in.readObject();
    String methodName = in.readUTF();
    Class<?>[] parameterTypes = (Class[])in.readObject();
    try
    {
      this.method = declaringClass.getMethod(methodName, parameterTypes);
    }
    catch (Exception e)
    {
      this.method = dummyMethod;
    }
  }
  
  protected static void m() {}
}
