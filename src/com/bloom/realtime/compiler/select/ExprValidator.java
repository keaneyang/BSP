package com.bloom.runtime.compiler.select;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bloom.runtime.BuiltInFunc;
import com.bloom.runtime.Context;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.Imports;
import com.bloom.runtime.compiler.TypeName;
import com.bloom.runtime.compiler.exprs.CastExprFactory;
import com.bloom.runtime.compiler.exprs.Expr;
import com.bloom.runtime.compiler.exprs.FieldRef;
import com.bloom.runtime.compiler.exprs.ParamRef;
import com.bloom.runtime.compiler.exprs.StaticFieldRef;
import com.bloom.runtime.compiler.exprs.TypeRef;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.utils.NameHelper;
import com.bloom.runtime.utils.NamePolicy;

import javassist.Modifier;

public abstract class ExprValidator
{
  private final Compiler compiler;
  private final Map<String, ParamRef> parameters;
  private final boolean acceptAgg;
  private final boolean acceptWildcard;
  
  public ExprValidator(Compiler compiler, Map<String, ParamRef> parameters, boolean acceptAgg, boolean acceptWildcard)
  {
    this.compiler = compiler;
    this.parameters = parameters;
    this.acceptAgg = acceptAgg;
    this.acceptWildcard = acceptWildcard;
  }
  
  public final void error(String msg, Object info)
  {
    this.compiler.error(msg, info);
  }
  
  public final Class<?> getTypeInfo(TypeName t)
  {
    return this.compiler.getClass(t);
  }
  
  public final ValueExpr cast(ValueExpr expr, Class<?> targetType)
  {
    return CastExprFactory.cast(this.compiler, expr, targetType);
  }
  
  public final List<Class<?>> getListOfStaticMethods(String funcName)
  {
    Context ctx = this.compiler.getContext();
    List<Class<?>> results = ctx.getImports().getStaticMethodRef(funcName, new Class[] { BuiltInFunc.class });
    if ((results != null) && (results.size() > 1)) {
      for (int i = 0; i < results.size(); i++) {
        if ((!((Class)results.get(i)).equals(BuiltInFunc.class)) && 
          (ctx.getCurApp() != null)) {
          ctx.getCurApp().importStatements.add("IMPORT STATIC " + ((Class)results.get(i)).getCanonicalName() + ".*;");
        }
      }
    }
    return results;
  }
  
  public final ValueExpr findStaticVariableOrClass(String name)
  {
    Context ctx = this.compiler.getContext();
    if (name.indexOf('.') != -1)
    {
      try
      {
        Class<?> c = ctx.getClass(NameHelper.getPrefix(name));
        Field f = NamePolicy.getField(c, NameHelper.getBasename(name));
        if (Modifier.isStatic(f.getModifiers())) {
          return new StaticFieldRef(f);
        }
      }
      catch (ClassNotFoundException|NoSuchFieldException|SecurityException e)
      {
        try
        {
          Class<?> c = ctx.getClass(name.toString());
          return new TypeRef(c);
        }
        catch (ClassNotFoundException e1) {}
      }
    }
    else
    {
      String basename = name;
      Field f = ctx.getImports().getStaticFieldRef(basename);
      if (f != null) {
        return new StaticFieldRef(f);
      }
      try
      {
        Class<?> c = ctx.getClass(basename);
        return new TypeRef(c);
      }
      catch (ClassNotFoundException e) {}
    }
    return null;
  }
  
  public Expr addParameter(ParamRef pref)
  {
    ParamRef ref = (ParamRef)this.parameters.get(pref.getName());
    if (ref == null)
    {
      int index = this.parameters.size();
      pref.setIndex(index);
      this.parameters.put(pref.getName(), pref);
      ref = pref;
    }
    return ref;
  }
  
  public boolean acceptAgg()
  {
    return this.acceptAgg;
  }
  
  public boolean acceptWildcard()
  {
    return this.acceptWildcard;
  }
  
  public void setHaveAggFuncs() {}
  
  public DataSet getDefaultDataSet()
  {
    return null;
  }
  
  public abstract Expr resolveAlias(String paramString);
  
  public abstract DataSet getDataSet(String paramString);
  
  public abstract FieldRef findField(String paramString);
}
