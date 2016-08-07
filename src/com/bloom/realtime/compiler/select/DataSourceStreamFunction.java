package com.bloom.runtime.compiler.select;

import com.bloom.classloading.WALoader;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.BuiltInFunc;
import com.bloom.runtime.Context;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.custom.StreamGeneratorDef;
import com.bloom.runtime.compiler.exprs.Constant;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.MetaInfo.StreamGenerator;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.utils.RuntimeUtils;
import com.bloom.runtime.utils.StringUtils;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

public class DataSourceStreamFunction
  extends DataSourcePrimary
{
  private final String funcName;
  private final List<ValueExpr> args;
  private final String alias;
  
  public DataSourceStreamFunction(String funcName, List<ValueExpr> args, String alias)
  {
    this.funcName = funcName;
    this.args = args;
    this.alias = alias;
  }
  
  public String toString()
  {
    return this.funcName + "(" + StringUtils.join(this.args) + ") AS " + this.alias;
  }
  
  public DataSet resolveDataSourceImpl(DataSource.Resolver r)
    throws MetaDataRepositoryException
  {
    Compiler co = r.getCompiler();
    Context ctx = co.getContext();
    String name = this.funcName;
    try
    {
      int index = name.lastIndexOf('.');
      String methodName;
      String methodName;
      Class<?> klass;
      if (index == -1)
      {
        Class<?> klass = BuiltInFunc.class;
        methodName = name;
      }
      else
      {
        String className = name.substring(0, index);
        methodName = name.substring(index + 1);
        klass = ctx.getClass(className);
      }
      int i = 1;
      Object[] params = new Object[this.args.size() + 1];
      Class<?>[] ptypes = new Class[this.args.size() + 1];
      params[0] = WALoader.get();
      ptypes[0] = ClassLoader.class;
      for (ValueExpr expr : this.args) {
        if (!(expr instanceof Constant))
        {
          r.error("stream function parameter must be a constant", expr);
        }
        else
        {
          Constant constant = (Constant)expr;
          params[i] = constant.value;
          ptypes[i] = constant.getType();
          i++;
        }
      }
      Method meth = klass.getMethod(methodName, ptypes);
      Object o = meth.invoke(null, params);
      StreamGeneratorDef gdef = (StreamGeneratorDef)o;
      MetaInfo.Type t = co.createAnonType(gdef.outputTypeID, gdef.outputType);
      ctx.putTmpObject(t);
      MetaInfo.StreamGenerator g = new MetaInfo.StreamGenerator();
      g.construct(RuntimeUtils.genRandomName("generator"), ctx.getCurNamespace(), gdef.outputTypeID, gdef.generatorClassName, gdef.args);
      
      g.getMetaInfoStatus().setAnonymous(true);
      ctx.putTmpObject(g);
      r.addTypeID(gdef.outputTypeID);
      return DataSetFactory.createStreamFunctionDS(r.getNextDataSetID(), this.alias, gdef.outputType, g);
    }
    catch (ClassNotFoundException e)
    {
      r.error("cannot load stream generator function: no such class", name);
    }
    catch (NoSuchMethodException e)
    {
      r.error("cannot load stream generator function: no such method", name);
    }
    catch (SecurityException e)
    {
      r.error("cannot load stream generator function: method inaccessible", name);
    }
    catch (IllegalAccessException e)
    {
      r.error("cannot load stream generator function: method inaccessible", name);
    }
    catch (IllegalArgumentException e)
    {
      r.error("cannot load stream generator function: illegal arguments", name);
    }
    catch (InvocationTargetException e)
    {
      r.error("cannot load stream generator function: exception in function", name);
    }
    return null;
  }
}
