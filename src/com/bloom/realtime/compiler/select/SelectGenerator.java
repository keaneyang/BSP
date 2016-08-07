package com.bloom.runtime.compiler.select;

import com.bloom.classloading.WALoader;
import com.bloom.classloading.BundleDefinition.Type;
import com.bloom.event.QueryResultEvent;
import com.bloom.proc.events.DynamicEvent;
import com.bloom.runtime.Pair;
import com.bloom.runtime.TraceOptions;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.CompilerUtils;
import com.bloom.runtime.compiler.exprs.Expr;
import com.bloom.runtime.compiler.exprs.FuncCall;
import com.bloom.runtime.compiler.exprs.ParamRef;
import com.bloom.runtime.compiler.exprs.Predicate;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.stmts.LimitClause;
import com.bloom.runtime.compiler.stmts.OrderByItem;
import com.bloom.runtime.compiler.stmts.Select;
import com.bloom.runtime.components.CQSubTaskJoin;
import com.bloom.runtime.components.CQSubTaskNoJoin;
import com.bloom.runtime.containers.DynamicEventWrapper;
import com.bloom.runtime.meta.CQExecutionPlan;
import com.bloom.runtime.meta.CQExecutionPlan.DataSource;
import com.bloom.runtime.utils.FieldToObject;
import com.bloom.uuid.UUID;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;
import com.bloom.runtime.utils.StringUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;

public class SelectGenerator
  extends Generator
{
  private final ExprGenerator exprGen;
  private final Compiler compiler;
  private final String cqName;
  private final String cqNamespace;
  private final Select select;
  private final TraceOptions traceOptions;
  private final DataSets dataSets;
  private final Map<String, ParamRef> parameters;
  private final Map<DataSet, List<IndexDesc>> indexes;
  private final List<SelectCompiler.Target> targets;
  private final Class<?> targetType;
  private final boolean targetIsObj;
  private final Map<String, Integer> targetFieldIndices;
  private final List<UUID> recordedTypes;
  private final List<FuncCall> aggregatedExprs;
  private final List<Condition.CondExpr> constantConditions;
  private final Map<DataSet, Condition> filterConditions;
  private final Map<DataSet, List<ValueExpr>> index0key;
  private final Map<DataSet, List<JoinDesc>> joinPlans;
  private final List<ValueExpr> groupBy;
  private final List<OrderByItem> orderBy;
  private final Predicate having;
  
  public SelectGenerator(Compiler compiler, String cqName, String cqNamespace, Select select, TraceOptions traceOptions, DataSets dataSets, Map<String, ParamRef> parameters, Map<DataSet, List<IndexDesc>> indexes, List<SelectCompiler.Target> targets, Class<?> targetType, boolean targetIsObj, Map<String, Integer> targetFieldIndices, List<UUID> recordedTypes, List<FuncCall> aggregatedExprs, List<Condition.CondExpr> constantConditions, Map<DataSet, Condition> filterConditions, Map<DataSet, List<ValueExpr>> index0key, Map<DataSet, List<JoinDesc>> joinPlans, List<ValueExpr> groupBy, List<OrderByItem> orderBy, Predicate having)
  {
    this.exprGen = new ExprGenerator(compiler, false)
    {
      public void setPrevEventIndex(int index) {}
    };
    this.compiler = compiler;
    this.cqName = cqName;
    this.cqNamespace = cqNamespace;
    this.select = select;
    this.traceOptions = traceOptions;
    this.dataSets = dataSets;
    this.parameters = parameters;
    this.indexes = indexes;
    this.targets = targets;
    this.targetType = targetType;
    this.targetIsObj = targetIsObj;
    this.targetFieldIndices = targetFieldIndices;
    this.recordedTypes = recordedTypes;
    this.aggregatedExprs = aggregatedExprs;
    this.constantConditions = constantConditions;
    this.filterConditions = filterConditions;
    this.index0key = index0key;
    this.joinPlans = joinPlans;
    this.groupBy = groupBy;
    this.orderBy = orderBy;
    this.having = having;
  }
  
  public CQExecutionPlan generate()
    throws NotFoundException, CannotCompileException, IOException
  {
    List<String> srcCodeList = new ArrayList();
    List<byte[]> subtasks = generateExecutionPlan(srcCodeList);
    List<CQExecutionPlan.DataSource> sources = genDataSourcesInfo();
    int indexCount = this.indexes.size();
    List<Class<?>> aggFuncClasses = getAggFuncClasses();
    List<RSFieldDesc> rsdesc = makeResultSetDesc();
    List<ParamDesc> paramsDesc = makeParamsDesc();
    return new CQExecutionPlan(rsdesc, paramsDesc, this.select.kindOfStream, sources, subtasks, indexCount, aggFuncClasses, this.recordedTypes, haveGroupBy(), haveAggFuncs(), this.traceOptions, this.dataSets.size(), isQueryStateful(), false, srcCodeList);
  }
  
  private boolean haveGroupBy()
  {
    return !this.groupBy.isEmpty();
  }
  
  private boolean haveAggFuncs()
  {
    return !this.aggregatedExprs.isEmpty();
  }
  
  private boolean haveGroupByOrAggFuncs()
  {
    return (haveGroupBy()) || (haveAggFuncs());
  }
  
  private List<ParamDesc> makeParamsDesc()
  {
    List<ParamDesc> params = new ArrayList();
    for (ParamRef r : this.parameters.values())
    {
      String pname = r.getName();
      int pindex = r.getIndex();
      if (pindex == -1) {
        throw new RuntimeException("Index for parameter <" + pname + "> is not set");
      }
      Class<?> ptype = r.getExpectedType();
      if (ptype == null) {
        throw new RuntimeException("Type for parameter <" + pname + "> is not indentified");
      }
      ParamDesc d = new ParamDesc(pname, pindex, ptype);
      params.add(d);
    }
    return params;
  }
  
  private boolean isQueryStateful()
  {
    for (DataSet ds : this.dataSets) {
      if (!ds.isStateful()) {
        return false;
      }
    }
    return true;
  }
  
  private List<RSFieldDesc> makeResultSetDesc()
  {
    List<RSFieldDesc> fields = new ArrayList();
    for (SelectCompiler.Target t : this.targets)
    {
      Class<?> type = t.expr.getType();
      if (this.targetType == null)
      {
        Class<?> objType = CompilerUtils.getBoxingType(type);
        if (objType != null) {
          type = objType;
        }
      }
      RSFieldDesc f = new RSFieldDesc(t.alias, type);
      fields.add(f);
    }
    return fields;
  }
  
  private List<CQExecutionPlan.DataSource> genDataSourcesInfo()
  {
    List<CQExecutionPlan.DataSource> sources = new ArrayList();
    for (DataSet ds : this.dataSets) {
      if (ds.isIterator() == null)
      {
        TraslatedSchemaInfo ts = ds.isTranslated();
        UUID expectedTypeID = ts == null ? null : ts.expectedTypeID();
        CQExecutionPlan.DataSource dsi = new CQExecutionPlan.DataSource(ds.getFullName(), ds.getUUID(), expectedTypeID);
        
        assert (ds.getID() == sources.size());
        sources.add(dsi);
      }
    }
    return sources;
  }
  
  private List<byte[]> generateExecutionPlan(List<String> srcCodeList)
    throws NotFoundException, CannotCompileException, IOException
  {
    String ns = this.cqNamespace;
    WALoader wal = WALoader.get();
    String uri = wal.getBundleUri(ns, BundleDefinition.Type.query, this.cqName);
    try
    {
      wal.lockBundle(uri);
      if (wal.isExistingBundle(uri)) {
        wal.removeBundle(uri);
      }
      wal.addBundleDefinition(ns, BundleDefinition.Type.query, this.cqName);
    }
    finally
    {
      wal.unlockBundle(uri);
    }
    boolean haveJoin = this.dataSets.size() > 1;
    Class<?> superClass = haveJoin ? CQSubTaskJoin.class : CQSubTaskNoJoin.class;
    
    List<byte[]> codes = new ArrayList();
    for (DataSet ds : this.dataSets) {
      if (ds.isIterator() == null)
      {
        byte[] code = genSubTaskClass(wal, uri, ds, superClass, srcCodeList);
        codes.add(code);
      }
    }
    return codes;
  }
  
  private List<Class<?>> getAggFuncClasses()
  {
    List<Class<?>> classes = new ArrayList();
    for (FuncCall fc : this.aggregatedExprs)
    {
      Class<?> c = fc.getAggrClass();
      assert (c != null);
      classes.add(c);
    }
    return classes;
  }
  
  private byte[] genSubTaskClass(WALoader wal, String bundleUri, DataSet ds, Class<?> superClass, List<String> srcCodeList)
    throws NotFoundException, CannotCompileException, IOException
  {
    List<String> methodsCode = genMethods(ds, superClass);
    
    ClassPool pool = wal.getBundlePool(bundleUri);
    StringBuilder src = new StringBuilder();
    String className = ("QueryExecPlan_" + this.cqNamespace + "_" + this.cqName + "_" + ds.getFullName() + "_" + ds.getName()).replace('.', '_');
    
    CtClass cc = pool.makeClass(className);
    CtClass sup = pool.get(superClass.getName());
    cc.setSuperclass(sup);
    
    src.append("public class " + className + " extends " + superClass.getName() + "\n{\n");
    
    genStaticInit(cc, src);
    this.exprGen.genVarsDeclaration(cc, src);
    for (String mCode : methodsCode) {
      if (mCode != null) {
        makeMethod(mCode, cc, src);
      }
    }
    src.append("//end of " + className + "\n}\n");
    cc.setModifiers(cc.getModifiers() & 0xFBFF);
    cc.setModifiers(1);
    
    boolean dumpcode = (this.traceOptions.traceFlags & 0x4) > 0;
    boolean debugPath = this.traceOptions.traceFilePath != null;
    byte[] code = (dumpcode) && (debugPath) ? null : cc.toBytecode();
    
    String sourceCode = src.toString();
    srcCodeList.add(sourceCode);
    if (dumpcode)
    {
      PrintStream out = TraceOptions.getTraceStream(this.traceOptions);
      out.println(sourceCode);
      if (debugPath)
      {
        String srcpath = this.traceOptions.traceFilePath + "/" + className + ".java";
        FileOutputStream stream = new FileOutputStream(srcpath);Throwable localThrowable2 = null;
        try
        {
          stream.write(sourceCode.getBytes());
        }
        catch (Throwable localThrowable1)
        {
          localThrowable2 = localThrowable1;throw localThrowable1;
        }
        finally
        {
          if (stream != null) {
            if (localThrowable2 != null) {
              try
              {
                stream.close();
              }
              catch (Throwable x2)
              {
                localThrowable2.addSuppressed(x2);
              }
            } else {
              stream.close();
            }
          }
        }
        code = CompilerUtils.compileJavaFile(srcpath);
      }
    }
    String report = CompilerUtils.verifyBytecode(code, wal);
    wal.addBundleClass(bundleUri, className, cc.toBytecode(), false);
    if (report != null) {
      throw new Error("Internal error: invalid bytecode\n" + report);
    }
    cc.detach();
    return code;
  }
  
  private List<String> genMethods(DataSet ds, Class<?> superClass)
  {
    this.exprGen.removeAllTmpVarsAndFrames();
    List<String> methodsCode = new ArrayList();
    methodsCode.add(genGetThisDS(ds, superClass));
    methodsCode.add(genUpdateIndexes(ds, superClass));
    methodsCode.add(genAddedFilter(ds, superClass));
    methodsCode.add(genRunImpl(ds, superClass));
    methodsCode.add(genRun(superClass));
    List<JoinDesc> joins = (List)this.joinPlans.get(ds);
    if ((joins != null) && (!joins.isEmpty()))
    {
      Iterator i$ = joins.iterator();
      if (i$.hasNext())
      {
        JoinDesc join = (JoinDesc)i$.next();
        methodsCode.add("public boolean isNotOuterJoin() { return " + (!join.isOuter) + "; } ");
      }
    }
    return methodsCode;
  }
  
  private String genUpdateIndexes(DataSet ds, Class<?> superClass)
  {
    List<IndexDesc> idxs = (List)this.indexes.get(ds);
    if (idxs != null)
    {
      StringBuilder body = new StringBuilder();
      for (IndexDesc idx : idxs)
      {
        String key = "key" + idx.id;
        genKeyExprs(body, idx.exprs, idx.getIndexKeySignature(), key);
        body.append("this.context.updateIndex(" + idx.id + ", " + key + ", event, doadd);\n");
      }
      assert (checkMethod("updateIndexes", superClass));
      return "public void updateIndexes(" + WAEvent.class.getName() + " event, boolean doadd)\n{\n" + body.toString() + "return;\n}\n";
    }
    return "public boolean isUpdateIndexesCodeGenerated()\n{\n return false;\n}\n";
  }
  
  private String genGetThisDS(DataSet ds, Class<?> superClass)
  {
    assert (checkMethod("getThisDS", superClass));
    return "public int getThisDS()\n{\nreturn " + ds.getID() + ";\n}\n";
  }
  
  private String genAddedFilter(DataSet ds, Class<?> superClass)
  {
    List<ValueExpr> keyExprs = (List)this.index0key.get(ds);
    if (keyExprs == null) {
      return null;
    }
    assert (checkMethod("getAdded", superClass));
    assert (checkMethod("getAddedFiltered", superClass));
    String key = "filterkey" + ds.getID();
    StringBuilder body = new StringBuilder();
    genKeyExprs(body, keyExprs, Expr.getSignature(keyExprs), key);
    return "public " + IBatch.class.getCanonicalName() + " getAdded()\n{\n" + body.toString() + "return getAddedFiltered(" + key + ");\n}\n";
  }
  
  private void genConstConditions(StringBuilder body)
  {
    if (!this.constantConditions.isEmpty())
    {
      body.append("//gen constant filter\n");
      for (Condition.CondExpr cx : this.constantConditions)
      {
        Pair<Var, String> p = genExpr(cx.expr);
        body.append((String)p.second);
        body.append("if(!" + p.first + ")\n\treturn;\n");
      }
    }
  }
  
  private void genFilterConditions(DataSet ds, StringBuilder body)
  {
    body.append("//gen filter\n");
    Condition filter = (Condition)this.filterConditions.get(ds);
    if (filter != null) {
      for (Condition.CondExpr cx : filter.getCondExprs())
      {
        Pair<Var, String> p = genExpr(cx.expr);
        body.append((String)p.second);
        body.append("if(!" + p.first + ")\n\treturn;\n");
      }
    }
  }
  
  private void genJoins(DataSet ds, StringBuilder body, Class<?> superClass)
  {
    body.append("//gen join\n");
    List<JoinDesc> joins = (List)this.joinPlans.get(ds);
    if ((joins != null) && (!joins.isEmpty()))
    {
      body.append("// ---- join " + ds.getName() + " with ... \n");
      for (JoinDesc join : joins)
      {
        int withDS = join.withDataSet.getID();
        String withDSName = join.withDataSet.getName();
        if (join.algorithm == JoinDesc.JoinAlgorithm.INDEX_LOOKUP)
        {
          body.append("// ---- index join with " + withDSName + "\n");
          String key = "joinkey" + withDS;
          genKeyExprs(body, join.searchExprs, join.getSearchKeySignature(), key);
          assert (checkMethod("createIndexIterator", superClass));
          body.append("createIndexIterator(" + withDS + ", " + join.joinIndex.id + ", " + key + ");\n");
        }
        else if (join.algorithm == JoinDesc.JoinAlgorithm.WINDOW_INDEX_LOOKUP)
        {
          body.append("// ---- window index join with " + withDSName + "\n");
          String key = "joinkey" + withDS;
          genKeyExprs(body, join.searchExprs, join.getSearchKeySignature(), key);
          assert (checkMethod("createWindowLookupIterator", superClass));
          body.append("createWindowLookupIterator(" + withDS + ", " + join.windowIndexID + ", " + key + ");\n");
        }
        else if (join.algorithm == JoinDesc.JoinAlgorithm.SCAN)
        {
          body.append("// ---- cross join with " + withDSName + "\n");
          IteratorInfo it = join.withDataSet.isIterator();
          if (it != null)
          {
            Pair<Var, String> fld = genExpr(it.getIterableField());
            body.append((String)fld.second);
            assert (checkMethod("createCollectionIterator", superClass));
            body.append("createCollectionIterator(" + withDS + ", " + fld.first + ");\n");
          }
          else
          {
            assert (checkMethod("createWindowIterator", superClass));
            body.append("createWindowIterator(" + withDS + ");\n");
          }
        }
        else if (join.algorithm == JoinDesc.JoinAlgorithm.WITH_STREAM)
        {
          body.append("// ---- stream join with " + withDSName + "\n");
          assert (checkMethod("createEmptyIterator", superClass));
          body.append("createEmptyIterator(" + withDS + ");\n");
        }
        else if (!$assertionsDisabled)
        {
          throw new AssertionError();
        }
        assert (checkMethod("fetch", superClass));
        if (join.isOuter)
        {
          body.append("// ---- start outer join with " + withDSName + "\n");
          body.append("boolean _found" + withDS + " = false;\n");
          body.append("while(true) {\n");
          body.append("    boolean _fetched" + withDS + " = fetch(" + withDS + ");\n");
          body.append("    if(!_fetched" + withDS + ") {\n");
          body.append("        if(_found" + withDS + ") break;\n");
          body.append("        _found" + withDS + " = true;\n");
          body.append("    }\n");
          for (Predicate pred : join.postFilters)
          {
            Pair<Var, String> p = genExpr(pred);
            body.append("// after join condition\n");
            body.append((String)p.second);
            body.append("if(!" + p.first + ") continue;\n");
          }
          if (!join.afterJoinConditions.isEmpty())
          {
            body.append("// outer join condition\n");
            for (Condition c : join.afterJoinConditions) {
              for (Condition.CondExpr cx : c.getCondExprs())
              {
                boolean isOuterJoinCond = cx.isOuterJoinCondtion(ds.id2bitset(), withDS);
                if (isOuterJoinCond)
                {
                  Pair<Var, String> p = genExpr(cx.expr);
                  body.append((String)p.second);
                  body.append("if(_fetched" + withDS + ") {\n");
                  body.append("    if(!" + p.first + ") continue;\n");
                  body.append("}\n");
                }
              }
            }
          }
          body.append("_found" + withDS + " = true;\n");
          if (!join.afterJoinConditions.isEmpty())
          {
            body.append("// after join condition\n");
            for (Condition c : join.afterJoinConditions) {
              for (Condition.CondExpr cx : c.getCondExprs())
              {
                boolean isOuterJoinCond = cx.isOuterJoinCondtion(ds.id2bitset(), withDS);
                if (!isOuterJoinCond)
                {
                  Pair<Var, String> p = genExpr(cx.expr);
                  body.append((String)p.second);
                  body.append("if(!" + p.first + ") continue;\n");
                }
              }
            }
          }
        }
        else
        {
          body.append("// ---- start join with " + join.withDataSet.getName() + "\n");
          body.append("while(fetch(" + withDS + ")) {\n");
          for (Predicate pred : join.postFilters)
          {
            Pair<Var, String> p = genExpr(pred);
            body.append((String)p.second);
            body.append("if(!" + p.first + ") continue;\n");
          }
          if (!join.afterJoinConditions.isEmpty()) {
            for (Condition c : join.afterJoinConditions) {
              for (Condition.CondExpr cx : c.getCondExprs())
              {
                Pair<Var, String> p = genExpr(cx.expr);
                body.append((String)p.second);
                body.append("if(!" + p.first + ") continue;\n");
              }
            }
          }
        }
      }
    }
    body.append("//most inner cycle\n");
  }
  
  private void genEndOfJoinLoop(DataSet ds, StringBuilder body)
  {
    List<JoinDesc> joins = (List)this.joinPlans.get(ds);
    if ((joins != null) && (!joins.isEmpty()))
    {
      body.append("//end of join\n");
      for (JoinDesc join : joins) {
        body.append("} // end of join with " + join.withDataSet.getName() + "\n");
      }
    }
  }
  
  private void genGroupByKey(StringBuilder body, Class<?> superClass)
  {
    body.append("//set group key\n");
    if (haveGroupByOrAggFuncs())
    {
      assert (checkMethod("setGroupByKeyAndAggVec", superClass));
      assert (checkMethod("setGroupByKeyAndDefaultAggVec", superClass));
      if (haveGroupBy())
      {
        Class<?>[] groupByKeyType = Expr.getSignature(this.groupBy);
        genKeyExprs(body, this.groupBy, groupByKeyType, "groupkey");
        body.append("setGroupByKeyAndAggVec(groupkey);\n");
      }
      else
      {
        body.append("setGroupByKeyAndDefaultAggVec();\n");
      }
    }
  }
  
  private void genAggExpressions(StringBuilder body)
  {
    body.append("//aggregate\n");
    if (haveAggFuncs()) {
      for (FuncCall fc : this.aggregatedExprs) {
        body.append(this.exprGen.genAggExprs(fc));
      }
    }
  }
  
  private void genHaving(StringBuilder body)
  {
    body.append("//having\n");
    if (this.having != null)
    {
      Pair<Var, String> p = genExpr(this.having);
      body.append((String)p.second);
      body.append("if(" + p.first + ") {\n");
    }
  }
  
  private void genOutputEvent(StringBuilder body)
  {
    body.append("//output\n");
    if (this.targetType != null)
    {
      String targetTypeName = this.targetType.getName();
      if (!this.targetIsObj)
      {
        body.append(targetTypeName + " res = new " + targetTypeName + "(System.currentTimeMillis());\n");
        body.append("boolean setFieldFlag = (res.fieldIsSet != null);\n");
        body.append("res.allFieldsSet = " + (this.targets.size() == this.targetFieldIndices.size() ? "true" : "false") + ";\n");
        if (this.select.distinct) {
          body.append("Object[] tmp = new Object[" + this.targets.size() + "];\n");
        }
        int i = 0;
        for (SelectCompiler.Target t : this.targets)
        {
          Pair<Var, String> s = genExpr(t.expr);
          body.append((String)s.second);
          if (this.select.distinct) {
            body.append("tmp[" + i + "] = " + FieldToObject.genConvert(((Var)s.first).getName()) + ";\n");
          }
          i++;
          body.append("res." + t.targetField.getName() + "=" + s.first + ";\n");
          Integer index = (Integer)this.targetFieldIndices.get(t.targetField.getName());
          if (index != null)
          {
            int pos = index.intValue() / 7;
            int offset = index.intValue() % 7;
            byte bitset = (byte)(1 << offset);
            body.append("if (setFieldFlag) res.fieldIsSet[" + pos + "] |= " + bitset + ";\n");
          }
        }
        if (this.select.distinct)
        {
          assert (checkMethod("setPayload", QueryResultEvent.class));
          
          body.append("res.payload = tmp;\n");
        }
      }
      else
      {
        assert (this.targets.size() == 1);
        ValueExpr te = ((SelectCompiler.Target)this.targets.get(0)).expr;
        Pair<Var, String> s = genExpr(te);
        body.append((String)s.second);
        if (te.getType().equals(DynamicEventWrapper.class))
        {
          targetTypeName = DynamicEvent.class.getName();
          body.append(targetTypeName + " res = " + s.first + ".event;\n");
        }
        else
        {
          body.append(targetTypeName + " res = " + s.first + ";\n");
        }
      }
    }
    else
    {
      String targetTypeName = QueryResultEvent.class.getName();
      body.append("Object[] tmp = new Object[" + this.targets.size() + "];\n");
      int i = 0;
      for (SelectCompiler.Target t : this.targets)
      {
        Pair<Var, String> s = genExpr(t.expr);
        body.append((String)s.second);
        body.append("tmp[" + i + "] = " + FieldToObject.genConvert(((Var)s.first).getName()) + ";\n");
        i++;
      }
      body.append(targetTypeName + " res = new " + targetTypeName + "(System.currentTimeMillis());\n");
      assert (checkMethod("setPayload", QueryResultEvent.class));
      body.append("res.setPayload(tmp);\n");
    }
    body.append("setOutputEvent(res);\n");
  }
  
  private void genOrderByKey(StringBuilder body)
  {
    body.append("//order by key\n");
    if (!this.orderBy.isEmpty())
    {
      List<ValueExpr> orderByExprs = getOrderByExprs();
      Class<?>[] orderByKeyType = Expr.getSignature(orderByExprs);
      genKeyExprs(body, orderByExprs, orderByKeyType, "orderkey", true);
      body.append("setOrderByKey(orderkey);\n");
    }
  }
  
  private void addOutputEventToBatch(StringBuilder body, Class<?> superClass)
  {
    assert (checkMethod("addEvent", superClass));
    if (this.select.linksrc) {
      body.append("linkSourceEvents(); // link source events\n");
    }
    body.append("addEvent(); //add result to the batch\n");
  }
  
  private void genEndOfHaving(StringBuilder body, Class<?> superClass)
  {
    assert (checkMethod("removeGroupKey", superClass));
    if (this.having != null) {
      if (haveGroupByOrAggFuncs()) {
        body.append("} else { removeGroupKey(); } // end of having\n");
      } else {
        body.append("} // end of having\n");
      }
    }
  }
  
  private String genRunImpl(DataSet ds, Class<?> superClass)
  {
    assert (ds.isIterator() == null);
    StringBuilder body = new StringBuilder();
    
    genConstConditions(body);
    genFilterConditions(ds, body);
    genJoins(ds, body, superClass);
    genGroupByKey(body, superClass);
    genAggExpressions(body);
    genHaving(body);
    genOutputEvent(body);
    genOrderByKey(body);
    addOutputEventToBatch(body, superClass);
    genEndOfHaving(body, superClass);
    genEndOfJoinLoop(ds, body);
    
    assert (checkMethod("runImpl", superClass));
    return "public void runImpl()\n{\n" + body.toString() + "return;\n}\n";
  }
  
  private String genRun(Class<?> superClass)
  {
    assert (checkMethod("initResultBatchBuilder", superClass));
    assert (checkMethod("run", superClass));
    StringBuilder body = new StringBuilder();
    body.append("public void run()\n{\n\t");
    body.append("initResultBatchBuilder(\n\t");
    if (haveGroupByOrAggFuncs()) {
      body.append("makeGrouped()");
    } else {
      body.append("makeNotGrouped()");
    }
    body.append(",\n\t");
    List<OrderByItem> ord = this.orderBy.isEmpty() ? null : this.select.orderBy;
    if (ord != null) {
      body.append("makeOrdered(" + ((OrderByItem)ord.get(0)).isAscending + ")");
    } else {
      body.append("makeNotOrdered()");
    }
    body.append(",\n\t");
    if (this.select.distinct) {
      body.append("makeRemoveDups()");
    } else {
      body.append("makeNotRemoveDups()");
    }
    body.append(",\n\t");
    LimitClause lim = this.select.limit;
    if (lim != null) {
      body.append("makeLimited(" + lim.offset + ", " + lim.limit + ")");
    } else {
      body.append("makeNotLimited()");
    }
    body.append(");\n\t");
    if (haveAggFuncs()) {
      body.append("processAggregated()");
    } else {
      body.append("processNotAggregated()");
    }
    body.append(";\n}\n");
    return body.toString();
  }
  
  private List<ValueExpr> getOrderByExprs()
  {
    new AbstractList()
    {
      public ValueExpr get(int index)
      {
        return ((OrderByItem)SelectGenerator.this.orderBy.get(index)).expr;
      }
      
      public int size()
      {
        return SelectGenerator.this.orderBy.size();
      }
    };
  }
  
  private static boolean checkMethod(String name, Class<?> klass)
  {
    for (Method m : klass.getMethods()) {
      if (m.getName().equals(name)) {
        return true;
      }
    }
    return false;
  }
  
  private Pair<Var, String> genExpr(Expr expr)
  {
    return this.exprGen.genExpr(expr);
  }
  
  private Pair<List<Var>, String> genExprs(List<? extends Expr> exprs)
  {
    return this.exprGen.genExprs(exprs);
  }
  
  private void genKeyExprs(StringBuilder body, List<? extends Expr> exprs, Class<?>[] sig, String keyVar)
  {
    genKeyExprs(body, exprs, sig, keyVar, false);
  }
  
  private void genKeyExprs(StringBuilder body, List<? extends Expr> exprs, Class<?>[] sig, String keyVar, boolean unique)
  {
    Pair<List<Var>, String> keyCode = genExprs(exprs);
    body.append((String)keyCode.second);
    List<String> keyArgs = new ArrayList();
    for (Var v : (List)keyCode.first) {
      keyArgs.add(v.getName());
    }
    assert (sig.length == keyArgs.size());
    int i = 0;
    for (Class<?> t : sig)
    {
      if (t.isPrimitive())
      {
        Class<?> bt = CompilerUtils.getBoxingType(t);
        String arg = (String)keyArgs.get(i);
        String newArg = "\n\t" + bt.getName() + ".valueOf(" + arg + ")";
        keyArgs.set(i, newArg);
      }
      i++;
    }
    if (unique) {
      keyArgs.add("genNextInt()");
    }
    String args = StringUtils.join(keyArgs);
    String recKeyType = RecordKey.class.getName();
    String factory = RecordKey.getObjArrayKeyFactory();
    String argArray = keyVar + "Args";
    body.append("Object[] " + argArray + " = { " + args + " };\n");
    body.append(recKeyType + " " + keyVar + " =\n\t" + factory + "(" + argArray + ");\n");
  }
  
  private void makeMethod(String mCode, CtClass cc, StringBuilder src)
  {
    try
    {
      CtMethod m = CtNewMethod.make(mCode, cc);
      cc.addMethod(m);
      src.append("\n").append(mCode);
    }
    catch (CannotCompileException e)
    {
      throw new RuntimeException("error in generated Java code\n" + mCode, e);
    }
  }
  
  private void genStaticInit(CtClass cc, StringBuilder src)
    throws CannotCompileException
  {
    this.exprGen.genStaticVarsDeclaration(cc, src);
    StringBuilder body = new StringBuilder();
    for (String expr : this.exprGen.getStaticInitExprs()) {
      body.append(expr);
    }
    String mCode = "public static void initStatic()\n{\n" + body.toString() + "return;\n}\n";
    makeMethod(mCode, cc, src);
  }
}
