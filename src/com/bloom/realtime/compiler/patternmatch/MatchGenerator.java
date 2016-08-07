package com.bloom.runtime.compiler.patternmatch;

import com.bloom.classloading.WALoader;
import com.bloom.classloading.BundleDefinition.Type;
import com.bloom.event.QueryResultEvent;
import com.bloom.proc.events.DynamicEvent;
import com.bloom.runtime.Context;
import com.bloom.runtime.Pair;
import com.bloom.runtime.TraceOptions;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.CompilerUtils;
import com.bloom.runtime.compiler.exprs.Expr;
import com.bloom.runtime.compiler.exprs.ParamRef;
import com.bloom.runtime.compiler.exprs.Predicate;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.select.DataSet;
import com.bloom.runtime.compiler.select.DataSets;
import com.bloom.runtime.compiler.select.ExprGenerator;
import com.bloom.runtime.compiler.select.Generator;
import com.bloom.runtime.compiler.select.ParamDesc;
import com.bloom.runtime.compiler.select.RSFieldDesc;
import com.bloom.runtime.compiler.select.TraslatedSchemaInfo;
import com.bloom.runtime.compiler.select.Var;
import com.bloom.runtime.compiler.select.SelectCompiler.Target;
import com.bloom.runtime.compiler.stmts.Select;
import com.bloom.runtime.components.CQPatternMatcher;
import com.bloom.runtime.components.CQPatternMatcher.MatcherContext;
import com.bloom.runtime.containers.DynamicEventWrapper;
import com.bloom.runtime.meta.CQExecutionPlan;
import com.bloom.runtime.meta.CQExecutionPlan.DataSource;
import com.bloom.runtime.meta.MetaInfo.Namespace;
import com.bloom.runtime.utils.FieldToObject;
import com.bloom.uuid.UUID;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.utils.StringUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;

public class MatchGenerator
  extends Generator
{
  private final ExprGenerator exprGen;
  private final Compiler compiler;
  private final String cqName;
  private final Select select;
  private final TraceOptions traceOptions;
  private final DataSets dataSets;
  private final Map<String, ParamRef> parameters;
  private final List<SelectCompiler.Target> targets;
  private final Class<?> targetType;
  private final boolean targetIsObj;
  private final Map<String, Integer> targetFieldIndices;
  private final List<UUID> recordedTypes;
  private final Collection<PatternVariable> variables;
  private final List<ValueExpr> partKey;
  private final ValidatedPatternNode pattern;
  private int prevEventIndex = 0;
  
  public MatchGenerator(Compiler compiler, String cqName, Select select, TraceOptions traceOptions, DataSets dataSets, Map<String, ParamRef> parameters, List<SelectCompiler.Target> targets, Class<?> targetType, boolean targetIsObj, Map<String, Integer> targetFieldIndices, List<UUID> recordedTypes, Collection<PatternVariable> variables, List<ValueExpr> partKey, ValidatedPatternNode pattern)
  {
    this.exprGen = new ExprGenerator(compiler, true)
    {
      public void setPrevEventIndex(int index)
      {
        if (index > MatchGenerator.this.prevEventIndex) {
          MatchGenerator.this.prevEventIndex = index;
        }
      }
    };
    this.compiler = compiler;
    this.cqName = cqName;
    this.select = select;
    this.traceOptions = traceOptions;
    this.dataSets = dataSets;
    this.parameters = parameters;
    this.targets = targets;
    this.targetType = targetType;
    this.targetIsObj = targetIsObj;
    this.targetFieldIndices = targetFieldIndices;
    this.recordedTypes = recordedTypes;
    this.variables = variables;
    this.partKey = partKey;
    this.pattern = pattern;
  }
  
  public CQExecutionPlan generate()
    throws NotFoundException, CannotCompileException, IOException
  {
    List<byte[]> subtasks = generateExecutionPlan();
    List<CQExecutionPlan.DataSource> dataSources = genDataSourcesInfo();
    List<RSFieldDesc> rsdesc = makeResultSetDesc();
    List<ParamDesc> paramsDesc = makeParamsDesc();
    return new CQExecutionPlan(rsdesc, paramsDesc, this.select.kindOfStream, dataSources, subtasks, 0, Collections.emptyList(), this.recordedTypes, false, false, this.traceOptions, this.dataSets.size(), isQueryStateful(), true, null);
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
  
  private List<byte[]> generateExecutionPlan()
    throws NotFoundException, CannotCompileException, IOException
  {
    String ns = this.compiler.getContext().getCurNamespace().name;
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
    byte[] code = genSubTaskClass(wal, uri, CQPatternMatcher.class);
    return Collections.singletonList(code);
  }
  
  private byte[] genSubTaskClass(WALoader wal, String bundleUri, Class<?> superClass)
    throws NotFoundException, CannotCompileException, IOException
  {
    List<String> methodsCode = genMethods(superClass);
    
    ClassPool pool = wal.getBundlePool(bundleUri);
    StringBuilder src = new StringBuilder();
    String className = ("QueryExecPlan_" + this.cqName).replace('.', '_');
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
    byte[] code = cc.toBytecode();
    String sourceCode = src.toString();
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
        CompilerUtils.compileJavaFile(srcpath);
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
  
  private List<String> genMethods(Class<?> superClass)
  {
    List<String> methodsCode = new ArrayList();
    methodsCode.add(genOutput(superClass));
    methodsCode.addAll(genVarExpr(superClass));
    methodsCode.add(genPartitionByKey(superClass));
    methodsCode.add(genCreateNode(superClass));
    methodsCode.add(genGetRootNodeId(superClass));
    methodsCode.add(genGetPrevEventIndex(superClass));
    return methodsCode;
  }
  
  private static String ctxParam()
  {
    return CQPatternMatcher.MatcherContext.class.getCanonicalName() + " ctx";
  }
  
  private static String curEventParam()
  {
    return "Object event";
  }
  
  private String genGetPrevEventIndex(Class<?> superClass)
  {
    assert (checkMethod("getPrevEventIndex", superClass));
    return "public int getPrevEventIndex() { return " + this.prevEventIndex + ";}\n";
  }
  
  private String genPartitionByKey(Class<?> superClass)
  {
    if ((this.partKey == null) || (this.partKey.isEmpty())) {
      return null;
    }
    this.exprGen.removeAllTmpVarsAndFrames();
    StringBuilder body = new StringBuilder();
    Pair<List<Var>, String> keyCode = genExprs(this.partKey);
    Class<?>[] sig = Expr.getSignature(this.partKey);
    body.append((String)keyCode.second);
    List<String> keyArgs = new ArrayList();
    for (Var v : (List)keyCode.first) {
      keyArgs.add(v.getName());
    }
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
    String args = StringUtils.join(keyArgs);
    String factory = RecordKey.getObjArrayKeyFactory();
    body.append("Object[] keyArgs = { " + args + " };\n");
    body.append("return " + factory + "(keyArgs);\n");
    String keyType = RecordKey.class.getCanonicalName();
    assert (checkMethod("makePartitionKey", superClass));
    return "public " + keyType + " makePartitionKey(" + curEventParam() + ")\n{\n" + body.toString() + "\n}\n";
  }
  
  private String genCreateNode(final Class<?> superClass)
  {
    final Map<Integer, String> cases = new TreeMap();
    MatchPatternGenerator gen = new MatchPatternGenerator()
    {
      private String makeNodeIdArray(List<ValidatedPatternNode> subnodes)
      {
        StringBuilder b = new StringBuilder();
        b.append("new int[] {");
        String sep = "";
        for (ValidatedPatternNode n : subnodes)
        {
          b.append(sep).append(n.getID());
          sep = ",";
        }
        b.append("}");
        return b.toString();
      }
      
      private void addCase(ValidatedPatternNode node, String method, Object... args)
      {
        assert (MatchGenerator.checkMethod(method, superClass));
        StringBuilder b = new StringBuilder();
        b.append(method).append("(ctx, ");
        b.append(node.getID());
        b.append(", ");
        b.append("\"");
        b.append(node.toString());
        b.append("\"");
        for (Object arg : args) {
          b.append(", ").append(arg);
        }
        b.append(")");
        cases.put(Integer.valueOf(node.getID()), b.toString());
      }
      
      public void emitAnchor(ValidatedPatternNode node, int id)
      {
        addCase(node, "createAnchor", new Object[] { Integer.valueOf(id) });
      }
      
      public void emitVariable(ValidatedPatternNode node, PatternVariable var)
      {
        if (var.isEventVar()) {
          addCase(node, "createVariable", new Object[] { Integer.valueOf(var.getId()) });
        } else {
          addCase(node, "createCondition", new Object[] { Integer.valueOf(var.getId()) });
        }
      }
      
      public void emitRepetition(ValidatedPatternNode node, ValidatedPatternNode subnode, PatternRepetition rep)
      {
        addCase(node, "createRepetition", new Object[] { Integer.valueOf(subnode.getID()), Integer.valueOf(rep.mintimes), Integer.valueOf(rep.maxtimes) });
        subnode.emit(this);
      }
      
      public void emitAlternation(ValidatedPatternNode node, List<ValidatedPatternNode> subnodes)
      {
        addCase(node, "createAlternation", new Object[] { makeNodeIdArray(subnodes) });
        for (ValidatedPatternNode n : subnodes) {
          n.emit(this);
        }
      }
      
      public void emitSequence(ValidatedPatternNode node, List<ValidatedPatternNode> subnodes)
      {
        addCase(node, "createSequence", new Object[] { makeNodeIdArray(subnodes) });
        for (ValidatedPatternNode n : subnodes) {
          n.emit(this);
        }
      }
    };
    this.pattern.emit(gen);
    StringBuilder body = new StringBuilder();
    body.append("switch(nodeid) {\n");
    for (Map.Entry<Integer, String> e : cases.entrySet())
    {
      int nodeid = ((Integer)e.getKey()).intValue();
      String fcall = (String)e.getValue();
      body.append("case " + nodeid + ": return " + fcall + ";\n");
    }
    body.append("default: throw new RuntimeException(\"Invalid pattern node id\" + nodeid);\n");
    body.append("}\n");
    assert (checkMethod("createNode", superClass));
    return "public Object createNode(" + ctxParam() + ", int nodeid)\n{\n" + body.toString() + "\n}\n";
  }
  
  private String genGetRootNodeId(Class<?> superClass)
  {
    assert (checkMethod("getRootNodeId", superClass));
    return "public int getRootNodeId() { return " + this.pattern.getID() + ";}\n";
  }
  
  private List<String> genVarExpr(Class<?> superClass)
  {
    List<String> methods = new ArrayList();
    StringBuilder exprEval = new StringBuilder();
    exprEval.append("switch(exprid) {\n");
    for (PatternVariable var : this.variables)
    {
      this.exprGen.removeAllTmpVarsAndFrames();
      final StringBuilder body = new StringBuilder();
      MatchExprGenerator egen = new MatchExprGenerator()
      {
        private void callFunc(String funcCall)
        {
          body.append("return ctx." + funcCall + ";\n");
        }
        
        public void emitCondition(int id, Predicate p)
        {
          Pair<Var, String> varAndCode = MatchGenerator.this.exprGen.genExpr(p);
          body.append((String)varAndCode.second);
          body.append("return " + varAndCode.first + ";\n");
        }
        
        public void emitVariable(int id, Predicate p, DataSet ds)
        {
          body.append("if(checkDS(dsid, " + ds.getID() + ")) return false;\n");
          Pair<Var, String> varAndCode = MatchGenerator.this.exprGen.genExpr(p);
          body.append((String)varAndCode.second);
          body.append("return " + varAndCode.first + ";\n");
        }
        
        public void emitCreateTimer(int id, long interval)
        {
          callFunc("createTimer(" + id + ", (long)" + interval + ")");
        }
        
        public void emitStopTimer(int id, int timerid)
        {
          callFunc("stopTimer(" + timerid + ")");
        }
        
        public void emitWaitTimer(int id, int timerid)
        {
          callFunc("waitTimer(" + timerid + ", event)");
        }
      };
      var.emit(egen);
      String code = "private boolean evalExpr" + var.getId() + "(" + ctxParam() + ", " + curEventParam() + ", int dsid, Long pos)\n{\n" + body.toString() + "\n}\n";
      
      methods.add(code);
      exprEval.append("case " + var.getId() + ": return evalExpr" + var.getId() + "(ctx, event, dsid, pos);\n");
    }
    exprEval.append("default: break;\n}/*end of switch*/\n");
    exprEval.append("throw new RuntimeException(\"unknown variable condition \" + exprid);\n");
    
    assert (checkMethod("evalExpr", superClass));
    String code = "public boolean evalExpr(" + ctxParam() + ", int exprid, " + curEventParam() + ", int dsid, Long pos)\n{\n" + exprEval.toString() + "\n}\n";
    
    methods.add(code);
    return methods;
  }
  
  private String genOutput(Class<?> superClass)
  {
    this.exprGen.removeAllTmpVarsAndFrames();
    StringBuilder body = new StringBuilder();
    body.append("//output\n");
    if (this.targetType != null)
    {
      String targetTypeName = this.targetType.getName();
      if (!this.targetIsObj)
      {
        body.append(targetTypeName + " res = new " + targetTypeName + "(System.currentTimeMillis());\n");
        body.append("boolean setFieldFlag = (res.fieldIsSet != null);\n");
        body.append("res.allFieldsSet = " + (this.targets.size() == this.targetFieldIndices.size() ? "true" : "false") + ";\n");
        for (SelectCompiler.Target t : this.targets)
        {
          Pair<Var, String> s = genExpr(t.expr);
          body.append((String)s.second);
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
    assert (checkMethod("output", superClass));
    return "public Object output(" + ctxParam() + ", " + curEventParam() + ")\n{\n" + body.toString() + "return res;\n}\n";
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
      throw new RuntimeException("error in generated Java code\n" + mCode + "\nall generated code\n" + src, e);
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
