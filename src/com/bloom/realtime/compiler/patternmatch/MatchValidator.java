package com.bloom.runtime.compiler.patternmatch;

import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.CompilerUtils;
import com.bloom.runtime.compiler.exprs.CastExprFactory;
import com.bloom.runtime.compiler.exprs.ComparePredicate;
import com.bloom.runtime.compiler.exprs.Constant;
import com.bloom.runtime.compiler.exprs.Expr;
import com.bloom.runtime.compiler.exprs.ExprCmd;
import com.bloom.runtime.compiler.exprs.FieldRef;
import com.bloom.runtime.compiler.exprs.FuncCall;
import com.bloom.runtime.compiler.exprs.ObjectRef;
import com.bloom.runtime.compiler.exprs.ParamRef;
import com.bloom.runtime.compiler.exprs.PatternVarRef;
import com.bloom.runtime.compiler.exprs.Predicate;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.select.DataSet;
import com.bloom.runtime.compiler.select.DataSets;
import com.bloom.runtime.compiler.select.DataSource;
import com.bloom.runtime.compiler.select.ExprValidator;
import com.bloom.runtime.compiler.select.SelectCompiler.Target;
import com.bloom.runtime.compiler.stmts.MatchClause;
import com.bloom.runtime.compiler.stmts.SelectTarget;
import com.bloom.runtime.compiler.visitors.ExprValidationVisitor;
import com.bloom.runtime.compiler.visitors.FindCommonSubexpression;
import com.bloom.runtime.compiler.visitors.FindCommonSubexpression.Rewriter;
import com.bloom.runtime.exceptions.AmbiguousFieldNameException;
import com.bloom.runtime.utils.Factory;
import com.bloom.runtime.utils.Permute;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class MatchValidator
{
  private final Map<String, ParamRef> parameters;
  private final DataSets dataSets;
  private final Compiler compiler;
  private Map<String, PatternVariable> variables = Factory.makeNameMap();
  private int nodeCount = 0;
  private int anchorCount = 0;
  private int varCount = 0;
  private final List<ValueExpr> partKey = new ArrayList();
  private ValidatedPatternNode pattern;
  private final List<SelectCompiler.Target> targets = new ArrayList();
  
  public MatchValidator(Compiler compiler, Map<String, ParamRef> parameters, DataSets dataSets)
  {
    this.compiler = compiler;
    this.parameters = parameters;
    this.dataSets = dataSets;
  }
  
  private void error(String message, Object info)
  {
    this.compiler.error(message, info);
  }
  
  private int newNodeId()
  {
    return this.nodeCount++;
  }
  
  private int newVarId()
  {
    return this.varCount++;
  }
  
  public void validate(MatchClause match, List<DataSource> from, List<SelectTarget> targets)
  {
    validateDefitions(match.definitions);
    validatePattern(match.pattern);
    validatePartitionBy(match.partitionkey, from);
    validateTargets(targets, match.partitionkey);
  }
  
  private void validateTargets(List<SelectTarget> selTargets, List<ValueExpr> partitionkey)
  {
    DataSet ds = (partitionkey != null) && (!partitionkey.isEmpty()) ? this.dataSets.get(0) : null;
    for (SelectTarget t : selTargets) {
      if (t.expr != null)
      {
        ValueExpr e = (ValueExpr)validateVarExpr(t.expr, ds);
        SelectCompiler.Target newt = new SelectCompiler.Target(e, t.alias);
        this.targets.add(newt);
      }
      else
      {
        error("only pattern variables can be referred", t);
      }
    }
  }
  
  private void validatePattern(PatternNode pattern)
  {
    this.pattern = pattern.validate(this);
  }
  
  public ValidatedPatternNode visitRestartAnchor()
  {
    return ValidatedPatternNode.makeRestartAnchor(newNodeId(), this.anchorCount++);
  }
  
  public ValidatedPatternNode visitVariable(String name)
  {
    PatternVariable var = (PatternVariable)this.variables.get(name);
    if (var == null) {
      error("unknown pattern variable", name);
    }
    return ValidatedPatternNode.makePatternVariable(newNodeId(), var);
  }
  
  public ValidatedPatternNode visitRepetition(PatternNode element, PatternRepetition repetition)
  {
    ValidatedPatternNode n = element.validate(this);
    return ValidatedPatternNode.makePatternRepetition(newNodeId(), n, repetition);
  }
  
  public ValidatedPatternNode visitComplexNode(ComplexNodeType type, List<PatternNode> elements)
  {
    List<ValidatedPatternNode> vnodes = new ArrayList();
    for (PatternNode n : elements)
    {
      ValidatedPatternNode vn = n.validate(this);
      vnodes.add(vn);
    }
    switch (type)
    {
    case ALTERNATION: 
      return ValidatedPatternNode.makeAlternation(newNodeId(), vnodes);
    case COMBINATION: 
      List<ValidatedPatternNode> variants = new ArrayList();
      for (List<ValidatedPatternNode> var : Permute.makePermutations(vnodes)) {
        variants.add(ValidatedPatternNode.makeAlternation(newNodeId(), var));
      }
      return ValidatedPatternNode.makeAlternation(newNodeId(), variants);
    case SEQUENCE: 
      return ValidatedPatternNode.makeSequence(newNodeId(), vnodes);
    }
    throw new IllegalArgumentException();
  }
  
  private void validateDefitions(List<PatternDefinition> definitions)
  {
    for (PatternDefinition def : definitions)
    {
      String name = def.varName;
      String sname = def.streamName;
      Predicate pred = def.predicate;
      if (this.variables.containsKey(name)) {
        error("pattern variable duplication", name);
      }
      PatternVariable var = null;
      if (sname == null)
      {
        var = PatternVariable.makeCondition(newVarId(), name, pred);
      }
      else
      {
        DataSet ds = resolveDataSource(sname);
        if (ds != null)
        {
          var = PatternVariable.makeVar(newVarId(), name, pred, sname, ds);
        }
        else
        {
          TimerFunc f = resolveTimerFunc(sname);
          if (f != null) {
            var = PatternVariable.makeTimerFunc(newVarId(), name, pred, sname, f);
          } else {
            error("unknown data source", sname);
          }
        }
      }
      this.variables.put(name, var);
    }
    for (PatternVariable var : this.variables.values()) {
      var.validate(this);
    }
  }
  
  private ValueExpr validateValueExpr(ValueExpr e)
  {
    ExprValidator ctx = new ExprValidator(this.compiler, this.parameters, false, false)
    {
      public DataSet getDataSet(String name)
      {
        return MatchValidator.this.dataSets.get(name);
      }
      
      public FieldRef findField(String name)
      {
        try
        {
          return MatchValidator.this.dataSets.findField(name);
        }
        catch (AmbiguousFieldNameException e)
        {
          error("ambigious field name reference", name);
        }
        return null;
      }
      
      public Expr resolveAlias(String name)
      {
        return null;
      }
    };
    return (ValueExpr)new ExprValidationVisitor(ctx).validate(e);
  }
  
  private void validatePartitionBy(List<ValueExpr> partitionkey, List<DataSource> from)
  {
    if ((partitionkey != null) && (!partitionkey.isEmpty()))
    {
      if (from.size() > 1) {
        error("partitioning can be used only with one data source", from);
      }
      for (ValueExpr e : partitionkey)
      {
        ValueExpr enew = validateValueExpr(e);
        this.partKey.add(enew);
      }
    }
  }
  
  private DataSet resolveDataSource(String name)
  {
    return this.dataSets.get(name);
  }
  
  private TimerFunc resolveTimerFunc(String name)
  {
    for (TimerFunc f : ) {
      if (f.getFuncName().equalsIgnoreCase(name)) {
        return f;
      }
    }
    return null;
  }
  
  private Expr validateVarExpr(Expr e, final DataSet ds)
  {
    ExprValidator ctx = new ExprValidator(this.compiler, this.parameters, true, false)
    {
      public DataSet getDataSet(String name)
      {
        return null;
      }
      
      public FieldRef findField(String name)
      {
        if (ds == null) {
          return null;
        }
        try
        {
          return ds.makeFieldRef(ds.makeRef(), name);
        }
        catch (NoSuchFieldException|SecurityException e) {}
        return null;
      }
      
      public Expr resolveAlias(String name)
      {
        PatternVariable var = (PatternVariable)MatchValidator.this.variables.get(name);
        if (var == null) {
          return null;
        }
        return new PatternVarRef(var);
      }
      
      public DataSet getDefaultDataSet()
      {
        return ds;
      }
    };
    return new ExprValidationVisitor(ctx).validate(e);
  }
  
  public Predicate validateVarPredicate(Predicate pred, DataSet ds)
  {
    return (Predicate)validateVarExpr(pred, ds);
  }
  
  public Predicate validateCondtion(Predicate pred)
  {
    return validateVarPredicate(pred, null);
  }
  
  private ValueExpr pred2val(Predicate pred, ExprCmd what)
  {
    if ((pred instanceof ComparePredicate))
    {
      ComparePredicate c = (ComparePredicate)pred;
      if (c.op == ExprCmd.BOOLEXPR)
      {
        ValueExpr e = (ValueExpr)c.args.get(0);
        if (e.op == what) {
          return e;
        }
      }
    }
    return null;
  }
  
  public long validateCreateTimer(Predicate pred)
  {
    Constant c = (Constant)pred2val(pred, ExprCmd.INTERVAL);
    if (c == null) {
      error("should be interval literal", pred);
    }
    return ((Long)c.value).longValue();
  }
  
  private int validateTimerPredicate(Predicate pred)
  {
    ObjectRef o = (ObjectRef)pred2val(pred, ExprCmd.VAR);
    PatternVariable var = null;
    if ((o == null) || ((var = (PatternVariable)this.variables.get(o.name)) == null) || (!var.isTimer())) {
      error("should be name of timer variable", pred);
    }
    return var.getId();
  }
  
  public int validateStopTimer(Predicate pred)
  {
    return validateTimerPredicate(pred);
  }
  
  public int validateWaitTimer(Predicate pred)
  {
    return validateTimerPredicate(pred);
  }
  
  public ValidatedPatternNode getPattern()
  {
    return this.pattern;
  }
  
  public List<ValueExpr> getPartitionKey()
  {
    return this.partKey;
  }
  
  public Collection<PatternVariable> getVariables()
  {
    return this.variables.values();
  }
  
  public List<SelectCompiler.Target> getTargets()
  {
    return this.targets;
  }
  
  public void analyze()
  {
    FindCommonSubexpression pfcs = makeCommonSubexprRewriter();
    ListIterator<ValueExpr> it = this.partKey.listIterator();
    while (it.hasNext())
    {
      ValueExpr e = (ValueExpr)it.next();
      ValueExpr enew = (ValueExpr)pfcs.rewrite(e);
      if (enew != null) {
        it.set(enew);
      }
    }
    FindCommonSubexpression tfcs = makeCommonSubexprRewriter();
    for (SelectCompiler.Target t : this.targets)
    {
      ValueExpr enew = (ValueExpr)tfcs.rewrite(t.expr);
      if (CompilerUtils.isParam(enew.getType())) {
        enew = cast(enew, String.class);
      }
      t.expr = enew;
    }
    for (PatternVariable var : this.variables.values()) {
      var.analyze(this);
    }
  }
  
  private ValueExpr cast(ValueExpr e, Class<?> targetType)
  {
    return CastExprFactory.cast(this.compiler, e, targetType);
  }
  
  private FindCommonSubexpression makeCommonSubexprRewriter()
  {
    FindCommonSubexpression.Rewriter rewriter = new FindCommonSubexpression.Rewriter()
    {
      Map<Expr, Expr> exprIndex = Factory.makeLinkedMap();
      Map<Integer, Expr> eliminated = Factory.makeLinkedMap();
      List<FuncCall> aggregatedExprs = new ArrayList();
      
      public Expr rewriteExpr(Expr e)
      {
        Expr commonSubExpr = (Expr)this.exprIndex.get(e);
        if (commonSubExpr == null)
        {
          this.exprIndex.put(e, e);
          return e;
        }
        if (!this.eliminated.containsKey(Integer.valueOf(e.eid))) {
          this.eliminated.put(Integer.valueOf(e.eid), e);
        }
        return commonSubExpr;
      }
      
      public void addAggrExpr(FuncCall aggFuncCall)
      {
        if (!this.aggregatedExprs.contains(aggFuncCall))
        {
          int index = this.aggregatedExprs.size();
          this.aggregatedExprs.add(aggFuncCall);
          aggFuncCall.setIndex(index);
        }
      }
    };
    new FindCommonSubexpression(rewriter)
    {
      public Expr visitPatternVarRef(PatternVarRef e, Object params)
      {
        if (isInAggFunc()) {
          e.setIndex(-2);
        }
        return e;
      }
    };
  }
  
  public Predicate analyzePredicate(Predicate p)
  {
    FindCommonSubexpression fcs = makeCommonSubexprRewriter();
    Predicate ret = (Predicate)fcs.rewrite(p);
    return ret;
  }
}
