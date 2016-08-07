package com.bloom.runtime.compiler.select;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.proc.events.DynamicEvent;
import com.bloom.runtime.Context;
import com.bloom.runtime.Pair;
import com.bloom.runtime.TraceOptions;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.CompilerUtils;
import com.bloom.runtime.compiler.TypeName;
import com.bloom.runtime.compiler.exprs.CastExprFactory;
import com.bloom.runtime.compiler.exprs.Constant;
import com.bloom.runtime.compiler.exprs.Expr;
import com.bloom.runtime.compiler.exprs.ExprCmd;
import com.bloom.runtime.compiler.exprs.FieldRef;
import com.bloom.runtime.compiler.exprs.FuncCall;
import com.bloom.runtime.compiler.exprs.LogicalPredicate;
import com.bloom.runtime.compiler.exprs.ParamRef;
import com.bloom.runtime.compiler.exprs.Predicate;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.exprs.WildCardExpr;
import com.bloom.runtime.compiler.patternmatch.MatchGenerator;
import com.bloom.runtime.compiler.patternmatch.MatchValidator;
import com.bloom.runtime.compiler.stmts.OrderByItem;
import com.bloom.runtime.compiler.stmts.Select;
import com.bloom.runtime.compiler.stmts.SelectTarget;
import com.bloom.runtime.compiler.visitors.ConditionAnalyzer;
import com.bloom.runtime.compiler.visitors.ExprValidationVisitor;
import com.bloom.runtime.compiler.visitors.FindCommonSubexpression;
import com.bloom.runtime.compiler.visitors.FindCommonSubexpression.Rewriter;
import com.bloom.runtime.containers.DynamicEventWrapper;
import com.bloom.runtime.exceptions.AmbiguousFieldNameException;
import com.bloom.runtime.meta.CQExecutionPlan;
import com.bloom.runtime.meta.MetaInfo.WAStoreView;
import com.bloom.runtime.meta.MetaInfo.WActionStore;
import com.bloom.runtime.utils.Factory;
import com.bloom.runtime.utils.NamePolicy;
import com.bloom.uuid.UUID;
import com.bloom.wactionstore.WActionStoreManager;
import com.bloom.wactionstore.WActionStores;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

public class SelectCompiler
{
  public static class Target
  {
    public ValueExpr expr;
    public String alias;
    public Field targetField;
    
    public Target(ValueExpr expr, String alias)
    {
      assert (alias != null);
      this.expr = expr;
      this.alias = alias;
      this.targetField = null;
    }
    
    public String toString()
    {
      return "Target(" + this.expr + " as " + this.alias + "->" + this.targetField + ")";
    }
  }
  
  private DataSets dataSets = new DataSets();
  private int nextDataSetID = 0;
  private int nextIndexID = 0;
  private final List<Condition.CondExpr> constantConditions = new ArrayList();
  private final Map<Condition.CondKey, Condition> joinConditions = Factory.makeMap();
  private final Map<Condition.CondKey, Condition> afterJoinFilterConditions = Factory.makeMap();
  private final Map<DataSet, Condition> filterConditions = Factory.makeMap();
  private final Map<DataSet, List<ValueExpr>> index0key = Factory.makeMap();
  private final Map<DataSet, List<JoinDesc>> joinPlans = Factory.makeMap();
  private final Map<DataSet, List<IndexDesc>> indexes = Factory.makeMap();
  private final Map<Expr, Expr> exprIndex = Factory.makeLinkedMap();
  private final List<Predicate> predicates = new ArrayList();
  private final List<ValueExpr> groupBy = new ArrayList();
  private final List<OrderByItem> orderBy = new ArrayList();
  private final List<FuncCall> aggregatedExprs = new ArrayList();
  private final List<Target> targets = new ArrayList();
  private Predicate having = null;
  private boolean haveAggFuncs = false;
  private boolean targetIsObj = false;
  private final Map<Integer, Expr> eliminated = Factory.makeLinkedMap();
  private final List<UUID> recordedTypes = new ArrayList();
  private final Map<String, Expr> alias2expr = Factory.makeMap();
  private Join.Tree joinTree = null;
  private final List<Condition.CondExpr> condIndex = new ArrayList();
  private final Map<String, ParamRef> parameters = Factory.makeNameLinkedMap();
  private final String cqName;
  private final String cqNamespace;
  private final Compiler compiler;
  private final Select select;
  private final Class<?> targetType;
  private final List<Field> targetFields;
  private final Map<String, Integer> targetFieldIndices;
  private final TraceOptions traceOptions;
  private final boolean isAdhoc;
  private MatchValidator matchValidator;
  
  public static CQExecutionPlan compileSelect(String cqName, String namespaceName, Compiler compiler, Select select, TraceOptions traceOptions, boolean isAdhoc)
    throws Exception
  {
    return compileSelectInto(cqName, namespaceName, compiler, select, traceOptions, null, null, null, null, isAdhoc);
  }
  
  public static CQExecutionPlan compileSelect(String cqName, String namespaceName, Compiler compiler, Select select, TraceOptions traceOptions)
    throws Exception
  {
    return compileSelectInto(cqName, namespaceName, compiler, select, traceOptions, null, null, null, null, false);
  }
  
  public static CQExecutionPlan compileSelectInto(String cqName, String namespaceName, Compiler compiler, Select select, TraceOptions traceOptions, Class<?> targetType, List<Field> targetFields, Map<String, Integer> targetFieldIndices, UUID outputTypeId, boolean isAdhoc)
    throws Exception
  {
    SelectCompiler c = new SelectCompiler(cqName, namespaceName, compiler, select, targetType, targetFields, targetFieldIndices, traceOptions, isAdhoc);
    if (outputTypeId != null) {
      c.addTypeID(outputTypeId);
    }
    return c.compile();
  }
  
  private SelectCompiler(String cqName, String namespaceName, Compiler compiler, Select select, Class<?> targetType, List<Field> targetFields, Map<String, Integer> targetFieldIndices, TraceOptions traceOptions, boolean isAdhoc)
  {
    this.cqName = cqName;
    this.cqNamespace = namespaceName;
    this.compiler = compiler;
    this.targetType = targetType;
    this.targetFields = targetFields;
    this.targetFieldIndices = targetFieldIndices;
    this.select = select;
    this.isAdhoc = isAdhoc;
    this.traceOptions = traceOptions;
  }
  
  private CQExecutionPlan compile()
    throws Exception
  {
    validate();
    if (this.compiler.getContext().isReadOnly()) {
      return null;
    }
    analyze();
    Generator gen;
    Generator gen;
    if (!haveMatch()) {
      gen = new SelectGenerator(this.compiler, this.cqName, this.cqNamespace, this.select, this.traceOptions, this.dataSets, this.parameters, this.indexes, this.targets, this.targetType, this.targetIsObj, this.targetFieldIndices, this.recordedTypes, this.aggregatedExprs, this.constantConditions, this.filterConditions, this.index0key, this.joinPlans, this.groupBy, this.orderBy, this.having);
    } else {
      gen = new MatchGenerator(this.compiler, this.cqName, this.select, this.traceOptions, this.dataSets, this.parameters, this.matchValidator.getTargets(), this.targetType, this.targetIsObj, this.targetFieldIndices, this.recordedTypes, this.matchValidator.getVariables(), this.matchValidator.getPartitionKey(), this.matchValidator.getPattern());
    }
    CQExecutionPlan plan = gen.generate();
    if ((this.traceOptions.traceFlags & 0x8) > 0)
    {
      PrintStream out = TraceOptions.getTraceStream(this.traceOptions);
      dump(out);
    }
    return plan;
  }
  
  private void error(String message, Object info)
  {
    this.compiler.error(message, info);
  }
  
  private void addTypeID(UUID type)
  {
    this.recordedTypes.add(type);
  }
  
  private ValueExpr cast(ValueExpr e, Class<?> targetType)
  {
    return CastExprFactory.cast(this.compiler, e, targetType);
  }
  
  private DataSet getDataSet(int id)
  {
    return this.dataSets.get(id);
  }
  
  private boolean haveGroupBy()
  {
    return !this.groupBy.isEmpty();
  }
  
  private boolean haveMatch()
  {
    return this.select.match != null;
  }
  
  private void validate()
    throws MetaDataRepositoryException
  {
    validateFrom();
    validateMatch();
    validateWhere();
    validateTargets();
    validateGroupBy();
    validateHaving();
    validateTargetStreamFields();
    validateOrderBy();
  }
  
  private void analyze()
    throws MetaDataRepositoryException
  {
    analyzeMatch();
    analyzePredicates();
    buildJoinPlan();
    analyzeGropyBy();
    analyzeHavingAndTargets();
    analyzeOrderBy();
    analyzeIndexedCondtions();
    pushDownFilters();
    analyzeWAStoreQuery();
  }
  
  private void analyzeMatch()
  {
    if (haveMatch()) {
      this.matchValidator.analyze();
    }
  }
  
  private void attachJsonQueryToWASView(DataSet ds, JsonNode query)
    throws MetaDataRepositoryException
  {
    MetaInfo.WAStoreView w = ds.isJsonWAStore().getStoreView();
    
    byte[] wasquery = AST2JSON.serialize(query);
    w.setQuery(wasquery);
    Context ctx = this.compiler.getContext();
    ctx.updateWAStoreView(w);
  }
  
  private void pushDownFilters()
    throws MetaDataRepositoryException
  {
    for (DataSet ds : this.dataSets)
    {
      JsonWAStoreInfo eds = ds.isJsonWAStore();
      if (eds != null)
      {
        Condition filter = (Condition)this.filterConditions.get(ds);
        WActionStoreManager m = WActionStores.getInstance(eds.getStore().properties);
        JsonNode query = new AST2JSON(m.getCapabilities()).transformFilter(ds, filter);
        attachJsonQueryToWASView(ds, query);
      }
    }
  }
  
  private void analyzeWAStoreQuery()
    throws MetaDataRepositoryException
  {
    if ((haveGroupBy()) && (!this.haveAggFuncs)) {
      return;
    }
    if (this.dataSets.size() != 1) {
      return;
    }
    DataSet ds = this.dataSets.get(0);
    JsonWAStoreInfo eds = ds.isJsonWAStore();
    if (eds == null) {
      return;
    }
    MetaInfo.WAStoreView w = eds.getStoreView();
    if (w.subscribeToUpdates) {
      return;
    }
    WActionStoreManager m = WActionStores.getInstance(eds.getStore().properties);
    JsonNode query = new AST2JSON(m.getCapabilities()).transform(this.dataSets, this.predicates, this.targets, this.groupBy, this.having, this.orderBy);
    if (query == null) {
      return;
    }
    attachJsonQueryToWASView(ds, query);
    this.predicates.clear();
    this.groupBy.clear();
    this.orderBy.clear();
    this.aggregatedExprs.clear();
    this.having = null;
    this.haveAggFuncs = false;
    List<RSFieldDesc> fields = new ArrayList();
    for (Target t : this.targets)
    {
      String name = null;
      Expr e = AST2JSON.skipCastOps(t.expr);
      if ((e instanceof FieldRef))
      {
        name = ((FieldRef)e).getName();
      }
      else if ((e instanceof FuncCall))
      {
        FuncCall c = (FuncCall)e;
        String funcName = c.getName().toLowerCase();
        if (c.args.size() == 0)
        {
          name = "$id:" + funcName;
        }
        else
        {
          Expr e2 = AST2JSON.skipCastOps((Expr)c.args.get(0));
          name = ((FieldRef)e2).getName() + ":" + funcName;
        }
      }
      else
      {
        if (!$assertionsDisabled) {
          throw new AssertionError();
        }
        throw new RuntimeException("internla error: cannot transform elasticsearch query");
      }
      fields.add(new RSFieldDesc(name, t.expr.getType()));
    }
    DataSet newds = DataSetFactory.createJSONWAStoreDS(ds.getID(), ds.getName(), null, fields, eds.getStoreView(), eds.getStore());
    
    List<FieldRef> flds = newds.makeListOfAllFields();
    assert (this.targets.size() == flds.size());
    int i = 0;
    for (Target t : this.targets) {
      t.expr = ((ValueExpr)flds.get(i++));
    }
    this.dataSets = new DataSets();
    this.dataSets.add(newds);
  }
  
  private void dump(PrintStream out)
  {
    dumpDataSets(out);
    dumpConditions(out);
    dumpIndexes(out);
    dumpJoinPlanEveryDataSet(out);
  }
  
  private DataSource.Resolver makeResolver()
  {
    new DataSource.Resolver()
    {
      public Class<?> getTypeInfo(TypeName typeName)
      {
        return SelectCompiler.this.compiler.getClass(typeName);
      }
      
      public TraceOptions getTraceOptions()
      {
        return SelectCompiler.this.traceOptions;
      }
      
      public int getNextDataSetID()
      {
        return SelectCompiler.access$208(SelectCompiler.this);
      }
      
      public Compiler getCompiler()
      {
        return SelectCompiler.this.compiler;
      }
      
      public void error(String message, Object info)
      {
        SelectCompiler.this.compiler.error(message, info);
      }
      
      public void addTypeID(UUID dataType)
      {
        SelectCompiler.this.recordedTypes.add(dataType);
      }
      
      public void addPredicate(Predicate p)
      {
        SelectCompiler.this.predicates.add(p);
      }
      
      public Join.Node addDataSourceJoin(DataSourceJoin dsJoin)
      {
        if ((dsJoin.joinCondition == null) && (dsJoin.kindOfJoin.isOuter())) {
          error("outer join requires join condition", dsJoin);
        }
        DataSets tmpdatasets = SelectCompiler.this.dataSets;
        SelectCompiler.this.dataSets = new DataSets();
        
        Join.Node nleft = dsJoin.left.addDataSource(this);
        Join.Node nright = dsJoin.right.addDataSource(this);
        Predicate jcond;
        Predicate jcond;
        if (dsJoin.joinCondition != null) {
          jcond = SelectCompiler.this.validatePredicate(dsJoin.joinCondition, false);
        } else {
          jcond = null;
        }
        for (DataSet ds : SelectCompiler.this.dataSets) {
          if (!tmpdatasets.add(ds)) {
            error("datasource name duplication", ds.getName());
          }
        }
        SelectCompiler.this.dataSets = tmpdatasets;
        return Join.createJoinNode(nleft, nright, jcond, dsJoin.kindOfJoin);
      }
      
      public Join.Node addDataSet(DataSet ds)
      {
        if (!SelectCompiler.this.dataSets.add(ds)) {
          error("datasource name duplication", ds.getName());
        }
        return Join.createDataSetNode(ds);
      }
      
      public boolean isAdhoc()
      {
        return SelectCompiler.this.isAdhoc;
      }
    };
  }
  
  private void validateFrom()
    throws MetaDataRepositoryException
  {
    DataSource.Resolver r = makeResolver();
    Map<String, DataSet> resolved = NamePolicy.makeNameMap();
    List<DataSourcePrimary> unresolved = new ArrayList();
    for (DataSource ds : this.select.from) {
      ds.resolveDataSource(r, resolved, unresolved, true);
    }
    while (!unresolved.isEmpty())
    {
      List<DataSourcePrimary> leftunresolved = new ArrayList();
      for (DataSourcePrimary pds : unresolved) {
        pds.resolveDataSource(r, resolved, leftunresolved, false);
      }
      if (leftunresolved.size() == unresolved.size()) {
        error("cannot resolved iterator", unresolved.get(0));
      }
      unresolved = leftunresolved;
    }
    Join.Node root = null;
    for (DataSource ds : this.select.from)
    {
      Join.Node n = ds.addDataSource(r);
      if (root == null) {
        root = n;
      } else {
        root = Join.createJoinNode(root, n, null, Join.Kind.CROSS);
      }
    }
    this.joinTree = Join.createJoinTree(root);
  }
  
  private void validateMatch()
  {
    if (haveMatch())
    {
      this.matchValidator = new MatchValidator(this.compiler, this.parameters, this.dataSets);
      this.matchValidator.validate(this.select.match, this.select.from, this.select.targets);
    }
  }
  
  private void validateWhere()
  {
    if (this.select.where != null) {
      validatePredicate(this.select.where, false);
    }
  }
  
  private ValueExpr checkExprIsIntConst(ValueExpr e)
  {
    if (e.op == ExprCmd.INT)
    {
      Constant c = (Constant)e;
      int index = ((Integer)c.value).intValue();
      if ((index >= 0) && (index <= this.select.targets.size()))
      {
        SelectTarget t = (SelectTarget)this.select.targets.get(index - 1);
        if (t.expr != null) {
          return t.expr;
        }
        error("cannot refer expression with index " + index, e);
      }
      error("cannot find expression with index " + index, e);
    }
    return e;
  }
  
  private void validateGroupBy()
  {
    if (this.select.groupBy != null) {
      for (ValueExpr e : this.select.groupBy)
      {
        ValueExpr ee = checkExprIsIntConst(e);
        ValueExpr enew = validateValueExpr(ee, false);
        checkComparable(enew);
        this.groupBy.add(enew);
      }
    }
  }
  
  private void validateOrderBy()
  {
    if (this.select.orderBy != null) {
      for (OrderByItem e : this.select.orderBy)
      {
        ValueExpr ee = checkExprIsIntConst(e.expr);
        ValueExpr enew = validateValueExpr(ee, true);
        checkComparable(enew);
        this.orderBy.add(new OrderByItem(enew, e.isAscending));
      }
    }
  }
  
  private void validateHaving()
  {
    if (this.select.having != null) {
      this.having = validatePredicate(this.select.having, true);
    }
  }
  
  private void validateTargets()
  {
    if (haveMatch()) {
      return;
    }
    for (SelectTarget t : this.select.targets) {
      if (t.expr != null)
      {
        ValueExpr e = validateValueExpr(t.expr, true, true);
        Iterator<String> aliases;
        if ((e instanceof WildCardExpr))
        {
          WildCardExpr wc = (WildCardExpr)e;
          aliases = wc.getAliases().iterator();
          for (ValueExpr ve : wc.getExprs())
          {
            String alias = (String)aliases.next();
            Target newt = new Target(ve, alias);
            this.targets.add(newt);
            
            this.alias2expr.put(alias, ve);
          }
        }
        else
        {
          Target newt = new Target(e, t.alias);
          this.targets.add(newt);
          
          this.alias2expr.put(t.alias, e);
        }
      }
      else
      {
        List<FieldRef> allFields = makeListOfAllFields();
        for (FieldRef r : allFields)
        {
          String alias = r.getName();
          Target newt = new Target(r, alias);
          this.targets.add(newt);
        }
      }
    }
  }
  
  private ValueExpr validateTargetType(ValueExpr e, Field target)
  {
    assert (e != null);
    Class<?> targetType = target.getType();
    Class<?> sourceType = e.getType();
    if (!CompilerUtils.isCastable(targetType, sourceType))
    {
      Object o = this.compiler.getExprText(e) == null ? null : e;
      error("incompatible types of field <" + target.getName() + "(" + targetType.getSimpleName() + ")> and SELECT expression (" + sourceType.getSimpleName() + ")", o);
    }
    return cast(e, targetType);
  }
  
  private void validateTargetStreamFields()
  {
    List<Target> selTargets = haveMatch() ? this.matchValidator.getTargets() : this.targets;
    
    List<SelectTarget> selectTargetsSrc = this.select.targets;
    if (this.targetFields == null) {
      return;
    }
    if (this.targetFields.isEmpty())
    {
      if (selTargets.size() == 1)
      {
        ValueExpr e = ((Target)selTargets.get(0)).expr;
        assert (e != null);
        Class<?> etype = e.getType();
        if (etype.equals(DynamicEventWrapper.class)) {
          etype = DynamicEvent.class;
        }
        if (this.targetType.isAssignableFrom(etype))
        {
          this.targetIsObj = true;
          return;
        }
      }
      List<Field> nsfields = CompilerUtils.getNonStaticFields(this.targetType);
      List<Field> fields = CompilerUtils.getNotSpecial(nsfields);
      int fieldsSize = fields.size();
      int targetsSize = selTargets.size();
      if (fieldsSize != targetsSize) {
        error("the number of SELECT expressions (" + targetsSize + ") not equal to the number of fields in output stream (" + fieldsSize + ")", selectTargetsSrc);
      }
      for (int i = 0; i < selTargets.size(); i++)
      {
        Target t = (Target)selTargets.get(i);
        t.targetField = ((Field)fields.get(i));
        t.expr = validateTargetType(t.expr, t.targetField);
      }
    }
    else
    {
      int fieldsSize = this.targetFields.size();
      int targetsSize = selTargets.size();
      if (fieldsSize != targetsSize) {
        error("different number of output fields (" + fieldsSize + ") and SELECT expressions (" + targetsSize + ")", selectTargetsSrc);
      }
      for (int i = 0; i < selTargets.size(); i++)
      {
        Target t = (Target)selTargets.get(i);
        t.targetField = ((Field)this.targetFields.get(i));
        t.expr = validateTargetType(t.expr, t.targetField);
      }
    }
  }
  
  private ValueExpr validateValueExpr(ValueExpr e, boolean canHaveAggFunc)
  {
    return validateValueExpr(e, canHaveAggFunc, false);
  }
  
  private Expr validateExpr(Expr e, boolean canHaveAggFunc, boolean acceptWildcard)
  {
    ExprValidator ctx = new ExprValidator(this.compiler, this.parameters, canHaveAggFunc, acceptWildcard)
    {
      public void setHaveAggFuncs()
      {
        SelectCompiler.this.haveAggFuncs = true;
      }
      
      public DataSet getDataSet(String name)
      {
        return SelectCompiler.this.dataSets.get(name);
      }
      
      public FieldRef findField(String name)
      {
        try
        {
          return SelectCompiler.this.dataSets.findField(name);
        }
        catch (AmbiguousFieldNameException e)
        {
          error("ambigious field name reference", name);
        }
        return null;
      }
      
      public Expr resolveAlias(String alias)
      {
        return (Expr)SelectCompiler.this.alias2expr.get(alias);
      }
    };
    return new ExprValidationVisitor(ctx).validate(e);
  }
  
  private ValueExpr validateValueExpr(ValueExpr e, boolean canHaveAggFunc, boolean acceptWildcard)
  {
    return (ValueExpr)validateExpr(e, canHaveAggFunc, acceptWildcard);
  }
  
  private Predicate validatePredicate(Predicate e, boolean canHaveAggFunc)
  {
    Predicate p = (Predicate)validateExpr(e, canHaveAggFunc, false);
    if (!canHaveAggFunc) {
      this.predicates.add(p);
    }
    return p;
  }
  
  private void addCondExpr(Condition.CondExpr e, Join.JoinNode jn)
  {
    this.condIndex.add(e);
  }
  
  private void addCondExpr(Map<Condition.CondKey, Condition> map, Condition.CondExpr e)
  {
    Condition.CondKey key = e.getCondKey();
    if (map.containsKey(key))
    {
      ((Condition)map.get(key)).exprs.add(e);
    }
    else
    {
      Condition cond = new Condition(e);
      map.put(key, cond);
    }
  }
  
  private void addFilter(Map<DataSet, Condition> map, Condition.CondExpr e)
  {
    BitSet datasets = e.datasets;
    assert (datasets.cardinality() == 1);
    int datasetID = datasets.nextSetBit(0);
    DataSet ds = getDataSet(datasetID);
    if (map.containsKey(ds))
    {
      ((Condition)map.get(ds)).exprs.add(e);
    }
    else
    {
      Condition cond = new Condition(e);
      map.put(ds, cond);
    }
  }
  
  private void analyzeAndPredicate(Predicate e, Join.JoinNode jn)
  {
    if (((e instanceof LogicalPredicate)) && (e.op == ExprCmd.AND))
    {
      LogicalPredicate lp = (LogicalPredicate)e;
      for (Predicate p : lp.args) {
        analyzeAndPredicate(p, jn);
      }
    }
    else
    {
      analyzePredicate(e, jn);
    }
  }
  
  private void analyzePredicates()
  {
    if ((this.having != null) && (this.select.groupBy == null) && (!this.haveAggFuncs))
    {
      this.predicates.add(this.having);
      this.having = null;
    }
    for (Predicate e : this.predicates)
    {
      Join.JoinNode jn = this.joinTree.getJoinPredicate(e);
      analyzeAndPredicate(e, jn);
    }
  }
  
  private void analyzePredicate(Predicate p, Join.JoinNode jn)
  {
    FindCommonSubexpression fcs = makeCommonSubexprRewriter();
    p = (Predicate)fcs.rewrite(p);
    ConditionAnalyzer ca = new ConditionAnalyzer(p, jn);
    Condition.CondExpr e = ca.getCondExpr();
    addCondExpr(e, jn);
    switch (e.datasets.cardinality())
    {
    case 0: 
      this.constantConditions.add(e);
      break;
    case 1: 
      addFilter(this.filterConditions, e);
      break;
    default: 
      if ((e instanceof Condition.JoinExpr)) {
        addCondExpr(this.joinConditions, e);
      } else {
        addCondExpr(this.afterJoinFilterConditions, e);
      }
      break;
    }
  }
  
  private static void dumpList(PrintStream out, String what, Collection<?> l)
  {
    if (!l.isEmpty())
    {
      out.println("------------------------------------------");
      out.println(what);
      for (Object o : l) {
        out.println("\t" + o);
      }
    }
  }
  
  private void dumpConditions(PrintStream out)
  {
    dumpList(out, "list of constant conditions", this.constantConditions);
    
    dumpList(out, "list of filter conditions", this.filterConditions.values());
    
    dumpList(out, "list of join conditions", this.joinConditions.values());
    
    dumpList(out, "list of after join filter conditions", this.afterJoinFilterConditions.values());
    
    dumpList(out, "list of before agg expressions", this.exprIndex.values());
    
    dumpList(out, "list of eliminated expressions", this.eliminated.values());
  }
  
  private void dumpDataSets(PrintStream out)
  {
    out.println("list of datasets");
    for (DataSet ds : this.dataSets) {
      out.println(ds);
    }
  }
  
  private void dumpIndexes(PrintStream out)
  {
    out.println("\nList of indexes for every dataset\n");
    for (Map.Entry<DataSet, List<IndexDesc>> e : this.indexes.entrySet())
    {
      DataSet ds = (DataSet)e.getKey();
      out.println("dataset: " + ds.getName() + " {" + ds.getID() + "}");
      for (IndexDesc id : (List)e.getValue()) {
        out.println("-- index: " + id);
      }
    }
  }
  
  private void dumpJoinPlanEveryDataSet(PrintStream out)
  {
    out.println("\nList of join plans for every dataset\n");
    for (Map.Entry<DataSet, List<JoinDesc>> e : this.joinPlans.entrySet())
    {
      DataSet ds = (DataSet)e.getKey();
      out.println("-----------------------------------------------------");
      out.println("dataset: " + ds.getName() + " {" + ds.getID() + "}");
      out.println("------- join plan -----------------------------------");
      for (JoinDesc jd : (List)e.getValue()) {
        out.println(jd);
      }
    }
  }
  
  private void analyzeIndexedCondtions()
  {
    for (Map.Entry<DataSet, Condition> e : this.filterConditions.entrySet())
    {
      DataSet ds = (DataSet)e.getKey();
      Condition cond = (Condition)e.getValue();
      List<Pair<FieldRef, ValueExpr>> l = cond.getEqExprs();
      Set<String> names = new TreeSet(String.CASE_INSENSITIVE_ORDER);
      for (Pair<FieldRef, ValueExpr> p : l)
      {
        String fieldName = ((FieldRef)p.first).getName();
        names.add(fieldName);
      }
      List<Integer> lst = ds.getListOfIndexesForFields(names);
      if (!lst.isEmpty())
      {
        int indexID = ((Integer)lst.get(0)).intValue();
        List<String> indexedFields = ds.indexFieldList(indexID);
        List<ValueExpr> key = new ArrayList();
        for (Iterator i$ = indexedFields.iterator(); i$.hasNext();)
        {
          indexField = (String)i$.next();
          for (Pair<FieldRef, ValueExpr> p : l)
          {
            String fieldName = ((FieldRef)p.first).getName();
            if (fieldName.equalsIgnoreCase(indexField)) {
              key.add(p.second);
            }
          }
        }
        String indexField;
        this.index0key.put(ds, key);
      }
    }
  }
  
  private void buildJoinPlan()
  {
    assert (this.dataSets.size() > 0);
    if (this.dataSets.size() == 1)
    {
      assert (this.joinConditions.isEmpty());
      if ((!$assertionsDisabled) && (!this.afterJoinFilterConditions.isEmpty())) {
        throw new AssertionError();
      }
    }
    else
    {
      List<Condition> joinConds = new ArrayList(this.joinConditions.values());
      for (Iterator i$ = joinConds.iterator(); i$.hasNext();)
      {
        c = (Condition)i$.next();
        Map<Integer, Set<String>> flds = c.getCondFields();
        for (Map.Entry<Integer, Set<String>> e : flds.entrySet())
        {
          int dsID = ((Integer)e.getKey()).intValue();
          Set<String> set = (Set)e.getValue();
          DataSet ds = getDataSet(((Integer)e.getKey()).intValue());
          List<Integer> lst = ds.getListOfIndexesForFields(set);
          if (!lst.isEmpty()) {
            c.updateFieldIndexesSupport(dsID, lst);
          }
        }
      }
      Condition c;
      Collections.sort(joinConds);
      joinConds = Collections.unmodifiableList(joinConds);
      List<Condition> afterJoinConds = new ArrayList(this.afterJoinFilterConditions.values());
      
      Collections.sort(afterJoinConds);
      afterJoinConds = Collections.unmodifiableList(afterJoinConds);
      
      BitSet all = new BitSet();
      all.set(0, this.dataSets.size());
      for (int i = all.nextSetBit(0); i >= 0; i = all.nextSetBit(i + 1))
      {
        BitSet bs = (BitSet)all.clone();
        bs.clear(i);
        makeJoinPlan(i, bs, joinConds, afterJoinConds);
      }
    }
  }
  
  private void checkComparable(Expr keyExpr)
  {
    Class<?> t = keyExpr.getType();
    if ((!t.isPrimitive()) && (!Comparable.class.isAssignableFrom(t))) {
      error("non-comparable expression", keyExpr);
    }
  }
  
  private void makeJoinPlan(int firstdsID, BitSet leftToJoin, List<Condition> _joinConds, List<Condition> _afterJoinConds)
  {
    DataSet firstDS = getDataSet(firstdsID);
    
    BitSet alreadyJoined = new BitSet();
    alreadyJoined.set(firstdsID);
    
    BitSet check = (BitSet)alreadyJoined.clone();
    check.or(leftToJoin);
    
    LinkedList<Condition> joinConds = new LinkedList(_joinConds);
    LinkedList<Condition> afterJoinConds = new LinkedList(_afterJoinConds);
    List<JoinDesc> joinPlan = new ArrayList();
    while (!leftToJoin.isEmpty())
    {
      boolean isOuter = false;
      
      Condition joinCond = null;
      List<Expr> searchExprs = Collections.emptyList();
      IndexDesc idx = null;
      List<Predicate> postFilters = Collections.emptyList();
      int windowIndexID = -1;
      
      int datasetID = findNextStreamJoin(leftToJoin);
      DataSet nextDS;
      JoinDesc.JoinAlgorithm algo;
      DataSet nextDS;
      if (datasetID >= 0)
      {
        JoinDesc.JoinAlgorithm algo = JoinDesc.JoinAlgorithm.WITH_STREAM;
        nextDS = getDataSet(datasetID);
      }
      else
      {
        Pair<Integer, Condition> ic = findBestIndexedJoinCondition(alreadyJoined, joinConds);
        if (ic != null)
        {
          JoinDesc.JoinAlgorithm algo = JoinDesc.JoinAlgorithm.WINDOW_INDEX_LOOKUP;
          datasetID = ((Integer)ic.first).intValue();
          DataSet nextDS = getDataSet(datasetID);
          joinCond = (Condition)ic.second;
          windowIndexID = ((Integer)((List)joinCond.dsIndexes.get(Integer.valueOf(datasetID))).get(0)).intValue();
          Pair<List<Expr>, List<Predicate>> searchAndFilter = joinCond.getIndexSearchAndPostCondition(datasetID, nextDS.indexFieldList(windowIndexID));
          
          searchExprs = (List)searchAndFilter.first;
          postFilters = (List)searchAndFilter.second;
        }
        else
        {
          Pair<Integer, Condition> p = null;
          if (p != null)
          {
            JoinDesc.JoinAlgorithm algo = JoinDesc.JoinAlgorithm.INDEX_LOOKUP;
            datasetID = ((Integer)p.first).intValue();
            DataSet nextDS = getDataSet(datasetID);
            joinCond = (Condition)p.second;
            searchExprs = joinCond.getSearchExprs(datasetID);
            List<Expr> indexExprs = joinCond.getIndexExprs(datasetID);
            idx = getIndexDesc(getDataSet(datasetID), indexExprs);
          }
          else
          {
            algo = JoinDesc.JoinAlgorithm.SCAN;
            datasetID = findBestCrossJoinCondition(alreadyJoined, leftToJoin, afterJoinConds);
            nextDS = getDataSet(datasetID);
          }
        }
      }
      isOuter = this.joinTree.isOuterJoin(alreadyJoined, datasetID, this.condIndex);
      alreadyJoined.set(datasetID);
      leftToJoin.clear(datasetID);
      assert (alreadyJoined.cardinality() > 1);
      JoinDesc desc = new JoinDesc(algo, alreadyJoined, nextDS, joinCond, idx, searchExprs, postFilters, windowIndexID, isOuter);
      
      joinPlan.add(desc);
    }
    assert (alreadyJoined.equals(check));
    
    afterJoinConds.addAll(0, joinConds);
    for (Map.Entry<DataSet, Condition> e : this.filterConditions.entrySet()) {
      if (((DataSet)e.getKey()).getID() != firstdsID) {
        afterJoinConds.add(0, e.getValue());
      }
    }
    alreadyJoined.clear();
    alreadyJoined.set(firstdsID);
    for (JoinDesc jd : joinPlan)
    {
      assert (jd.joinSet.cardinality() > 1);
      assert (alreadyJoined.cardinality() + 1 == jd.joinSet.cardinality());
      alreadyJoined.or(jd.joinSet);
      addAfterJoinCondtions(jd, alreadyJoined, afterJoinConds);
    }
    assert (afterJoinConds.isEmpty());
    assert (!this.joinPlans.containsKey(firstDS));
    this.joinPlans.put(firstDS, joinPlan);
  }
  
  private int findNextStreamJoin(BitSet leftToJoin)
  {
    for (int i = leftToJoin.nextSetBit(0); i >= 0; i = leftToJoin.nextSetBit(i + 1))
    {
      DataSet ds = getDataSet(i);
      if (!ds.isStateful()) {
        return i;
      }
    }
    return -1;
  }
  
  private Pair<Integer, Condition> findBestIndexedJoinCondition(BitSet alreadyJoined, LinkedList<Condition> joinConds)
  {
    ListIterator<Condition> it = joinConds.listIterator();
    while (it.hasNext())
    {
      Condition cond = (Condition)it.next();
      if (cond.idxCount > 0)
      {
        BitSet set = (BitSet)cond.dataSets.clone();
        
        set.andNot(alreadyJoined);
        if (set.cardinality() == 1)
        {
          int relID = set.nextSetBit(0);
          List<Integer> indexes = (List)cond.dsIndexes.get(Integer.valueOf(relID));
          if (indexes != null)
          {
            it.remove();
            return Pair.make(Integer.valueOf(relID), cond);
          }
        }
      }
    }
    return null;
  }
  
  private Pair<Integer, Condition> findBestInnerJoinCondition(BitSet alreadyJoined, LinkedList<Condition> joinConds)
  {
    ListIterator<Condition> it = joinConds.listIterator();
    while (it.hasNext())
    {
      Condition cond = (Condition)it.next();
      if (cond.isJoinable())
      {
        BitSet set = (BitSet)cond.dataSets.clone();
        
        set.andNot(alreadyJoined);
        if (set.cardinality() == 1)
        {
          int relID = set.nextSetBit(0);
          if (cond.isIndexable(relID))
          {
            it.remove();
            return Pair.make(Integer.valueOf(relID), cond);
          }
        }
      }
    }
    return null;
  }
  
  private boolean derivedHasParentJoined(int id, BitSet alreadyJoined)
  {
    DataSet ds = getDataSet(id);
    IteratorInfo it = ds.isIterator();
    if (it == null) {
      return true;
    }
    DataSet parent = it.getParent();
    return alreadyJoined.get(parent.getID());
  }
  
  private int findBestCrossJoinCondition(BitSet alreadyJoined, BitSet leftToJoin, List<Condition> afterJoinConds)
  {
    BitSet left = (BitSet)leftToJoin.clone();
    for (Condition cond : afterJoinConds)
    {
      BitSet set = (BitSet)cond.dataSets.clone();
      set.andNot(alreadyJoined);
      if (set.cardinality() == 1)
      {
        int id = set.nextSetBit(0);
        if (derivedHasParentJoined(id, alreadyJoined)) {
          return id;
        }
      }
      left.and(set);
    }
    int id = 0;
    if (!left.isEmpty())
    {
      do
      {
        id = left.nextSetBit(id);
        if (id == -1) {
          break;
        }
      } while (!derivedHasParentJoined(id, alreadyJoined));
      return id;
      
      id = 0;
    }
    do
    {
      id = leftToJoin.nextSetBit(id);
      if (id == -1) {
        break;
      }
    } while (!derivedHasParentJoined(id, alreadyJoined));
    return id;
  }
  
  private IndexDesc getIndexDesc(DataSet ds, List<Expr> indexExprs)
  {
    List<IndexDesc> ilist;
    if (this.indexes.containsKey(ds))
    {
       ilist = (List)this.indexes.get(ds);
      for (IndexDesc id : ilist) {
        if (id.exprs.equals(indexExprs)) {
          return id;
        }
      }
    }
    else
    {
      ilist = new ArrayList();
      this.indexes.put(ds, ilist);
    }
    IndexDesc id = new IndexDesc(this.nextIndexID++, ds, indexExprs);
    ilist.add(id);
    return id;
  }
  
  private void addAfterJoinCondtions(JoinDesc jd, BitSet alreadyJoined, LinkedList<Condition> afterJoinConds)
  {
    ListIterator<Condition> it = afterJoinConds.listIterator();
    while (it.hasNext())
    {
      Condition cond = (Condition)it.next();
      BitSet set = (BitSet)cond.dataSets.clone();
      set.andNot(alreadyJoined);
      if (set.isEmpty())
      {
        it.remove();
        jd.afterJoinConditions.add(cond);
      }
    }
  }
  
  private FindCommonSubexpression makeCommonSubexprRewriter()
  {
    FindCommonSubexpression.Rewriter rewriter = new FindCommonSubexpression.Rewriter()
    {
      public Expr rewriteExpr(Expr e)
      {
        Expr commonSubExpr = (Expr)SelectCompiler.this.exprIndex.get(e);
        if (commonSubExpr == null)
        {
          SelectCompiler.this.exprIndex.put(e, e);
          return e;
        }
        if (!SelectCompiler.this.eliminated.containsKey(Integer.valueOf(e.eid))) {
          SelectCompiler.this.eliminated.put(Integer.valueOf(e.eid), e);
        }
        return commonSubExpr;
      }
      
      public void addAggrExpr(FuncCall aggFuncCall)
      {
        if (!SelectCompiler.this.aggregatedExprs.contains(aggFuncCall))
        {
          int index = SelectCompiler.this.aggregatedExprs.size();
          SelectCompiler.this.aggregatedExprs.add(aggFuncCall);
          aggFuncCall.setIndex(index);
        }
      }
    };
    new FindCommonSubexpression(rewriter) {};
  }
  
  private void analyzeGropyBy()
  {
    if (this.select.groupBy != null)
    {
      FindCommonSubexpression fcs = makeCommonSubexprRewriter();
      ListIterator<ValueExpr> it = this.groupBy.listIterator();
      while (it.hasNext())
      {
        ValueExpr e = (ValueExpr)it.next();
        ValueExpr enew = (ValueExpr)fcs.rewrite(e);
        if (enew != null) {
          it.set(enew);
        }
      }
    }
  }
  
  private void analyzeOrderBy()
  {
    if (this.select.orderBy != null)
    {
      FindCommonSubexpression fcs = makeCommonSubexprRewriter();
      ListIterator<OrderByItem> it = this.orderBy.listIterator();
      while (it.hasNext())
      {
        OrderByItem e = (OrderByItem)it.next();
        ValueExpr enew = (ValueExpr)fcs.rewrite(e.expr);
        if (enew != null) {
          it.set(new OrderByItem(enew, e.isAscending));
        }
      }
    }
  }
  
  private List<FieldRef> makeListOfAllFields()
  {
    List<FieldRef> allFields = new ArrayList();
    for (DataSet ds : this.dataSets)
    {
      List<FieldRef> fields = ds.makeListOfAllFields();
      allFields.addAll(fields);
    }
    return allFields;
  }
  
  private void analyzeHavingAndTargets()
  {
    FindCommonSubexpression fcs = makeCommonSubexprRewriter();
    if (this.having != null) {
      this.having = ((Predicate)fcs.rewrite(this.having));
    }
    for (Target t : this.targets)
    {
      ValueExpr enew = (ValueExpr)fcs.rewrite(t.expr);
      if (CompilerUtils.isParam(enew.getType())) {
        enew = cast(enew, String.class);
      }
      t.expr = enew;
    }
  }
}
