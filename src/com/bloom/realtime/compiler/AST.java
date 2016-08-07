package com.bloom.runtime.compiler;

import com.bloom.drop.DropMetaObject;
import com.bloom.drop.DropMetaObject.DropRule;
import com.bloom.kafkamessaging.StreamPersistencePolicy;
import com.bloom.metaRepository.MDClientOps;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.ActionType;
import com.bloom.runtime.DeploymentStrategy;
import com.bloom.runtime.Interval;
import com.bloom.runtime.Pair;
import com.bloom.runtime.Property;
import com.bloom.runtime.WactionStorePersistencePolicy;
import com.bloom.runtime.compiler.exprs.ArrayConstructor;
import com.bloom.runtime.compiler.exprs.ArrayTypeRef;
import com.bloom.runtime.compiler.exprs.Case;
import com.bloom.runtime.compiler.exprs.CaseExpr;
import com.bloom.runtime.compiler.exprs.CastExpr;
import com.bloom.runtime.compiler.exprs.ClassRef;
import com.bloom.runtime.compiler.exprs.ComparePredicate;
import com.bloom.runtime.compiler.exprs.Constant;
import com.bloom.runtime.compiler.exprs.ConstructorCall;
import com.bloom.runtime.compiler.exprs.Expr;
import com.bloom.runtime.compiler.exprs.ExprCmd;
import com.bloom.runtime.compiler.exprs.FieldRef;
import com.bloom.runtime.compiler.exprs.FuncArgs;
import com.bloom.runtime.compiler.exprs.FuncCall;
import com.bloom.runtime.compiler.exprs.IndexExpr;
import com.bloom.runtime.compiler.exprs.InstanceOfPredicate;
import com.bloom.runtime.compiler.exprs.IntegerOperation;
import com.bloom.runtime.compiler.exprs.LogicalPredicate;
import com.bloom.runtime.compiler.exprs.MethodCall;
import com.bloom.runtime.compiler.exprs.NumericOperation;
import com.bloom.runtime.compiler.exprs.ObjectRef;
import com.bloom.runtime.compiler.exprs.ParamRef;
import com.bloom.runtime.compiler.exprs.Predicate;
import com.bloom.runtime.compiler.exprs.PredicateAsValueExpr;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.exprs.WildCardExpr;
import com.bloom.runtime.compiler.patternmatch.ComplexNodeType;
import com.bloom.runtime.compiler.patternmatch.PatternDefinition;
import com.bloom.runtime.compiler.patternmatch.PatternNode;
import com.bloom.runtime.compiler.patternmatch.PatternRepetition;
import com.bloom.runtime.compiler.select.DataSource;
import com.bloom.runtime.compiler.select.DataSourceImplicitWindowOrWactionStoreView;
import com.bloom.runtime.compiler.select.DataSourceJoin;
import com.bloom.runtime.compiler.select.DataSourceNestedCollection;
import com.bloom.runtime.compiler.select.DataSourceStream;
import com.bloom.runtime.compiler.select.DataSourceStreamFunction;
import com.bloom.runtime.compiler.select.DataSourceView;
import com.bloom.runtime.compiler.select.DataSourceWactionStoreView;
import com.bloom.runtime.compiler.select.Join.Kind;
import com.bloom.runtime.compiler.stmts.ActionStmt;
import com.bloom.runtime.compiler.stmts.AdapterDescription;
import com.bloom.runtime.compiler.stmts.AlterDeploymentGroupStmt;
import com.bloom.runtime.compiler.stmts.AlterStmt;
import com.bloom.runtime.compiler.stmts.ConnectStmt;
import com.bloom.runtime.compiler.stmts.CreateAdHocSelectStmt;
import com.bloom.runtime.compiler.stmts.CreateAppOrFlowStatement;
import com.bloom.runtime.compiler.stmts.CreateCacheStmt;
import com.bloom.runtime.compiler.stmts.CreateCqStmt;
import com.bloom.runtime.compiler.stmts.CreateDashboardStatement;
import com.bloom.runtime.compiler.stmts.CreateDeploymentGroupStmt;
import com.bloom.runtime.compiler.stmts.CreateNamespaceStatement;
import com.bloom.runtime.compiler.stmts.CreatePropertySetStmt;
import com.bloom.runtime.compiler.stmts.CreatePropertyVariableStmt;
import com.bloom.runtime.compiler.stmts.CreateRoleStmt;
import com.bloom.runtime.compiler.stmts.CreateShowStreamStmt;
import com.bloom.runtime.compiler.stmts.CreateSorterStmt;
import com.bloom.runtime.compiler.stmts.CreateSourceOrTargetStmt;
import com.bloom.runtime.compiler.stmts.CreateStreamStmt;
import com.bloom.runtime.compiler.stmts.CreateTypeStmt;
import com.bloom.runtime.compiler.stmts.CreateUserStmt;
import com.bloom.runtime.compiler.stmts.CreateVisualizationStmt;
import com.bloom.runtime.compiler.stmts.CreateWASStmt;
import com.bloom.runtime.compiler.stmts.CreateWindowStmt;
import com.bloom.runtime.compiler.stmts.DeployStmt;
import com.bloom.runtime.compiler.stmts.DeploymentRule;
import com.bloom.runtime.compiler.stmts.DropStmt;
import com.bloom.runtime.compiler.stmts.DumpStmt;
import com.bloom.runtime.compiler.stmts.EmptyStmt;
import com.bloom.runtime.compiler.stmts.EndBlockStmt;
import com.bloom.runtime.compiler.stmts.EventType;
import com.bloom.runtime.compiler.stmts.ExceptionHandler;
import com.bloom.runtime.compiler.stmts.ExecPreparedStmt;
import com.bloom.runtime.compiler.stmts.ExportAppStmt;
import com.bloom.runtime.compiler.stmts.ExportDataStmt;
import com.bloom.runtime.compiler.stmts.ExportStreamSchemaStmt;
import com.bloom.runtime.compiler.stmts.GracePeriod;
import com.bloom.runtime.compiler.stmts.GrantPermissionToStmt;
import com.bloom.runtime.compiler.stmts.GrantRoleToStmt;
import com.bloom.runtime.compiler.stmts.ImportDataStmt;
import com.bloom.runtime.compiler.stmts.ImportStmt;
import com.bloom.runtime.compiler.stmts.InputClause;
import com.bloom.runtime.compiler.stmts.InputOutputSink;
import com.bloom.runtime.compiler.stmts.LimitClause;
import com.bloom.runtime.compiler.stmts.LoadFileStmt;
import com.bloom.runtime.compiler.stmts.LoadUnloadJarStmt;
import com.bloom.runtime.compiler.stmts.MappedStream;
import com.bloom.runtime.compiler.stmts.MatchClause;
import com.bloom.runtime.compiler.stmts.MonitorStmt;
import com.bloom.runtime.compiler.stmts.OrderByItem;
import com.bloom.runtime.compiler.stmts.OutputClause;
import com.bloom.runtime.compiler.stmts.PrintMetaDataStmt;
import com.bloom.runtime.compiler.stmts.QuitStmt;
import com.bloom.runtime.compiler.stmts.RecoveryDescription;
import com.bloom.runtime.compiler.stmts.RevokePermissionFromStmt;
import com.bloom.runtime.compiler.stmts.RevokeRoleFromStmt;
import com.bloom.runtime.compiler.stmts.Select;
import com.bloom.runtime.compiler.stmts.SelectTarget;
import com.bloom.runtime.compiler.stmts.SetStmt;
import com.bloom.runtime.compiler.stmts.SorterInOutRule;
import com.bloom.runtime.compiler.stmts.Stmt;
import com.bloom.runtime.compiler.stmts.UpdateUserInfoStmt;
import com.bloom.runtime.compiler.stmts.UseStmt;
import com.bloom.runtime.compiler.stmts.UserProperty;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.IntervalPolicy;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.security.ObjectPermission;
import com.bloom.security.WASecurityManager;
import com.bloom.security.ObjectPermission.Action;
import com.bloom.security.ObjectPermission.ObjectType;
import com.bloom.utility.Utility;
import com.bloom.event.SimpleEvent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import scala.xml.parsing.FatalError;

public class AST
{
  public final Grammar parser;
  
  private void error(String msg, Object obj)
  {
    this.parser.parseError(msg, obj);
  }
  
  private static String getExprText(Object obj, Grammar p)
  {
    return p == null ? "" : p.lex.getExprText(obj);
  }
  
  public static <T> List<T> NewList(T first_item)
  {
    return new ArrayList(Collections.singletonList(first_item));
  }
  
  public static <T> List<T> NewList(T first_item, T second_item)
  {
    return new ArrayList(Arrays.asList(new Object[] { first_item, second_item }));
  }
  
  public static <T> List<T> NewList(List<T> list, T next_item)
  {
    List<T> newlist = new ArrayList(list);
    newlist.add(next_item);
    return newlist;
  }
  
  public static <T> List<T> AddToList(List<T> list, T next_item)
  {
    list.add(next_item);
    return list;
  }
  
  public static MappedStream CreateMappedStream(String streamName, List<Property> mappingProps)
  {
    return new MappedStream(streamName, mappingProps);
  }
  
  public AST(Grammar parser)
  {
    this.parser = parser;
  }
  
  public Interval ParseDSInterval(String literal, int flags)
  {
    Interval i = Interval.parseDSInterval(literal, flags);
    if (i == null) {
      error("invalid interval literal", literal);
    }
    return i;
  }
  
  public Interval ParseYMInterval(String literal, int flags)
  {
    Interval i = Interval.parseYMInterval(literal, flags);
    if (i == null) {
      error("invalid interval literal", literal);
    }
    return i;
  }
  
  public static Interval makeDSInterval(double val)
  {
    long l = Math.round(val * 1000000.0D);
    return new Interval(l);
  }
  
  public static Interval makeDSInterval(long val, int flags)
  {
    switch (flags)
    {
    case 1: 
      return new Interval(val * 1000000L);
    case 2: 
      return new Interval(val * 1000000L * 60L);
    case 4: 
      return new Interval(val * 1000000L * 60L * 60L);
    case 8: 
      return new Interval(val * 1000000L * 60L * 60L * 24L);
    }
   
    return null;
  }
  
  public static Interval makeYMInterval(long val, int flags)
  {
    switch (flags)
    {
    case 16: 
      return new Interval(val);
    case 32: 
      return new Interval(val * 12L);
    }
    return null;
  }
  
  public static TypeName CreateType(String typename, int array_dimensions)
  {
    return new TypeName(typename, array_dimensions);
  }
  
  public static ValueExpr NewNumericExpr(ExprCmd op, ValueExpr left, ValueExpr right)
  {
    if (((left instanceof NumericOperation)) && (left.op == op))
    {
      NumericOperation o = (NumericOperation)left;
      return new NumericOperation(op, NewList(o.args, right));
    }
    return new NumericOperation(op, NewList(left, right));
  }
  
  public static ValueExpr NewIntegerExpr(ExprCmd op, ValueExpr left, ValueExpr right)
  {
    if (((left instanceof IntegerOperation)) && (left.op == op))
    {
      IntegerOperation o = (IntegerOperation)left;
      return new IntegerOperation(op, NewList(o.args, right));
    }
    return new IntegerOperation(op, NewList(left, right));
  }
  
  public static ValueExpr NewUnaryNumericExpr(ExprCmd op, ValueExpr expr)
  {
    return new NumericOperation(op, NewList(expr));
  }
  
  public static ValueExpr NewUnaryIntegerExpr(ExprCmd op, ValueExpr expr)
  {
    return new IntegerOperation(op, NewList(expr));
  }
  
  public static ValueExpr NewIntegerConstant(Integer literal)
  {
    return Constant.newInt(literal.intValue());
  }
  
  public static ValueExpr NewLongConstant(Long literal)
  {
    return Constant.newLong(literal);
  }
  
  public static ValueExpr NewFloatConstant(Float literal)
  {
    return Constant.newFloat(literal);
  }
  
  public static ValueExpr NewDoubleConstant(Double literal)
  {
    return Constant.newDouble(literal);
  }
  
  public static ValueExpr NewNullConstant()
  {
    return Constant.newNull();
  }
  
  public static ValueExpr NewBoolConstant(boolean value)
  {
    return Constant.newBool(Boolean.valueOf(value));
  }
  
  public static ValueExpr NewDateConstant(String literal)
  {
    return Constant.newDate(literal);
  }
  
  public static ValueExpr NewTimestampConstant(String literal)
  {
    return Constant.newDate(literal);
  }
  
  public static ValueExpr NewDateTimeConstant(String literal)
  {
    return Constant.newDateTime(literal);
  }
  
  public static ValueExpr NewYMIntervalConstant(Interval value)
  {
    return Constant.newYMInterval(value);
  }
  
  public static ValueExpr NewDSIntervalConstant(Interval value)
  {
    return Constant.newDSInterval(value);
  }
  
  public static ValueExpr NewSimpleCaseExpression(ValueExpr selector, List<Case> caselist, ValueExpr opt_else)
  {
    return new CaseExpr(selector, caselist, opt_else);
  }
  
  public static ValueExpr NewSearchedCaseExpression(List<Case> caselist, ValueExpr opt_else)
  {
    return new CaseExpr(null, caselist, opt_else);
  }
  
  public static Predicate NewInstanceOfExpr(ValueExpr expr, TypeName type)
  {
    return new InstanceOfPredicate(expr, type);
  }
  
  public static Predicate NewCompareExpr(ExprCmd op, ValueExpr left, ValueExpr right)
  {
    return new ComparePredicate(op, NewList(left, right));
  }
  
  public static Predicate NewLikeExpr(ValueExpr expr, boolean not, ValueExpr pattern)
  {
    Predicate p = new ComparePredicate(ExprCmd.LIKE, NewList(expr, pattern));
    return not ? NewNotExpr(p) : p;
  }
  
  public static Predicate NewBetweenExpr(ValueExpr expr, boolean not, ValueExpr lowerbound, ValueExpr upperbound)
  {
    List<ValueExpr> l = new ArrayList(3);
    Collections.addAll(l, new ValueExpr[] { expr, lowerbound, upperbound });
    Predicate p = new ComparePredicate(ExprCmd.BETWEEN, l);
    return not ? NewNotExpr(p) : p;
  }
  
  public static Predicate NewIsNullExpr(ValueExpr expr, boolean not)
  {
    Predicate p = new ComparePredicate(ExprCmd.ISNULL, NewList(expr));
    return not ? NewNotExpr(p) : p;
  }
  
  public static Predicate NewInListExpr(ValueExpr expr, boolean not, List<ValueExpr> list)
  {
    if (list == null) {
      list = Collections.emptyList();
    }
    List<ValueExpr> l = new ArrayList(1 + list.size());
    l.add(expr);
    l.addAll(list);
    Predicate p = new ComparePredicate(ExprCmd.INLIST, l);
    return not ? NewNotExpr(p) : p;
  }
  
  public static Predicate NewOrExpr(Predicate left, Predicate right)
  {
    if (((left instanceof LogicalPredicate)) && (left.op == ExprCmd.OR))
    {
      LogicalPredicate p = (LogicalPredicate)left;
      return new LogicalPredicate(ExprCmd.OR, NewList(p.args, right));
    }
    return new LogicalPredicate(ExprCmd.OR, NewList(left, right));
  }
  
  public static Predicate NewAndExpr(Predicate left, Predicate right)
  {
    if (((left instanceof LogicalPredicate)) && (left.op == ExprCmd.AND))
    {
      LogicalPredicate p = (LogicalPredicate)left;
      return new LogicalPredicate(ExprCmd.AND, NewList(p.args, right));
    }
    return new LogicalPredicate(ExprCmd.AND, NewList(left, right));
  }
  
  public static Predicate NewNotExpr(Predicate expr)
  {
    return new LogicalPredicate(ExprCmd.NOT, NewList(expr));
  }
  
  public static FuncArgs NewFuncArgs(List<ValueExpr> args, int options)
  {
    if (args == null) {
      args = new ArrayList();
    }
    return new FuncArgs(args, options);
  }
  
  public static ValueExpr NewStringConstant(String val)
  {
    return Constant.newString(val);
  }
  
  public static ValueExpr NewObjectConstructorExpr(String typeName, List<ValueExpr> args)
  {
    if (args == null) {
      args = Collections.emptyList();
    }
    TypeName tn = new TypeName(typeName, 0);
    return new ConstructorCall(tn, args);
  }
  
  public static ValueExpr NewArryConstructorExpr(String typeName, List<ValueExpr> indexes, int brackets)
  {
    TypeName tn = new TypeName(typeName, indexes.size() + brackets);
    return new ArrayConstructor(tn, indexes);
  }
  
  public static ValueExpr NewCastExpr(ValueExpr e, TypeName t)
  {
    return new CastExpr(t, e);
  }
  
  public static ValueExpr NewIdentifierRef(String name)
  {
    return new ObjectRef(name);
  }
  
  public static ValueExpr NewFuncCall(String funcName, FuncArgs args)
  {
    return new FuncCall(funcName, args.args, args.options);
  }
  
  private void checkValidPredecessor(ValueExpr e)
  {
    if ((e instanceof ArrayTypeRef)) {
      error("invalid expression", e);
    }
  }
  
  public ValueExpr NewFieldRef(ValueExpr e, String fieldName)
  {
    checkValidPredecessor(e);
    return FieldRef.createUnresolvedFieldRef(e, fieldName);
  }
  
  public ValueExpr NewMethodCall(ValueExpr e, String funcName, FuncArgs args)
  {
    checkValidPredecessor(e);
    args.args.add(0, e);
    return new MethodCall(funcName, args.args, args.options);
  }
  
  public static ValueExpr NewIndexExpr(ValueExpr e, List<ValueExpr> indexes)
  {
    ValueExpr res = e;
    for (ValueExpr idx : indexes)
    {
      ValueExpr tmp = new IndexExpr(idx, res);
      res.setOriginalExpr(tmp);
      res = tmp;
    }
    return res;
  }
  
  public ValueExpr NewArrayTypeRef(ValueExpr e, String typeName, int brackets)
  {
    String name = getClassNamePrefix(null, e);
    if (name == null) {
      error("invalid type name", e);
    }
    name = name + "." + typeName;
    TypeName tn = new TypeName(name, brackets);
    return new ArrayTypeRef(tn);
  }
  
  public static ValueExpr NewArrayTypeRef(String typeName, int brackets)
  {
    TypeName tn = new TypeName(typeName, brackets);
    return new ArrayTypeRef(tn);
  }
  
  private String getClassNamePrefix(String name, ValueExpr e)
  {
    if ((e instanceof ObjectRef))
    {
      ObjectRef o = (ObjectRef)e;
      name = o.name + (name == null ? "" : new StringBuilder().append(".").append(name).toString());
      return name;
    }
    if ((e instanceof FieldRef))
    {
      FieldRef f = (FieldRef)e;
      name = f.getName() + (name == null ? "" : new StringBuilder().append(".").append(name).toString());
      return getClassNamePrefix(name, f.getExpr());
    }
    return null;
  }
  
  public ValueExpr NewClassRef(ValueExpr e)
  {
    if ((e instanceof ArrayTypeRef))
    {
      ArrayTypeRef a = (ArrayTypeRef)e;
      return new ClassRef(a.typeName);
    }
    String name = getClassNamePrefix(null, e);
    if (name == null) {
      error("invalid type name", e);
    }
    TypeName tn = new TypeName(name.toString(), 0);
    return new ClassRef(tn);
  }
  
  public ValueExpr NewDataSetAllFields(ValueExpr e)
  {
    if ((e instanceof ObjectRef))
    {
      ObjectRef o = (ObjectRef)e;
      return new WildCardExpr(o.name);
    }
    error("invalid syntax", e);
    return e;
  }
  
  public ValueExpr NewParameterRef(String pname)
  {
    return new ParamRef(pname);
  }
  
  public static Stmt ImportClassDeclaration(String pkgname, boolean isStatic)
  {
    return new ImportStmt(pkgname, false, isStatic);
  }
  
  public static Stmt ImportPackageDeclaration(String pkgname, boolean isStatic)
  {
    return new ImportStmt(pkgname, true, isStatic);
  }
  
  public static Stmt CreateCqStatement(String cq_name, Boolean doReplace, String dest_stream_name, List<String> field_name_list, Select select, String selectText)
  {
    if (dest_stream_name == null) {
      return CreateAdHocSelectStmt(select, selectText, cq_name);
    }
    if (field_name_list == null) {
      field_name_list = Collections.emptyList();
    }
    return new CreateCqStmt(cq_name, doReplace, dest_stream_name, field_name_list, select, selectText);
  }
  
  public static Stmt CreateCqStatement(String cq_name, Boolean doReplace, String dest_stream_name, List<String> field_name_list, List<String> partition_fields, Select select, String selectText)
  {
    if (dest_stream_name == null) {
      throw new RuntimeException("Not correct usage.");
    }
    if (field_name_list == null) {
      field_name_list = Collections.emptyList();
    }
    CreateCqStmt stmt = new CreateCqStmt(cq_name, doReplace, dest_stream_name, field_name_list, select, selectText);
    stmt.setPartitionFieldList(partition_fields);
    return stmt;
  }
  
  public static Stmt CreateCqStatement(String cq_name, Boolean doReplace, String dest_stream_name, List<String> field_name_list, Select select, Grammar p)
  {
    String selectText = getExprText(select, p);
    return CreateCqStatement(cq_name, doReplace, dest_stream_name, field_name_list, select, selectText);
  }
  
  public static Stmt CreateCqStatement(String cq_name, Boolean doReplace, String dest_stream_name, List<String> field_name_list, List<String> partition_fields, Select select, Grammar p)
  {
    String selectText = getExprText(select, p);
    return CreateCqStatement(cq_name, doReplace, dest_stream_name, field_name_list, partition_fields, select, selectText);
  }
  
  public static Stmt CreateStreamStatementWithoutType(String stream_name, Boolean doReplace, List<String> partition_fields, GracePeriod gp, StreamPersistencePolicy spp)
  {
    return new CreateStreamStmt(stream_name, doReplace, partition_fields, SimpleEvent.class.getName(), null, gp, spp);
  }
  
  public static Stmt CreateStreamStatement(String stream_name, Boolean doReplace, List<String> partition_fields, TypeDefOrName typedef, GracePeriod gp, StreamPersistencePolicy spp)
  {
    return new CreateStreamStmt(stream_name, doReplace, partition_fields, typedef.typeName, typedef.typeDef, gp, spp);
  }
  
  public static Stmt CreateTypeStatement(String type_name, Boolean doReplace, TypeDefOrName typeDef)
  {
    return new CreateTypeStmt(type_name, doReplace, typeDef);
  }
  
  public static Stmt CreateWindowStatement(String window_name, Boolean doReplace, String stream_name, Pair<IntervalPolicy, IntervalPolicy> window_len, boolean isJumping, List<String> partition_fields)
  {
    return new CreateWindowStmt(window_name, doReplace, stream_name, window_len, isJumping, partition_fields, null);
  }
  
  public static Stmt DropStatement(EntityType objectType, String objectName, DropMetaObject.DropRule dropRule)
  {
    return new DropStmt(objectType, objectName, dropRule);
  }
  
  public static DataSource Join(DataSource left, DataSource right, Join.Kind kindOfJoin, Predicate joinCond)
  {
    return new DataSourceJoin(left, right, kindOfJoin, joinCond);
  }
  
  public static OutputClause newOutputClause(String stream, MappedStream mp, List<String> part, List<TypeField> fields, Select select, Grammar parser)
  {
    return new OutputClause(stream, mp, part, fields, select, getExprText(select, parser));
  }
  
  public static Select CreateSelect(boolean distinct, int kind, List<SelectTarget> targets, List<DataSource> from, Predicate where, List<ValueExpr> group, Predicate having, List<OrderByItem> orderby, LimitClause limit, boolean linksrc)
  {
    return new Select(distinct, kind, targets, from, where, group, having, orderby, limit, linksrc, null);
  }
  
  public static SelectTarget SelectTarget(Expr expr, String alias, Grammar p)
  {
    if (alias == null) {
      alias = getExprText(expr, p);
    }
    assert (alias != null);
    ValueExpr valexpr;
    if ((expr instanceof Predicate))
    {
      if (expr.op == ExprCmd.BOOLEXPR)
      {
        valexpr = (ValueExpr)((ComparePredicate)expr).args.get(0);
      }
      else
      {
         valexpr = new PredicateAsValueExpr((Predicate)expr);
        valexpr.setOriginalExpr(expr);
      }
    }
    else
    {
      valexpr = (ValueExpr)expr;
    }
    return new SelectTarget(valexpr, alias);
  }
  
  public static SelectTarget SelectTargetAll()
  {
    return new SelectTarget(null, null);
  }
  
  public static DataSource SourceStream(String stream_name, String opt_typename, String alias)
  {
    return new DataSourceStream(stream_name, opt_typename, alias);
  }
  
  public static DataSource SourceView(Select subSelect, String alias, Grammar p)
  {
    String selectText = getExprText(subSelect, p);
    return new DataSourceView(subSelect, alias, selectText);
  }
  
  public static DataSource SourceStreamFunction(String funcName, List<ValueExpr> args, String alias)
  {
    if (args == null) {
      args = Collections.emptyList();
    }
    return new DataSourceStreamFunction(funcName, args, alias);
  }
  
  public static TypeField TypeField(String name, TypeName type, boolean iskey)
  {
    return new TypeField(name, type, iskey);
  }
  
  public static Stmt CreateUseNamespaceStmt(String schemaName)
  {
    return new UseStmt(EntityType.NAMESPACE, schemaName, false);
  }
  
  public static Stmt CreateAlterAppOrFlowStmt(EntityType type, String name, boolean recompile)
  {
    return new UseStmt(type, name, recompile);
  }
  
  public static Property CreateProperty(String n, Object v)
  {
    return new Property(n, v);
  }
  
  public static Stmt CreateSourceStatement(String n, Boolean r, AdapterDescription src, AdapterDescription parser, List<OutputClause> oc)
  {
    List<InputOutputSink> ll = new ArrayList();
    ll.addAll(oc);
    return new CreateSourceOrTargetStmt(EntityType.SOURCE, n, r, src, parser, ll);
  }
  
  public static Stmt CreateTargetStatement(String n, Boolean r, AdapterDescription dest, AdapterDescription formatter, String stream, List<String> partition_fields)
  {
    List<InputOutputSink> oc = new ArrayList();
    oc.add(new InputClause(stream, null, partition_fields, null));
    return new CreateSourceOrTargetStmt(EntityType.TARGET, n, r, dest, formatter, oc);
  }
  
  public static Stmt CreateSubscriptionStatement(String n, Boolean r, AdapterDescription dest, AdapterDescription formatter, String stream, List<String> partition_fields)
  {
    AdapterDescription temp = null;
    Property p = new Property("isSubscription", "true");
    if (dest.getProps() == null)
    {
      List<Property> list = new ArrayList();
      list.add(p);
      temp = new AdapterDescription(dest.getAdapterTypeName(), list);
    }
    else
    {
      dest.getProps().add(p);
      temp = new AdapterDescription(dest.getAdapterTypeName(), dest.getProps());
    }
    List<InputOutputSink> oc = new ArrayList();
    oc.add(new OutputClause(stream, null, partition_fields, null, null, null));
    
    return new CreateSourceOrTargetStmt(EntityType.TARGET, n, r, temp, formatter, oc);
  }
  
  public static Stmt CreateCacheStatement(String n, Boolean r, AdapterDescription src, AdapterDescription parser, List<Property> query_props, String typename)
  {
    return new CreateCacheStmt(EntityType.CACHE, n, r, src, parser, query_props, typename);
  }
  
  public static Stmt CreateFlowStatement(String n, Boolean r, EntityType type, List<Pair<EntityType, String>> l, Boolean encrypt, RecoveryDescription recov, ExceptionHandler eh)
  {
    return new CreateAppOrFlowStatement(type, n, r, l, encrypt, recov, eh, null, null);
  }
  
  public static Stmt CreateDeploymentGroupStatement(String groupname, List<String> deploymentGroup, long minServers)
  {
    return new CreateDeploymentGroupStmt(groupname, deploymentGroup, minServers);
  }
  
  public static Stmt CreatePropertySet(String n, Boolean r, List<Property> props)
  {
    return new CreatePropertySetStmt(n, r, props);
  }
  
  public static Stmt CreatePropertyVariable(String paramname, Object paramvalue, Boolean r)
  {
    Serializable s = ((paramvalue instanceof Serializable)) || (paramvalue == null) ? (Serializable)paramvalue : paramvalue.toString();
    return new CreatePropertyVariableStmt(paramname, s, r);
  }
  
  public static Stmt CreateStartStmt(String appOrFlowName, EntityType type, RecoveryDescription recov)
  {
    return new ActionStmt(ActionType.START, appOrFlowName, type, recov);
  }
  
  public static Stmt CreateStopStmt(String appOrFlowName, EntityType type)
  {
    return new ActionStmt(ActionType.STOP, appOrFlowName, type);
  }
  
  public static Stmt CreateResumeStmt(String appOrFlowName, EntityType type)
  {
    return new ActionStmt(ActionType.RESUME, appOrFlowName, type);
  }
  
  public static Stmt CreateWASStatement(String n, Boolean r, TypeDefOrName def, List<EventType> ets, WactionStorePersistencePolicy wactionStorePersistencePolicy)
  {
    return new CreateWASStmt(n, r, def, ets, wactionStorePersistencePolicy.howOften, wactionStorePersistencePolicy.properties);
  }
  
  public static EventType CreateEventType(String tpname, List<String> key)
  {
    return new EventType(tpname, key);
  }
  
  public static Stmt CreateNamespaceStatement(String n, Boolean r)
  {
    return new CreateNamespaceStatement(n, r);
  }
  
  public static Stmt CreateAdHocSelectStmt(Select select, String selectText, String queryName)
  {
    return new CreateAdHocSelectStmt(select, selectText, queryName);
  }
  
  public static Stmt CreateAdHocSelectStmt(Select select, Grammar p, String queryName)
  {
    String selectText = getExprText(select, p);
    return CreateAdHocSelectStmt(select, selectText, queryName);
  }
  
  public static Stmt CreateShowStmt(String stream_name)
  {
    return new CreateShowStreamStmt(stream_name);
  }
  
  public static Stmt CreateShowStmt(String stream_name, int line_count)
  {
    return new CreateShowStreamStmt(stream_name, line_count);
  }
  
  public static Stmt CreateShowStmt(String stream_name, int line_count, boolean isTungsten)
  {
    return new CreateShowStreamStmt(stream_name, line_count, isTungsten);
  }
  
  public static Stmt CreateStatusStmt(String appName)
  {
    return new ActionStmt(ActionType.STATUS, appName, EntityType.APPLICATION);
  }
  
  public static Stmt CreateUserStatement(String userid, String password, UserProperty prop, String authLayerPropSetName)
  {
    return new CreateUserStmt(userid, password, prop, authLayerPropSetName);
  }
  
  public static Stmt CreateRoleStatement(String name)
  {
    return new CreateRoleStmt(name);
  }
  
  public static Stmt RevokePermissionFromRole(@NotNull List<ObjectPermission.Action> listOfPrivilege, @Nullable List<ObjectPermission.ObjectType> objectType, @NotNull String name, @Nullable String roleName)
  {
    return new RevokePermissionFromStmt(listOfPrivilege, objectType, name, roleName, EntityType.ROLE);
  }
  
  public static Stmt RevokePermissionFromUser(@NotNull List<ObjectPermission.Action> listOfPrivilege, @Nullable List<ObjectPermission.ObjectType> objectType, @NotNull String name, @Nullable String userName)
  {
    return new RevokePermissionFromStmt(listOfPrivilege, objectType, name, userName, EntityType.USER);
  }
  
  public Stmt RevokeRoleFromRole(List<String> rolename, String rolename_2)
  {
    return new RevokeRoleFromStmt(rolename, rolename_2, EntityType.ROLE);
  }
  
  public Stmt RevokeRoleFromUser(List<String> rolename, String username)
  {
    return new RevokeRoleFromStmt(rolename, username, EntityType.USER);
  }
  
  public static Stmt GrantPermissionToRole(@NotNull List<ObjectPermission.Action> listOfPrivilege, @Nullable List<ObjectPermission.ObjectType> objectType, @NotNull String name, @Nullable String roleName)
  {
    return new GrantPermissionToStmt(listOfPrivilege, objectType, name, roleName, EntityType.ROLE);
  }
  
  public static Stmt GrantPermissionToUser(@NotNull List<ObjectPermission.Action> listOfPrivilege, @Nullable List<ObjectPermission.ObjectType> objectType, @NotNull String name, @Nullable String userName)
  {
    return new GrantPermissionToStmt(listOfPrivilege, objectType, name, userName, EntityType.USER);
  }
  
  public Stmt GrantRoleToRole(List<String> rolename, String rolename_2)
  {
    return new GrantRoleToStmt(rolename, rolename_2, EntityType.ROLE);
  }
  
  public Stmt GrantRoleToUser(List<String> rolename, String username)
  {
    return new GrantRoleToStmt(rolename, username, EntityType.USER);
  }
  
  public static Stmt CreateLoadUnloadJarStmt(String pathToJar, boolean doLoad)
  {
    return new LoadUnloadJarStmt(pathToJar, doLoad);
  }
  
  public static Stmt Connect(String username)
  {
    return new ConnectStmt(username, null, null, null);
  }
  
  public static Stmt ImportData(String what, String where, Boolean replace)
  {
    return new ImportDataStmt(what, where, replace);
  }
  
  public static Stmt ExportData(String what)
  {
    return ExportData(what, null);
  }
  
  public static Stmt ExportData(String what, String where)
  {
    return new ExportDataStmt(what, where);
  }
  
  public static Stmt ExportTypes(String appname, String jarpath, String format)
  {
    return new ExportAppStmt(appname, jarpath, format);
  }
  
  public static Stmt Connect(String username, String password)
  {
    return new ConnectStmt(username, password, null, null);
  }
  
  public static Stmt Set(String paramname, Object paramvalue)
  {
    Serializable s = ((paramvalue instanceof Serializable)) || (paramvalue == null) ? (Serializable)paramvalue : paramvalue.toString();
    return new SetStmt(paramname, s);
  }
  
  public static Stmt endStmt(List<Property> compileOptions, Stmt s, Grammar p)
  {
    String stmtText = getExprText(s, p);
    s.setSourceText(stmtText, compileOptions);
    return s;
  }
  
  public static OrderByItem CreateOrderByItem(ValueExpr expr, boolean isAscending)
  {
    return new OrderByItem(expr, isAscending);
  }
  
  public static LimitClause CreateLimitClause(int limit, int offset)
  {
    return new LimitClause(limit, offset);
  }
  
  public static AdapterDescription CreateAdapterDesc(String type, List<Property> props)
  {
    return new AdapterDescription(type, props);
  }
  
  public static RecoveryDescription CreateRecoveryDesc(int type, long interval)
  {
    return new RecoveryDescription(type, interval);
  }
  
  public static ExceptionHandler CreateExceptionHandler(List<Property> props)
  {
    return new ExceptionHandler(props);
  }
  
  public static Stmt emptyStmt()
  {
    return new EmptyStmt();
  }
  
  public static Stmt CreateVisualization(String objectName, String filename)
  {
    return new CreateVisualizationStmt(objectName, filename);
  }
  
  public static Stmt UpdateUserInfoStmt(String userid, List<Property> props)
  {
    return new UpdateUserInfoStmt(userid, props);
  }
  
  public static DataSource ImplicitWindowOverStreamOrWactionStoreView(String streamOrWactionStoreName, String alias, boolean isJumping, Pair<IntervalPolicy, IntervalPolicy> wind, List<String> part)
  {
    return new DataSourceImplicitWindowOrWactionStoreView(streamOrWactionStoreName, alias, wind, isJumping, part);
  }
  
  public static DataSource WactionStoreView(String wactionStoreName, String alias, Boolean isJumping, Interval range, boolean subscribeToUpdates)
  {
    return new DataSourceWactionStoreView(wactionStoreName, alias, isJumping, range, subscribeToUpdates);
  }
  
  public static DataSource SourceNestedCollection(String fieldName, String alias)
  {
    return new DataSourceNestedCollection(fieldName, alias, null, null);
  }
  
  public static DataSource SourceNestedCollectionOfType(String fieldName, String alias, TypeName type)
  {
    return new DataSourceNestedCollection(fieldName, alias, type, null);
  }
  
  public static DataSource SourceNestedCollectionWithFields(String fieldName, String alias, List<TypeField> fields)
  {
    return new DataSourceNestedCollection(fieldName, alias, null, fields);
  }
  
  public static Stmt CreateEndStmt(String appOrFlowName, EntityType type)
  {
    return new EndBlockStmt(appOrFlowName, type);
  }
  
  public static Stmt CreateUndeployStmt(String appOrFlowName, EntityType type)
  {
    return new ActionStmt(ActionType.UNDEPLOY, appOrFlowName, type);
  }
  
  public static Stmt CreateDeployStmt(EntityType type, DeploymentRule appRule, List<DeploymentRule> flowRules, List<Pair<String, String>> options)
  {
    return new DeployStmt(type, appRule, flowRules, options);
  }
  
  public static DeploymentRule CreateDeployRule(DeploymentStrategy ds, String flowName, String deploymentgroup)
  {
    return new DeploymentRule(ds, flowName, deploymentgroup);
  }
  
  public SorterInOutRule CreateSorterInOutRule(String instream, String fieldname, String outstream)
  {
    return new SorterInOutRule(instream, fieldname, outstream);
  }
  
  public Stmt CreateSorterStatement(Boolean r, String n, Interval i, List<SorterInOutRule> l, String errorStream)
  {
    return new CreateSorterStmt(n, r, i, l, errorStream);
  }
  
  public static GracePeriod CreateGracePeriodClause(Interval t, String fieldname)
  {
    return new GracePeriod(t, fieldname);
  }
  
  public static Predicate BooleanExprPredicate(ValueExpr e)
  {
    ArrayList<ValueExpr> args = new ArrayList();
    args.add(e);
    return new ComparePredicate(ExprCmd.BOOLEXPR, args);
  }
  
  public static Stmt ExecPreparedQuery(String queryName, List<Property> params)
  {
    if (params == null) {
      params = Collections.emptyList();
    }
    return new ExecPreparedStmt(queryName, params);
  }
  
  private static List<String> extractPermissionsFromActionEntityTypePair(List<Pair<ObjectPermission.Action, EntityType>> pair, String optional_domain)
  {
    List<String> permissionList = new ArrayList();
    for (Pair<ObjectPermission.Action, EntityType> pairEntry : pair)
    {
      StringBuilder code = new StringBuilder();
      if (optional_domain == null) {
        code.append("*:");
      } else {
        code.append(optional_domain + ":");
      }
      code.append(pairEntry.first);
      code.append(":");
      code.append(((EntityType)pairEntry.second).toString().toLowerCase());
      code.append(":*");
      permissionList.add(code.toString());
    }
    return permissionList;
  }
  
  private static List<String> extractPermissionListFromActionsList(List<ObjectPermission.Action> actionsList, String objectName)
  {
    List<String> permissionList = new ArrayList();
    String domain = Utility.splitDomain(objectName);
    String name = Utility.splitName(objectName);
    MetaInfo.MetaObject mo = null;
    for (EntityType et : EntityType.values()) {
      try
      {
        if (et.isGlobal()) {
          domain = "Global";
        }
        if ((name != null) && (domain != null) && (et != null))
        {
          mo = MDClientOps.getINSTANCE().getMetaObjectByName(et, domain, name, null, WASecurityManager.TOKEN);
          if (mo != null) {
            break;
          }
        }
      }
      catch (MetaDataRepositoryException e) {}
    }
    if (mo == null) {
      throw new FatalError("Coudn't find metaobject " + objectName);
    }
    for (ObjectPermission.Action action : actionsList)
    {
      StringBuilder code = new StringBuilder();
      code.append(domain + ":");
      code.append(action.toString().toLowerCase() + ":");
      code.append(mo.type.name().toLowerCase() + ":");
      code.append(name);
      permissionList.add(code.toString());
    }
    return permissionList;
  }
  
  public static Stmt CreateAlterDeploymentGroup(boolean add, String groupname, List<String> deploymentgroup)
  {
    return new AlterDeploymentGroupStmt(add, groupname, deploymentgroup);
  }
  
  public static Stmt CreateWaitStmt(final int millisec)
  {
    return new Stmt()
    {
      public Object compile(Compiler c)
        throws MetaDataRepositoryException
      {
        try
        {
          Thread.sleep(millisec);
        }
        catch (InterruptedException e)
        {
          e.printStackTrace();
        }
        return null;
      }
    };
  }
  
  public static Stmt CreateDashboardStatement(Boolean doReplace, String filename)
  {
    return new CreateDashboardStatement(doReplace, filename);
  }
  
  public Select CreateSelectMatch(Boolean distinct, Integer kind, List<SelectTarget> target, List<DataSource> from, PatternNode pattern, List<PatternDefinition> definitions, List<ValueExpr> partitionkey)
  {
    return new Select(distinct.booleanValue(), kind.intValue(), target, from, null, null, null, null, null, false, new MatchClause(pattern, definitions, partitionkey));
  }
  
  public PatternNode NewPatternAlternation(PatternNode a, PatternNode b)
  {
    return PatternNode.makeComplexNode(ComplexNodeType.ALTERNATION, a, b);
  }
  
  public PatternNode NewPatternCombination(PatternNode a, PatternNode b)
  {
    return PatternNode.makeComplexNode(ComplexNodeType.COMBINATION, a, b);
  }
  
  public PatternNode NewPatternSequence(PatternNode a, PatternNode b)
  {
    return PatternNode.makeComplexNode(ComplexNodeType.SEQUENCE, a, b);
  }
  
  public static PatternDefinition NewPatternDefinition(String var, String streamName, Predicate p)
  {
    return new PatternDefinition(var, streamName, p);
  }
  
  public PatternNode NewPatternElement(String name)
  {
    return PatternNode.makeVariable(name);
  }
  
  public PatternNode NewPatternRepetition(PatternNode p, PatternRepetition r)
  {
    return PatternNode.makeRepetition(p, r);
  }
  
  public PatternNode NewPatternRestartAnchor()
  {
    return PatternNode.makeRestartAnchor();
  }
  
  public PatternRepetition PatternRepetition(int mintimes, int maxtimes)
  {
    return new PatternRepetition(mintimes, maxtimes, 0);
  }
  
  public static Stmt MonitorStatement(List<String> param_list)
  {
    return new MonitorStmt(param_list);
  }
  
  public Stmt printMetaData(String v, String type, String objname)
    throws MetaDataRepositoryException
  {
    return new PrintMetaDataStmt(v, type, objname);
  }
  
  public static Stmt loadFile(String fname)
  {
    return new LoadFileStmt(fname.trim());
  }
  
  public static Stmt QuitStmt()
  {
    return new QuitStmt();
  }
  
  public Stmt GrantStatement(@NotNull List<ObjectPermission.Action> listOfPrivilege, @Nullable List<ObjectPermission.ObjectType> objectType, @NotNull String name, @Nullable String userName, @Nullable String roleName)
  {
    String objName = roleName != null ? roleName : userName != null ? userName : null;
    if (objName == null) {
      throw new RuntimeException("Neither role nor user selected");
    }
    Stmt stmt = userName != null ? GrantPermissionToUser(listOfPrivilege, objectType, name, userName) : GrantPermissionToRole(listOfPrivilege, objectType, name, roleName);
    return stmt;
  }
  
  public Stmt RevokeStatement(List<ObjectPermission.Action> listOfPrivilege, @Nullable List<ObjectPermission.ObjectType> objectType, String name, @Nullable String userName, @Nullable String roleName)
  {
    String objName = roleName != null ? roleName : userName != null ? userName : null;
    if (objName == null) {
      throw new RuntimeException("Neither role nor user selected");
    }
    Stmt stmt = userName != null ? RevokePermissionFromUser(listOfPrivilege, objectType, name, userName) : RevokePermissionFromRole(listOfPrivilege, objectType, name, roleName);
    return stmt;
  }
  
  public static Stmt CreateDumpStatement(String cqname, Integer dumpmode, Integer action)
  {
    return new DumpStmt(cqname, dumpmode.intValue(), action.intValue());
  }
  
  public static Stmt CreateAlterStmt(String objectName, Boolean enablePartitionBy, List<String> partitionBy, Boolean enablePersistence, StreamPersistencePolicy persistencePolicy)
  {
    return new AlterStmt(objectName, enablePartitionBy, partitionBy, enablePersistence, persistencePolicy);
  }
  
  public static StreamPersistencePolicy createStreamPersistencePolicy(String fullyQualifiedNameOfPropSet)
  {
    return new StreamPersistencePolicy(fullyQualifiedNameOfPropSet);
  }
  
  public ExportStreamSchemaStmt ExportStreamSchema(String streamname, String opt_path, String opt_filename)
  {
    return new ExportStreamSchemaStmt(streamname, opt_path, opt_filename);
  }
  
  public Stmt Export(String name, String schema)
  {
    if (name.equalsIgnoreCase("metadata")) {
      return ExportData(name);
    }
    return ExportStreamSchema(name, null, null);
  }
  
  public Stmt Export(String name, String schema, String where, String filename)
  {
    if (name.equalsIgnoreCase("metadata")) {
      return ExportData(name, where);
    }
    return ExportStreamSchema(name, where, filename);
  }
}
