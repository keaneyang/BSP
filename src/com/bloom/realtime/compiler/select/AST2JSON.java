package com.bloom.runtime.compiler.select;

import com.bloom.runtime.compiler.exprs.CaseExpr;
import com.bloom.runtime.compiler.exprs.CastExpr;
import com.bloom.runtime.compiler.exprs.CastOperation;
import com.bloom.runtime.compiler.exprs.ComparePredicate;
import com.bloom.runtime.compiler.exprs.Constant;
import com.bloom.runtime.compiler.exprs.DataSetRef;
import com.bloom.runtime.compiler.exprs.Expr;
import com.bloom.runtime.compiler.exprs.ExprCmd;
import com.bloom.runtime.compiler.exprs.FieldRef;
import com.bloom.runtime.compiler.exprs.FuncCall;
import com.bloom.runtime.compiler.exprs.LogicalPredicate;
import com.bloom.runtime.compiler.exprs.NumericOperation;
import com.bloom.runtime.compiler.exprs.Predicate;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.stmts.OrderByItem;
import com.bloom.runtime.compiler.visitors.ExpressionVisitorDefaultImpl;
import com.bloom.wactionstore.constants.Capability;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

public class AST2JSON
{
  private final Set<Capability> caps;
  
  private static enum ExprKind
  {
    PROJECTION,  WHERE,  HAVING,  GROUPBY,  ORDERBY;
    
    private ExprKind() {}
  }
  
  public AST2JSON(Set<Capability> capabilities)
  {
    this.caps = capabilities;
  }
  
  private void check(Capability cap)
  {
    if (!this.caps.contains(cap)) {
      throw new InvalidWASQueryExprException();
    }
  }
  
  public JsonNode transformFilter(DataSet ds, Condition filter)
  {
    try
    {
      ObjectNode stmt = JsonNodeFactory.instance.objectNode();
      ArrayNode select = array();
      select.add(text("*"));
      check(Capability.SELECT);
      stmt.set("select", select);
      ArrayNode from = array();
      from.add(text(ds.getFullName()));
      check(Capability.FROM);
      stmt.set("from", from);
      if (filter != null)
      {
        ArrayNode where = array();
        for (Condition.CondExpr cx : filter.getCondExprs()) {
          try
          {
            JsonNode expr = genExpr(cx.expr, ExprKind.WHERE);
            where.add(expr);
          }
          catch (InvalidWASQueryExprException e) {}
        }
        int size = where.size();
        if (size > 0)
        {
          JsonNode val = size > 1 ? obj1("and", where) : where.get(0);
          check(Capability.WHERE);
          stmt.set("where", val);
        }
      }
      return stmt;
    }
    catch (InvalidWASQueryExprException e) {}
    return null;
  }
  
  public static void addTimeBounds(JsonNode query, long startTime, long endTime)
  {
    JsonNode start = makePredicate(getCmpOp(ExprCmd.GTEQ), text("$timestamp"), num(startTime));
    
    JsonNode end = makePredicate(getCmpOp(ExprCmd.LTEQ), text("$timestamp"), num(endTime));
    
    ObjectNode q = (ObjectNode)query;
    JsonNode where = q.get("where");
    if (where != null)
    {
      ArrayNode wand = (ArrayNode)where.get("and");
      if (wand != null)
      {
        wand.insert(0, start);
        wand.insert(1, end);
      }
      else
      {
        ArrayNode and = array();
        and.add(start);
        and.add(end);
        and.add(where);
        q.set("where", obj1("and", and));
      }
    }
    else
    {
      ArrayNode and = array();
      and.add(start);
      and.add(end);
      q.set("where", obj1("and", and));
    }
  }
  
  public JsonNode transform(DataSets dataSets, List<Predicate> predicates, List<SelectCompiler.Target> targets, List<ValueExpr> groupBy, Predicate having, List<OrderByItem> orderBy)
  {
    try
    {
      ObjectNode stmt = JsonNodeFactory.instance.objectNode();
      generateSelect(stmt, targets);
      generateFrom(stmt, dataSets);
      generateWhere(stmt, predicates);
      generateGroupBy(stmt, groupBy);
      generateHaving(stmt, having);
      generateOrderBy(stmt, orderBy);
      return stmt;
    }
    catch (InvalidWASQueryExprException e) {}
    return null;
  }
  
  public static byte[] serialize(JsonNode n)
  {
    ObjectMapper mapper = new ObjectMapper();
    try
    {
      return mapper.writeValueAsBytes(n);
    }
    catch (JsonProcessingException e)
    {
      throw new RuntimeException(e);
    }
  }
  
  public static JsonNode deserialize(byte[] image)
  {
    ObjectMapper mapper = new ObjectMapper();
    try
    {
      return mapper.readTree(image);
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }
  }
  
  public static String prettyPrint(JsonNode n)
  {
    ObjectMapper mapper = new ObjectMapper();
    try
    {
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(n);
    }
    catch (JsonProcessingException e)
    {
      throw new RuntimeException(e);
    }
  }
  
  public static Expr skipCastOps(Expr e)
  {
    while ((e instanceof CastOperation))
    {
      CastOperation c = (CastOperation)e;
      e = (Expr)c.args.get(0);
    }
    return e;
  }
  
  private static NumericNode num(long x)
  {
    return JsonNodeFactory.instance.numberNode(x);
  }
  
  private static ArrayNode array()
  {
    return JsonNodeFactory.instance.arrayNode();
  }
  
  private static JsonNode text(String s)
  {
    TextNode n = JsonNodeFactory.instance.textNode(s);
    return n;
  }
  
  private static JsonNode bool(boolean b)
  {
    BooleanNode n = JsonNodeFactory.instance.booleanNode(b);
    return n;
  }
  
  private static JsonNode obj1(String key, JsonNode value)
  {
    ObjectNode ret = JsonNodeFactory.instance.objectNode();
    ret.set(key, value);
    return ret;
  }
  
  private static JsonNode obj2(String key1, JsonNode value1, String key2, JsonNode value2)
  {
    ObjectNode ret = JsonNodeFactory.instance.objectNode();
    ret.set(key1, value1);
    ret.set(key2, value2);
    return ret;
  }
  
  private static JsonNode makePredicate(String oper, JsonNode attr, JsonNode val)
  {
    ObjectNode pred = JsonNodeFactory.instance.objectNode();
    pred.set("oper", text(oper));
    pred.set("attr", attr);
    if (val.isArray())
    {
      ArrayNode a = (ArrayNode)val;
      if (a.size() > 1) {
        pred.set("values", a);
      } else {
        pred.set("value", a.get(0));
      }
    }
    else
    {
      pred.set("value", val);
    }
    return pred;
  }
  
  private JsonNode genExpr(Expr e, ExprKind kind)
  {
    Expression2JsonTransformer t = new Expression2JsonTransformer(e, kind);
    return t.result;
  }
  
  private void generateSelect(ObjectNode stmt, List<SelectCompiler.Target> targets)
  {
    ArrayNode select = array();
    for (SelectCompiler.Target t : targets)
    {
      JsonNode expr = genExpr(t.expr, ExprKind.PROJECTION);
      select.add(expr);
    }
    check(Capability.SELECT);
    stmt.set("select", select);
  }
  
  private void generateFrom(ObjectNode stmt, DataSets dataSets)
  {
    ArrayNode from = array();
    for (DataSet ds : dataSets)
    {
      JsonNode val = text(ds.getFullName());
      from.add(val);
    }
    check(Capability.FROM);
    stmt.set("from", from);
  }
  
  private void generateWhere(ObjectNode stmt, List<Predicate> predicates)
  {
    if (!predicates.isEmpty())
    {
      ArrayNode where = array();
      for (Predicate p : predicates)
      {
        JsonNode expr = genExpr(p, ExprKind.WHERE);
        where.add(expr);
      }
      JsonNode val = where.size() > 1 ? obj1("and", where) : where.get(0);
      check(Capability.WHERE);
      stmt.set("where", val);
    }
  }
  
  private void generateGroupBy(ObjectNode stmt, List<ValueExpr> groupBy)
  {
    if ((groupBy != null) && (!groupBy.isEmpty()))
    {
      ArrayNode groupby = array();
      for (ValueExpr e : groupBy)
      {
        JsonNode expr = genExpr(e, ExprKind.GROUPBY);
        groupby.add(expr);
      }
      check(Capability.GROUP_BY);
      stmt.set("groupby", groupby);
    }
  }
  
  private void generateHaving(ObjectNode stmt, Predicate having)
  {
    if (having != null)
    {
      JsonNode expr = genExpr(having, ExprKind.HAVING);
      check(Capability.HAVING);
      stmt.set("having", expr);
    }
  }
  
  private void generateOrderBy(ObjectNode stmt, List<OrderByItem> orderBy)
  {
    if ((orderBy != null) && (!orderBy.isEmpty()))
    {
      ArrayNode orderby = array();
      for (OrderByItem e : orderBy)
      {
        JsonNode expr = genExpr(e.expr, ExprKind.ORDERBY);
        orderby.add(obj2("attr", expr, "ascending", bool(e.isAscending)));
      }
      check(Capability.ORDER_BY);
      stmt.set("orderby", orderby);
    }
  }
  
  private static String getCmpOp(ExprCmd op)
  {
    switch (op)
    {
    case EQ: 
      return "eq";
    case NOTEQ: 
      return "neq";
    case GT: 
      return "gt";
    case GTEQ: 
      return "gte";
    case LT: 
      return "lt";
    case LTEQ: 
      return "lte";
    case ISNULL: 
      return "isnull";
    case LIKE: 
      return "like";
    case INLIST: 
      return "in";
    case BETWEEN: 
      return "between";
    }
    return null;
  }
  
  private static String getOp(ExprCmd op)
  {
    switch (op)
    {
    case UMINUS: 
      return "-";
    case UPLUS: 
      return "";
    case PLUS: 
      return "+";
    case MINUS: 
      return "-";
    case DIV: 
      return "/";
    case MUL: 
      return "*";
    case MOD: 
      return "%";
    case BITAND: 
      return "&";
    case BITOR: 
      return "|";
    case BITXOR: 
      return "^";
    case INVERT: 
      return "~";
    case LSHIFT: 
      return "<<";
    case RSHIFT: 
      return ">>";
    case URSHIFT: 
      return ">>>";
    }
    return null;
  }
  
  private static final DateTimeFormatter fmt = new DateTimeFormatterBuilder().append(ISODateTimeFormat.date()).appendLiteral(' ').append(ISODateTimeFormat.time()).toFormatter();
  
  private static class Jexpr
  {
    final ArrayNode args = AST2JSON.access$100();
    JsonNode value;
    
    public String toString()
    {
      return "JEXPR(value:" + this.value + ", args:" + this.args + ")";
    }
  }
  
  private class Expression2JsonTransformer
    extends ExpressionVisitorDefaultImpl<AST2JSON.Jexpr>
  {
    private final AST2JSON.ExprKind kind;
    JsonNode result;
    
    public Expression2JsonTransformer(Expr e, AST2JSON.ExprKind kind)
    {
      if ((e instanceof DataSetRef)) {
        throw new AST2JSON.InvalidWASQueryExprException();
      }
      if ((e instanceof Constant)) {
        throw new AST2JSON.InvalidWASQueryExprException();
      }
      this.kind = kind;
      AST2JSON.Jexpr ret = new AST2JSON.Jexpr();
      visitExpr(e, ret);
      this.result = ret.args.get(0);
    }
    
    public Expr visitExpr(Expr e, AST2JSON.Jexpr ret)
    {
      AST2JSON.Jexpr var = new AST2JSON.Jexpr();
      
      e.visitArgs(this, var);
      
      e.visit(this, var);
      ret.args.add(var.value);
      return null;
    }
    
    public Expr visitExprDefault(Expr e, AST2JSON.Jexpr ret)
    {
      throw new AST2JSON.InvalidWASQueryExprException();
    }
    
    public Expr visitNumericOperation(NumericOperation operation, AST2JSON.Jexpr ret)
    {
      throw new AST2JSON.InvalidWASQueryExprException();
    }
    
    public Expr visitConstant(Constant constant, AST2JSON.Jexpr ret)
    {
      assert (ret.args.size() == 0);
      Object cval = constant.value;
      JsonNode value = null;
      switch (constant.op.ordinal())
      {
      case 25: 
      case 26: 
      case 27: 
      case 28: 
      case 29: 
        value = AST2JSON.text(cval.toString());
        break;
      case 30: 
        value = AST2JSON.obj2("unit", AST2JSON.access$300("seconds"), "amount", AST2JSON.access$300("" + ((Long)cval).longValue() / 1000000L));
        
        break;
      case 31: 
        value = JsonNodeFactory.instance.nullNode();
        break;
      case 32: 
        value = AST2JSON.text((String)cval);
        break;
      case 33: 
        Timestamp t = (Timestamp)cval;
        DateTime d = new DateTime(t.getTime());
        value = AST2JSON.text(d.toString(AST2JSON.fmt));
        break;
      case 34: 
        DateTime dt = (DateTime)cval;
        value = AST2JSON.text(dt.toString(AST2JSON.fmt));
        break;
      default: 
        break;
      }
      ret.value = value;
      return null;
    }
    
    public Expr visitCase(CaseExpr caseExpr, AST2JSON.Jexpr ret)
    {
      throw new AST2JSON.InvalidWASQueryExprException();
    }
    
    private JsonNode transformAggFuncName(String name)
    {
      name = name.toLowerCase();
      switch (name)
      {
      case "avg": 
        AST2JSON.this.check(Capability.AGGREGATION_AVG); break;
      case "count": 
        AST2JSON.this.check(Capability.AGGREGATION_COUNT); break;
      case "sum": 
        AST2JSON.this.check(Capability.AGGREGATION_SUM); break;
      case "min": 
        AST2JSON.this.check(Capability.AGGREGATION_MIN); break;
      case "max": 
        AST2JSON.this.check(Capability.AGGREGATION_MAX); break;
      case "first": 
        AST2JSON.this.check(Capability.AGGREGATION_FIRST); break;
      case "last": 
        AST2JSON.this.check(Capability.AGGREGATION_LAST); break;
      default: 
        throw new AST2JSON.InvalidWASQueryExprException();
      }
      return AST2JSON.text(name);
    }
    
    public Expr visitFuncCall(FuncCall funcCall, AST2JSON.Jexpr ret)
    {
      String funcName = funcCall.getName();
      if (this.kind == AST2JSON.ExprKind.ORDERBY) {
        throw new AST2JSON.InvalidWASQueryExprException();
      }
      if (funcCall.getAggrDesc() != null)
      {
        JsonNode arg;
        if (ret.args.size() == 0)
        {
          assert (funcName.equalsIgnoreCase("count"));
          arg = AST2JSON.text("$id");
        }
        else
        {
          assert (ret.args.size() == 1);
          arg = ret.args.get(0);
        }
        if (funcCall.isDistinct()) {
          throw new AST2JSON.InvalidWASQueryExprException();
        }
        ret.value = AST2JSON.obj2("oper", transformAggFuncName(funcName), "attr", arg);
      }
      else if ((funcCall.isCustomFunc() != null) && (funcName.equalsIgnoreCase("anyattrlike")))
      {
        AST2JSON.this.check(Capability.LIKE);
        AST2JSON.this.check(Capability.DATA_ANY);
        if (!checkConst((Expr)funcCall.getFuncArgs().get(1))) {
          throw new AST2JSON.InvalidWASQueryExprException();
        }
        ret.value = AST2JSON.makePredicate("like", AST2JSON.access$300("*any"), ret.args.get(1));
      }
      else if (funcName.equalsIgnoreCase("compile__like__pattern"))
      {
        ret.value = ret.args.get(0);
      }
      else
      {
        throw new AST2JSON.InvalidWASQueryExprException();
      }
      return null;
    }
    
    public Expr visitFieldRef(FieldRef fieldRef, AST2JSON.Jexpr ret)
    {
      ret.value = AST2JSON.text(fieldRef.getName());
      return null;
    }
    
    public Expr visitLogicalPredicate(LogicalPredicate logicalPredicate, AST2JSON.Jexpr ret)
    {
      JsonNode value = null;
      switch (logicalPredicate.op.ordinal())
      {
      case 35: 
        value = AST2JSON.obj1("and", ret.args);
        break;
      case 36: 
        value = AST2JSON.obj1("or", ret.args);
        break;
      case 37: 
        value = AST2JSON.obj1("not", ret.args.get(0));
        break;
      default: 
        if (!$assertionsDisabled) {
          throw new AssertionError();
        }
        break;
      }
      ret.value = value;
      return null;
    }
    
    private boolean checkAttr(Expr e)
    {
      return AST2JSON.skipCastOps(e) instanceof FieldRef;
    }
    
    private boolean checkConst(Expr e)
    {
      return AST2JSON.skipCastOps(e) instanceof Constant;
    }
    
    private boolean validateCond(ComparePredicate p)
    {
      if (p.args.size() < 2) {
        return false;
      }
      Iterator<ValueExpr> args = p.args.iterator();
      if (!checkAttr((Expr)args.next())) {
        return false;
      }
      while (args.hasNext()) {
        if (!checkConst((Expr)args.next())) {
          return false;
        }
      }
      return true;
    }
    
    public Expr visitComparePredicate(ComparePredicate p, AST2JSON.Jexpr ret)
    {
      switch (p.op.ordinal())
      {
      case 38: 
        throw new AST2JSON.InvalidWASQueryExprException();
      case 7: 
      case 39: 
        throw new AST2JSON.InvalidWASQueryExprException();
      case 1: 
      case 2: 
      case 3: 
      case 4: 
      case 5: 
      case 6: 
      case 8: 
      case 9: 
      case 10: 
        if (!validateCond(p)) {
          throw new AST2JSON.InvalidWASQueryExprException();
        }
        break;
      case 11: 
      case 12: 
      case 13: 
      case 14: 
      case 15: 
      case 16: 
      case 17: 
      case 18: 
      case 19: 
      case 20: 
      case 21: 
      case 22: 
      case 23: 
      case 24: 
      case 25: 
      case 26: 
      case 27: 
      case 28: 
      case 29: 
      case 30: 
      case 31: 
      case 32: 
      case 33: 
      case 34: 
      case 35: 
      case 36: 
      case 37: 
      default: 
        if (!$assertionsDisabled) {
          throw new AssertionError();
        }
        break;
      }
      ArrayNode args = ret.args;
      JsonNode attr = args.remove(0);
      ret.value = AST2JSON.makePredicate(AST2JSON.access$900(p.op), attr, args);
      return null;
    }
    
    public Expr visitDataSetRef(DataSetRef dataSetRef, AST2JSON.Jexpr ret)
    {
      ret.value = AST2JSON.text(dataSetRef.getDataSet().getName());
      return null;
    }
    
    public Expr visitCastExpr(CastExpr castExpr, AST2JSON.Jexpr ret)
    {
      JsonNode value = ret.args.get(0);
      
      ret.value = value;
      return null;
    }
    
    public Expr visitCastOperation(CastOperation castOp, AST2JSON.Jexpr ret)
    {
      JsonNode value = ret.args.get(0);
      
      ret.value = value;
      return null;
    }
  }
  
  private static class InvalidWASQueryExprException
    extends RuntimeException
  {
    private static final long serialVersionUID = -5463707233439365065L;
  }
}
