package com.bloom.runtime.compiler.stmts;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import org.apache.log4j.Logger;

import com.bloom.runtime.compiler.exprs.Predicate;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.select.DataSource;

public class Select
  implements Serializable
{
  private static final Logger logger = Logger.getLogger(Select.class.getName());
  public final boolean distinct;
  public final int kindOfStream;
  public final List<SelectTarget> targets;
  public List<DataSource> from;
  public final Predicate where;
  public final List<ValueExpr> groupBy;
  public final Predicate having;
  public final List<OrderByItem> orderBy;
  public final LimitClause limit;
  public final boolean linksrc;
  public final MatchClause match;
  
  public Select(boolean distinct, int kind, List<SelectTarget> targets, List<DataSource> from, Predicate where, List<ValueExpr> groupBy, Predicate having, List<OrderByItem> orderBy, LimitClause limit, boolean linksrc, MatchClause match)
  {
    this.distinct = distinct;
    this.kindOfStream = kind;
    this.targets = targets;
    this.from = from;
    this.where = where;
    this.groupBy = groupBy;
    this.having = having;
    this.orderBy = orderBy;
    this.limit = limit;
    this.linksrc = linksrc;
    this.match = match;
  }
  
  public String toString()
  {
    return "SELECT " + (this.distinct ? "DISTINCT " : "") + this.kindOfStream + "\n" + this.targets + "\nFROM " + this.from + "\nWHERE " + this.where + "\nGROUP BY " + this.groupBy + "\nHAVING " + this.having + (this.match != null ? "\n" + this.match : "");
  }
  
  public Select copyDeep()
  {
    Select select = new Select(this.distinct, this.kindOfStream, this.targets, this.from, null, null, null, null, null, false, this.match);
    try
    {
      return (Select)copy(select);
    }
    catch (Exception e)
    {
      if (logger.isDebugEnabled()) {
        logger.debug(e.getMessage(), e);
      }
    }
    return this;
  }
  
  public static Object copy(Object orig)
    throws Exception
  {
    Object obj = null;
    
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bos);
    out.writeObject(orig);
    out.flush();
    out.close();
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
    
    obj = in.readObject();
    
    return obj;
  }
}
