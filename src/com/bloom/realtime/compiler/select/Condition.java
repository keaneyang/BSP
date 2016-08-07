package com.bloom.runtime.compiler.select;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.bloom.runtime.Pair;
import com.bloom.runtime.compiler.exprs.ComparePredicate;
import com.bloom.runtime.compiler.exprs.DataSetRef;
import com.bloom.runtime.compiler.exprs.Expr;
import com.bloom.runtime.compiler.exprs.ExprCmd;
import com.bloom.runtime.compiler.exprs.FieldRef;
import com.bloom.runtime.compiler.exprs.Predicate;
import com.bloom.runtime.compiler.exprs.UnboxCastOperation;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.visitors.CheckNullRejection;
import com.bloom.runtime.utils.Factory;
import com.bloom.runtime.utils.NamePolicy;

public class Condition
  implements Comparable<Condition>
{
  public final List<CondExpr> exprs;
  public final BitSet dataSets;
  public final CondKey key;
  public final Map<Integer, List<Integer>> dsIndexes;
  public int idxCount;
  
  public static class CondKey
  {
    public final BitSet leftset;
    public final BitSet rightset;
    
    public CondKey(BitSet leftset, BitSet rightset)
    {
      this.leftset = leftset;
      this.rightset = rightset;
    }
    
    public int hashCode()
    {
      return 37 * this.leftset.hashCode() + this.rightset.hashCode();
    }
    
    public boolean equals(Object other)
    {
      if (this == other) {
        return true;
      }
      if (!(other instanceof CondKey)) {
        return false;
      }
      CondKey o = (CondKey)other;
      if ((this.leftset.equals(o.leftset)) && (this.rightset.equals(o.rightset))) {
        return true;
      }
      if ((this.leftset.equals(o.rightset)) && (this.rightset.equals(o.leftset))) {
        return true;
      }
      return false;
    }
    
    public boolean isJoinable()
    {
      return (!this.leftset.isEmpty()) && (!this.rightset.isEmpty());
    }
    
    public String toString()
    {
      return "CondKey(" + this.leftset + "=" + this.rightset + ")";
    }
  }
  
  public static class CondExpr
  {
    public final Join.JoinNode joinInfo;
    public final Predicate expr;
    public final BitSet datasets;
    
    public CondExpr(Predicate expr, BitSet datasets, Join.JoinNode joinInfo)
    {
      this.joinInfo = joinInfo;
      this.expr = expr;
      this.datasets = ((BitSet)datasets.clone());
    }
    
    public String toString()
    {
      return "(" + this.expr + ")" + (this.joinInfo == null ? "" : this.joinInfo);
    }
    
    public boolean similar(CondExpr other)
    {
      return false;
    }
    
    public boolean isIndexable(int relID)
    {
      return false;
    }
    
    public Expr getIndexExpr(int relID)
    {
      return null;
    }
    
    public Expr getSearchExpr(int relID)
    {
      return null;
    }
    
    public Condition.CondKey getCondKey()
    {
      return new Condition.CondKey(this.datasets, new BitSet());
    }
    
    public List<Pair<Integer, String>> getFields()
    {
      return null;
    }
    
    public boolean innerTableFilterInOuterJoin(int ds)
    {
      if ((this.joinInfo != null) && (this.joinInfo.kindOfJoin.isOuter()))
      {
        if ((this.joinInfo.kindOfJoin.isLeft()) && 
          (this.joinInfo.right.getDataSets().get(ds))) {
          return true;
        }
        if ((this.joinInfo.kindOfJoin.isRight()) && 
          (this.joinInfo.left.getDataSets().get(ds))) {
          return true;
        }
      }
      return false;
    }
    
    public boolean isOuterJoinCondtion(BitSet alreadyJoined, int datasetID)
    {
      if ((this.joinInfo != null) && (this.joinInfo.kindOfJoin.isOuter()))
      {
        if (this.joinInfo.kindOfJoin.isLeft())
        {
          BitSet outer = this.joinInfo.left.getDataSets();
          BitSet inner = this.joinInfo.right.getDataSets();
          if ((outer.intersects(alreadyJoined)) && (inner.get(datasetID))) {
            return true;
          }
        }
        if (this.joinInfo.kindOfJoin.isRight())
        {
          BitSet outer = this.joinInfo.right.getDataSets();
          BitSet inner = this.joinInfo.left.getDataSets();
          if ((outer.intersects(alreadyJoined)) && (inner.get(datasetID))) {
            return true;
          }
        }
      }
      return false;
    }
    
    public boolean acceptNulls(int datasetID)
    {
      return CheckNullRejection.acceptNulls(this.expr, datasetID);
    }
    
    public boolean rejectsNulls(int datasetID)
    {
      if ((this.joinInfo == null) || (this.joinInfo.kindOfJoin.isInner())) {
        return (this.datasets.get(datasetID)) && (!acceptNulls(datasetID));
      }
      return false;
    }
    
    private Pair<FieldRef, ValueExpr> isEqPred(ValueExpr l, ValueExpr r)
    {
      if (r.isConst())
      {
        if ((l instanceof UnboxCastOperation)) {
          l = ((UnboxCastOperation)l).getExpr();
        }
        if ((l instanceof FieldRef)) {
          return Pair.make((FieldRef)l, r);
        }
      }
      return null;
    }
    
    public Pair<FieldRef, ValueExpr> isEqPredicate()
    {
      if (this.expr.op == ExprCmd.EQ)
      {
        ComparePredicate cp = (ComparePredicate)this.expr;
        if (cp.args.size() == 2)
        {
          ValueExpr e1 = (ValueExpr)cp.args.get(0);
          ValueExpr e2 = (ValueExpr)cp.args.get(1);
          Pair<FieldRef, ValueExpr> ret = isEqPred(e1, e2);
          if (ret == null) {
            ret = isEqPred(e2, e1);
          }
          return ret;
        }
      }
      return null;
    }
  }
  
  public static class JoinExpr
    extends Condition.CondExpr
  {
    public final Expr left;
    public final BitSet leftset;
    public final Expr right;
    public final BitSet rightset;
    
    public JoinExpr(Predicate expr, BitSet datasets, Expr left, BitSet leftset, Expr right, BitSet rightset, Join.JoinNode joinInfo)
    {
      super(datasets, joinInfo);
      this.left = left;
      this.leftset = ((BitSet)leftset.clone());
      this.right = right;
      this.rightset = ((BitSet)rightset.clone());
    }
    
    public String toString()
    {
      return "(" + this.expr + ")" + " {" + this.leftset + "} (" + this.left + ") = (" + this.right + ") {" + this.rightset + "}";
    }
    
    public boolean similar(Condition.CondExpr other)
    {
      if ((other instanceof JoinExpr))
      {
        JoinExpr o = (JoinExpr)other;
        if ((this.leftset.equals(o.leftset)) && (this.rightset.equals(o.rightset))) {
          return true;
        }
        if ((this.leftset.equals(o.rightset)) && (this.rightset.equals(o.leftset))) {
          return true;
        }
      }
      return false;
    }
    
    public boolean isIndexable(int relID)
    {
      if ((this.leftset.get(relID)) && (this.leftset.cardinality() == 1)) {
        return true;
      }
      if ((this.rightset.get(relID)) && (this.rightset.cardinality() == 1)) {
        return true;
      }
      return false;
    }
    
    public Expr getIndexExpr(int relID)
    {
      assert (isIndexable(relID));
      if (this.leftset.get(relID))
      {
        assert (this.leftset.cardinality() == 1);
        return this.left;
      }
      assert (this.rightset.get(relID));
      assert (this.rightset.cardinality() == 1);
      return this.right;
    }
    
    public Expr getSearchExpr(int relID)
    {
      assert (isIndexable(relID));
      if (this.leftset.get(relID))
      {
        assert (this.leftset.cardinality() == 1);
        return this.right;
      }
      assert (this.rightset.get(relID));
      assert (this.rightset.cardinality() == 1);
      return this.left;
    }
    
    public Condition.CondKey getCondKey()
    {
      return new Condition.CondKey(this.leftset, this.rightset);
    }
    
    public List<Pair<Integer, String>> getFields()
    {
      List<Pair<Integer, String>> ret = new ArrayList(2);
      if ((this.left instanceof FieldRef))
      {
        FieldRef f = (FieldRef)this.left;
        ValueExpr obj = f.getExpr();
        if ((obj instanceof DataSetRef))
        {
          DataSetRef ref = (DataSetRef)obj;
          ret.add(Pair.make(Integer.valueOf(ref.getDataSet().getID()), f.getName()));
        }
      }
      if ((this.right instanceof FieldRef))
      {
        FieldRef f = (FieldRef)this.right;
        ValueExpr obj = f.getExpr();
        if ((obj instanceof DataSetRef))
        {
          DataSetRef ref = (DataSetRef)obj;
          ret.add(Pair.make(Integer.valueOf(ref.getDataSet().getID()), f.getName()));
        }
      }
      return ret;
    }
  }
  
  public Condition(CondExpr firstExpr)
  {
    this.exprs = new LinkedList();
    this.exprs.add(firstExpr);
    this.dataSets = firstExpr.datasets;
    this.key = firstExpr.getCondKey();
    this.dsIndexes = Factory.makeMap();
    this.idxCount = 0;
  }
  
  public String toString()
  {
    return this.dataSets + " " + this.exprs;
  }
  
  public int compareTo(Condition o)
  {
    if (this == o) {
      return 0;
    }
    int c1 = this.dataSets.cardinality();
    int c2 = o.dataSets.cardinality();
    if (c1 != c2) {
      return c1 < c2 ? -1 : 1;
    }
    if (this.idxCount > o.idxCount) {
      return -1;
    }
    if (this.idxCount < o.idxCount) {
      return 1;
    }
    return 0;
  }
  
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Condition)) {
      return false;
    }
    Condition other = (Condition)o;
    return (this.exprs.equals(other.exprs)) && (this.dataSets.equals(other.dataSets)) && (this.key.equals(other.key));
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(this.key.hashCode()).append(this.dataSets.hashCode()).append(this.exprs.hashCode()).toHashCode();
  }
  
  public boolean isIndexable(int relID)
  {
    assert (this.dataSets.get(relID));
    return ((CondExpr)this.exprs.get(0)).isIndexable(relID);
  }
  
  public boolean isSimilar(CondExpr e)
  {
    return ((CondExpr)this.exprs.get(0)).similar(e);
  }
  
  public List<Expr> getIndexExprs(int relID)
  {
    List<Expr> indexVector = new ArrayList();
    for (CondExpr e : this.exprs) {
      indexVector.add(e.getIndexExpr(relID));
    }
    return indexVector;
  }
  
  public List<Expr> getSearchExprs(int relID)
  {
    List<Expr> searchVector = new ArrayList();
    for (CondExpr e : this.exprs) {
      searchVector.add(e.getSearchExpr(relID));
    }
    return searchVector;
  }
  
  public boolean isJoinable()
  {
    return this.key.isJoinable();
  }
  
  public List<CondExpr> getCondExprs()
  {
    return this.exprs;
  }
  
  public Map<Integer, Set<String>> getCondFields()
  {
    Map<Integer, Set<String>> ret = Factory.makeMap();
    for (CondExpr e : this.exprs)
    {
      List<Pair<Integer, String>> flds = e.getFields();
      for (Pair<Integer, String> p : flds)
      {
        Set<String> set = (Set)ret.get(p.first);
        if (set == null)
        {
          set = new TreeSet(String.CASE_INSENSITIVE_ORDER);
          ret.put(p.first, set);
        }
        set.add(p.second);
      }
    }
    return ret;
  }
  
  public void updateFieldIndexesSupport(int dsID, List<Integer> lst)
  {
    assert (this.dsIndexes.get(Integer.valueOf(dsID)) == null);
    this.dsIndexes.put(Integer.valueOf(dsID), lst);
    this.idxCount += lst.size();
  }
  
  private static int findKeyFieldIndex(List<String> indexFieldList, String name)
  {
    int i = 0;
    for (String fieldName : indexFieldList)
    {
      if (NamePolicy.isEqual(name, fieldName)) {
        return i;
      }
      i++;
    }
    return -1;
  }
  
  public Pair<List<Expr>, List<Predicate>> getIndexSearchAndPostCondition(int datasetID, List<String> indexFieldList)
  {
    List<Expr> idxExprs = getIndexExprs(datasetID);
    List<Expr> searchExprs = getSearchExprs(datasetID);
    assert (idxExprs.size() == searchExprs.size());
    Expr[] searchExprsForIndex = new Expr[indexFieldList.size()];
    List<Predicate> postFilters = new ArrayList();
    Iterator<Expr> it = searchExprs.iterator();
    for (Expr e : idxExprs)
    {
      Expr se = (Expr)it.next();
      int fieldIndex = -1;
      if ((e instanceof FieldRef))
      {
        FieldRef f = (FieldRef)e;
        ValueExpr obj = f.getExpr();
        if ((obj instanceof DataSetRef))
        {
          DataSetRef ref = (DataSetRef)obj;
          assert (datasetID == ref.getDataSet().getID());
          fieldIndex = findKeyFieldIndex(indexFieldList, f.getName());
        }
      }
      if (fieldIndex != -1)
      {
        searchExprsForIndex[fieldIndex] = se;
      }
      else
      {
        Predicate postFilter = new ComparePredicate(ExprCmd.EQ, Arrays.asList(new ValueExpr[] { (ValueExpr)se, (ValueExpr)e }));
        
        postFilters.add(postFilter);
      }
    }
    for (Expr e : searchExprsForIndex) {
      assert (e != null);
    }
    List<Expr> searchExprsForIndexList = Arrays.asList(searchExprsForIndex);
    return Pair.make(searchExprsForIndexList, postFilters);
  }
  
  public List<Pair<FieldRef, ValueExpr>> getEqExprs()
  {
    List<Pair<FieldRef, ValueExpr>> ret = new ArrayList();
    for (CondExpr e : this.exprs)
    {
      Pair<FieldRef, ValueExpr> p = e.isEqPredicate();
      if (p != null) {
        ret.add(p);
      }
    }
    return ret;
  }
}
