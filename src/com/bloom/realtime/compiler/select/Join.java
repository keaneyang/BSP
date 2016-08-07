package com.bloom.runtime.compiler.select;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;

import com.bloom.runtime.compiler.exprs.Predicate;

public class Join
{
  private static final int LEFT_OUTER_JOIN = 1;
  private static final int RIGHT_OUTER_JOIN = 2;
  
  public static enum Kind
  {
    INNER(0),  CROSS(0),  LEFT(1),  RIGHT(2),  FULL(3);
    
    private final int flags;
    
    private Kind(int flags)
    {
      this.flags = flags;
    }
    
    public boolean isInner()
    {
      return this.flags == 0;
    }
    
    public boolean isOuter()
    {
      return this.flags != 0;
    }
    
    public boolean isLeft()
    {
      return (this.flags & 0x1) > 0;
    }
    
    public boolean isRight()
    {
      return (this.flags & 0x2) > 0;
    }
  }
  
  public static abstract interface Node
  {
    public abstract BitSet getDataSets();
  }
  
  public static class LeafNode
    implements Join.Node
  {
    DataSet dataset;
    
    public BitSet getDataSets()
    {
      return this.dataset.id2bitset();
    }
    
    public String toString()
    {
      return "DS(" + this.dataset.getFullName() + " " + this.dataset.getID() + ")";
    }
  }
  
  public static class JoinNode
    implements Join.Node
  {
    public Join.Node left;
    public Join.Node right;
    Predicate joinCondition;
    public Join.Kind kindOfJoin;
    
    public BitSet getDataSets()
    {
      BitSet bs = this.left.getDataSets();
      bs.or(this.right.getDataSets());
      return bs;
    }
    
    public String toString()
    {
      return this.left + " " + this.kindOfJoin + " " + this.right + " ON " + this.joinCondition;
    }
  }
  
  public static class Tree
  {
    Join.Node root;
    LinkedHashMap<Integer, Join.JoinNode> joins = new LinkedHashMap();
    
    public void print(PrintStream out)
    {
      printNode(0, this.root, out);
    }
    
    public String toString()
    {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(os);
      print(ps);
      return os.toString();
    }
    
    private static void printNode(int level, Join.Node n, PrintStream out)
    {
      level++;
      String ident = new String(new char[level]).replace("\000", "--");
      if ((n instanceof Join.JoinNode))
      {
        Join.JoinNode jn = (Join.JoinNode)n;
        out.println(ident + " " + jn.kindOfJoin + " " + jn.joinCondition);
        printNode(level, jn.left, out);
        printNode(level, jn.right, out);
      }
      else
      {
        Join.LeafNode ln = (Join.LeafNode)n;
        out.println(ident + " source:" + ln.dataset);
      }
    }
    
    public Join.JoinNode getJoinPredicate(Predicate p)
    {
      return (Join.JoinNode)this.joins.get(Integer.valueOf(p.eid));
    }
    
    public void addNodeToJoinList(Join.Node n)
    {
      if ((n instanceof Join.JoinNode))
      {
        Join.JoinNode jn = (Join.JoinNode)n;
        if (jn.joinCondition != null) {
          this.joins.put(Integer.valueOf(jn.joinCondition.eid), jn);
        }
        addNodeToJoinList(jn.left);
        addNodeToJoinList(jn.right);
      }
    }
    
    public boolean isOuterJoin(BitSet alreadyJoined, int datasetID, List<Condition.CondExpr> condIndex)
    {
      boolean outerJoin = false;
      for (Condition.CondExpr e : condIndex) {
        if (e.isOuterJoinCondtion(alreadyJoined, datasetID))
        {
          outerJoin = true;
          break;
        }
      }
      if (outerJoin) {
        for (Condition.CondExpr e : condIndex) {
          if (e.rejectsNulls(datasetID)) {
            outerJoin = false;
          }
        }
      }
      return outerJoin;
    }
  }
  
  public static Node createDataSetNode(DataSet dataset)
  {
    LeafNode n = new LeafNode();
    n.dataset = dataset;
    return n;
  }
  
  public static Node createJoinNode(Node left, Node right, Predicate joinCond, Kind kind)
  {
    JoinNode n = new JoinNode();
    n.left = left;
    n.right = right;
    n.joinCondition = joinCond;
    n.kindOfJoin = kind;
    return n;
  }
  
  public static Tree createJoinTree(Node root)
  {
    Tree t = new Tree();
    t.root = root;
    t.addNodeToJoinList(root);
    return t;
  }
}
