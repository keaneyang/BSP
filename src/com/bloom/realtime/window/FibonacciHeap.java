package com.bloom.runtime.window;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class FibonacciHeap
{
  private Node min;
  private int n;
  
  public FibonacciHeap()
  {
    this.min = null;
    this.n = 0;
  }
  
  private void consolidate()
  {
    Node[] A = new Node[45];
    Node start = this.min;
    Node w = this.min;
    do
    {
      Node x = w;
      Node nextW = w.right;
      int d = x.degree;
      while (A[d] != null)
      {
        Node y = A[d];
        if (x.key > y.key)
        {
          Node temp = y;
          y = x;
          x = temp;
        }
        if (y == start) {
          start = start.right;
        }
        if (y == nextW) {
          nextW = nextW.right;
        }
        y.link(x);
        A[d] = null;
        d++;
      }
      A[d] = x;
      w = nextW;
    } while (w != start);
    this.min = start;
    for (Node a : A) {
      if ((a != null) && (a.key < this.min.key)) {
        this.min = a;
      }
    }
  }
  
  public void decreaseKey(Node x, long k)
  {
    x.key = k;
    Node y = x.parent;
    if ((y != null) && (k < y.key))
    {
      y.cut(x, this.min);
      y.cascadingCut(this.min);
    }
    if (k < this.min.key) {
      this.min = x;
    }
  }
  
  public boolean isEmpty()
  {
    return this.min == null;
  }
  
  public Node insert(Object x, long key)
  {
    Node node = new Node(x, key);
    if (this.min != null)
    {
      node.right = this.min;
      node.left = this.min.left;
      this.min.left = node;
      node.left.right = node;
      if (key < this.min.key) {
        this.min = node;
      }
    }
    else
    {
      this.min = node;
    }
    this.n += 1;
    return node;
  }
  
  public Node min()
  {
    return this.min;
  }
  
  public Node removeMin()
  {
    Node z = this.min;
    if (z == null) {
      return null;
    }
    if (z.child != null)
    {
      z.child.parent = null;
      for (Node x = Node.access$500(z).right; x != z.child; x = x.right) {
        x.parent = null;
      }
      Node minleft = this.min.left;
      Node zchildleft = Node.access$500(z).left;
      this.min.left = zchildleft;
      zchildleft.right = this.min;
      z.child.left = minleft;
      minleft.right = z.child;
    }
    z.left.right = z.right;
    z.right.left = z.left;
    if (z == z.right)
    {
      this.min = null;
    }
    else
    {
      this.min = z.right;
      consolidate();
    }
    this.n -= 1;
    return z;
  }
  
  public int size()
  {
    return this.n;
  }
  
  public static FibonacciHeap union(FibonacciHeap H1, FibonacciHeap H2)
  {
    FibonacciHeap H = new FibonacciHeap();
    if ((H1 != null) && (H2 != null))
    {
      H.min = H1.min;
      if (H.min != null)
      {
        if (H2.min != null)
        {
          H.min.right.left = H2.min.left;
          H2.min.left.right = H.min.right;
          H.min.right = H2.min;
          H2.min.left = H.min;
          if (H2.min.key < H1.min.key) {
            H.min = H2.min;
          }
        }
      }
      else {
        H.min = H2.min;
      }
      H1.n += H2.n;
    }
    return H;
  }
  
  public static class Node
  {
    private Object data;
    private long key;
    private Node parent;
    private Node child;
    private Node right;
    private Node left;
    private int degree;
    private boolean mark;
    
    long getKey()
    {
      return this.key;
    }
    
    Object getData()
    {
      return this.data;
    }
    
    public Node(Object data, long key)
    {
      this.data = data;
      this.key = key;
      this.right = this;
      this.left = this;
    }
    
    public void cascadingCut(Node min)
    {
      Node z = this.parent;
      if (z != null) {
        if (this.mark)
        {
          z.cut(this, min);
          z.cascadingCut(min);
        }
        else
        {
          this.mark = true;
        }
      }
    }
    
    public void cut(Node x, Node min)
    {
      x.left.right = x.right;
      x.right.left = x.left;
      this.degree -= 1;
      if (this.degree == 0) {
        this.child = null;
      } else if (this.child == x) {
        this.child = x.right;
      }
      x.right = min;
      x.left = min.left;
      min.left = x;
      x.left.right = x;
      x.parent = null;
      x.mark = false;
    }
    
    public void link(Node parent)
    {
      this.left.right = this.right;
      this.right.left = this.left;
      this.parent = parent;
      if (parent.child == null)
      {
        parent.child = this;
        this.right = this;
        this.left = this;
      }
      else
      {
        this.left = parent.child;
        this.right = parent.child.right;
        parent.child.right = this;
        this.right.left = this;
      }
      parent.degree += 1;
      this.mark = false;
    }
  }
  
  public <T> List<T> toList()
  {
    List<T> list = new ArrayList();
    if (this.min == null) {
      return list;
    }
    Stack<Node> stack = new Stack();
    stack.push(this.min);
    while (!stack.empty())
    {
      Node curr = (Node)stack.pop();
      list.add(curr.data);
      if (curr.child != null) {
        stack.push(curr.child);
      }
      Node start = curr;
      curr = curr.right;
      while (curr != start)
      {
        list.add(curr.data);
        if (curr.child != null) {
          stack.push(curr.child);
        }
        curr = curr.right;
      }
    }
    return list;
  }
  
  public String toString()
  {
    return "FibonacciHeap" + toList();
  }
}
