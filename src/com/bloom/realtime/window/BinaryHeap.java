package com.bloom.runtime.window;

import java.util.ArrayList;
import java.util.List;

public class BinaryHeap<T>
{
  public static class Item<T>
  {
    final T value;
    long key;
    int index;
    
    Item(T value, long key)
    {
      this.value = value;
      this.key = key;
    }
    
    public String toString()
    {
      return this.key + ":" + this.value;
    }
  }
  
  private final List<Item<T>> heap = new ArrayList();
  
  public void push(Item<T> it)
  {
    it.index = this.heap.size();
    this.heap.add(it);
    pushUp(it.index);
  }
  
  public Item<T> pop()
  {
    if (this.heap.size() > 0)
    {
      swap(0, this.heap.size() - 1);
      Item<T> result = (Item)this.heap.remove(this.heap.size() - 1);
      pushDown(0);
      return result;
    }
    return null;
  }
  
  public Item<T> getFirst()
  {
    return get(0);
  }
  
  public Item<T> get(int index)
  {
    return (Item)this.heap.get(index);
  }
  
  public int size()
  {
    return this.heap.size();
  }
  
  public boolean isEmpty()
  {
    return this.heap.isEmpty();
  }
  
  private boolean isKeyGreaterOrEqual(int first, int last)
  {
    return isGreaterOrEqual(get(first).key, get(last).key);
  }
  
  private static boolean isGreaterOrEqual(long key1, long key2)
  {
    return key2 >= key1;
  }
  
  private int parent(int i)
  {
    return (i - 1) / 2;
  }
  
  private int left(int i)
  {
    return 2 * i + 1;
  }
  
  private int right(int i)
  {
    return 2 * i + 2;
  }
  
  private void swap(int i, int j)
  {
    Item<T> tmpi = (Item)this.heap.get(i);
    Item<T> tmpj = (Item)this.heap.get(j);
    tmpj.index = i;
    this.heap.set(i, tmpj);
    tmpi.index = j;
    this.heap.set(j, tmpi);
  }
  
  public void check(int i)
  {
    if (((Item)this.heap.get(i)).index != i) {
      throw new RuntimeException("index error");
    }
    Item<T> val = get(i);
    int left = left(i);
    if (left < this.heap.size())
    {
      Item<T> lval = get(left);
      if (!isGreaterOrEqual(val.key, lval.key)) {
        throw new RuntimeException("broken heap");
      }
      check(left);
    }
    int right = right(i);
    if (right < this.heap.size())
    {
      Item<T> rval = get(right);
      if (!isGreaterOrEqual(val.key, rval.key)) {
        throw new RuntimeException("broken heap");
      }
      check(right);
    }
  }
  
  private void pushDown(int i)
  {
    int left = left(i);
    int right = right(i);
    int largest = i;
    if ((left < this.heap.size()) && (!isKeyGreaterOrEqual(largest, left))) {
      largest = left;
    }
    if ((right < this.heap.size()) && (!isKeyGreaterOrEqual(largest, right))) {
      largest = right;
    }
    if (largest != i)
    {
      swap(largest, i);
      pushDown(largest);
    }
  }
  
  private void pushUp(int i)
  {
    while ((i > 0) && (!isKeyGreaterOrEqual(parent(i), i)))
    {
      swap(parent(i), i);
      i = parent(i);
    }
  }
  
  public void changePriority(Item<T> it, long newkey)
  {
    long oldkey = it.key;
    it.key = newkey;
    if (!isGreaterOrEqual(newkey, oldkey)) {
      pushDown(it.index);
    } else {
      pushUp(it.index);
    }
  }
  
  public String toString()
  {
    StringBuffer s = new StringBuffer("Heap:\n");
    int rowStart = 0;
    int rowSize = 1;
    for (int i = 0; i < this.heap.size(); i++)
    {
      if (i == rowStart + rowSize)
      {
        s.append('\n');
        rowStart = i;
        rowSize *= 2;
      }
      Item<T> it = (Item)this.heap.get(i);
      assert (it.index == i);
      s.append(it);
      s.append(" ");
    }
    return s.toString();
  }
  
  public List<T> toList()
  {
    List<T> l = new ArrayList();
    for (Item<T> it : this.heap) {
      l.add(it.value);
    }
    return l;
  }
}
