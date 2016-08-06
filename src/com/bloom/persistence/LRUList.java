package com.bloom.persistence;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class LRUList
{
  Hashtable<Object, LRUList.ListElement> listIndex;
  LRUList.ListElement head;
  LRUList.ListElement tail;
  
  public LRUList()
  {
    this.listIndex = new Hashtable();
    this.head = null;
    this.tail = null;
  }
  
  public LRUList add(Object item)
  {
    synchronized (this.listIndex)
    {
      LRUList.ListElement element = (ListElement)this.listIndex.get(item);
      if (element != null)
      {
        remove(element);
      }
      else
      {
        element = new ListElement(item);
        this.listIndex.put(item, element);
      }
      insertBeginning(element);
    }
    return this;
  }
  
  public LRUList remove(Object item)
  {
    synchronized (this.listIndex)
    {
      LRUList.ListElement element = (ListElement)this.listIndex.get(item);
      if (element != null)
      {
        remove(element);
        this.listIndex.remove(item);
      }
    }
    return this;
  }
  
  public Object removeLeastRecentlyUsed()
  {
	Object item = null;
    synchronized (this.listIndex)
    {
      if (this.tail == null) {
        return null;
      }
      LRUList.ListElement returnValue = this.tail;
      remove(this.tail);
      this.listIndex.remove(returnValue.item);
      item = returnValue.item;
    }
    return item;
  }
  
  private void insertBefore(LRUList.ListElement element, LRUList.ListElement newElement)
  {
    synchronized (this.listIndex)
    {
      newElement.prev = element.prev;
      newElement.next = element;
      if (element.prev == null) {
        this.head = newElement;
      } else {
        element.prev.next = newElement;
      }
      element.prev = newElement;
    }
  }
  
  protected void insertBeginning(LRUList.ListElement newElement)
  {
    synchronized (this.listIndex)
    {
      if (this.head == null)
      {
        this.head = newElement;
        this.tail = newElement;
        newElement.prev = null;
        newElement.next = null;
      }
      else
      {
        insertBefore(this.head, newElement);
      }
    }
  }
  
  private void remove(LRUList.ListElement element)
  {
    synchronized (this.listIndex)
    {
      if (element.prev == null) {
        this.head = element.next;
      } else {
        element.prev.next = element.next;
      }
      if (element.next == null) {
        this.tail = element.prev;
      } else {
        element.next.prev = element.prev;
      }
    }
  }
  
  public List toList()
  {
    ArrayList retList = new ArrayList();
    synchronized (this.listIndex)
    {
      LRUList.ListElement element = this.head;
      while (element != null)
      {
        retList.add(element.item);
        element = element.next;
      }
    }
    return retList;
  }
  
  public int size()
  {
    return this.listIndex.size();
  }
  
  private class ListElement
  {
    public LRUList.ListElement prev = null;
    public LRUList.ListElement next = null;
    public Object item;
    
    public ListElement(Object item)
    {
      this.item = item;
    }
  }
}
