package com.bloom.runtime.window;

import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.runtime.Server;
import com.bloom.runtime.containers.Batch;
import com.bloom.uuid.UUID;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.bloom.recovery.Position;
import com.bloom.recovery.PositionResponse;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

public abstract class BufWindow
  implements Runnable
{
  static class Bucket
    extends AtomicReferenceArray<WEntry>
  {
    private static final long serialVersionUID = -5690701562483619810L;
    final long id;
    
    Bucket(WEntry[] vals, long id)
    {
      super();
      this.id = id;
    }
    
    public String toString()
    {
      char[] s = new char[length()];
      for (int i = 0; i < length(); i++) {
        s[i] = (get(i) == null ? 95 : '#');
      }
      return this.id + " [" + new String(s) + "]";
    }
  }
  
  private final ConcurrentSkipListMap<Long, Bucket> elements = new ConcurrentSkipListMap();
  private volatile transient long vHead = 0L;
  private volatile transient long vTail = 0L;
  private volatile transient long vOldestSnapshotHead = 0L;
  private final SnapshotRefIndex activeSnapshots;
  private final BufferManager owner;
  private RecordKey partKey;
  public static final int BUCKET_SIZE = 64;
  public static final long BUCKET_INDEX_MASK = 63L;
  public static final long BUCKET_MASK = -64L;
  private Bucket newvalues;
  Snapshot snapshot;
  Position recentEmittedEvents = null;
  final SlidingPolicy.OutputBuffer outputBuffer;
  
  protected BufWindow(BufferManager owner)
  {
    this.owner = owner;
    this.activeSnapshots = BufferManager.snIndexFac.create();
    this.snapshot = makeEmptySnapshot();
    this.outputBuffer = getFactory().createOutputBuffer(this);
  }
  
  public BufWindowFactory getFactory()
  {
    return this.owner.getFactory();
  }
  
  public WindowPolicy getPolicy()
  {
    return getFactory().getPolicy();
  }
  
  public void setPartKey(RecordKey pk)
  {
    this.partKey = pk;
  }
  
  public RecordKey getPartKey()
  {
    return this.partKey;
  }
  
  public int getLogicalSize()
  {
    return (int)(getTail() - getHead());
  }
  
  public int numOfElements()
  {
    return this.elements.size();
  }
  
  public int numOfActiveSnapshots()
  {
    return this.activeSnapshots.size();
  }
  
  boolean isEmpty()
  {
    return this.elements.isEmpty();
  }
  
  boolean hasNoActiveSnapshots()
  {
    return this.activeSnapshots.isEmpty();
  }
  
  boolean canBeShutDown()
  {
    return (isEmpty()) && (hasNoActiveSnapshots());
  }
  
  void shutdown()
  {
    cancel();
  }
  
  Snapshot getSnapshot()
  {
    return this.snapshot;
  }
  
  void addNewRows(IBatch newEntries)
  {
    long timestamp = System.nanoTime();
    for (Object o : newEntries)
    {
      WAEvent data = (WAEvent)o;
      long id = this.vTail;
      WEntry e = new WEntry(data, id, timestamp);
      int idx = (int)(id & 0x3F);
      if ((idx == 0) || (this.elements.isEmpty()))
      {
        this.newvalues = new Bucket(new WEntry[64], id);
        this.elements.put(Long.valueOf(id & 0xFFFFFFFFFFFFFFC0), this.newvalues);
      }
      this.newvalues.set(idx, e);
      this.vTail += 1L;
    }
    update(newEntries);
  }
  
  public void removeRefAndSetOldestSnapshot(SnapshotRef ref)
  {
    long newOldestSnapshot = this.activeSnapshots.removeAndGetOldest(ref);
    if (newOldestSnapshot == -1L) {
      newOldestSnapshot = this.snapshot.vHead;
    }
    clean(newOldestSnapshot);
  }
  
  private void checkInvariants()
  {
    long wHead = this.snapshot.vHead;
    long tail = getTail();
    assert (getHead() <= wHead);
    assert (wHead <= tail);
    assert (getHead() <= this.vOldestSnapshotHead);
    assert (this.vOldestSnapshotHead <= tail);
    assert (this.vOldestSnapshotHead <= wHead);
  }
  
  private WEntry get(long index)
  {
    assert (index >= getHead());
    assert (index < getTail());
    Bucket bucket = (Bucket)this.elements.get(Long.valueOf(index & 0xFFFFFFFFFFFFFFC0));
    if (bucket == null) {
      return null;
    }
    try
    {
      return (WEntry)bucket.get((int)(index & 0x3F));
    }
    catch (Throwable e)
    {
      throw e;
    }
  }
  
  private List<Bucket> getRange(long from, long to)
  {
    assert (from <= to);
    assert (from >= getHead());
    assert (to <= getTail());
    long from_bucket = from & 0xFFFFFFFFFFFFFFC0;
    long to_bucket = to - 1L & 0xFFFFFFFFFFFFFFC0;
    int nbuckets = (int)((to_bucket - from_bucket) / 64L) + 1;
    if (nbuckets == 1) {
      return Collections.singletonList(this.elements.get(Long.valueOf(from_bucket)));
    }
    long index = from_bucket;
    List<Bucket> range = new ArrayList(nbuckets);
    for (int i = 0; i < nbuckets; i++)
    {
      Bucket bucket = (Bucket)this.elements.get(Long.valueOf(index));
      range.add(bucket);
      index += 64L;
    }
    return range;
  }
  
  private void clean(long oldestSeen)
  {
    assert (this.vOldestSnapshotHead <= oldestSeen);
    assert (oldestSeen <= getTail());
    if (oldestSeen != this.vOldestSnapshotHead)
    {
      this.vOldestSnapshotHead = oldestSeen;
      if (oldestSeen == getTail())
      {
        this.elements.clear();
        this.vHead = getTail();
        this.owner.addEmptyBuffer(this);
      }
      else
      {
        long head = getHead();
        assert (oldestSeen >= head);
        if (oldestSeen > head)
        {
          ConcurrentNavigableMap<Long, Bucket> t = this.elements.headMap(Long.valueOf(oldestSeen & 0xFFFFFFFFFFFFFFC0));
          
          t.clear();
          this.vHead = oldestSeen;
        }
      }
    }
  }
  
  WAEvent getData(long index)
  {
    if (index == getTail()) {
      return null;
    }
    WEntry e = get(index);
    return e.data;
  }
  
  private final boolean isSmallSnapshot(int size)
  {
    return size <= 64;
  }
  
  Snapshot makeEmptySnapshot()
  {
    return new EmptySnapshot(getTail());
  }
  
  Snapshot makeSnapshot(Snapshot prev, int size)
  {
    long end = getTail();
    long start = end - size;
    if (start < prev.vHead) {
      return makeOverlappingSnapshot(prev, end);
    }
    return makeSnapshot(start, end);
  }
  
  Snapshot makeSnapshot(long start, long end)
  {
    assert (start <= end);
    assert (this.vOldestSnapshotHead <= getTail());
    assert (start >= this.vOldestSnapshotHead);
    assert (end <= getTail());
    int size = (int)(end - start);
    if (size == 0) {
      return new EmptySnapshot(start);
    }
    if (size == 1) {
      return new OneItemSnapshot(get(start));
    }
    if (isSmallSnapshot(size)) {
      return new BuffersListSnapshot(start, end, getRange(start, end));
    }
    return makeRangeSnapshot(start, end);
  }
  
  Snapshot makeOneItemSnapshot(long start)
  {
    assert (this.vOldestSnapshotHead <= getTail());
    assert (start >= this.vOldestSnapshotHead);
    assert (start <= getTail());
    return new OneItemSnapshot(get(start));
  }
  
  private Snapshot makeOverlappingSnapshot(Snapshot sn, long end)
  {
    int size = (int)(end - sn.vHead);
    if (size == sn.size()) {
      return sn;
    }
    assert (sn.vHead <= end);
    assert (end <= getTail());
    if (size == 1) {
      return new OneItemSnapshot(get(sn.vHead));
    }
    if (isSmallSnapshot(size)) {
      return new BuffersListSnapshot(sn.vHead, end, getRange(sn.vHead, end));
    }
    if ((sn instanceof MapRangeSnapshot)) {
      return new OverlappingSnapshot((MapRangeSnapshot)sn, end, this.elements);
    }
    return makeRangeSnapshot(sn.vHead, end);
  }
  
  private Snapshot makeRangeSnapshot(long start, long end)
  {
    Snapshot sn = new MapRangeSnapshot(start, end, this.elements);
    this.activeSnapshots.add(this, sn, this.owner.getRefQueue());
    return sn;
  }
  
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    long wHead = this.snapshot.vHead;
    sb.append(" PartKey:" + this.partKey);
    sb.append(" Head:" + getHead());
    sb.append(" Tail:" + getTail());
    sb.append(" Window:" + wHead);
    sb.append(" OldestSnapshot:" + this.vOldestSnapshotHead);
    sb.append("\n");
    if (this.elements.isEmpty())
    {
      sb.append("No data\n");
    }
    else
    {
      String sep = "";
      sb.append("[\n");
      for (Bucket vals : this.elements.values())
      {
        int len = vals.length();
        for (int j = 0; j < len; j++)
        {
          WEntry e = (WEntry)vals.get(j);
          if ((e == null) || ((e.id >= getHead()) && (e.id < getTail())))
          {
            if ((e != null) && (e.id == wHead)) {
              sb.append("|\n");
            } else {
              sb.append(sep);
            }
            sb.append(e);
            sep = ",\n";
          }
        }
      }
      sb.append("\n]\n");
    }
    sb.append("Window(" + getFactory() + " snaphot:" + this.snapshot + " at " + this.snapshot.vHead + ")");
    
    return sb.toString();
  }
  
  void checkAndDumpState(PrintStream out)
  {
    checkInvariants();
    out.println(this);
  }
  
  void dumpActiveSnapshots(PrintStream out)
  {
    List<? extends SnapshotRef> l = this.activeSnapshots.toList();
    int sz = l.size();
    out.println("dump active snapshots: count:" + sz + " partKey:" + this.partKey);
    int n = sz <= 100 ? sz : 50;
    int i = 0;
    for (SnapshotRef ref : l)
    {
      if (i == n) {
        break;
      }
      out.println(ref);
    }
    if (sz <= 100) {
      return;
    }
    out.println("...");
    Iterator<? extends SnapshotRef> it = l.listIterator(l.size() - 50);
    while (it.hasNext())
    {
      SnapshotRef ref = (SnapshotRef)it.next();
      out.println(ref);
    }
  }
  
  void dumpBuffer(PrintStream out)
  {
    int sz = (int)(getTail() - getHead());
    out.println("dump buffer: elemnts count:" + sz + " partKey:" + this.partKey);
    int n = sz <= 100 ? sz : 50;
    int i = 0;
    for (Bucket vals : this.elements.values())
    {
      int len = vals.length();
      for (int j = 0; j < len; j++)
      {
        WEntry e = (WEntry)vals.get(j);
        if ((e != null) && (e.id >= getHead()) && (e.id < getTail()))
        {
          if (i++ == n) {
            break;
          }
          out.println(e);
        }
      }
    }
    if (sz <= 100) {
      return;
    }
    out.println("...");
    i = 0;
    Iterator<Long> it = this.elements.keySet().descendingIterator();
    while (it.hasNext())
    {
      Bucket vals = (Bucket)this.elements.get(it.next());
      int len = vals.length();
      for (int j = 0; j < len; j++)
      {
        WEntry e = (WEntry)vals.get(j);
        if ((e != null) && (e.id >= getHead()) && (e.id < getTail()))
        {
          if (i++ == 50) {
            return;
          }
          out.println(e);
        }
      }
    }
  }
  
  List<WAEvent> setNewWindowSnapshot(Snapshot newsn)
  {
    long oldHead = this.snapshot.vHead;
    long newHead = newsn.vHead;
    if (oldHead != newHead)
    {
      assert (newHead > oldHead);
      Snapshot diff = makeOverlappingSnapshot(this.snapshot, newHead);
      this.snapshot = newsn;
      if (this.activeSnapshots.isEmpty()) {
        clean(this.snapshot.vHead);
      }
      return diff;
    }
    this.snapshot = newsn;
    return Collections.emptyList();
  }
  
  ScheduledExecutorService getScheduler()
  {
    return this.owner.getScheduler();
  }
  
  long getTail()
  {
    return this.vTail;
  }
  
  long getHead()
  {
    return this.vHead;
  }
  
  Long findHead()
  {
    if (this.elements.isEmpty()) {
      return null;
    }
    UUID windowid = this.owner.getWindowID();
    Position waitPosition = (Position)this.owner.windowCheckpoints.get(windowid);
    if (waitPosition == null) {
      return Long.valueOf(this.vHead);
    }
    Position mustExceedThis = waitPosition.getLowPositionForComponent(windowid);
    
    Iterator<Long> iter = this.elements.keySet().iterator();
    String distId = this.partKey != null ? this.partKey.toString() : null;
    while (iter.hasNext())
    {
      Long elementsIndex = (Long)iter.next();
      Bucket entries = (Bucket)this.elements.get(elementsIndex);
      for (int entryIndex = 0; entryIndex < entries.length(); entryIndex++)
      {
        WEntry item = (WEntry)entries.get(entryIndex);
        if (item != null)
        {
          Position dataPositionAugmented = item.data.position.createAugmentedPosition(windowid, distId);
          if (!dataPositionAugmented.precedes(mustExceedThis))
          {
            waitPosition.removePathsWhichStartWith(dataPositionAugmented.values());
            
            PositionResponse response = new PositionResponse(windowid, Server.server.getServerID(), windowid, dataPositionAugmented);
            
            HazelcastSingleton.get().getTopic("#SharedCheckpoint_" + windowid).publish(response);
            if (waitPosition.isEmpty()) {
              this.owner.windowCheckpoints.remove(windowid);
            }
            long result = elementsIndex.longValue() | entryIndex;
            return Long.valueOf(result);
          }
        }
      }
    }
    return null;
  }
  
  protected abstract void update(IBatch paramIBatch);
  
  void cancel()
  {
    if (this.outputBuffer != null) {
      this.outputBuffer.stop();
    }
  }
  
  private void publish(IBatch newEntries, List<WAEvent> oldEntries)
  {
    if (this.outputBuffer != null) {
      this.outputBuffer.update(this.snapshot, newEntries, Batch.asBatch(oldEntries));
    } else {
      getFactory().receive(this.partKey, this.snapshot, newEntries, Batch.asBatch(oldEntries));
    }
  }
  
  void notifyOnUpdate(IBatch newEntries, List<WAEvent> oldEntries)
  {
    publish(newEntries, oldEntries);
    for (Object o : newEntries)
    {
      WAEvent ev = (WAEvent)o;
      if (this.recentEmittedEvents == null) {
        synchronized (this)
        {
          if (this.recentEmittedEvents == null) {
            this.recentEmittedEvents = new Position();
          }
        }
      }
      this.recentEmittedEvents.mergeLowerPositions(ev.position);
    }
  }
  
  void notifyOnTimer(IBatch newEntries, List<WAEvent> oldEntries)
  {
    publish(newEntries, oldEntries);
  }
  
  void notifyOnUpdate(List<WAEvent> newEntries, List<WAEvent> oldEntries)
  {
    notifyOnUpdate(Batch.asBatch(newEntries), oldEntries);
  }
  
  void notifyOnTimer(List<WAEvent> newEntries, List<WAEvent> oldEntries)
  {
    notifyOnTimer(Batch.asBatch(newEntries), oldEntries);
  }
  
  Snapshot makeNewAttrSnapshot(IBatch newEntries, CmpAttrs attrComparator)
  {
    long tail = getTail();
    long newHead = tail;
    WAEvent last = (WAEvent)newEntries.last();
    
    Iterator<WEntry> it = this.snapshot.itemIterator();
    while (it.hasNext())
    {
      WEntry e = (WEntry)it.next();
      if (attrComparator.inRange(e.data, last))
      {
        newHead = e.id;
        break;
      }
    }
    int n;
    if (newHead == tail)
    {
      n = newEntries.size();
      for (Object o : newEntries)
      {
        WAEvent obj = (WAEvent)o;
        if (attrComparator.inRange(obj, last))
        {
          newHead = tail - n;
          break;
        }
        n--;
      }
    }
    Snapshot newsn = makeSnapshot(newHead, tail);
    return newsn;
  }
  
  public Position getRecentEmittedEvents()
  {
    synchronized (this.recentEmittedEvents)
    {
      Position result = this.recentEmittedEvents;
      this.recentEmittedEvents = new Position();
      return result;
    }
  }
  
  protected Future<?> schedule(long interval)
  {
    return getScheduler().schedule(this, interval, TimeUnit.NANOSECONDS);
  }
  
  protected Future<?> scheduleAtFixedRate(long interval)
  {
    return getScheduler().scheduleAtFixedRate(this, interval, interval, TimeUnit.NANOSECONDS);
  }
  
  public void run()
  {
    try
    {
      onTimer();
    }
    catch (Throwable e)
    {
      e.printStackTrace(System.err);
      throw e;
    }
  }
  
  protected void onTimer()
  {
    throw new UnsupportedOperationException();
  }
}
