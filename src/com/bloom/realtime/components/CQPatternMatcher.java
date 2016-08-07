package com.bloom.runtime.components;

import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class CQPatternMatcher
{
  private CQTask cqtask;
  private ScheduledThreadPoolExecutor scheduler;
  private AtomicInteger nextCtxId;
  private Map<RecordKey, MatcherPartition> partitions;
  
  static enum Status
  {
    NEEDMORE,  SUCCESS;
    
    private Status() {}
  }
  
  static class Ack
  {
    final CQPatternMatcher.Status status;
    final long pos;
    
    Ack(CQPatternMatcher.Status status, long pos)
    {
      this.status = status;
      this.pos = pos;
    }
    
    boolean advancedPos(long prevpos)
    {
      return this.pos > prevpos;
    }
    
    boolean samePos(long prevpos)
    {
      return this.pos == prevpos;
    }
    
    boolean samePosAndNeedMore(long prevpos)
    {
      return (samePos(prevpos)) && (needMore());
    }
    
    boolean needMore()
    {
      return this.status == CQPatternMatcher.Status.NEEDMORE;
    }
    
    boolean success()
    {
      return this.status == CQPatternMatcher.Status.SUCCESS;
    }
    
    public String toString()
    {
      return "Ack(" + this.status + ", " + this.pos + ")";
    }
  }
  
  public CQPatternMatcher()
  {
    this.nextCtxId = new AtomicInteger();
  }
  
  class MatcherPartition
  {
    private long curBufPos = 0L;
    private final RecordKey partKey;
    private final ConcurrentSkipListMap<Long, CQPatternMatcher.PEvent> eventBuffer = new ConcurrentSkipListMap();
    private CQPatternMatcher.MatcherContext ctx;
    private Object lastEvent;
    
    MatcherPartition(RecordKey partKey)
    {
      this.partKey = partKey;
      this.ctx = new CQPatternMatcher.MatcherContext(CQPatternMatcher.this, this, 0L, false);
    }
    
    public synchronized void addEvent(int streamID, Object event)
    {
      this.lastEvent = event;
      long pos = this.curBufPos++;
      this.eventBuffer.put(Long.valueOf(pos), new CQPatternMatcher.PEvent(streamID, event));
      this.ctx = this.ctx.tryMatch();
      long removeUpTo = this.ctx.startPos - CQPatternMatcher.this.getPrevEventIndex();
      this.eventBuffer.headMap(Long.valueOf(removeUpTo)).clear();
    }
    
    public void stop()
    {
      this.ctx.close();
    }
    
    public void trace(String string)
    {
      if (CQPatternMatcher.this.cqtask.isTracingExecution())
      {
        PrintStream out = CQPatternMatcher.this.cqtask.getTracingStream();
        out.println(string);
      }
    }
  }
  
  public void init(CQTask cqTask, ScheduledThreadPoolExecutor scheduler)
  {
    this.cqtask = cqTask;
    this.scheduler = scheduler;
    this.partitions = new HashMap();
    boolean hasPartitions = false;
    try
    {
      if (!getClass().getMethod("makePartitionKey", new Class[] { Object.class }).getDeclaringClass().equals(CQPatternMatcher.class)) {
        hasPartitions = true;
      }
    }
    catch (NoSuchMethodException|SecurityException e) {}
    if (!hasPartitions)
    {
      MatcherPartition mp = new MatcherPartition(null);
      this.partitions.put(null, mp);
    }
  }
  
  public void stop()
  {
    for (MatcherPartition p : this.partitions.values()) {
      p.stop();
    }
  }
  
  public void addBatch(int streamID, ITaskEvent event)
  {
    IBatch<WAEvent> waEventIBatch = event.batch();
    for (WAEvent e : waEventIBatch)
    {
      RecordKey key = makePartitionKey(e.data);
      MatcherPartition p = (MatcherPartition)this.partitions.get(key);
      if (p == null)
      {
        p = new MatcherPartition(key);
        this.partitions.put(key, p);
      }
      p.addEvent(streamID, e.data);
    }
  }
  
  ScheduledThreadPoolExecutor getScheduler()
  {
    return this.scheduler;
  }
  
  private static Ack makeAck(Status status, long pos)
  {
    return new Ack(status, pos);
  }
  
  private static Ack ackSuccess(long pos)
  {
    return makeAck(Status.SUCCESS, pos);
  }
  
  private static Ack ackNeedMore(long pos)
  {
    return makeAck(Status.NEEDMORE, pos);
  }
  
  public abstract boolean evalExpr(MatcherContext paramMatcherContext, int paramInt1, Object paramObject, int paramInt2, Long paramLong);
  
  public abstract Object output(MatcherContext paramMatcherContext, Object paramObject);
  
  public abstract Object createNode(MatcherContext paramMatcherContext, int paramInt);
  
  public abstract int getRootNodeId();
  
  public abstract int getPrevEventIndex();
  
  public RecordKey makePartitionKey(Object event)
  {
    return null;
  }
  
  public boolean checkDS(int dsid, int expected_ds)
  {
    return dsid != expected_ds;
  }
  
  public void doOutput(Object o)
  {
    this.cqtask.doOutput(Collections.singletonList(new WAEvent(o)), Collections.emptyList());
  }
  
  public void doCleanup(RecordKey key)
  {
    MatcherPartition p = (MatcherPartition)this.partitions.get(key);
    if (p != null) {
      this.partitions.remove(key);
    }
  }
  
  public static class PEvent
  {
    final int ds;
    final Object event;
    
    public PEvent(int ds, Object event)
    {
      this.ds = ds;
      this.event = event;
    }
    
    public String toString()
    {
      return this.ds + "-> " + this.event;
    }
  }
  
  public PatternNode createAlternation(MatcherContext ctx, int nodeid, String src, int[] nodeids)
  {
    return new Alternation(ctx, nodeid, src, nodeids);
  }
  
  public PatternNode createAnchor(MatcherContext ctx, int nodeid, String src, int anchorid)
  {
    return new AnchorNode(ctx, nodeid, src);
  }
  
  public PatternNode createCondition(MatcherContext ctx, int nodeid, String src, int exprid)
  {
    return new Condition(ctx, nodeid, src, exprid);
  }
  
  public PatternNode createRepetition(MatcherContext ctx, int nodeid, String src, int subnodeid, int min, int max)
  {
    return new Repetition(ctx, nodeid, src, subnodeid, min, max);
  }
  
  public PatternNode createSequence(MatcherContext ctx, int nodeid, String src, int[] nodeids)
  {
    return new Sequence(ctx, nodeid, src, nodeids);
  }
  
  public PatternNode createVariable(MatcherContext ctx, int nodeid, String src, int exprid)
  {
    return new Variable(ctx, nodeid, src, exprid);
  }
  
  public class TimerEvent
    implements Runnable
  {
    final CQPatternMatcher.MatcherContext ctx;
    final int timerid;
    Future<?> task;
    private volatile boolean cancelled = false;
    
    TimerEvent(CQPatternMatcher.MatcherContext ctx, int timerid)
    {
      this.ctx = ctx;
      this.timerid = timerid;
    }
    
    public void run()
    {
      if (!this.cancelled) {
        this.ctx.createTimerEvent(this);
      }
    }
    
    void cancel()
    {
      this.cancelled = true;
      if (this.task != null)
      {
        this.task.cancel(true);
        if ((this.task instanceof Runnable)) {
          CQPatternMatcher.this.getScheduler().remove((Runnable)this.task);
        }
      }
    }
  }
  
  public class MatcherContext
  {
    private final CQPatternMatcher.MatcherPartition owner;
    private final CQPatternMatcher.PatternNode root;
    private final long startPos;
    private long curPos;
    private Long lastAnchor;
    private final int contextId;
    private final Map<Integer, CQPatternMatcher.TimerEvent> timers = new HashMap();
    private final Map<Integer, List<Object>> vars = new HashMap();
    
    MatcherContext(CQPatternMatcher.MatcherPartition owner, long startPos, boolean eval)
    {
      this.owner = owner;
      this.contextId = CQPatternMatcher.this.nextCtxId.getAndIncrement();
      this.startPos = startPos;
      this.curPos = startPos;
      int rootId = CQPatternMatcher.this.getRootNodeId();
      this.root = makeNode(rootId);
      if (eval)
      {
        CQPatternMatcher.Ack ret = this.root.eval(this, this.curPos, false);
        if ((!ret.success()) && (!ret.samePos(this.curPos))) {
          this.curPos += 1L;
        }
      }
    }
    
    public MatcherContext tryMatch()
    {
      do
      {
        CQPatternMatcher.Ack ret = this.root.eval(this, this.curPos, false);
        if (ret.success())
        {
          close();
          Object o = CQPatternMatcher.this.output(this, CQPatternMatcher.MatcherPartition.access$300(this.owner));
          CQPatternMatcher.this.doOutput(o);
          boolean reeval = true;
          if ((this.lastAnchor == null) && (CQPatternMatcher.MatcherPartition.access$400(this.owner) != null))
          {
            CQPatternMatcher.this.doCleanup(CQPatternMatcher.MatcherPartition.access$400(this.owner));
            reeval = false;
          }
          long fromPos = getRestartPosOnSuccess(ret.pos);
          return new MatcherContext(CQPatternMatcher.this, this.owner, fromPos, reeval);
        }
        if (ret.samePos(this.curPos))
        {
          close();
          long fromPos = getRestartPosOnFail();
          return new MatcherContext(CQPatternMatcher.this, this.owner, fromPos, true);
        }
        this.curPos += 1L;
      } while (getEventAt(this.curPos) != null);
      return this;
    }
    
    public void close()
    {
      for (CQPatternMatcher.TimerEvent pte : this.timers.values()) {
        pte.cancel();
      }
    }
    
    public void createTimerEvent(CQPatternMatcher.TimerEvent te)
    {
      this.owner.addEvent(-1, te);
    }
    
    public boolean createTimer(int id, long interval)
    {
      CQPatternMatcher.TimerEvent te = new CQPatternMatcher.TimerEvent(CQPatternMatcher.this, this, id);
      te.task = CQPatternMatcher.this.getScheduler().schedule(te, interval, TimeUnit.MICROSECONDS);
      
      CQPatternMatcher.TimerEvent pte = (CQPatternMatcher.TimerEvent)this.timers.put(Integer.valueOf(id), te);
      if (pte != null) {
        pte.cancel();
      }
      return true;
    }
    
    public boolean stopTimer(int timerid)
    {
      CQPatternMatcher.TimerEvent pte = (CQPatternMatcher.TimerEvent)this.timers.get(Integer.valueOf(timerid));
      if (pte != null) {
        pte.cancel();
      }
      return true;
    }
    
    public boolean waitTimer(int timerid, Object event)
    {
      if ((event instanceof CQPatternMatcher.TimerEvent))
      {
        CQPatternMatcher.TimerEvent te = (CQPatternMatcher.TimerEvent)event;
        if ((this == te.ctx) && (te.timerid == timerid)) {
          return true;
        }
      }
      return false;
    }
    
    private long getRestartPosOnSuccess(long endOfMatchedPattern)
    {
      return this.lastAnchor == null ? endOfMatchedPattern : this.lastAnchor.longValue();
    }
    
    private long getRestartPosOnFail()
    {
      return this.startPos + 1L;
    }
    
    public boolean eval(int exprid, Long pos)
    {
      if (pos != null)
      {
        CQPatternMatcher.PEvent event = getEventAt(pos.longValue());
        if (event == null) {
          return false;
        }
        return CQPatternMatcher.this.evalExpr(this, exprid, event.event, event.ds, pos);
      }
      return CQPatternMatcher.this.evalExpr(this, exprid, null, 0, pos);
    }
    
    private CQPatternMatcher.PEvent getEventAt(long pos)
    {
      return (CQPatternMatcher.PEvent)CQPatternMatcher.MatcherPartition.access$500(this.owner).get(Long.valueOf(pos));
    }
    
    public Object getEventAt(Long pos, int offset)
    {
      CQPatternMatcher.PEvent pe = (CQPatternMatcher.PEvent)CQPatternMatcher.MatcherPartition.access$500(this.owner).get(Long.valueOf(pos.longValue() - offset));
      if (pe == null) {
        return null;
      }
      return pe.event;
    }
    
    public CQPatternMatcher.PatternNode makeNode(int nodeid)
    {
      return (CQPatternMatcher.PatternNode)CQPatternMatcher.this.createNode(this, nodeid);
    }
    
    public void setAnchor(long pos)
    {
      if (this.lastAnchor == null) {
        this.lastAnchor = Long.valueOf(pos);
      }
    }
    
    public void addVar(int varid, long pos)
    {
      CQPatternMatcher.PEvent event = getEventAt(pos);
      if (event != null)
      {
        List<Object> var = (List)this.vars.get(Integer.valueOf(varid));
        if (var == null)
        {
          var = new ArrayList();
          this.vars.put(Integer.valueOf(varid), var);
        }
        var.add(event.event);
      }
    }
    
    public Object getVar(int varid, int index)
    {
      List<Object> var = (List)this.vars.get(Integer.valueOf(varid));
      if (var != null)
      {
        if (index == -1) {
          index = var.size() - 1;
        }
        try
        {
          return var.get(index);
        }
        catch (IndexOutOfBoundsException e) {}
      }
      return null;
    }
    
    public void trace(String string)
    {
      this.owner.trace(string);
    }
  }
  
  public static abstract class PatternNode
  {
    private final int nodeid;
    private final String src;
    private long startpos = -1L;
    
    public PatternNode(CQPatternMatcher.MatcherContext ctx, int nodeid, String src)
    {
      this.nodeid = nodeid;
      this.src = src;
    }
    
    public abstract CQPatternMatcher.Ack eval(CQPatternMatcher.MatcherContext paramMatcherContext, long paramLong, boolean paramBoolean);
    
    public abstract boolean canAcceptMore();
    
    public void setStart(long pos)
    {
      if (this.startpos == -1L) {
        this.startpos = pos;
      }
    }
    
    public long getStartPos()
    {
      return this.startpos;
    }
    
    public int getId()
    {
      return this.nodeid;
    }
    
    public String getSource()
    {
      return this.src;
    }
    
    public String toString()
    {
      return this.nodeid + "(" + this.src + ")->" + this.startpos;
    }
  }
  
  public static class AnchorNode
    extends CQPatternMatcher.PatternNode
  {
    public AnchorNode(CQPatternMatcher.MatcherContext ctx, int nodeid, String src)
    {
      super(nodeid, src);
    }
    
    public CQPatternMatcher.Ack eval(CQPatternMatcher.MatcherContext ctx, long pos, boolean greedy)
    {
      setStart(pos);
      
      ctx.setAnchor(pos);
      return CQPatternMatcher.ackSuccess(pos);
    }
    
    public boolean canAcceptMore()
    {
      return false;
    }
  }
  
  public static class Condition
    extends CQPatternMatcher.PatternNode
  {
    private final int exprid;
    
    public Condition(CQPatternMatcher.MatcherContext ctx, int nodeid, String src, int exprid)
    {
      super(nodeid, src);
      this.exprid = exprid;
    }
    
    public CQPatternMatcher.Ack eval(CQPatternMatcher.MatcherContext ctx, long pos, boolean greedy)
    {
      setStart(pos);
      
      boolean ok = ctx.eval(this.exprid, null);
      if (ok) {
        return CQPatternMatcher.ackSuccess(pos);
      }
      return CQPatternMatcher.ackNeedMore(pos);
    }
    
    public boolean canAcceptMore()
    {
      return false;
    }
  }
  
  public static class Variable
    extends CQPatternMatcher.PatternNode
  {
    private final int exprid;
    
    public Variable(CQPatternMatcher.MatcherContext ctx, int nodeid, String src, int exprid)
    {
      super(nodeid, src);
      this.exprid = exprid;
    }
    
    public CQPatternMatcher.Ack eval(CQPatternMatcher.MatcherContext ctx, long pos, boolean greedy)
    {
      setStart(pos);
      
      boolean ok = ctx.eval(this.exprid, Long.valueOf(pos));
      if (ok)
      {
        ctx.addVar(this.exprid, pos);
        ctx.trace("matched " + getSource());
        return CQPatternMatcher.ackSuccess(pos + 1L);
      }
      ctx.trace("not matched " + getSource());
      return CQPatternMatcher.ackNeedMore(pos);
    }
    
    public boolean canAcceptMore()
    {
      return false;
    }
  }
  
  public static class Repetition
    extends CQPatternMatcher.PatternNode
  {
    private final int subnodeid;
    private final int min;
    private final int max;
    private final List<CQPatternMatcher.PatternNode> repeated = new ArrayList();
    
    public Repetition(CQPatternMatcher.MatcherContext ctx, int nodeid, String src, int subnodeid, int min, int max)
    {
      super(nodeid, src);
      this.subnodeid = subnodeid;
      this.min = min;
      this.max = max;
    }
    
    public CQPatternMatcher.Ack eval(CQPatternMatcher.MatcherContext ctx, long pos, boolean greedy)
    {
      setStart(pos);
      if (!this.repeated.isEmpty())
      {
        CQPatternMatcher.PatternNode node = (CQPatternMatcher.PatternNode)this.repeated.get(this.repeated.size() - 1);
        
        CQPatternMatcher.Ack ret = node.eval(ctx, pos, greedy);
        if (ret.needMore()) {
          return ret;
        }
        if (ret.advancedPos(pos))
        {
          if (thisNeeddMore()) {
            return CQPatternMatcher.ackNeedMore(ret.pos);
          }
          return ret;
        }
      }
      if (thisNeeddMore())
      {
        CQPatternMatcher.PatternNode node = ctx.makeNode(this.subnodeid);
        CQPatternMatcher.Ack ret = node.eval(ctx, pos, greedy);
        if (!ret.samePosAndNeedMore(pos)) {
          this.repeated.add(node);
        }
        if ((ret.needMore()) || (thisNeeddMore())) {
          return CQPatternMatcher.ackNeedMore(ret.pos);
        }
        return ret;
      }
      if ((greedy) && (canAcceptMore()))
      {
        CQPatternMatcher.PatternNode node = ctx.makeNode(this.subnodeid);
        CQPatternMatcher.Ack ret = node.eval(ctx, pos, greedy);
        if (ret.success())
        {
          this.repeated.add(node);
          return ret;
        }
      }
      return CQPatternMatcher.ackSuccess(pos);
    }
    
    public boolean canAcceptMore()
    {
      return this.repeated.size() < this.max;
    }
    
    private boolean thisNeeddMore()
    {
      return this.repeated.size() < this.min;
    }
  }
  
  public static class Sequence
    extends CQPatternMatcher.PatternNode
  {
    private int cur;
    private final List<CQPatternMatcher.PatternNode> seq = new ArrayList();
    
    public Sequence(CQPatternMatcher.MatcherContext ctx, int nodeid, String src, int[] seq)
    {
      super(nodeid, src);
      this.cur = 0;
      for (int id : seq) {
        this.seq.add(ctx.makeNode(id));
      }
    }
    
    public CQPatternMatcher.Ack eval(CQPatternMatcher.MatcherContext ctx, long pos, boolean greedy)
    {
      setStart(pos);
      if (this.cur < this.seq.size())
      {
        CQPatternMatcher.PatternNode node = (CQPatternMatcher.PatternNode)this.seq.get(this.cur);
        CQPatternMatcher.Ack ret = node.eval(ctx, pos, greedy);
        if (ret.needMore())
        {
          if (!ret.samePosAndNeedMore(pos)) {
            return ret;
          }
          return tryRecompute(ctx, pos);
        }
        this.cur += 1;
        if (ret.advancedPos(pos))
        {
          if (this.cur == this.seq.size()) {
            return ret;
          }
          return CQPatternMatcher.ackNeedMore(ret.pos);
        }
        return eval(ctx, pos, greedy);
      }
      return CQPatternMatcher.ackSuccess(pos);
    }
    
    private int findAcceptMore(int lastcompletednode, long lastconsumedpos)
    {
      int canAcceptMore = -1;
      int i = lastcompletednode;
      while (i >= 0)
      {
        CQPatternMatcher.PatternNode node = (CQPatternMatcher.PatternNode)this.seq.get(i);
        if (node.canAcceptMore()) {
          canAcceptMore = i;
        }
        if (node.getStartPos() != lastconsumedpos) {
          break;
        }
        i--;
      }
      return canAcceptMore;
    }
    
    CQPatternMatcher.Ack tryRecompute(CQPatternMatcher.MatcherContext ctx, long pos)
    {
      int i = findAcceptMore(this.cur - 1, pos - 1L);
      if (i < 0) {
        return CQPatternMatcher.ackNeedMore(pos);
      }
      long cpos = pos;
      CQPatternMatcher.Ack ret;
      for (;;)
      {
        CQPatternMatcher.PatternNode node = (CQPatternMatcher.PatternNode)this.seq.get(i);
        
        ret = node.eval(ctx, cpos, true);
        if (!ret.success()) {
          break;
        }
        i++;
        if (i > this.cur) {
          break;
        }
        cpos = ret.pos;
      }
      this.cur = i;
      return ret;
    }
    
    public boolean canAcceptMore()
    {
      return findAcceptMore(this.cur, ((CQPatternMatcher.PatternNode)this.seq.get(this.cur)).getStartPos()) >= 0;
    }
  }
  
  public static class Alternation
    extends CQPatternMatcher.PatternNode
  {
    private final List<CQPatternMatcher.PatternNode> alternatives = new ArrayList();
    private List<CQPatternMatcher.PatternNode> left;
    
    public Alternation(CQPatternMatcher.MatcherContext ctx, int nodeid, String src, int[] alternatives)
    {
      super(nodeid, src);
      for (int id : alternatives) {
        this.alternatives.add(ctx.makeNode(id));
      }
      this.left = this.alternatives;
    }
    
    public CQPatternMatcher.Ack eval(CQPatternMatcher.MatcherContext ctx, long pos, boolean greedy)
    {
      setStart(pos);
      boolean consumed = false;
      List<CQPatternMatcher.PatternNode> needmore = new ArrayList();
      List<CQPatternMatcher.PatternNode> variants = greedy ? this.alternatives : this.left;
      for (CQPatternMatcher.PatternNode n : variants)
      {
        CQPatternMatcher.Ack ret = n.eval(ctx, pos, greedy);
        if (ret.needMore()) {
          needmore.add(n);
        }
        if (ret.advancedPos(pos)) {
          consumed = true;
        }
      }
      this.left = needmore;
      if (needmore.size() == this.alternatives.size()) {
        return CQPatternMatcher.ackNeedMore(consumed ? pos + 1L : pos);
      }
      return CQPatternMatcher.ackSuccess(consumed ? pos + 1L : pos);
    }
    
    public boolean canAcceptMore()
    {
      for (CQPatternMatcher.PatternNode n : this.alternatives) {
        if (n.canAcceptMore()) {
          return true;
        }
      }
      return false;
    }
  }
}
