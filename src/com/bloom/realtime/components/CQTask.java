package com.bloom.runtime.components;

import com.bloom.classloading.WALoader;
import com.bloom.distribution.WAQueue;
import com.bloom.exception.RuntimeInterruptedException;
import com.bloom.exception.ServerException;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.BaseServer;
import com.bloom.runtime.Pair;
import com.bloom.runtime.Property;
import com.bloom.runtime.Server;
import com.bloom.runtime.TraceOptions;
import com.bloom.runtime.channels.Channel;
import com.bloom.runtime.compiler.CompilerUtils;
import com.bloom.runtime.compiler.select.ParamDesc;
import com.bloom.runtime.containers.Range;
import com.bloom.runtime.containers.TaskEvent;
import com.bloom.runtime.meta.CQExecutionPlan;
import com.bloom.runtime.meta.CQExecutionPlan.DataSource;
import com.bloom.runtime.meta.MetaInfo.CQ;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.runtime.utils.Factory;
import com.bloom.runtime.window.Window;
import com.bloom.uuid.UUID;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.bloom.event.Event;
import com.bloom.event.SimpleEvent;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.IRange;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.log4j.Logger;

public class CQTask
  extends FlowComponent
  implements PubSub, Restartable, Compound
{
  private static class CQLoader
    extends ClassLoader
  {
    public CQLoader(ClassLoader parent)
    {
      super();
    }
    
    public Class<?> loadByteCode(byte[] bytecode)
    {
      String className = CompilerUtils.getClassName(bytecode);
      return defineClass(className, bytecode, 0, bytecode.length);
    }
  }
  
  private static class AggMapEntry
  {
    int refCount;
    final Object[] aggregates;
    
    AggMapEntry(Object[] aggregates)
    {
      this.aggregates = ((Object[])aggregates.clone());
      this.refCount = 0;
    }
  }
  
  private static class SchemaTraslator
  {
    TranslatedSchema current;
    Map<String, Pair<String, Integer>> originalType;
    UUID originalTypeID;
  }
  
  private static class TraslatedSchemaCache
  {
    static final int cacheSize = 2000;
    final CQTask.SchemaTraslator[] translators;
    final Map<UUID, TranslatedSchema> schemaCache;
    
    TraslatedSchemaCache(int count)
    {
      this.translators = new CQTask.SchemaTraslator[count];
      this.schemaCache = new LinkedHashMap(2000, 0.75F, true)
      {
        private static final long serialVersionUID = 1L;
        
        protected boolean removeEldestEntry(Map.Entry<UUID, TranslatedSchema> eldest)
        {
          return size() > 2000;
        }
      };
    }
  }
  
  private static Logger logger = Logger.getLogger(CQTask.class);
  private final MetaInfo.CQ cqinfo;
  private final CQExecutionPlan plan;
  private Map<RecordKey, AggMapEntry> aggTable;
  private Object[] params;
  private final Map<String, ParamDesc> paramsMap;
  private IRange[] state;
  private final int count;
  private final Class<?>[] subTaskFactories;
  private WindowIndex[] indexes;
  private Channel output;
  private volatile long inputTotal = 0L;
  private volatile long outputTotal = 0L;
  private volatile boolean running = false;
  private Subscriber dataSink;
  Publisher[] dataSources;
  private ITaskEvent curInput;
  private final TraslatedSchemaCache schemaCache;
  private final CQPatternMatcher matcher;
  private TraceOptions traceOptions;
  private WAQueue outQueue = null;
  byte[] isDataReceived;
  
  public CQTask(MetaInfo.CQ cqinfo, BaseServer srv)
    throws Exception
  {
    super(srv, cqinfo);
    ClassLoader cl = WALoader.get();
    assert (cl != null);
    this.cqinfo = cqinfo;
    this.plan = cqinfo.plan;
    this.output = srv.createSimpleChannel();
    this.count = this.plan.dataSources.size();
    this.subTaskFactories = new Class[this.count];
    this.params = new Object[this.plan.paramsDesc.size()];
    this.paramsMap = Factory.makeNameMap();
    this.traceOptions = cqinfo.plan.traceOptions;
    for (ParamDesc d : this.plan.paramsDesc) {
      this.paramsMap.put(d.paramName, d);
    }
    CQLoader classLoader = new CQLoader(cl);
    int i = 0;
    for (byte[] subtask : this.plan.code)
    {
      Class<?> subTaskClass = classLoader.loadByteCode(subtask);
      Method m = subTaskClass.getMethod("initStatic", new Class[0]);
      m.invoke(null, new Object[0]);
      this.subTaskFactories[(i++)] = subTaskClass;
    }
    this.schemaCache = createSchemaCache(this.plan.dataSources, srv);
    if (this.plan.isMatch())
    {
      this.matcher = ((CQPatternMatcher)this.subTaskFactories[0].newInstance());
      this.matcher.init(this, srv.getScheduler());
    }
    else
    {
      this.matcher = null;
    }
  }
  
  public void setTraceOptions(TraceOptions to, WAQueue clientQueue)
  {
    this.traceOptions = to;
    this.outQueue = clientQueue;
  }
  
  public void connectParts(Flow flow)
    throws Exception
  {
    this.dataSink = (this.cqinfo.stream != null ? flow.getSubscriber(this.cqinfo.stream) : null);
    this.dataSources = new Publisher[this.count];
    this.isDataReceived = new byte[this.count];
    int i = 0;
    for (CQExecutionPlan.DataSource ds : this.plan.dataSources)
    {
      this.isDataReceived[i] = 0;
      this.dataSources[(i++)] = flow.getPublisher(ds.getDataSourceID());
    }
  }
  
  public Stream connectStream()
    throws Exception
  {
    this.dataSink = (this.cqinfo.stream != null ? srv().getStream(this.cqinfo.stream, null) : null);
    this.dataSources = new Publisher[this.count];
    this.isDataReceived = new byte[this.count];
    int i = 0;
    for (CQExecutionPlan.DataSource ds : this.plan.dataSources)
    {
      this.isDataReceived[i] = 0;
      this.dataSources[(i++)] = ((Publisher)srv().getOpenObject(ds.getDataSourceID()));
    }
    return (Stream)this.dataSink;
  }
  
  public synchronized void start()
    throws Exception
  {
    if (this.running) {
      return;
    }
    this.running = true;
    try
    {
      this.aggTable = Factory.makeMap();
      
      this.state = new IRange[this.count];
      for (int i = 0; i < this.count; i++) {
        this.state[i] = Range.emptyRange();
      }
      this.indexes = new WindowIndex[this.plan.indexCount];
      for (int i = 0; i < this.plan.indexCount; i++) {
        this.indexes[i] = new WindowIndex();
      }
      if (this.dataSink != null) {
        srv().subscribe(this, this.dataSink);
      }
      for (int i = 0; i < this.count; i++) {
        srv().subscribe(this.dataSources[i], new Link(this, i));
      }
    }
    catch (Exception e)
    {
      this.running = false;
      throw e;
    }
  }
  
  public void stop()
    throws Exception
  {
    if (!this.running) {
      return;
    }
    try
    {
      if (this.matcher != null) {
        this.matcher.stop();
      }
      if (this.dataSink != null) {
        srv().unsubscribe(this, this.dataSink);
      }
      for (int i = 0; i < this.count; i++) {
        srv().unsubscribe(this.dataSources[i], new Link(this, i));
      }
    }
    finally
    {
      resetProcessThread();
      this.running = false;
    }
  }
  
  public boolean isRunning()
  {
    return this.running;
  }
  
  public void close()
    throws Exception
  {
    if (logger.isDebugEnabled()) {
      logger.debug("Closing CQ " + getMetaName() + "...");
    }
    if (logger.isInfoEnabled()) {
      logger.info("cq " + getMetaName() + " processed " + this.outputTotal);
    }
    stop();
    this.output.close();
  }
  
  public void receive(Object linkID, ITaskEvent event)
    throws Exception
  {
    this.curInput = event;
    if ((!this.running) || (isFlowInError())) {
      return;
    }
    setProcessThread();
    try
    {
      this.inputTotal += event.batch().size();
      int streamID = ((Integer)linkID).intValue();
      dumpInput(event, streamID);
      if ((event.getFlags() & 0x2) != 0) {
        this.aggTable = Factory.makeMap();
      }
      if (event.snapshotUpdate())
      {
        IRange r = event.snapshot();
        this.state[streamID] = r;
        return;
      }
      if (this.matcher != null)
      {
        this.matcher.addBatch(streamID, event);
      }
      else
      {
        this.isDataReceived[streamID] = 1;
        Class<?> taskFactory = this.subTaskFactories[streamID];
        CQSubTask task = (CQSubTask)taskFactory.newInstance();
        task.init(this);
        task.setEvent(event);
        synchronized (this)
        {
          this.state[streamID] = this.state[streamID].update(event.snapshot());
          task.updateState();
        }
        task.run();
        task.cleanState();
      }
    }
    catch (Exception ex)
    {
      logger.error("Problem running CQ " + this.cqinfo.name + " for event " + event.toString() + " : ", ex);
      notifyAppMgr(EntityType.CQ, this.cqinfo.name, getMetaID(), ex, "cq receive", new Object[] { event });
      throw ex;
    }
  }
  
  IRange[] copyState()
  {
    return (IRange[])Arrays.copyOf(this.state, this.count);
  }
  
  void updateIndex(int index, RecordKey key, WAEvent value, boolean doadd)
  {
    this.indexes[index].update(key, value, doadd);
  }
  
  public Object[] getAggVec(RecordKey key, boolean isAdd)
  {
    Map<RecordKey, AggMapEntry> aggs = this.aggTable;
    
    AggMapEntry val = (AggMapEntry)aggs.get(key);
    assert ((val != null) || (isAdd));
    Object[] aggVec;
    if (val == null)
    {
      Object[] aggVec = createNewAggVec();
      val = new AggMapEntry(aggVec);
      aggs.put(key, val);
    }
    else
    {
      aggVec = val.aggregates;
    }
    if (isAdd)
    {
      val.refCount += 1;
    }
    else
    {
      val.refCount -= 1;
      if (val.refCount == 0) {
        aggs.remove(key);
      }
    }
    return aggVec;
  }
  
  private Object[] createNewAggVec()
  {
    List<Class<?>> aggObjFactories = this.plan.aggObjFactories;
    int aggObjCount = aggObjFactories.size();
    Object[] aggVec = new Object[aggObjCount];
    for (int i = 0; i < aggObjCount; i++) {
      try
      {
        aggVec[i] = ((Class)aggObjFactories.get(i)).newInstance();
      }
      catch (InstantiationException|IllegalAccessException e)
      {
        logger.error(e);
      }
    }
    return aggVec;
  }
  
  int getStreamCount()
  {
    return this.count;
  }
  
  int getDataSetCount()
  {
    return this.plan.dataSetCount;
  }
  
  Iterator<WAEvent> createIndexIterator(int index, RecordKey key)
  {
    return this.indexes[index].createIterator(key);
  }
  
  void doOutput(List<WAEvent> xnew, List<WAEvent> xold)
  {
    dumpOutput(xnew, xold);
    if ((xnew.isEmpty()) && (xold.isEmpty())) {
      return;
    }
    if (!canOutput()) {
      return;
    }
    this.outputTotal += 1L;
    try
    {
      TaskEvent event = TaskEvent.createStreamEvent(xnew, xold, this.plan.isStateful());
      
      this.output.publish(event);
    }
    catch (RuntimeInterruptedException e)
    {
      throw e;
    }
    catch (Exception e)
    {
      logger.error(getMetaName() + " unable to run the CQ Task!", e);
    }
  }
  
  public Channel getChannel()
  {
    return this.output;
  }
  
  private boolean canOutput()
  {
    switch (this.plan.kindOfOutputStream)
    {
    case 1: 
      return !this.curInput.batch().isEmpty();
    case 2: 
      return !this.curInput.removedBatch().isEmpty();
    }
    return true;
  }
  
  private Long prevIt = null;
  private Long prevInputRate = null;
  private Long prevOt = null;
  private Long prevOutputRate = null;
  
  public void addSpecificMonitorEvents(MonitorEventsCollection monEvs)
  {
    Long it = Long.valueOf(this.inputTotal);
    Long ot = Long.valueOf(this.outputTotal);
    
    long timeStamp = monEvs.getTimeStamp();
    if (!it.equals(this.prevIt)) {
      monEvs.add(MonitorEvent.Type.INPUT, it);
    }
    if (this.prevTimeStamp != null)
    {
      Long ir = Long.valueOf(Math.ceil(1000.0D * (it.longValue() - this.prevIt.longValue()) / (timeStamp - this.prevTimeStamp.longValue())));
      if (!ir.equals(this.prevInputRate))
      {
        monEvs.add(MonitorEvent.Type.INPUT_RATE, ir);
        monEvs.add(MonitorEvent.Type.RATE, ir);
        this.prevInputRate = ir;
      }
    }
    if (!ot.equals(this.prevOt))
    {
      monEvs.add(MonitorEvent.Type.OUTPUT, ot);
      monEvs.add(MonitorEvent.Type.LATEST_ACTIVITY, Long.valueOf(System.currentTimeMillis()));
    }
    if (this.prevTimeStamp != null)
    {
      Long or = Long.valueOf(Math.ceil(1000.0D * (ot.longValue() - this.prevOt.longValue()) / (timeStamp - this.prevTimeStamp.longValue())));
      if (!or.equals(this.prevOutputRate))
      {
        monEvs.add(MonitorEvent.Type.OUTPUT_RATE, or);
        this.prevOutputRate = or;
      }
    }
    this.prevIt = it;
    this.prevOt = ot;
  }
  
  public String toString()
  {
    return getMetaUri() + " - " + getMetaID() + " RUNTIME";
  }
  
  TranslatedSchema getTranslatedSchema(UUID dynamicEventTypeID, int dataSetIndex)
    throws MetaDataRepositoryException
  {
    SchemaTraslator[] translators = this.schemaCache.translators;
    Map<UUID, TranslatedSchema> cache = this.schemaCache.schemaCache;
    if (translators[dataSetIndex].current.typeID.equals(dynamicEventTypeID)) {
      return translators[dataSetIndex].current;
    }
    TranslatedSchema t = (TranslatedSchema)cache.get(dynamicEventTypeID);
    if (t == null)
    {
      t = createTranslatedSchema(dynamicEventTypeID, translators[dataSetIndex], srv());
      
      cache.put(dynamicEventTypeID, t);
    }
    translators[dataSetIndex].current = t;
    return t;
  }
  
  private static TraslatedSchemaCache createSchemaCache(List<CQExecutionPlan.DataSource> dataSources, BaseServer srv)
    throws ServerException, MetaDataRepositoryException
  {
    int i = 0;
    for (CQExecutionPlan.DataSource ds : dataSources) {
      if (ds.typeID != null) {
        i++;
      }
    }
    if (i == 0) {
      return null;
    }
    TraslatedSchemaCache cache = new TraslatedSchemaCache(dataSources.size());
    i = 0;
    for (CQExecutionPlan.DataSource ds : dataSources)
    {
      UUID typeID = ds.typeID;
      if (typeID != null)
      {
        SchemaTraslator st = cache.translators[i] = new SchemaTraslator(null);
        st.originalTypeID = typeID;
        st.originalType = getTypeInfo(typeID, srv);
        st.current = createTranslatedSchema(typeID, st, srv);
      }
      i++;
    }
    return cache;
  }
  
  private static TranslatedSchema createTranslatedSchema(UUID toTypeID, SchemaTraslator st, BaseServer srv)
    throws MetaDataRepositoryException
  {
    boolean instanceOfExpectedType = false;
    Map<String, Pair<String, Integer>> originalType = st.originalType;
    MetaInfo.Type toType;
    try
    {
      toType = srv.getTypeInfo(toTypeID);
      if (toTypeID.equals(st.originalTypeID))
      {
        instanceOfExpectedType = true;
      }
      else
      {
        MetaInfo.Type t = toType;
        while (t.extendsType != null)
        {
          if (t.extendsType.equals(st.originalTypeID))
          {
            instanceOfExpectedType = true;
            break;
          }
          t = srv.getTypeInfo(t.extendsType);
        }
      }
    }
    catch (ServerException e)
    {
      throw new RuntimeException("cannot find dynamic type info", e);
    }
    TranslatedSchema res = new TranslatedSchema();
    res.typeID = toTypeID;
    res.instanceOfExpectedType = instanceOfExpectedType;
    res.realFieldIndex = new int[toType.fields.size()];
    int i = 0;
    for (Map.Entry<String, String> fld : toType.fields.entrySet())
    {
      String fieldName = (String)fld.getKey();
      String fieldType = (String)fld.getValue();
      Pair<String, Integer> typeAndIndex = (Pair)originalType.get(fieldName);
      int index = -1;
      if (typeAndIndex != null) {
        if (((String)typeAndIndex.first).equals(fieldType)) {
          index = ((Integer)typeAndIndex.second).intValue();
        }
      }
      res.realFieldIndex[i] = index;
      i++;
    }
    return res;
  }
  
  private static Map<String, Pair<String, Integer>> getTypeInfo(UUID typeID, BaseServer srv)
    throws ServerException, MetaDataRepositoryException
  {
    Map<String, Pair<String, Integer>> fields = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    MetaInfo.Type type = srv.getTypeInfo(typeID);
    int i = 0;
    for (Map.Entry<String, String> fld : type.fields.entrySet())
    {
      String fieldName = (String)fld.getKey();
      String fieldType = (String)fld.getValue();
      fields.put(fieldName, Pair.make(fieldType, Integer.valueOf(i)));
      i++;
    }
    return fields;
  }
  
  private int inStats = 0;
  private int outStats = 0;
  
  private void dumpOutput(List<WAEvent> xnew, List<WAEvent> xold)
  {
    boolean dumpConsole = false;
    if ((this.traceOptions.traceFlags & 0x1) > 0)
    {
      boolean multiline = (this.traceOptions.traceFlags & 0x20) != 0;
      
      dumpConsole = true;
    }
    boolean dumpServer = false;
    if ((this.plan.traceOptions.traceFlags & 0x1) > 0)
    {
      boolean multiline = (this.traceOptions.traceFlags & 0x20) != 0;
      
      dumpServer = true;
    }
    if ((dumpConsole) || (dumpServer))
    {
      this.outStats += 1;
      dumpOutput(xnew, "added", this.outStats, dumpConsole, dumpServer);
      dumpOutput(xold, "removed", this.outStats, dumpConsole, dumpServer);
    }
  }
  
  private void dumpInput(ITaskEvent e, int inputID)
  {
    boolean dumpConsole = false;
    if ((this.traceOptions.traceFlags & 0x1) > 0)
    {
      boolean multiline = (this.traceOptions.traceFlags & 0x20) != 0;
      
      dumpConsole = true;
    }
    boolean dumpServer = false;
    if ((this.plan.traceOptions.traceFlags & 0x1) > 0)
    {
      boolean multiline = (this.traceOptions.traceFlags & 0x20) != 0;
      
      dumpServer = true;
    }
    if ((dumpConsole) || (dumpServer))
    {
      this.inStats += 1;
      String inputName = ((CQExecutionPlan.DataSource)this.plan.dataSources.get(inputID)).name;
      if (e.snapshotUpdate())
      {
        dumpInput(e.snapshot().all(), "new-snaphost", inputName, this.inStats, dumpConsole, dumpServer);
      }
      else
      {
        dumpInput(e.batch(), "added__", inputName, this.inStats, dumpConsole, dumpServer);
        
        dumpInput(e.removedBatch(), "removed", inputName, this.inStats, dumpConsole, dumpServer);
      }
    }
  }
  
  private void dumpInput(IBatch l, String what, String datasrc, int counter, boolean dumpConsole, boolean dumpServer)
  {
    ObjectNode stmt = JsonNodeFactory.instance.objectNode();
    stmt.put("datasource", datasrc);
    stmt.put("IO", "I");
    stmt.put("type", what);
    stmt.put("counter", this.inStats);
    stmt.put("serverid", Server.getServerName());
    stmt.put("queryname", getMetaFullName());
    if (!l.isEmpty())
    {
      ArrayNode dataArray = JsonNodeFactory.instance.arrayNode();
      
      PrintStream out = TraceOptions.getTraceStream(this.traceOptions);
      out.print(counter + " " + getMetaName() + " input <" + datasrc + ">" + what + ": ");
      for (Object x : l)
      {
        WAEvent o = (WAEvent)x;
        ObjectNode dataNode = JsonNodeFactory.instance.objectNode();
        if ((o.data instanceof Event))
        {
          Event e = (Event)o.data;
          out.print(Arrays.deepToString(e.getPayload()));
          dataNode.put("data", Arrays.deepToString(e.getPayload()));
        }
        else
        {
          dataNode.put("data", o.data.toString());
          out.print(o.data);
        }
        dataArray.add(dataNode);
        out.print(";");
        out.println();
      }
      out.println();
      stmt.set("data", dataArray);
    }
    if ((dumpConsole) && (this.outQueue != null)) {
      this.outQueue.put(stmt);
    }
    if (dumpServer)
    {
      PrintStream out = TraceOptions.getTraceStream(this.plan.traceOptions);
      if (out != null) {
        out.println(stmt);
      }
    }
  }
  
  private void dumpOutput(List<WAEvent> l, String what, int counter, boolean dumpConsole, boolean dumpServer)
  {
    ObjectNode stmt = JsonNodeFactory.instance.objectNode();
    stmt.put("datasource", getMetaFullName());
    stmt.put("IO", "O");
    stmt.put("type", what);
    stmt.put("counter", counter);
    stmt.put("serverid", Server.getServerName());
    stmt.put("queryname", getMetaFullName());
    if (!l.isEmpty())
    {
      ArrayNode dataArray = JsonNodeFactory.instance.arrayNode();
      PrintStream out = TraceOptions.getTraceStream(this.traceOptions);
      out.print(counter + " " + getMetaName() + " output " + what + " ");
      for (WAEvent o : l)
      {
        ObjectNode dataNode = JsonNodeFactory.instance.objectNode();
        StringBuilder strbld = new StringBuilder();
        SimpleEvent e = (SimpleEvent)o.data;
        strbld.append(Arrays.deepToString(e.getPayload()));
        strbld.append(";");
        if (e.linkedSourceEvents != null)
        {
          strbld.append("H(");
          for (Object[] hobjs : e.linkedSourceEvents)
          {
            strbld.append(hobjs.length + "@");
            for (Object hobj : hobjs)
            {
              strbld.append("{");
              if (hobj != null)
              {
                SimpleEvent ee = (SimpleEvent)hobj;
                strbld.append(Arrays.deepToString(ee.getPayload()));
              }
              else
              {
                strbld.append("null");
              }
              strbld.append("}");
            }
          }
          strbld.append(");");
        }
        out.print(strbld.toString());
        dataNode.put("data", strbld.toString());
        dataArray.add(dataNode);
      }
      out.println();
      stmt.set("data", dataArray);
    }
    if ((dumpConsole) && (this.outQueue != null)) {
      this.outQueue.put(stmt);
    }
    if (dumpServer)
    {
      PrintStream out = TraceOptions.getTraceStream(this.plan.traceOptions);
      if (out != null) {
        out.println(stmt);
      }
    }
  }
  
  public Iterator<SimpleEvent> makeSnapshotIterator(final int ds)
  {
    new Iterator()
    {
      final Iterator<WAEvent> it = CQTask.this.state[ds].all().iterator();
      
      public boolean hasNext()
      {
        return this.it.hasNext();
      }
      
      public SimpleEvent next()
      {
        return (SimpleEvent)((WAEvent)this.it.next()).data;
      }
      
      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    };
  }
  
  String getSourceName(int ds)
  {
    return ((CQExecutionPlan.DataSource)this.cqinfo.plan.dataSources.get(ds)).name;
  }
  
  boolean isTracingExecution()
  {
    return (this.traceOptions.traceFlags & 0x10) > 0;
  }
  
  PrintStream getTracingStream()
  {
    return TraceOptions.getTraceStream(this.traceOptions);
  }
  
  AggAlgo howToAggregate()
  {
    return this.plan.howToAggregate;
  }
  
  Object getParam(int index)
  {
    return this.params[index];
  }
  
  void bindParam(ParamDesc d, Object val)
  {
    this.params[d.index] = val;
  }
  
  public int getParamIndex(String paramName)
  {
    ParamDesc d = (ParamDesc)this.paramsMap.get(paramName);
    if (d == null) {
      throw new RuntimeException("Invalid parameter's name <" + paramName + ">");
    }
    return d.index;
  }
  
  public void bindParameter(String paramName, Object val)
  {
    ParamDesc d = (ParamDesc)this.paramsMap.get(paramName);
    if (d == null) {
      throw new RuntimeException("Invalid parameter's name <" + paramName + ">");
    }
    bindParam(d, val);
  }
  
  public void bindParameter(int index, Object val)
  {
    try
    {
      ParamDesc d = (ParamDesc)this.plan.paramsDesc.get(index);
      bindParam(d, val);
    }
    catch (IndexOutOfBoundsException e)
    {
      throw new RuntimeException("Parameter's index <" + index + "> is out of bounds");
    }
  }
  
  public boolean bindParameters(List<Property> params)
  {
    if (params == null) {
      return this.paramsMap.isEmpty();
    }
    BitSet pbmap = new BitSet(this.paramsMap.size());
    for (Property p : params)
    {
      ParamDesc d = (ParamDesc)this.paramsMap.get(p.name);
      if (d != null)
      {
        pbmap.set(d.index);
        bindParam(d, p.value);
      }
    }
    return pbmap.cardinality() == this.paramsMap.size();
  }
  
  Boolean allSourcesReady = null;
  Boolean atLeastOneDSisStreaming;
  
  public boolean allStreamingSourcesReady()
  {
    if (this.allSourcesReady != null) {
      return this.allSourcesReady.booleanValue();
    }
    for (int i = 0; i < this.isDataReceived.length; i++) {
      if (this.isDataReceived[i] == 0) {
        return false;
      }
    }
    this.allSourcesReady = Boolean.valueOf(true);
    return true;
  }
  
  boolean atLeastOneDSisStreaming()
  {
    if (this.atLeastOneDSisStreaming != null) {
      return this.atLeastOneDSisStreaming.booleanValue();
    }
    this.atLeastOneDSisStreaming = Boolean.valueOf(false);
    for (Publisher pub : this.dataSources) {
      if ((pub.getClass().getCanonicalName().equalsIgnoreCase(Stream.class.getCanonicalName())) || (pub.getClass().getCanonicalName().equalsIgnoreCase(Window.class.getCanonicalName()))) {
        this.atLeastOneDSisStreaming = Boolean.valueOf(true);
      }
    }
    return this.atLeastOneDSisStreaming.booleanValue();
  }
}
