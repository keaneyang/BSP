package com.bloom.jmqmessaging;

import com.bloom.messaging.From;
import com.bloom.messaging.Handler;
import com.bloom.messaging.MessagingProvider;
import com.bloom.messaging.MessagingSystem;
import com.bloom.messaging.Sender;
import com.bloom.messaging.SocketType;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.runtime.components.FlowComponent;
import com.bloom.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;

public class Executor<O>
  implements Handler
{
  private static Logger logger = Logger.getLogger(Executor.class);
  String name;
  private transient MessagingSystem ms;
  private O on;
  UUID thisId;
  private From from;
  private static final String requestHandler = "-requestHandler";
  private static final String responseHandler = "-responseHandler";
  private Map<Long, ExecutableFuture<O, ?>> futures = new ConcurrentHashMap();
  private volatile boolean running;
  private Executor<O>.ExecutorMembershipListener membershipListener;
  private String membershipListenerId;
  
  class ExecutorMembershipListener
    implements MembershipListener
  {
    ExecutorMembershipListener() {}
    
    public void memberAdded(MembershipEvent membershipEvent) {}
    
    public void memberRemoved(MembershipEvent membershipEvent)
    {
      UUID removedNode = new UUID(membershipEvent.getMember().getUuid());
      Executor.logger.info(Executor.this.name + ": Server " + removedNode + " has gone down");
      InterruptedException ie = new InterruptedException("Remote Server " + removedNode + " has terminated");
      for (Map.Entry<Long, Executor.ExecutableFuture<O, ?>> futureEntry : Executor.this.futures.entrySet()) {
        if (((Executor.ExecutableFuture)futureEntry.getValue()).where.equals(removedNode))
        {
          Executor.logger.info(Executor.this.name + ": Cancelling future " + ((Executor.ExecutableFuture)futureEntry.getValue()).request.executable.getClass().getSimpleName());
          
          ((Executor.ExecutableFuture)futureEntry.getValue()).setResponse(new Executor.ExecutionResponse(((Long)futureEntry.getKey()).longValue(), ie));
        }
      }
    }
    
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {}
  }
  
  public Executor(String name, O on)
  {
    this.name = ("ExecutorService-" + name);
    this.ms = MessagingProvider.getMessagingSystem("com.bloom.jmqmessaging.ZMQSystem");
    try
    {
      this.ms.createReceiver(PullReceiver.class, this, this.name.concat("-requestHandler"), false, null);
      this.ms.startReceiver(this.name.concat("-requestHandler"), new HashMap());
      
      this.ms.createReceiver(PullReceiver.class, this, this.name.concat("-responseHandler"), false, null);
      this.ms.startReceiver(this.name.concat("-responseHandler"), new HashMap());
    }
    catch (Exception e)
    {
      logger.error(e.getMessage(), e);
    }
    this.on = on;
    this.thisId = HazelcastSingleton.getNodeId();
    this.from = new From(this.thisId, this.name);
    this.membershipListener = new ExecutorMembershipListener();
    this.membershipListenerId = HazelcastSingleton.get().getCluster().addMembershipListener(this.membershipListener);
  }
  
  public void close()
  {
    try
    {
      this.ms.stopReceiver(this.name.concat("-requestHandler"));
      this.ms.stopReceiver(this.name.concat("-responseHandler"));
      stop();
      for (Sender senders : this.senderMapForRequests.values()) {
        senders.stop();
      }
      for (Sender senders : this.senderMapForResponses.values()) {
        senders.stop();
      }
      this.senderMapForRequests.clear();
      this.senderMapForResponses.clear();
      
      HazelcastSingleton.get().getCluster().removeMembershipListener(this.membershipListenerId);
    }
    catch (Exception e) {}
  }
  
  public void start()
  {
    this.running = true;
  }
  
  public void stop()
  {
    InterruptedException ie = new InterruptedException("Executor has been closed");
    for (Map.Entry<Long, ExecutableFuture<O, ?>> futureEntry : this.futures.entrySet()) {
      ((ExecutableFuture)futureEntry.getValue()).setResponse(new ExecutionResponse(((Long)futureEntry.getKey()).longValue(), ie));
    }
    this.running = false;
  }
  
  public UUID local()
  {
    return this.thisId;
  }
  
  public O on()
  {
    return (O)this.on;
  }
  
  public static AtomicLong requestIdSeq = new AtomicLong(1L);
  
  public static class ExecutionRequest<O, V>
    implements Serializable, KryoSerializable
  {
    private static final long serialVersionUID = -3810004296646978979L;
    public From from;
    public long requestId;
    public Executable<O, V> executable;
    
    public ExecutionRequest() {}
    
    public ExecutionRequest(From from, long requestId, Executable<O, V> executable)
    {
      this.from = from;
      this.requestId = requestId;
      this.executable = executable;
    }
    
    public void write(Kryo kryo, Output output)
    {
      this.from.write(kryo, output);
      output.writeLong(this.requestId);
      kryo.writeClassAndObject(output, this.executable);
    }
    
    public void read(Kryo kryo, Input input)
    {
      this.from = new From();
      this.from.read(kryo, input);
      this.requestId = input.readLong();
      this.executable = ((Executable)kryo.readClassAndObject(input));
    }
  }
  
  public static class ExecutionResponse<V>
    implements Serializable, KryoSerializable
  {
    private static final long serialVersionUID = -5966990272616101600L;
    public long requestId;
    public V response;
    public Throwable exception;
    
    public ExecutionResponse() {}
    
    public ExecutionResponse(long requestId, V response)
    {
      this.requestId = requestId;
      this.response = response;
    }
    
    public ExecutionResponse(long requestId, Throwable exception)
    {
      this.requestId = requestId;
      this.exception = exception;
    }
    
    public void write(Kryo kryo, Output output)
    {
      output.writeLong(this.requestId);
      output.writeBoolean(this.response != null);
      if (this.response != null) {
        kryo.writeClassAndObject(output, this.response);
      }
      output.writeBoolean(this.exception != null);
      if (this.exception != null) {
        kryo.writeClassAndObject(output, this.exception);
      }
    }
    
    public void read(Kryo kryo, Input input)
    {
      this.requestId = input.readLong();
      Throwable caughtException = null;
      try
      {
        boolean hasResponse = input.readBoolean();
        if (hasResponse) {
          this.response = kryo.readClassAndObject(input);
        }
      }
      catch (Throwable t)
      {
        caughtException = new RuntimeException("Could not deserialize response object", t);
      }
      boolean hasException = input.readBoolean();
      if (hasException) {
        this.exception = ((Throwable)kryo.readClassAndObject(input));
      }
      if (caughtException != null) {
        this.exception = caughtException;
      }
    }
  }
  
  public static abstract interface Latent
  {
    public abstract long getLatency();
  }
  
  public static class ExecutableFuture<O, V>
    implements Future<V>, Executor.Latent
  {
    UUID where;
    Executor.ExecutionRequest<O, V> request;
    Executor.ExecutionResponse<V> response;
    private ReentrantLock responseLock = new ReentrantLock();
    private Condition responseAvailable = this.responseLock.newCondition();
    long startTime;
    long endTime;
    boolean local = true;
    
    public ExecutableFuture(Executor.ExecutionRequest<O, V> request, UUID where)
    {
      this.where = where;
      this.request = request;
      this.startTime = System.nanoTime();
    }
    
    public Executor.ExecutionRequest<O, V> getRequest()
    {
      return this.request;
    }
    
    public void setLocal(boolean local)
    {
      this.local = local;
    }
    
    public boolean isLocal()
    {
      return this.local;
    }
    
    public UUID getWhere()
    {
      return this.where;
    }
    
    public void setResponse(Executor.ExecutionResponse response)
    {
      try
      {
        this.responseLock.lock();
        this.response = response;
        this.endTime = System.nanoTime();
        this.responseAvailable.signalAll();
      }
      finally
      {
        this.responseLock.unlock();
      }
    }
    
    public long getLatency()
    {
      return this.endTime - this.startTime;
    }
    
    public boolean cancel(boolean mayInterruptIfRunning)
    {
      return false;
    }
    
    public boolean isCancelled()
    {
      return false;
    }
    
    public boolean isDone()
    {
      if (!this.request.executable.hasResponse()) {
        return true;
      }
      return this.response != null;
    }
    
    public V get()
      throws InterruptedException, ExecutionException
    {
      if (!this.request.executable.hasResponse()) {
        return null;
      }
      try
      {
        this.responseLock.lock();
        if (this.response == null) {
          this.responseAvailable.await();
        }
      }
      finally
      {
        this.responseLock.unlock();
      }
      if (this.response.exception != null)
      {
        if ((this.response.exception instanceof InterruptedException)) {
          throw ((InterruptedException)this.response.exception);
        }
        throw new ExecutionException("Execution on " + this.request.from + " failed.", this.response.exception);
      }
      return (V)this.response.response;
    }
    
    public V get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException
    {
      if (!this.request.executable.hasResponse()) {
        return null;
      }
      try
      {
        this.responseLock.lock();
        if (this.response == null) {
          this.responseAvailable.await(timeout, unit);
        }
      }
      finally
      {
        this.responseLock.unlock();
      }
      if (this.response.exception != null)
      {
        if ((this.response.exception instanceof InterruptedException)) {
          throw ((InterruptedException)this.response.exception);
        }
        throw new ExecutionException("Execution on " + this.request.from + " failed.", this.response.exception);
      }
      if (this.response == null) {
        throw new TimeoutException("Execution on " + this.request.from + " timed out after " + timeout + " " + unit.name().toLowerCase());
      }
      return (V)this.response.response;
    }
  }
  
  public <V> Future<V> execute(UUID where, Executable<O, V> executable)
  {
    ExecutionRequest<O, V> request = new ExecutionRequest(this.from, requestIdSeq.getAndIncrement(), executable);
    ExecutableFuture<O, V> future = new ExecutableFuture(request, where);
    if (this.thisId.equals(where))
    {
      future.setLocal(true);
      executable.setOn(on());
      ExecutionResponse<V> response = null;
      try
      {
        V result = executable.call();
        if (executable.hasResponse()) {
          response = new ExecutionResponse(request.requestId, result);
        }
      }
      catch (Throwable t)
      {
        response = new ExecutionResponse(request.requestId, t);
      }
      if (response != null) {
        future.setResponse(response);
      }
      updateStat(this.directExecution, request.executable.getClass());
    }
    else
    {
      future.setLocal(false);
      if (executable.hasResponse())
      {
        if (!this.running) {
          throw new IllegalStateException("Trying to make a blocking call in stopped state");
        }
        this.futures.put(Long.valueOf(request.requestId), future);
      }
      send(where, request, true);
      updateStat(this.sentRequests, request.executable.getClass());
    }
    return future;
  }
  
  private Map<Class<?>, AtomicInteger> directExecution = new ConcurrentHashMap();
  private Map<Class<?>, AtomicInteger> sentRequests = new ConcurrentHashMap();
  private Map<Class<?>, AtomicInteger> sentResponses = new ConcurrentHashMap();
  private Map<Class<?>, AtomicInteger> rcvdRequests = new ConcurrentHashMap();
  private Map<Class<?>, AtomicInteger> rcvdResponses = new ConcurrentHashMap();
  
  private void updateStat(Map<Class<?>, AtomicInteger> which, Class<?> what)
  {
    AtomicInteger ai = (AtomicInteger)which.get(what);
    if (ai == null)
    {
      ai = new AtomicInteger(1);
      which.put(what, ai);
    }
    else
    {
      ai.incrementAndGet();
    }
  }
  
  private int getStat(Map<Class<?>, AtomicInteger> which, Class<?> what)
  {
    AtomicInteger ai = (AtomicInteger)which.get(what);
    if (ai == null) {
      return 0;
    }
    return ai.get();
  }
  
  public void outputStats()
  {
    Set<Class<?>> classes = new HashSet();
    classes.addAll(this.directExecution.keySet());
    classes.addAll(this.sentRequests.keySet());
    classes.addAll(this.sentResponses.keySet());
    classes.addAll(this.rcvdRequests.keySet());
    classes.addAll(this.rcvdResponses.keySet());
    System.out.println("Overall stats:");
    for (Class<?> c : classes) {
      System.out.println(c.getSimpleName() + " direct " + getStat(this.directExecution, c) + " sentreq " + getStat(this.sentRequests, c) + " sentrsp " + getStat(this.sentResponses, c) + " rcvdreq " + getStat(this.rcvdRequests, c) + " rcvdrsp " + getStat(this.rcvdResponses, c));
    }
  }
  
  Map<UUID, Sender> senderMapForRequests = new ConcurrentHashMap();
  Map<UUID, Sender> senderMapForResponses = new ConcurrentHashMap();
  
  private void send(UUID where, Object what, boolean isRequest)
  {
    Sender ss;
    try
    {
      if (isRequest)
      {
         ss = (Sender)this.senderMapForRequests.get(where);
        if (ss == null)
        {
          ss = this.ms.getConnectionToReceiver(where, this.name.concat("-requestHandler"), SocketType.PUSH, false);
          this.senderMapForRequests.put(where, ss);
        }
      }
      else
      {
        ss = (Sender)this.senderMapForResponses.get(where);
        if (ss == null)
        {
          ss = this.ms.getConnectionToReceiver(where, this.name.concat("-responseHandler"), SocketType.PUSH, false);
          this.senderMapForResponses.put(where, ss);
        }
      }
    }
    catch (Exception e)
    {
      throw new RuntimeException("Could not get connection to " + this.name + " on " + where, e);
    }
    ss.send(what);
  }
  
  private <V> void sendResponse(ExecutionRequest<O, ?> request, ExecutionResponse<V> response)
  {
    send(request.from.getFrom(), response, false);
    updateStat(this.sentResponses, request.executable.getClass());
  }
  
  private void handleRequest(ExecutionRequest<O, ?> request)
  {
    Executable<O, ?> executable = request.executable;
    executable.setOn(on());
    updateStat(this.rcvdRequests, request.executable.getClass());
    
    ExecutionResponse<?> response = null;
    try
    {
      Object result = executable.call();
      if (executable.hasResponse()) {
        response = new ExecutionResponse(request.requestId, result);
      }
    }
    catch (Throwable t)
    {
      response = new ExecutionResponse(request.requestId, t);
    }
    if (response != null) {
      sendResponse(request, response);
    }
  }
  
  private void handleResponse(ExecutionResponse<?> response)
  {
    ExecutableFuture<O, ?> future = (ExecutableFuture)this.futures.get(Long.valueOf(response.requestId));
    if (future != null)
    {
      future.setResponse(response);
      this.futures.remove(Long.valueOf(response.requestId));
      updateStat(this.rcvdResponses, future.request.executable.getClass());
    }
    else
    {
      logger.warn("Received response with no matching request id=" + response.requestId);
    }
  }
  
  public void onMessage(Object data)
  {
    if ((data instanceof ExecutionRequest)) {
      handleRequest((ExecutionRequest)data);
    } else if ((data instanceof ExecutionResponse)) {
      handleResponse((ExecutionResponse)data);
    }
  }
  
  public String getName()
  {
    return this.name;
  }
  
  public FlowComponent getOwner()
  {
    return null;
  }
}
