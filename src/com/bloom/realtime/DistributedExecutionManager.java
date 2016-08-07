package com.bloom.runtime;

import com.bloom.exception.Warning;
import com.bloom.wactionstore.exceptions.ConnectionException;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.spi.exception.TargetDisconnectedException;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class DistributedExecutionManager
{
  public static Set<Member> getServerByAddress(HazelcastInstance hz, String server)
  {
    for (Member m : hz.getCluster().getMembers())
    {
      String ip = m.getInetSocketAddress().toString();
      if (server.equals(ip)) {
        return Collections.singleton(m);
      }
    }
    return Collections.emptySet();
  }
  
  public static Set<Member> getAllServers(HazelcastInstance hz)
  {
    return hz.getCluster().getMembers();
  }
  
  public static Set<Member> getAllServersButMe(HazelcastInstance hz)
  {
    Set<Member> mems = new HashSet(hz.getCluster().getMembers());
    mems.remove(hz.getCluster().getLocalMember());
    return mems;
  }
  
  public static Set<Member> getFirstServer(HazelcastInstance hz)
  {
    Iterator i$ = hz.getCluster().getMembers().iterator();
    if (i$.hasNext())
    {
      Member m = (Member)i$.next();
      return Collections.singleton(m);
    }
    return Collections.emptySet();
  }
  
  public static Set<Member> getServerByPartKey(HazelcastInstance hz, Object key)
  {
    Member m = hz.getPartitionService().getPartition(key).getOwner();
    if (m != null) {
      return Collections.singleton(m);
    }
    return getFirstServer(hz);
  }
  
  public static Set<Member> getLocalServer(HazelcastInstance hz)
  {
    return Collections.singleton(hz.getCluster().getLocalMember());
  }
  
  public static <T> Collection<T> exec(HazelcastInstance hz, Callable<T> task, Set<Member> srvs)
  {
    IExecutorService executorService = hz.getExecutorService("executor");
    try
    {
      Map<Member, Future<T>> futureMap = executorService.submitToMembers(task, srvs);
      
      List<T> list = new ArrayList(futureMap.size());
      for (Member m : futureMap.keySet())
      {
        Future<T> future = (Future)futureMap.get(m);
        list.add(future.get());
      }
      return list;
    }
    catch (ExecutionException|InterruptedException e)
    {
      if ((e.getCause() instanceof Warning)) {
        throw ((Warning)e.getCause());
      }
      throw new RuntimeException(e);
    }
  }
  
  public static <T> T execOnAny(HazelcastInstance hz, Callable<T> task)
  {
    IExecutorService executorService = hz.getExecutorService("executor");
    int count = 0;
    for (;;)
    {
      try
      {
        Future<T> future = executorService.submit(task);
        return (T)future.get();
      }
      catch (ExecutionException|InterruptedException e)
      {
        if ((e.getCause() instanceof TargetDisconnectedException))
        {
          if (count >= 5) {
            throw new RuntimeException(e);
          }
          count++;
        }
        else
        {
          if ((e.getCause() instanceof Warning)) {
            throw ((Warning)e.getCause());
          }
          throw new RuntimeException(e);
        }
      }
      catch (ConnectionException e)
      {
        if (count >= 5) {
          throw new RuntimeException(e);
        }
        count++;
      }
    }
  }
}
