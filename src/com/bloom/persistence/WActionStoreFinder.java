package com.bloom.persistence;

import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.RemoteCall;
import com.bloom.runtime.DistributedExecutionManager;
import com.bloom.uuid.AuthToken;
import com.bloom.waction.WActionApiQueryHandler;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

public class WActionStoreFinder
{
  public static Collection<Boolean> findAServerWhereWSIsDeployed(String wsName, AuthToken token)
  {
    Collection<Boolean> collection = null;
    HazelcastInstance hz = HazelcastSingleton.get();
    Set<Member> members = DistributedExecutionManager.getAllServersButMe(hz);
    if ((members != null) && (!members.isEmpty()))
    {
      IsWactionStoreDeployed isDeployed = new IsWactionStoreDeployed(wsName, token);
      collection = DistributedExecutionManager.exec(hz, isDeployed, members);
      if (collection != null) {
        collection.removeAll(Collections.singleton(Boolean.valueOf(false)));
      }
    }
    return collection;
  }
  
  public static class IsWactionStoreDeployed
    implements RemoteCall<Boolean>
  {
    String wsName = null;
    AuthToken token = null;
    
    public IsWactionStoreDeployed(String wsName, AuthToken authToken)
    {
      this.wsName = wsName;
      this.token = authToken;
    }
    
    public Boolean call()
      throws Exception
    {
      if (WActionApiQueryHandler.canFetchWactions(this.wsName, this.token)) {
        return new Boolean(true);
      }
      return new Boolean(false);
    }
  }
}
