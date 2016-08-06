package com.bloom.kafkamessaging;

import java.util.ArrayList;
import java.util.List;
import kafka.cluster.Broker;

@Deprecated
public class PartitionLeaderReplica
{
  protected final Broker leader;
  protected final int partitionId;
  protected final List<Broker> replicas;
  protected final List<String> replicas_as_string;
  
  public PartitionLeaderReplica(Broker leader, int partitionId, List<Broker> replicas)
  {
    this.leader = leader;
    this.partitionId = partitionId;
    this.replicas = replicas;
    this.replicas_as_string = new ArrayList(replicas.size());
    for (Broker broker : replicas) {
      this.replicas_as_string.add(broker.connectionString());
    }
  }
  
  public String toString()
  {
    return "[PLR part=" + this.partitionId + " leader=" + this.leader + "]";
  }
}
