package com.bloom.utility;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.log4j.Logger;

import com.bloom.uuid.UUID;

public class GraphUtility
{
  private static Logger logger = Logger.getLogger(GraphUtility.class);
  
  public static Map<UUID, Integer> calculateInDegree(Map<UUID, Set<UUID>> graph)
  {
    Map<UUID, Integer> inDegrees = new HashMap();
    Iterator<Map.Entry<UUID, Set<UUID>>> graphIterator = graph.entrySet().iterator();
    while (graphIterator.hasNext())
    {
      Map.Entry<UUID, Set<UUID>> graphEntry = (Map.Entry)graphIterator.next();
      inDegrees.put(graphEntry.getKey(), Integer.valueOf(0));
    }
    graphIterator = graph.entrySet().iterator();
    while (graphIterator.hasNext())
    {
      Map.Entry<UUID, Set<UUID>> graphEntry = (Map.Entry)graphIterator.next();
      Set<UUID> setOfNeighborVertexes = (Set)graphEntry.getValue();
      for (UUID uuidOfNeigborVertex : setOfNeighborVertexes)
      {
        Integer n = (Integer)inDegrees.get(uuidOfNeigborVertex);
        inDegrees.put(uuidOfNeigborVertex, Integer.valueOf((n == null ? 0 : n.intValue()) + 1));
      }
    }
    return inDegrees;
  }
  
  public static List<UUID> topologicalSort(Map<UUID, Set<UUID>> graph)
    throws Exception
  {
    Map<UUID, Integer> inDegrees = calculateInDegree(graph);
    List<UUID> resultSet = new ArrayList();
    ArrayDeque<UUID> nodesWithNoIncomingEdges = new ArrayDeque();
    for (Map.Entry<UUID, Integer> entry : inDegrees.entrySet()) {
      if (((Integer)entry.getValue()).intValue() == 0) {
        nodesWithNoIncomingEdges.add(entry.getKey());
      }
    }
    while (!nodesWithNoIncomingEdges.isEmpty())
    {
      UUID node = (UUID)nodesWithNoIncomingEdges.pop();
      resultSet.add(node);
      for (UUID adjacentNode : (Set)graph.get(node))
      {
        inDegrees.put(adjacentNode, Integer.valueOf(((Integer)inDegrees.get(adjacentNode)).intValue() - 1));
        if (((Integer)inDegrees.get(adjacentNode)).intValue() == 0) {
          nodesWithNoIncomingEdges.push(adjacentNode);
        }
      }
    }
    for (Map.Entry<UUID, Integer> entry : inDegrees.entrySet()) {
      if (((Integer)entry.getValue()).intValue() != 0) {
        throw new Exception("Cycle in graph");
      }
    }
    return resultSet;
  }
  
  public static Logger getLogger()
  {
    return logger;
  }
  
  public static void setLogger(Logger logger)
  {
    logger = logger;
  }
}
