package com.bloom.metaRepository;

import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.CQExecutionPlan;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.CQ;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Namespace;
import com.bloom.runtime.meta.MetaInfo.Source;
import com.bloom.runtime.meta.MetaInfo.Target;
import com.bloom.runtime.meta.MetaInfo.WActionStore;
import com.bloom.runtime.meta.MetaInfo.Window;
import com.bloom.uuid.UUID;

import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Logger;

public class MetaDataGraph
{
  private static Logger logger = Logger.getLogger(MetaDataGraph.class);
  private MetaInfo.Namespace namespace;
  private Hashtable<UUID, MetaDataNode> nodes = new Hashtable();
  
  public MetaDataGraph(MetaInfo.Namespace namespace)
  {
    this.namespace = namespace;
    buildNodes();
  }
  
  public Set<MetaInfo.WActionStore> getDownstreamWActionStores(MetaInfo.Source source)
  {
    MetaDataNode sourceNode = getNode(source.uuid);
    if (!sourceNode.getMetaDataClass().equals(MetaInfo.Source.class))
    {
      logger.error("Expected MetaInfo.Source but found " + sourceNode.getMetaDataClass());
      return null;
    }
    Set<MetaInfo.WActionStore> result = depthFirstSearch(sourceNode, MetaInfo.WActionStore.class);
    
    return result;
  }
  
  public Collection<MetaInfo.MetaObject> getDownstreamComponents(MetaInfo.MetaObject source)
  {
    MetaDataNode sourceNode = getNode(source.uuid);
    if (!sourceNode.getMetaDataClass().equals(MetaInfo.Source.class))
    {
      logger.error("Expected MetaInfo.Source but found " + sourceNode.getMetaDataClass());
      return null;
    }
    Set<MetaInfo.MetaObject> result = depthFirstSearch(sourceNode, MetaInfo.MetaObject.class);
    
    return result;
  }
  
  private  Set depthFirstSearch(MetaDataNode node, Class targetClass)
  {
    HashSet result = new HashSet();
    for (MetaDataNode downstreamNode : node.getDownstreamNodes())
    {
      if (targetClass.isAssignableFrom(downstreamNode.getMetaObject().getClass())) {
        result.add(downstreamNode.getMetaObject());
      }
      result.addAll(depthFirstSearch(downstreamNode, targetClass));
    }
    return result;
  }
  
  private MetaDataNode getNode(UUID objectID)
  {
    if (this.nodes.containsKey(objectID)) {
      return (MetaDataNode)this.nodes.get(objectID);
    }
    return getNode((MetaInfo.MetaObject)MDCache.getInstance().get(null, objectID, null, null, null, MDConstants.typeOfGet.BY_UUID));
  }
  
  private MetaDataNode getNode(MetaInfo.MetaObject mo)
  {
    if (this.nodes.containsKey(mo.uuid)) {
      return (MetaDataNode)this.nodes.get(mo.uuid);
    }
    if (mo.type == EntityType.SOURCE) {
      return buildNodeSource((MetaInfo.Source)mo);
    }
    if (mo.type == EntityType.CQ) {
      return buildNodeCQ((MetaInfo.CQ)mo);
    }
    if (mo.type == EntityType.TARGET) {
      return buildNodeTarget((MetaInfo.Target)mo);
    }
    if (mo.type == EntityType.WINDOW) {
      return buildNodeWindow((MetaInfo.Window)mo);
    }
    return buildNode(mo);
  }
  
  private void buildNodes()
  {
    this.nodes.clear();
    List<MetaInfo.MetaObject> lObjects = (List)MDCache.getInstance().get(null, null, this.namespace.nsName, this.namespace.name, null, MDConstants.typeOfGet.BY_NAMESPACE);
    for (MetaInfo.MetaObject mo : lObjects) {
      getNode(mo.uuid);
    }
  }
  
  private MetaDataNode buildNodeCQ(MetaInfo.CQ metaObject)
  {
    MetaDataNode node = new MetaDataNode();
    
    node.setMetaObject(metaObject);
    if (metaObject.plan != null) {
      for (UUID ds : metaObject.plan.getDataSources())
      {
        MetaDataNode stream = getNode(ds);
        node.addUpstreamNode(stream);
        stream.addDownstreamNode(node);
      }
    }
    if (metaObject.stream != null)
    {
      MetaDataNode stream = getNode(metaObject.stream);
      node.addDownstreamNode(stream);
      stream.addUpstreamNode(node);
    }
    this.nodes.put(metaObject.uuid, node);
    return node;
  }
  
  private MetaDataNode buildNodeSource(MetaInfo.Source metaObject)
  {
    MetaDataNode node = new MetaDataNode();
    
    node.setMetaObject(metaObject);
    if (metaObject.outputStream != null)
    {
      MetaDataNode outputStream = getNode(metaObject.outputStream);
      node.addDownstreamNode(outputStream);
      outputStream.addUpstreamNode(node);
    }
    this.nodes.put(metaObject.uuid, node);
    return node;
  }
  
  private MetaDataNode buildNodeTarget(MetaInfo.Target metaObject)
  {
    MetaDataNode node = new MetaDataNode();
    
    node.setMetaObject(metaObject);
    if (metaObject.inputStream != null)
    {
      MetaDataNode stream = getNode(metaObject.inputStream);
      node.addUpstreamNode(stream);
      stream.addDownstreamNode(node);
    }
    this.nodes.put(metaObject.uuid, node);
    return node;
  }
  
  private MetaDataNode buildNodeWindow(MetaInfo.Window metaObject)
  {
    MetaDataNode node = new MetaDataNode();
    
    node.setMetaObject(metaObject);
    if (metaObject.stream != null)
    {
      MetaDataNode stream = getNode(metaObject.stream);
      node.addUpstreamNode(stream);
      stream.addDownstreamNode(node);
    }
    this.nodes.put(metaObject.uuid, node);
    return node;
  }
  
  private MetaDataNode buildNode(MetaInfo.MetaObject metaObject)
  {
    MetaDataNode node = new MetaDataNode();
    
    node.setMetaObject(metaObject);
    
    this.nodes.put(metaObject.uuid, node);
    return node;
  }
  
  private class MetaDataNode
  {
    private MetaInfo.MetaObject metaObject;
    private final Set<MetaDataNode> downstreamNodes;
    private final Set<MetaDataNode> upstreamNodes;
    
    public MetaDataNode()
    {
      this.metaObject = null;
      this.downstreamNodes = new HashSet();
      this.upstreamNodes = new HashSet();
    }
    
    public void setMetaObject(MetaInfo.MetaObject metaObject)
    {
      this.metaObject = metaObject;
    }
    
    public MetaInfo.MetaObject getMetaObject()
    {
      return this.metaObject;
    }
    
    public Set<MetaDataNode> getDownstreamNodes()
    {
      return this.downstreamNodes;
    }
    
    public void addUpstreamNode(MetaDataNode node)
    {
      this.upstreamNodes.add(node);
    }
    
    public void addDownstreamNode(MetaDataNode node)
    {
      this.downstreamNodes.add(node);
    }
    
    public Object getMetaDataClass()
    {
      return this.metaObject.getClass();
    }
  }
}
