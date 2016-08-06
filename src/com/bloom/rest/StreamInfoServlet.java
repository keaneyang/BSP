package com.bloom.rest;

import com.bloom.jmqmessaging.ZMQSystem;
import com.bloom.messaging.MessagingProvider;
import com.bloom.messaging.MessagingSystem;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.metaRepository.MetadataRepositoryUtils;
import com.bloom.runtime.Server;
import com.bloom.runtime.channels.DistributedChannel;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.components.Stream;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.security.WASecurityManager;
import com.bloom.ser.KryoSingleton;
import com.bloom.uuid.UUID;
import com.bloom.jmqmessaging.StreamInfoResponse;
import com.bloom.jmqmessaging.ZMQReceiverInfo;
import com.bloom.messaging.ReceiverInfo;
import com.bloom.runtime.DistLink;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.client.RedirectException;
import org.apache.log4j.Logger;

public class StreamInfoServlet
  extends HttpServlet
{
  private static final long serialVersionUID = 2538840396215530681L;
  private static final Logger logger = Logger.getLogger(StreamInfoServlet.class);
  
  private List<DistLink> getSubscriberForStream(String streamName)
    throws MetaDataRepositoryException, RedirectException
  {
    if (Server.server == null) {
      throw new IllegalStateException("getSubscriberForStream can only be called from a server node");
    }
    String[] tokenized = streamName.split(":");
    if (tokenized.length != 2) {
      throw new IllegalArgumentException("Illegal stream name argument :" + streamName + ". Argument should be of format NAMESPACE:STREAMNAME");
    }
    MetaInfo.Stream streamMetaObj = (MetaInfo.Stream)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.STREAM, tokenized[0], tokenized[1], null, WASecurityManager.TOKEN);
    if (streamMetaObj == null) {
      throw new NoSuchElementException("Stream '" + streamName + "' does not exist");
    }
    Stream streamObj = (Stream)Server.server.getOpenObject(streamMetaObj.getUuid());
    Iterator i$;
    MetaInfo.Server srv;
    Map<String, Set<UUID>> srvObjectUUIDs;
    if (streamObj == null)
    {
      Set<MetaInfo.Server> servers = MetadataRepository.getINSTANCE().getByEntityType(EntityType.SERVER, WASecurityManager.TOKEN);
      for (i$ = servers.iterator(); i$.hasNext();)
      {
        srv = (MetaInfo.Server)i$.next();
        if (!srv.isAgent)
        {
          srvObjectUUIDs = srv.getCurrentUUIDs();
          for (String app : srvObjectUUIDs.keySet())
          {
            Set<UUID> objects = (Set)srvObjectUUIDs.get(app);
            if (objects.contains(streamMetaObj.uuid))
            {
              String redirectUri = srv.getWebBaseUri() + "/streams/" + streamName;
              if (logger.isTraceEnabled()) {
                logger.trace("Stream not found on this node.Setting redirect URI to " + redirectUri);
              }
              throw new RedirectException(redirectUri);
            }
          }
        }
      }
    }
    
    
    if (streamObj == null) {
      throw new NoSuchElementException("Stream '" + streamName + "' does not exist");
    }
    Set<DistLink> subList = streamObj.getDistributedChannel().getSubscribers();
    List<DistLink> retVal = new ArrayList();
    retVal.addAll(subList);
    return retVal;
  }
  
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException
  {
    String streamName = request.getPathInfo();
    if (streamName.startsWith("/")) {
      streamName = streamName.substring(1);
    }
    int index = streamName.indexOf(':');
    if (index == -1)
    {
      logger.warn("Failure in retrieving stream information for " + streamName + ".Missing ':' from streamName");
      response.setStatus(400);
      response.getWriter().write("Invalid streamName '" + streamName + "'. Missing colon character between namespace and name");
      return;
    }
    String namespace = streamName.substring(0, index);
    String name = streamName.substring(index + 1, streamName.length());
    if (Server.server == null)
    {
      response.setStatus(501);
      response.getWriter().write("Request received by an invalid Bloom Server");
      return;
    }
    MessagingSystem msgSystem = MessagingProvider.getMessagingSystem(MessagingProvider.ZMQ_SYSTEM);
    if (!(msgSystem instanceof ZMQSystem))
    {
      response.setStatus(501);
      response.getWriter().write("Request received by an invalid Bloom Server");
      return;
    }
    ZMQSystem zmqMsgSystem = (ZMQSystem)msgSystem;
    
    Map.Entry<UUID, ReceiverInfo> streamDetails = null;
    List<DistLink> subscriberList = null;
    boolean isEncrypted = false;
    try
    {
      streamDetails = zmqMsgSystem.searchStreamName(streamName);
      if (streamDetails == null)
      {
        response.setStatus(404);
        response.getWriter().write("No Stream found for '" + streamName + "'");
        return;
      }
      MetaInfo.Stream streamInfo = (MetaInfo.Stream)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.STREAM, namespace, name, null, WASecurityManager.TOKEN);
      if (streamInfo == null)
      {
        response.setStatus(404);
        response.getWriter().write("No Stream found for '" + streamName + "'");
        return;
      }
      MetaInfo.Flow appInfo = MetadataRepositoryUtils.getAppMetaObjectBelongsTo(streamInfo);
      if (appInfo != null) {
        isEncrypted = appInfo.encrypted;
      }
    }
    catch (Exception e)
    {
      logger.warn("Failed to retrieve stream information for " + streamName, e);
      response.setStatus(404);
      response.getWriter().write("No Stream found for '" + streamName + "'");
      return;
    }
    try
    {
      subscriberList = getSubscriberForStream(streamName);
    }
    catch (NoSuchElementException nse)
    {
      response.setStatus(404);
      response.getWriter().write("No Stream found for '" + streamName + "'");
      return;
    }
    catch (RedirectException nse)
    {
      response.setStatus(302);
      response.addHeader("Location", nse.getMessage());
      return;
    }
    catch (Exception e)
    {
      logger.warn("Failed to retrieve stream information for " + streamName, e);
      response.setStatus(404);
      response.getWriter().write("No Stream found for '" + streamName + "'");
      return;
    }
    StreamInfoResponse streamResponse = new StreamInfoResponse((UUID)streamDetails.getKey(), (ZMQReceiverInfo)streamDetails.getValue(), subscriberList, isEncrypted);
    
    byte[] data = KryoSingleton.write(streamResponse, false);
    
    response.setStatus(200);
    response.setContentType("application/x-bloom-serialized");
    response.setContentLength(data.length);
    
    response.getOutputStream().write(data);
    response.getOutputStream().flush();
  }
}
