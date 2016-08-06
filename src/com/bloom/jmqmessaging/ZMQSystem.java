package com.bloom.jmqmessaging;

import com.bloom.messaging.Handler;
import com.bloom.messaging.MessagingSystem;
import com.bloom.messaging.NoDataConsumerException;
import com.bloom.messaging.Receiver;
import com.bloom.messaging.Sender;
import com.bloom.messaging.SocketType;
import com.bloom.messaging.TransportMechanism;
import com.bloom.messaging.UnknownObjectException;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.runtime.exceptions.NodeNotFoundException;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.uuid.UUID;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.RuntimeInterruptedException;
import com.bloom.jmqmessaging.ZMQReceiverInfo;
import com.bloom.messaging.ReceiverInfo;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.ClosedSelectorException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQException;

import zmq.ZError;
import zmq.ZError.IOException;

public class ZMQSystem
  implements MessagingSystem
{
  private static Logger logger = Logger.getLogger(ZMQSystem.class);
  private final String ipAddress;
  private final UUID serverID;
  private final Boolean isTcpAsyncDisabled;
  private Map<UUID, List<ReceiverInfo>> HostToReceiverInfo;
  private Map<UUID, String> serverIdToMacId;
  private Map<String, ZMQReceiver> receiversInThisSystem;
  private final ZContext ctx;
  private List<UUID> sameHostPeers;
  private String macId;
  private final String IS_ASYNC_STRING = System.getProperty("com.bloom.config.asyncSend", "True");
  private final boolean IS_ASYNC = this.IS_ASYNC_STRING.equalsIgnoreCase("True");
  
  public ZMQSystem(UUID serverID, String ipAddress)
  {
    this.serverID = serverID;
    this.ipAddress = ipAddress;
    this.HostToReceiverInfo = HazelcastSingleton.get().getMap("#HostToMessengerMap");
    this.serverIdToMacId = HazelcastSingleton.get().getMap("#serverIdToMacId");
    this.macId = HazelcastSingleton.getMacID();
    String tcpAsync = System.getProperty("com.bloom.config.disable-tcp-async", "false");
    this.isTcpAsyncDisabled = new Boolean((tcpAsync != null) && (tcpAsync.equalsIgnoreCase("true")));
    synchronized (this.HostToReceiverInfo)
    {
      this.serverIdToMacId.put(this.serverID, this.macId);
      if (!this.HostToReceiverInfo.containsKey(this.serverID)) {
        this.HostToReceiverInfo.put(this.serverID, new ArrayList());
      }
    }
    this.receiversInThisSystem = new ConcurrentHashMap();
    this.sameHostPeers = new ArrayList();
    
    this.ctx = new ZContext(1);
    if (logger.isInfoEnabled()) {
      logger.info("ZMQ System Initialized: ID=" + serverID + " Address=" + ipAddress + " MAC=" + this.macId + "\n" + "ZMQ Known MacIds: " + this.serverIdToMacId.entrySet());
    }
  }
  
  public void createReceiver(Class clazz, Handler handler, String name, boolean encrypted, MetaInfo.Stream paramNotUsed)
  {
    if (clazz == null) {
      throw new NullPointerException("Can't create a Receiver out of a Null Class!");
    }
    if (name == null) {
      throw new NullPointerException("Name of a Receiver can't be Null!");
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Creating receiver for " + name);
    }
    Object[] constructorParams = new Object[4];
    constructorParams[0] = this.ctx;
    constructorParams[1] = handler;
    constructorParams[2] = name;
    constructorParams[3] = Boolean.valueOf(encrypted);
    Class[] classes = { ZContext.class, Handler.class, String.class, Boolean.TYPE };
    if (clazz.getSuperclass().equals(ZMQReceiver.class)) {
      try
      {
        Constructor cc = clazz.getDeclaredConstructor(classes);
        ZMQReceiver receiver = (ZMQReceiver)cc.newInstance(constructorParams);
        receiver.setIpAddress(this.ipAddress);
        receiver.setServerId(this.serverID);
        receiver.makeAddress();
        this.receiversInThisSystem.put(receiver.getName(), receiver);
      }
      catch (NoSuchMethodException e)
      {
        e.printStackTrace();
      }
      catch (InvocationTargetException e)
      {
        e.printStackTrace();
      }
      catch (InstantiationException e)
      {
        e.printStackTrace();
      }
      catch (IllegalAccessException e)
      {
        e.printStackTrace();
      }
    } else {
      throw new UnknownObjectException("Can't create a Receiver for : " + clazz.getName() + ", expects it to be a Sub Class of a certain Platform class");
    }
  }
  
  public Receiver getReceiver(String name)
  {
    return (Receiver)this.receiversInThisSystem.get(name);
  }
  
  public void startReceiver(String name, Map<Object, Object> properties)
    throws Exception
  {
    ZMQReceiver messenger = (ZMQReceiver)this.receiversInThisSystem.get(name);
    if (messenger != null)
    {
      try
      {
        messenger.start(properties);
      }
      catch (Exception e)
      {
        logger.error("Problem starting receiver", e);
        throw e;
      }
      updateReceiverMap(messenger);
    }
    else
    {
      throw new NullPointerException(name + " Receiver is not created.");
    }
  }
  
  public Sender getConnectionToReceiver(UUID peerID, String name, SocketType type, boolean isEncrypted)
    throws NodeNotFoundException, InterruptedException
  {
    if (peerID == null) {
      throw new NoDataConsumerException("PeerID is null, can't find component " + name + " on null.");
    }
    if (name == null) {
      throw new NullPointerException("Component name is null, looking for component with name null on WAServer with ID : " + peerID + " is not possible.");
    }
    if (type == null) {
      throw new NullPointerException("Socket type is null, can't create a Sender to " + name + " on  WAServer with ID : " + peerID);
    }
    Sender sender = null;
    if (this.serverID.equals(peerID))
    {
      sender = getInProcInterfaceToReceiver(name, type);
    }
    else if (this.sameHostPeers.contains(peerID))
    {
      sender = getIpcInterfaceToReceiver(peerID, name, type, isEncrypted);
    }
    else if (this.serverIdToMacId != null)
    {
      String serverMacId = (String)this.serverIdToMacId.get(this.serverID);
      String peerMacId = (String)this.serverIdToMacId.get(peerID);
      if ((serverMacId != null) && (peerMacId != null) && (serverMacId.equals(peerMacId)))
      {
        if (!this.sameHostPeers.contains(peerID)) {
          this.sameHostPeers.add(peerID);
        }
        sender = getIpcInterfaceToReceiver(peerID, name, type, isEncrypted);
      }
      else
      {
        sender = getTcpInterfaceToReceiver(peerID, name, type, isEncrypted);
      }
    }
    else
    {
      sender = getTcpInterfaceToReceiver(peerID, name, type, isEncrypted);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Sender for " + name + " on peer: " + peerID + " created");
    }
    return sender;
  }
  
  public Sender getInProcInterfaceToReceiver(String name, SocketType type)
    throws NodeNotFoundException, InterruptedException
  {
    ReceiverInfo rinfo = getReceiverInfo(this.serverID, name);
    if (rinfo == null) {
      throw new NodeNotFoundException(name + " does not exist on " + this.serverID);
    }
    if (!(rinfo instanceof ZMQReceiverInfo)) {
      throw new NodeNotFoundException(name + " is not a ZMQReceiverInfo, it is a " + rinfo.getClass());
    }
    ZMQReceiverInfo info = (ZMQReceiverInfo)rinfo;
    ZMQReceiver receiver = (ZMQReceiver)this.receiversInThisSystem.get(info.getName());
    Sender sender = this.IS_ASYNC ? new InprocAsyncSender(this.ctx, this.serverID, info, type, receiver) : new InprocSender(this.ctx, this.serverID, info, type, receiver);
    
    sender.start();
    return sender;
  }
  
  public Sender getIpcInterfaceToReceiver(UUID peerID, String name, SocketType type, boolean isEncrypted)
    throws NodeNotFoundException, InterruptedException
  {
    ReceiverInfo rinfo = getReceiverInfo(peerID, name);
    if (rinfo == null) {
      throw new NodeNotFoundException(name + " does not exist on " + this.serverID);
    }
    if (!(rinfo instanceof ZMQReceiverInfo)) {
      throw new NodeNotFoundException(name + " is not a ZMGReceiverInfo, it is a " + rinfo.getClass());
    }
    ZMQReceiverInfo info = (ZMQReceiverInfo)rinfo;
    Sender ipc = this.isTcpAsyncDisabled.booleanValue() ? new TcpSender(this.ctx, this.serverID, info, type, TransportMechanism.IPC, isEncrypted) : new TcpAsyncSender(this.ctx, this.serverID, info, type, TransportMechanism.IPC, isEncrypted);
    
    ipc.start();
    return ipc;
  }
  
  public Sender getTcpInterfaceToReceiver(UUID peerID, String name, SocketType type, boolean isEncrypted)
    throws NodeNotFoundException, InterruptedException
  {
    ReceiverInfo rinfo = getReceiverInfo(peerID, name);
    if (rinfo == null) {
      throw new NodeNotFoundException(name + " does not exist on " + this.serverID);
    }
    if (!(rinfo instanceof ZMQReceiverInfo)) {
      throw new NodeNotFoundException(name + " is not a ZMGReceiverInfo, it is a " + rinfo.getClass());
    }
    ZMQReceiverInfo info = (ZMQReceiverInfo)rinfo;
    Sender tcp = this.isTcpAsyncDisabled.booleanValue() ? new TcpSender(this.ctx, this.serverID, info, type, TransportMechanism.TCP, isEncrypted) : new TcpAsyncSender(this.ctx, this.serverID, info, type, TransportMechanism.TCP, isEncrypted);
    
    tcp.start();
    return tcp;
  }
  
  public void updateReceiverMap(ZMQReceiver rcvr)
  {
    if (rcvr == null) {
      throw new NullPointerException("Receiver can't be null!");
    }
    synchronized (this.HostToReceiverInfo)
    {
      List<ReceiverInfo> zmqReceiverInfoList = (List)this.HostToReceiverInfo.get(this.serverID);
      
      ZMQReceiverInfo info = new ZMQReceiverInfo(rcvr.getName(), this.ipAddress);
      if (validateCorrectness(zmqReceiverInfoList, info))
      {
        info.setAddress(rcvr.getAddress());
        zmqReceiverInfoList.add(info);
        this.HostToReceiverInfo.put(this.serverID, zmqReceiverInfoList);
        if (logger.isTraceEnabled()) {
          logger.trace("ZMQ Messengers on current(" + this.serverID.toString() + ") ZMQ System : \n" + this.receiversInThisSystem.keySet());
        }
      }
      else if (logger.isDebugEnabled())
      {
        logger.debug("Receiver " + rcvr.getName() + " already exists");
      }
    }
  }
  
  public boolean validateCorrectness(List<ReceiverInfo> zmqReceiverInfoList, ZMQReceiverInfo info)
  {
    if (info == null) {
      throw new NullPointerException("Can't validate correctness of a Null Object!");
    }
    if (zmqReceiverInfoList.isEmpty()) {
      return true;
    }
    for (ReceiverInfo mInfo : zmqReceiverInfoList) {
      if (mInfo.equals(info)) {
        return false;
      }
    }
    return true;
  }
  
  public ReceiverInfo getReceiverInfo(UUID uuid, String name)
    throws InterruptedException
  {
    ReceiverInfo info = null;
    int count = 0;
    while (info == null)
    {
      synchronized (this.HostToReceiverInfo)
      {
        List<ReceiverInfo> returnInfo = null;
        try
        {
          returnInfo = (List)this.HostToReceiverInfo.get(uuid);
        }
        catch (RuntimeInterruptedException e)
        {
          throw new InterruptedException(e.getMessage());
        }
        if ((returnInfo != null) && (!returnInfo.isEmpty())) {
          for (ReceiverInfo anInfo : returnInfo)
          {
            String name2 = anInfo.getName();
            if (name.equals(name2)) {
              info = anInfo;
            }
          }
        }
      }
      if ((info != null) || (count >= 10)) {
        break;
      }
      Thread.sleep(500L);
      count++;
    }
    return info;
  }
  
  public boolean stopReceiver(String name)
    throws Exception
  {
    if (name == null) {
      throw new NullPointerException("Can't Undeploy a Receiver with Null Value!");
    }
    boolean isAlive = killReceiver(name);
    synchronized (this.HostToReceiverInfo)
    {
      ArrayList<ReceiverInfo> rcvrs = (ArrayList)this.HostToReceiverInfo.get(this.serverID);
      boolean removed = false;
      for (ReceiverInfo rcvrInfo : rcvrs) {
        if (name.equalsIgnoreCase(rcvrInfo.getName()))
        {
          removed = rcvrs.remove(rcvrInfo);
          break;
        }
      }
      if ((!isAlive) && (removed))
      {
        if (logger.isDebugEnabled()) {
          logger.debug("Stopped Receiver : " + name);
        }
      }
      else {
        throw new RuntimeException("Receiver : " + name + " not found");
      }
      this.HostToReceiverInfo.put(this.serverID, rcvrs);
      this.receiversInThisSystem.remove(name);
    }
    if (logger.isTraceEnabled())
    {
      logger.trace("HostToReceiver : " + this.HostToReceiverInfo.entrySet());
      logger.trace("All receievers : " + this.receiversInThisSystem.entrySet());
    }
    return isAlive;
  }
  
  public boolean killReceiver(String name)
    throws Exception
  {
    ZMQReceiver messenger = (ZMQReceiver)this.receiversInThisSystem.get(name);
    boolean isAlive;
    if (messenger != null)
    {
       isAlive = messenger.stop();
      while (isAlive)
      {
        isAlive = messenger.stop();
        if (logger.isDebugEnabled()) {
          logger.debug("Trying to stop receiver : " + name);
        }
        Thread.sleep(100L);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Stopped Receiver : " + name);
      }
    }
    else
    {
      throw new NullPointerException(name + " Receiver does not exist");
    }
    
    return isAlive;
  }
  
  public boolean stopAllReceivers()
  {
    boolean isAlive = false;
    for (String name : this.receiversInThisSystem.keySet()) {
      try
      {
        isAlive = stopReceiver(name);
      }
      catch (Exception e)
      {
        logger.debug(e.getMessage());
      }
    }
    return isAlive;
  }
  
  public void shutdown()
  {
    boolean isAlive = stopAllReceivers();
    while (isAlive)
    {
      try
      {
        Thread.sleep(200L);
        isAlive = stopAllReceivers();
      }
      catch (InterruptedException e) {}
      logger.debug("Tryin to Stop All Receivers");
    }
    synchronized (this.HostToReceiverInfo)
    {
      this.HostToReceiverInfo.remove(this.serverID.toString());
      this.serverIdToMacId.remove(this.serverID);
      this.receiversInThisSystem.clear();
    }
    try
    {
      this.ctx.destroy();
    }
    catch (ZError.IOException|ClosedSelectorException|ZMQException e)
    {
      logger.warn(e.getMessage());
    }
  }
  
  public boolean cleanupAllReceivers()
  {
    boolean isAlive = true;
    for (String name : this.receiversInThisSystem.keySet()) {
      try
      {
        isAlive = killReceiver(name);
      }
      catch (Exception e)
      {
        logger.debug(e.getMessage());
      }
    }
    return isAlive;
  }
  
  public Map.Entry<UUID, ReceiverInfo> searchStreamName(String streamName)
  {
	  
    synchronized (this.HostToReceiverInfo)
    {
    	
      Set<UUID> peerUUID = this.HostToReceiverInfo.keySet();
      for (Iterator i = peerUUID.iterator(); i.hasNext();)
      {
        UUID peer = (UUID)i.next();
        List<ReceiverInfo> receiverList = (List)this.HostToReceiverInfo.get(peer);
        final UUID pUuid = peer;
        for (final ReceiverInfo receiverInfo : receiverList) {
          if (receiverInfo.getName().startsWith(streamName)) {
            new Map.Entry()
            {
              public ReceiverInfo setValue(ReceiverInfo value)
              {
                return null;
              }
              
              public ReceiverInfo getValue()
              {
                return receiverInfo;
              }
              
              public UUID getKey()
              {
                return pUuid;
              }
            };
          }
        }
      }
      
      return null;
    }
  }
  
  public Class getReceiverClass()
  {
    return PullReceiver.class;
  }
}
