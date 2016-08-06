package com.bloom.jmqmessaging;

import com.bloom.messaging.Handler;
import com.bloom.messaging.Receiver;
import com.bloom.uuid.UUID;
import com.bloom.messaging.Address;

import java.util.Map;
import org.zeromq.ZContext;

public abstract class ZMQReceiver
  implements Receiver
{
  private final ZContext ctx;
  private final Handler rcvr;
  private final String name;
  private Address address = null;
  private String ipAddress;
  private UUID serverID;
  private int tcpPort = -1;
  private int ipcPort = -1;
  protected final boolean isEncrypted;
  
  public ZMQReceiver(ZContext ctx, Handler rcvr, String name, boolean encrypted)
  {
    this.ctx = ctx;
    this.rcvr = rcvr;
    this.name = name;
    this.isEncrypted = encrypted;
  }
  
  public void setIpAddress(String ip)
  {
    this.ipAddress = ip;
  }
  
  public void setServerId(UUID sId)
  {
    this.serverID = sId;
  }
  
  public abstract void start(Map<Object, Object> paramMap)
    throws Exception;
  
  public abstract boolean stop()
    throws Exception;
  
  public int getTcpPort()
  {
    return this.tcpPort;
  }
  
  public void setTcpPort(int port)
  {
    this.tcpPort = port;
  }
  
  public int getIpcPort()
  {
    return this.ipcPort;
  }
  
  public void setIpcPort(int ipcPort)
  {
    this.ipcPort = ipcPort;
  }
  
  public String getName()
  {
    return this.name;
  }
  
  public Handler getRcvr()
  {
    return this.rcvr;
  }
  
  public ZContext getCtx()
  {
    return this.ctx;
  }
  
  public String getTcpAddress()
  {
    return this.address.getTcp();
  }
  
  public String getIpcAddress()
  {
    return this.address.getIpc();
  }
  
  public String getInProcAddress()
  {
    return this.address.getInproc();
  }
  
  public Address getAddress()
  {
    return this.address;
  }
  
  public Address makeAddress()
  {
    String tcpAddress = "tcp://" + this.ipAddress + ":" + (getTcpPort() == -1 ? "*" : Integer.valueOf(getTcpPort()));
    
    String ipcAddress = "ipc://" + this.serverID + ":" + getName() + ":" + (getIpcPort() == -1 ? "*" : Integer.valueOf(getIpcPort()));
    String inprocAddress = "inproc://" + this.serverID + ":" + getName();
    this.address = new Address(getName(), tcpAddress, ipcAddress, inprocAddress);
    return this.address;
  }
}
