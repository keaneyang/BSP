package com.bloom.discovery;

import com.bloom.security.WASecurityManager;
import com.bloom.uuid.UUID;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketTimeoutException;
import java.util.Enumeration;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;

public class UdpDiscoveryClient
{
  public static final String DISCOVERY_REQUEST_HEADER = "DISCOVER BLOOM SERVER";
  private static final int MAX_DATAGRAM_PACKET_SIZE = 1024;
  private static final int TIMEOUT_IN_MSEC = 5000;
  private static final Logger logger = Logger.getLogger(UdpDiscoveryClient.class);
  private final int serverDiscoveryPort;
  private final UUID saltId;
  private final String clusterName;
  
  public UdpDiscoveryClient(int port, String cluster, UUID salt)
  {
    this.serverDiscoveryPort = port;
    this.saltId = salt;
    this.clusterName = cluster;
  }
  
  public String discoverBloomServers()
  {
    DatagramSocket socket = null;
    try
    {
      socket = new DatagramSocket();
      socket.setBroadcast(true);
      
      byte[] request = getDiscoveryMessage().getBytes("UTF-8");
      sendRequestToBroadcastAddress(socket, request);
      sendRequestToAllInterfaces(socket, request);
      byte[] recvBuf;
      recvBuf = new byte[1024];
      DatagramPacket recvPacket = new DatagramPacket(recvBuf, recvBuf.length);
      socket.setSoTimeout(5000);
      
      int maxAttempts = 10;
      String resp;
      while (maxAttempts-- > 0)
      {
        socket.receive(recvPacket);
        
        resp = new String(recvPacket.getData()).trim();
        String payload = null;
        if (this.saltId != null) {
          payload = WASecurityManager.decrypt(resp, this.saltId.toEightBytes());
        } else {
          payload = resp;
        }
        StringTokenizer tokenizer = new StringTokenizer(payload, "\n");
        if (tokenizer.countTokens() > 1)
        {
          String header = tokenizer.nextToken().trim();
          if ("BLOOM SERVER NODE".equals(header)) {
            return tokenizer.nextToken().trim();
          }
        }
      }
      return null;
    }
    catch (SocketTimeoutException ste)
    {
      logger.info("No Bloom Server could be discovered within the specified timeout. Either specify Server Address or check the network setting to enable broadcast");
      return null;
    }
    catch (Exception e)
    {
     
      logger.warn("No Bloom Server could be discovered due to exception", e);
      return null;
    }
    finally
    {
      if (socket != null) {
        socket.close();
      }
    }
  }
  
  private void sendRequestToBroadcastAddress(DatagramSocket socket, byte[] request)
    throws IOException
  {
    DatagramPacket packet = new DatagramPacket(request, request.length, InetAddress.getByName("255.255.255.255"), this.serverDiscoveryPort);
    socket.send(packet);
  }
  
  private void sendRequestToAllInterfaces(DatagramSocket socket, byte[] request)
    throws IOException
  {
    Enumeration<NetworkInterface> intefaces = NetworkInterface.getNetworkInterfaces();
    while (intefaces.hasMoreElements())
    {
      NetworkInterface networkInterface = (NetworkInterface)intefaces.nextElement();
      if ((!networkInterface.isLoopback()) && (networkInterface.isUp())) {
        for (InterfaceAddress iAddress : networkInterface.getInterfaceAddresses())
        {
          InetAddress broadcastAddress = iAddress.getBroadcast();
          if (broadcastAddress != null)
          {
            DatagramPacket sendpacket = new DatagramPacket(request, request.length, broadcastAddress, this.serverDiscoveryPort);
            socket.send(sendpacket);
          }
        }
      }
    }
  }
  
  private String getDiscoveryMessage()
  {
    StringBuffer buffer = new StringBuffer("DISCOVER BLOOM SERVER");
    buffer.append("\nCluster:" + this.clusterName);
    buffer.append("\nRequestTime:" + System.currentTimeMillis());
    return buffer.toString();
  }
}
