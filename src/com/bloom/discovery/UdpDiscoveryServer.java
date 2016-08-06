package com.bloom.discovery;

import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.UUID;
import com.hazelcast.core.Endpoint;
import com.hazelcast.core.HazelcastInstance;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.security.GeneralSecurityException;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;

public class UdpDiscoveryServer
  implements Runnable
{
  public static final String DISCOVERY_RESPONSE_HEADER = "BLOOM SERVER NODE";
  private static final int MAX_DATAGRAM_PACKET_SIZE = 1024;
  private static final Logger logger = Logger.getLogger(UdpDiscoveryServer.class);
  private final int boundPort;
  private final String hazelcastBindAddress;
  private final int hazelcastBindPort;
  private final UUID salt;
  private final String clusterName;
  private DatagramChannel channel;
  
  public UdpDiscoveryServer(int boundPort, UUID salt)
  {
    this(boundPort, salt, HazelcastSingleton.getBindingInterface(), ((InetSocketAddress)HazelcastSingleton.get().getLocalEndpoint().getSocketAddress()).getPort(), HazelcastSingleton.getClusterName());
  }
  
  public UdpDiscoveryServer(int boundPort, UUID salt, String serverIP, int serverPort, String cluster)
  {
    this.boundPort = boundPort;
    this.salt = salt;
    this.hazelcastBindAddress = serverIP;
    this.hazelcastBindPort = serverPort;
    this.clusterName = cluster;
  }
  
  public void run()
  {
    try
    {
      Selector selector = Selector.open();
      this.channel = DatagramChannel.open();
      this.channel.configureBlocking(false);
      this.channel.register(selector, 1);
      try
      {
        this.channel.socket().bind(new InetSocketAddress(this.boundPort));
        this.channel.register(selector, 1, new ClientRecord());
        while (!Thread.currentThread().isInterrupted()) {
          if (selector.select() != 0)
          {
            Iterator<SelectionKey> keyIter = selector.selectedKeys().iterator();
            while (keyIter.hasNext())
            {
              SelectionKey key = (SelectionKey)keyIter.next();
              if (key.isReadable()) {
                handleRead(key);
              }
              if ((key.isValid()) && (key.isWritable())) {
                handleWrite(key);
              }
              keyIter.remove();
            }
          }
        }
      }
      finally
      {
        if (this.channel != null)
        {
          this.channel.close();
          this.channel = null;
        }
      }
    }
    catch (ClosedByInterruptException ie) {}catch (Exception e)
    {
      logger.warn("UdpDiscoveryServer stopping to respond to Discovery requests", e);
    }
  }
  
  private boolean verifyDiscoveryRequest(String discoveryPayload)
    throws ClosedByInterruptException
  {
    StringTokenizer tokenizer = new StringTokenizer(discoveryPayload, "\n");
    if (tokenizer.countTokens() < 2) {
      return false;
    }
    String header = tokenizer.nextToken();
    String requestedCluster = tokenizer.nextToken();
    if ((header == null) || (header.trim().isEmpty())) {
      return false;
    }
    if ("DISCOVER BLOOM SERVER".equals(header))
    {
      if ((requestedCluster == null) || (requestedCluster.trim().isEmpty())) {
        return false;
      }
      StringTokenizer clusterTokenizer = new StringTokenizer(requestedCluster, ":");
      if (clusterTokenizer.countTokens() < 2) {
        return false;
      }
      String clusterHeader = clusterTokenizer.nextToken();
      String requestedClusterName = clusterTokenizer.nextToken();
      return this.clusterName.equals(requestedClusterName);
    }
    return false;
  }
  
  private String formatResponse()
    throws UnsupportedEncodingException, GeneralSecurityException
  {
    StringBuffer buffer = new StringBuffer("BLOOM SERVER NODE \n");
    buffer.append(this.hazelcastBindAddress + ":" + this.hazelcastBindPort);
    if (this.salt != null) {
      return WASecurityManager.encrypt(buffer.toString(), this.salt.toEightBytes());
    }
    return buffer.toString();
  }
  
  private void handleRead(SelectionKey key)
    throws IOException
  {
    DatagramChannel channel = (DatagramChannel)key.channel();
    ClientRecord clientRecord = (ClientRecord)key.attachment();
    clientRecord.buffer.clear();
    clientRecord.clientAddress = channel.receive(clientRecord.buffer);
    if ((clientRecord.clientAddress != null) && 
      (verifyDiscoveryRequest(new String(clientRecord.buffer.array())))) {
      key.interestOps(4);
    }
  }
  
  private void handleWrite(SelectionKey key)
    throws GeneralSecurityException, IOException
  {
    DatagramChannel channel = (DatagramChannel)key.channel();
    ClientRecord clientRecord = (ClientRecord)key.attachment();
    clientRecord.buffer.clear();
    clientRecord.buffer.put(formatResponse().getBytes("UTF-8"));
    clientRecord.buffer.flip();
    int bytesSent = channel.send(clientRecord.buffer, clientRecord.clientAddress);
    if (bytesSent != 0) {
      key.interestOps(1);
    }
  }
  
  private static class ClientRecord
  {
    public SocketAddress clientAddress;
    public ByteBuffer buffer = ByteBuffer.allocate(1024);
  }
}
