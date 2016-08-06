package com.bloom.tungsten;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;
import java.util.concurrent.Future;
import org.apache.log4j.Logger;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocket.Connection;
import org.eclipse.jetty.websocket.WebSocket.OnTextMessage;
import org.eclipse.jetty.websocket.WebSocketClient;
import org.eclipse.jetty.websocket.WebSocketClientFactory;

public class SimpleWebSocketClient
{
  private static Logger logger = Logger.getLogger(SimpleWebSocketClient.class);
  String url;
  WebSocketClient client = null;
  ClientSideSocket socket = null;
  WebSocket.Connection connection;
  WebSocketClientFactory factory = new WebSocketClientFactory();
  
  public SimpleWebSocketClient(String url)
  {
    this.url = url;
  }
  
  public void start()
  {
    URI destinationUri = null;
    try
    {
      if (this.url != null) {
        destinationUri = new URI(this.url);
      }
      if (!this.factory.isStarted()) {
        this.factory.start();
      }
      this.client = this.factory.newWebSocketClient();
      this.socket = new ClientSideSocket();
      Future<WebSocket.Connection> future = this.client.open(destinationUri, this.socket);
      while (future == null) {
        Thread.sleep(10L);
      }
      this.connection = ((WebSocket.Connection)future.get());
    }
    catch (Throwable t)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("error making a socket connection to url :" + this.url);
      }
    }
  }
  
  public boolean isConnected()
  {
    return this.connection != null;
  }
  
  public void sendMessages(String msg)
  {
    if ((this.connection == null) || (!this.connection.isOpen())) {
      start();
    }
    try
    {
      this.connection.sendMessage(msg);
    }
    catch (IOException e)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("error sending message", e);
      }
    }
  }
  
  public void close()
  {
    if ((this.connection != null) && 
      (!this.connection.isOpen())) {
      this.connection.close();
    }
    this.connection = null;
  }
  
  public static void main(String[] args)
    throws IOException, URISyntaxException, InterruptedException
  {
    String url = "ws://localhost:9080/rmiws/";
    String json = "{\"class\":\"com.bloom.runtime.QueryValidator\",\"method\":\"compileText\",\"params\":[\"01e4a8b4-ee1f-ab31-8fe0-427e6e16d19b\", \"#command#;\"],\"callbackIndex\":2}";
    
    SimpleWebSocketClient tungsteClient = new SimpleWebSocketClient(url);
    tungsteClient.start();
    System.out.println("sending 1st message to remove end point");
    tungsteClient.sendMessages(json);
    Thread.currentThread();Thread.sleep(1000L);
    Scanner scan = new Scanner(System.in);
    String cmd = "help;";
    while (!"quit".equalsIgnoreCase(cmd))
    {
      cmd = scan.nextLine();
      json.replace("#command#", cmd);
      tungsteClient.sendMessages(cmd);
    }
    System.out.println("... done");
    tungsteClient.close();
  }
  
  public static class ClientSideSocket
    implements WebSocket.OnTextMessage
  {
    public void onOpen(WebSocket.Connection connection)
    {
      if (SimpleWebSocketClient.logger.isDebugEnabled()) {
        SimpleWebSocketClient.logger.debug("socket connection opened");
      }
    }
    
    public void onClose(int closeCode, String message)
    {
      if (SimpleWebSocketClient.logger.isDebugEnabled()) {
        SimpleWebSocketClient.logger.debug("socket connection closed");
      }
    }
    
    public void onMessage(String data)
    {
      if (SimpleWebSocketClient.logger.isDebugEnabled()) {
        SimpleWebSocketClient.logger.debug("received data from server socket :" + data);
      }
    }
  }
}
