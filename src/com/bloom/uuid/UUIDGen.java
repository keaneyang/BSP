 package com.bloom.uuid;
 
 import java.io.BufferedReader;
 import java.io.File;
 import java.io.IOException;
 import java.io.InputStream;
 import java.io.InputStreamReader;
 import java.io.OutputStream;
 import java.io.PrintStream;
 import java.net.InetAddress;
 import java.net.NetworkInterface;
 import java.net.SocketException;
 import java.net.UnknownHostException;
 import java.util.Enumeration;
 import java.util.concurrent.atomic.AtomicLong;

import com.bloom.uuid.Hex;
import com.bloom.uuid.MACAddressParser;
import com.bloom.uuid.UUID;
import com.bloom.uuid.UUIDGenThread;
 
 
 
 public final class UUIDGen
 {
   private static String macAddress = null;
   
 
 
 
   private static long clockSeqAndNode = Long.MIN_VALUE;
   
   static
   {
     try {
       Class.forName("java.net.InterfaceAddress");
       macAddress = Class.forName("com.bloom.uuid.UUIDGen$HardwareAddressLookup").newInstance().toString();
     }
     catch (ExceptionInInitializerError err) {}catch (ClassNotFoundException ex) {}catch (LinkageError err) {}catch (IllegalAccessException ex) {}catch (InstantiationException ex) {}catch (SecurityException ex) {}
     
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
     if (macAddress == null)
     {
       Process p = null;
       BufferedReader in = null;
       try
       {
         String osname = System.getProperty("os.name", "");String osver = System.getProperty("os.version", "");
         
         if (osname.startsWith("Windows")) {
           p = Runtime.getRuntime().exec(new String[] { "ipconfig", "/all" }, null);
 
 
 
         }
         else if ((osname.startsWith("Solaris")) || (osname.startsWith("SunOS")))
         {
           if (osver.startsWith("5.11")) {
             p = Runtime.getRuntime().exec(new String[] { "dladm", "show-phys", "-m" }, null);
           }
           else
           {
             String hostName = getFirstLineOfCommand(new String[] { "uname", "-n" });
             if (hostName != null) {
               p = Runtime.getRuntime().exec(new String[] { "/usr/sbin/arp", hostName }, null);
             }
             
           }
           
         }
         else if (new File("/usr/sbin/lanscan").exists()) {
           p = Runtime.getRuntime().exec(new String[] { "/usr/sbin/lanscan" }, null);
 
         }
         else if (new File("/sbin/ifconfig").exists()) {
           p = Runtime.getRuntime().exec(new String[] { "/sbin/ifconfig", "-a" }, null);
         }
         
 
         if (p != null) {
           in = new BufferedReader(new InputStreamReader(p.getInputStream()), 128);
           
           String l = null;
           while ((l = in.readLine()) != null) {
             macAddress = MACAddressParser.parse(l);
             if ((macAddress != null) && (Hex.parseShort(macAddress) != 255))
             {
               break;
 
             }
             
           }
           
         }
         
 
       }
       catch (SecurityException ex) {}catch (IOException ex) {}finally
       {
 
         if (p != null) {
           if (in != null) {
             try {
               in.close();
             }
             catch (IOException ex) {}
           }
           
           try
           {
             p.getErrorStream().close();
           }
           catch (IOException ex) {}
           
           try
           {
             p.getOutputStream().close();
           }
           catch (IOException ex) {}
           
 
           p.destroy();
         }
       }
     }
     
 
     if (macAddress != null) {
       clockSeqAndNode |= Hex.parseLong(macAddress);
     } else {
       try
       {
         byte[] local = InetAddress.getLocalHost().getAddress();
         clockSeqAndNode |= local[0] << 24 & 0xFF000000;
         clockSeqAndNode |= local[1] << 16 & 0xFF0000;
         clockSeqAndNode |= local[2] << 8 & 0xFF00;
         clockSeqAndNode |= local[3] & 0xFF;
       }
       catch (UnknownHostException ex) {
         clockSeqAndNode |= (Math.random() * 2.147483647E9D);
       }
     }
     
 
 
     clockSeqAndNode |= (Math.random() * 16383.0D) << 48;
   }
   
 
 
 
 
 
 
   public static long getClockSeqAndNode()
   {
     return clockSeqAndNode;
   }
   
 
 
 
 
 
 
   public static long newTime()
   {
     return createTime(System.currentTimeMillis());
   }
   
 
 
 
 
 
   private static AtomicLong atomicLastTime = new AtomicLong(Long.MIN_VALUE);
   
 
 
 
 
 
 
 
 
   public static long createTime(long currentTimeMillis)
   {
     long timeMillis = currentTimeMillis * 10000L + 122192928000000000L;
     
 
 
 
 
 
 
 
 
 
     long currNewLastTime = atomicLastTime.get();
     if (timeMillis > currNewLastTime) {
       atomicLastTime.compareAndSet(currNewLastTime, timeMillis);
     }
     return atomicLastTime.incrementAndGet();
   }
   
 
 
 
 
 
   public static String getMACAddress()
   {
     return macAddress;
   }
   
 
 
 
 
 
 
   static String getFirstLineOfCommand(String... commands)
     throws IOException
   {
     Process p = null;
     BufferedReader reader = null;
     try
     {
       p = Runtime.getRuntime().exec(commands);
       reader = new BufferedReader(new InputStreamReader(p.getInputStream()), 128);
       
 
       return reader.readLine();
     }
     finally {
       if (p != null) {
         if (reader != null) {
           try {
             reader.close();
           }
           catch (IOException ex) {}
         }
         
         try
         {
           p.getErrorStream().close();
         }
         catch (IOException ex) {}
         
         try
         {
           p.getOutputStream().close();
         }
         catch (IOException ex) {}
         
 
         p.destroy();
       }
     }
   }
   
 
 
 
 
 
 
 
   static class HardwareAddressLookup
   {
     public String toString()
     {
       String out = null;
       try {
         Enumeration<NetworkInterface> ifs = NetworkInterface.getNetworkInterfaces();
         if (ifs != null) {
           while (ifs.hasMoreElements()) {
             NetworkInterface iface = (NetworkInterface)ifs.nextElement();
             byte[] hardware = iface.getHardwareAddress();
             if ((hardware != null) && (hardware.length == 6) && (hardware[1] != -1))
             {
               out = Hex.append(new StringBuilder(36), hardware).toString();
               break;
             }
           }
         }
       }
       catch (SocketException ex) {}
       
 
       return out;
     }
   }
   
   public static class UUIDGenThread extends Thread
   {
     int numCalcs = 10000000;
     
     public UUIDGenThread(int numCalcs) {
       this.numCalcs = numCalcs;
     }
     
     public void run() {
       for (int i = 0; i < this.numCalcs; i++) {
         UUID uuid = new UUID(System.currentTimeMillis());
         uuid.getTime();
       }
     }
   }
   
 
   public static void main(String[] args)
   {
     int numCalcs = 1000000;
     long bytes = 0L;
     
     int numThreads = 4;
     Thread[] t = new Thread[numThreads];
     long stime = System.currentTimeMillis();
     for (int i = 0; i < numThreads; i++) {
       t[i] = new UUIDGenThread(numCalcs);
       t[i].start();
     }
     for (int i = 0; i < numThreads; i++) {
       try {
         t[i].join();
       } catch (InterruptedException e) {
         e.printStackTrace();
       }
     }
     long etime = System.currentTimeMillis();
     long diff = etime - stime;
     if (diff == 0L) diff = 1L;
     double rate = numCalcs * 1000.0D / diff;
     double brate = bytes * 1000.0D / diff;
     System.out.println("KS: " + numCalcs + " ops " + bytes + " bytes in " + diff + " ms - rate = " + rate + " ops/s " + brate + " bytes/s");
   }
 }
