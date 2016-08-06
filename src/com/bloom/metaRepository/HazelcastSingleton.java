package com.bloom.metaRepository;

import com.bloom.classloading.WALoader;
import com.bloom.discovery.UdpDiscoveryClient;
import com.bloom.runtime.ConsoleReader;
import com.bloom.runtime.NodeStartUp;
import com.bloom.runtime.Server;
import com.bloom.security.WASecurityManager;
import com.bloom.ser.CustomHazelcastKryoSerializer;
import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.HazelcastClientProxy;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.ClientService;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Endpoint;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.bloom.event.SimpleEvent;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class HazelcastSingleton
{
  public static enum BloomClusterMemberType
  {
    SERVERNODE,  AGENTNODE,  CONSOLENODE;
    
    private BloomClusterMemberType() {}
  }
  
  static class TungstenProperties
  {
    static int TIMEOUT_SEC = 3000;
    static int TIMEOUT_NUM = 3;
  }
  
  private static Logger logger = Logger.getLogger(HazelcastSingleton.class);
  private static volatile HazelcastInstance instance = null;
  private static final String default_cluster_name = "TestCluster";
  private static Config config = null;
  private static volatile UUID nodeId = null;
  private static final int multicastPort = 54327;
  private static String InterfaceToBind = null;
  public static boolean passedInterfacesDidNotMatchAvailableInterfaces = false;
  private static String interfaceValuePassedFromSytemProperties = null;
  public static int isFirstMember = -1;
  public static Map<UUID, Endpoint> activeClusterMembers = null;
  private static String macID;
  public static boolean noClusterJoin = false;
  
  public static void main(String[] args)
  {
    get();
  }
  
  public static boolean isFirstMember()
  {
    if (isFirstMember == -1)
    {
      Cluster c = instance.getCluster();
      Set<Member> memberSet = c.getMembers();
      Iterator i$ = memberSet.iterator();
      if (i$.hasNext())
      {
        Member member = (Member)i$.next();
        if (c.getLocalMember().equals(member)) {
          isFirstMember = 1;
        } else {
          isFirstMember = 0;
        }
      }
    }
    return isFirstMember == 1;
  }
  
  public static List<String> getLoopBackIP()
  {
    List<String> ip = new ArrayList();
    try
    {
      if (logger.isInfoEnabled()) {
        logger.info(InetAddress.getLocalHost());
      }
      for (NetworkInterface iface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
        if ((iface.isLoopback()) && (iface.isUp()))
        {
          List<InterfaceAddress> list = iface.getInterfaceAddresses();
          for (InterfaceAddress iadd : list) {
            if ((iadd.getBroadcast() != null) || (iadd.getAddress().equals(InetAddress.getLocalHost())))
            {
              ip.add(iadd.getAddress().getHostAddress());
              if (logger.isInfoEnabled()) {
                logger.info("Added " + ip + " to list of usable interfaces");
              }
            }
            else
            {
              logger.warn("Found " + iadd.getAddress().getHostAddress() + " , but does not have broadcast capabilities.");
            }
          }
        }
      }
    }
    catch (SocketException|UnknownHostException e)
    {
      logger.error(e.getMessage());
    }
    return ip;
  }
  
  public static List<String> getInterface()
  {
    List<String> ip = new ArrayList();
    try
    {
      for (NetworkInterface iface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
        if ((!iface.isLoopback()) && (iface.isUp()))
        {
          List<InterfaceAddress> list = iface.getInterfaceAddresses();
          for (InterfaceAddress iadd : list) {
            if (iadd.getBroadcast() != null)
            {
              ip.add(iadd.getAddress().getHostAddress());
              if (logger.isInfoEnabled()) {
                logger.info("Added " + ip + " to list of usable interfaces");
              }
            }
          }
        }
      }
    }
    catch (SocketException e)
    {
      logger.error(e.getMessage());
    }
    if (ip.isEmpty()) {
      ip.addAll(getLoopBackIP());
    }
    return ip;
  }
  
  public static UUID getNodeId()
  {
    if (nodeId == null) {
      synchronized (HazelcastSingleton.class)
      {
        if (nodeId == null)
        {
          String id = get().getLocalEndpoint().getUuid();
          nodeId = new UUID(id);
        }
      }
    }
    return nodeId;
  }
  
  private static String generateRandomMacID()
  {
    int parts = 6;
    String alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    int alphabetLength = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".length();
    String randomMacID = new String();
    Random random = new Random();
    while (parts-- > 0)
    {
      Character firstPosition = Character.valueOf("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".charAt(random.nextInt(alphabetLength)));
      Character secondPosition = Character.valueOf("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".charAt(random.nextInt(alphabetLength)));
      randomMacID = randomMacID + firstPosition.toString() + secondPosition.toString() + "-";
    }
    return randomMacID.substring(0, randomMacID.length() - 1);
  }
  
  public static void setMacID()
  {
    try
    {
      InetAddress address = InetAddress.getByName(InterfaceToBind);
      NetworkInterface networkInterface = NetworkInterface.getByInetAddress(address);
      if (networkInterface != null)
      {
        byte[] hardwareAddress = networkInterface.getHardwareAddress();
        if (hardwareAddress != null)
        {
          StringBuilder builder = new StringBuilder();
          for (int ii = 0; ii < hardwareAddress.length; ii++) {
            builder.append(String.format("%02X%s", new Object[] { Byte.valueOf(hardwareAddress[ii]), ii < hardwareAddress.length - 1 ? "-" : "" }));
          }
          macID = builder.toString();
        }
        else
        {
          macID = generateRandomMacID();
          if (logger.isDebugEnabled()) {
            logger.debug("Using a generated MAC ID : " + macID);
          }
        }
      }
      else
      {
        logger.warn("The network interface " + InterfaceToBind + " is currently not available!");
        System.exit(-1);
      }
    }
    catch (UnknownHostException e)
    {
      logger.error(e.getMessage());
    }
    catch (SocketException e)
    {
      logger.error(e.getMessage());
    }
  }
  
  public static String getMacID()
  {
    if (macID == null) {
      get();
    }
    return macID;
  }
  
  public static String chooseInterface(List<String> ListOfAvailableInterfaces)
  {
    for (int i = 0; i < ListOfAvailableInterfaces.size(); i++) {
      System.out.println(i + 1 + ". " + (String)ListOfAvailableInterfaces.get(i));
    }
    if (ListOfAvailableInterfaces.size() == 0)
    {
      printf("No available interfaces found to connect, exiting now..");
      
      System.exit(0);
    }
    if (ListOfAvailableInterfaces.size() == 1)
    {
      printf("Since there is only 1 interface selecting it by default \n");
      InterfaceToBind = (String)ListOfAvailableInterfaces.get(0);
    }
    else
    {
      String s = null;
      int option;
      try
      {
        printf("Please enter an option number to choose the corresponding interface : \n");
        for (;;)
        {
          s = ConsoleReader.readLineRespond("");
          InterfaceToBind = (String)ListOfAvailableInterfaces.get(Integer.parseInt(s) - 1);

          try
          {
            option = Integer.parseInt(s);
          }
          catch (NumberFormatException e)
          {
            
            System.err.println("Not a valid input. Enter a number between 1 and " + ListOfAvailableInterfaces.size());
          }
        }
      }
      catch (IOException e)
      {
        if (logger.isInfoEnabled()) {
          logger.info("IO error trying to get an option number" + e.getMessage());
        }
      }
    }
    return InterfaceToBind;
  }
  
  public static void initializeBackingStore()
  {
    if (Server.persistenceIsEnabled())
    {
      MapStoreConfig UUIDToURLStoreCfg = new MapStoreConfig();
      UUIDToURLStoreCfg.setClassName("com.bloom.metaRepository.UUIDUrlMetaStore").setEnabled(false);
      UUIDToURLStoreCfg.setWriteDelaySeconds(0);
      MapConfig UUIDToURLMapCfg = getMapConfig("#uuidToUrl");
      UUIDToURLMapCfg.setMapStoreConfig(UUIDToURLStoreCfg);
      config.addMapConfig(UUIDToURLMapCfg);
      
      MapStoreConfig URLToObjectStoreCfg = new MapStoreConfig();
      URLToObjectStoreCfg.setClassName("com.bloom.metaRepository.UrlObjMetaStore").setEnabled(false);
      URLToObjectStoreCfg.setWriteDelaySeconds(0);
      MapConfig URLToObjectMapCfg = getMapConfig("#urlToMetaObject");
      URLToObjectMapCfg.setMapStoreConfig(URLToObjectStoreCfg);
      config.addMapConfig(URLToObjectMapCfg);
    }
  }
  
  public static String getBindingInterface()
  {
    return InterfaceToBind;
  }
  
  private static void setInstance(HazelcastInstance newInstance, WALoader loader)
  {
    synchronized (HazelcastSingleton.class)
    {
      instance = newInstance;
      activeClusterMembers = createActiveMembersMap(newInstance.getCluster());
      loader.init();
    }
  }
  
  public static HazelcastInstance get(String clusterName, String passedInterfaces, BloomClusterMemberType memType)
    throws RuntimeException
  {
    if (instance == null) {
      synchronized (HazelcastSingleton.class)
      {
        if (instance == null) {
          return init(clusterName, passedInterfaces, memType);
        }
      }
    }
    return instance;
  }
  
  public static HazelcastInstance get(String clusterName, String passedInterfaces)
    throws RuntimeException
  {
    return get(clusterName, passedInterfaces, BloomClusterMemberType.SERVERNODE);
  }
  
  public static HazelcastInstance get(String clusterName, BloomClusterMemberType memType)
    throws RuntimeException
  {
    return get(clusterName, null, memType);
  }
  
  public static HazelcastInstance get(String clusterName)
    throws RuntimeException
  {
    return get(clusterName, null, BloomClusterMemberType.SERVERNODE);
  }
  
  public static HazelcastInstance get()
    throws RuntimeException
  {
    return get(null);
  }
  
  private static HazelcastInstance init(String clusterName, String passedInterfaces, BloomClusterMemberType memType)
    throws RuntimeException
  {
    if (logger.isInfoEnabled()) {
      logger.info("re-creating new hazelcast instance");
    }
    Config cfg = createConfig(clusterName, passedInterfaces);
    if (noClusterJoin)
    {
      JoinConfig jn = cfg.getNetworkConfig().getJoin();
      jn.getMulticastConfig().setEnabled(false);
      jn.getAwsConfig().setEnabled(false);
      jn.getTcpIpConfig().setEnabled(false);
    }
    WALoader loader = WALoader.get(false);
    cfg.setClassLoader(loader);
    cfg.setProperty("hazelcast.shutdownhook.enabled", "false");
    cfg.setProperty("hazelcast.wait.seconds.before.join", "1");
    
    initializeBackingStore();
    if (memType != BloomClusterMemberType.SERVERNODE)
    {
      ClientConfig clientConfig = new ClientConfig();
      
      String isEc2Cluster = System.getProperty("com.bloom.config.enable-ec2Clustering");
      Boolean isEc2 = new Boolean((isEc2Cluster != null) && (isEc2Cluster.equalsIgnoreCase("TRUE")));
      if (isEc2.booleanValue())
      {
        ClientAwsConfig clientAwsConfig = new ClientAwsConfig();
        clientAwsConfig.setInsideAws(false);
        clientAwsConfig.setAccessKey(System.getProperty("com.bloom.config.enable-ec2AccessKey"));
        clientAwsConfig.setSecretKey(System.getProperty("com.bloom.config.enable-ec2SecretKey"));
        String region = System.getProperty("com.bloom.config.enable-ec2Region", "us-east-1");
        clientAwsConfig.setRegion(region);
        clientAwsConfig.setHostHeader("ec2." + region + ".amazonaws.com");
        clientAwsConfig.setSecurityGroupName(System.getProperty("com.bloom.config.enable-ec2SecurityGroup"));
        clientAwsConfig.setEnabled(true);
        clientConfig.getNetworkConfig().setAwsConfig(clientAwsConfig);
      }
      else
      {
        String serverNodeAddress = System.getProperty("com.bloom.config.servernode.address");
        List<String> serverMembers = new ArrayList();
        if ((serverNodeAddress == null) || (serverNodeAddress.trim().isEmpty()))
        {
          UdpDiscoveryClient discoveryClient = new UdpDiscoveryClient(9080, clusterName, WASecurityManager.ENCRYPTION_SALT);
          try
          {
            String discoveredNode = discoveryClient.discoverBloomServers().trim();
            serverMembers.add(discoveredNode);
            if (discoveredNode == null) {
              throw new RuntimeException("Failed to discover Bloom Server in the network. Please specify address using -S option");
            }
          }
          catch (Exception e)
          {
            throw new RuntimeException("Failed to discover Bloom Server in the network. Please specify address using -S option");
          }
        }
        else
        {
          serverMembers = Arrays.asList(serverNodeAddress.split(","));
        }
        clientConfig.getNetworkConfig().setAddresses(serverMembers);
      }
      clientConfig.getGroupConfig().setName(clusterName);
      String password = "BloomClusterPassword";
      try
      {
        password = WASecurityManager.encrypt(password + clusterName, WASecurityManager.ENCRYPTION_SALT.toEightBytes());
      }
      catch (Throwable e) {}
      clientConfig.getGroupConfig().setPassword(password);
      printf("Connecting to cluster " + clusterName + ".");
      
      clientConfig.getSerializationConfig().getSerializerConfigs().add(new SerializerConfig().setTypeClass(UUID.class).setImplementation(new CustomHazelcastKryoSerializer(UUID.class, 9900)));
      
      clientConfig.getSerializationConfig().getSerializerConfigs().add(new SerializerConfig().setTypeClass(AuthToken.class).setImplementation(new CustomHazelcastKryoSerializer(AuthToken.class, 9901)));
      
      clientConfig.getSerializationConfig().getSerializerConfigs().add(new SerializerConfig().setTypeClass(ArrayList.class).setImplementation(new CustomHazelcastKryoSerializer(ArrayList.class, 9903)));
      
      clientConfig.getSerializationConfig().getSerializerConfigs().add(new SerializerConfig().setTypeClass(SimpleEvent.class).setImplementation(new CustomHazelcastKryoSerializer(SimpleEvent.class, 9902)));
      try
      {
        instance = HazelcastClient.newHazelcastClient(clientConfig);
      }
      catch (Exception e)
      {
        printf("..Failed\n");
        throw new RuntimeException("Failed to initialize a client connection to cluster " + clusterName);
      }
    }
    else
    {
      cfg.getSerializationConfig().getSerializerConfigs().add(new SerializerConfig().setTypeClass(UUID.class).setImplementation(new CustomHazelcastKryoSerializer(UUID.class, 9900)));
      
      cfg.getSerializationConfig().getSerializerConfigs().add(new SerializerConfig().setTypeClass(AuthToken.class).setImplementation(new CustomHazelcastKryoSerializer(AuthToken.class, 9901)));
      
      cfg.getSerializationConfig().getSerializerConfigs().add(new SerializerConfig().setTypeClass(ArrayList.class).setImplementation(new CustomHazelcastKryoSerializer(ArrayList.class, 9903)));
      
      cfg.getSerializationConfig().getSerializerConfigs().add(new SerializerConfig().setTypeClass(SimpleEvent.class).setImplementation(new CustomHazelcastKryoSerializer(SimpleEvent.class, 9902)));
      
      instance = Hazelcast.newHazelcastInstance(cfg);
    }
    activeClusterMembers = createActiveMembersMap(instance.getCluster());
    
    loader.init();
    return instance;
  }
  
  public static HazelcastInstance initIfPopulated(String clusterName, String passedInterfaces)
  {
    WALoader loader = WALoader.get(false);
    HazelcastInstance instanceCandidate = createHazelcastInstance(clusterName, passedInterfaces, loader);
    
    Set<Member> members = instanceCandidate.getCluster().getMembers();
    for (Member member : members) {
      if (!member.localMember())
      {
        setInstance(instanceCandidate, loader);
        return instanceCandidate;
      }
    }
    return null;
  }
  
  private static HazelcastInstance createHazelcastInstance(String clusterName, String passedInterfaces, WALoader loader)
  {
    if (logger.isInfoEnabled()) {
      logger.info("re-creating new hazelcast instance");
    }
    Config cfg = createConfig(clusterName, passedInterfaces);
    if (noClusterJoin)
    {
      JoinConfig jn = cfg.getNetworkConfig().getJoin();
      jn.getMulticastConfig().setEnabled(false);
      jn.getAwsConfig().setEnabled(false);
      jn.getTcpIpConfig().setEnabled(false);
    }
    cfg.setClassLoader(loader);
    cfg.setProperty("hazelcast.shutdownhook.enabled", "false");
    cfg.setProperty("hazelcast.wait.seconds.before.join", "1");
    
    initializeBackingStore();
    return Hazelcast.newHazelcastInstance(cfg);
  }
  
  public static boolean isClientMember()
  {
    return (instance != null) && ((instance instanceof HazelcastClientProxy));
  }
  
  public static String getClusterName()
  {
    if (instance == null) {
      throw new IllegalStateException("getClusterName can be called only after initialization");
    }
    if ((instance instanceof HazelcastClientProxy)) {
      return ((HazelcastClientProxy)instance).getClientConfig().getGroupConfig().getName();
    }
    return instance.getConfig().getGroupConfig().getName();
  }
  
  private static Map<UUID, Endpoint> createActiveMembersMap(Cluster c)
  {
    ConcurrentHashMap<UUID, Endpoint> map = new ConcurrentHashMap();
    for (Member m : c.getMembers()) {
      map.put(new UUID(m.getUuid()), m);
    }
    c.addMembershipListener(new MembershipListener()
    {
      public void memberAdded(MembershipEvent membershipEvent)
      {
        Member m = membershipEvent.getMember();
        this.val$map.put(new UUID(m.getUuid()), m);
      }
      
      public void memberRemoved(MembershipEvent membershipEvent)
      {
        Member m = membershipEvent.getMember();
        this.val$map.remove(new UUID(m.getUuid()));
      }
      
      public void memberAttributeChanged(MemberAttributeEvent arg0) {}
    });
    if (!isClientMember()) {
      instance.getClientService().addClientListener(new ClientListener()
      {
        public void clientDisconnected(Client client)
        {
          HazelcastSingleton.activeClusterMembers.remove(new UUID(client.getUuid()));
        }
        
        public void clientConnected(Client client)
        {
          HazelcastSingleton.activeClusterMembers.put(new UUID(client.getUuid()), client);
        }
      });
    }
    return map;
  }
  
  public static Map<UUID, Endpoint> getActiveMembersMap()
  {
    return activeClusterMembers;
  }
  
  public static void setDBDetailsForMetaDataRepository(String DBLocation, String DBName, String DBUname, String DBPassword)
  {
    MetaDataDBOps.setDBDetails(DBLocation, DBName, DBUname, DBPassword);
  }
  
  private static byte[] intToByteArray(int value)
  {
    return new byte[] { (byte)(value >>> 24), (byte)(value >>> 16), (byte)(value >>> 8), (byte)value };
  }
  
  private static Config createConfig(String clusterName, String passedInterfaces)
    throws RuntimeException
  {
    if (clusterName == null)
    {
      String cName = System.getProperty("com.bloom.config.clusterName");
      if (cName == null) {
        clusterName = "TestCluster";
      } else {
        clusterName = cName;
      }
    }
    config = new Config();
    NetworkConfig network = config.getNetworkConfig();
    JoinConfig join = network.getJoin();
    
    String isEc2Cluster = System.getProperty("com.bloom.config.enable-ec2Clustering");
    Boolean isEc2 = new Boolean((isEc2Cluster != null) && (isEc2Cluster.equalsIgnoreCase("TRUE")));
    String isTcpIpCluster = System.getProperty("com.bloom.config.enable-tcpipClustering");
    Boolean isTcpIp = new Boolean((isTcpIpCluster != null) && (isTcpIpCluster.equalsIgnoreCase("TRUE")));
    if (isEc2.booleanValue())
    {
      printf("Using Ec2 clustering to discover the cluster members\n");
      join.getMulticastConfig().setEnabled(false);
      join.getTcpIpConfig().setEnabled(false);
      join.getAwsConfig().setEnabled(true);
      join.getAwsConfig().setAccessKey(System.getProperty("com.bloom.config.enable-ec2AccessKey"));
      join.getAwsConfig().setSecretKey(System.getProperty("com.bloom.config.enable-ec2SecretKey"));
      String region = System.getProperty("com.bloom.config.enable-ec2Region", "us-east-1");
      join.getAwsConfig().setHostHeader("ec2." + region + ".amazonaws.com");
      join.getAwsConfig().setRegion(region);
      join.getAwsConfig().setSecurityGroupName(System.getProperty("com.bloom.config.enable-ec2SecurityGroup"));
    }
    else if (isTcpIp.booleanValue())
    {
      printf("Using TcpIp clustering to discover the cluster members\n");
      join.getMulticastConfig().setEnabled(false);
      join.getTcpIpConfig().setEnabled(true);
      String members = System.getProperty("com.bloom.config.servernode.address");
      if (members != null)
      {
        List<String> memberList = Arrays.asList(members.split(","));
        printf(memberList.toString() + "\n");
        List<String> resolvedNames = new ArrayList();
        for (String member : memberList) {
          try
          {
            String host = member;
            String port = null;
            if (member.contains(":"))
            {
              host = member.split(":")[0];
              port = member.split(":")[1];
            }
            String hostAddress = InetAddress.getByName(host).getHostAddress();
            if (port != null) {
              hostAddress = hostAddress + ":" + port;
            }
            resolvedNames.add(hostAddress);
          }
          catch (UnknownHostException e)
          {
            resolvedNames.add(member);
          }
        }
        printf("Resolved Cluster Members to join " + resolvedNames + "\n");
        join.getTcpIpConfig().setMembers(resolvedNames);
      }
      else
      {
        List<String> memberList = getMembersFromFile();
        if (memberList != null) {
          printf(memberList.toString() + "\n");
        } else {
          printf("No member found in file");
        }
        if (memberList != null) {
          join.getTcpIpConfig().setMembers(memberList);
        }
      }
    }
    else
    {
      byte[] chosen = intToByteArray(clusterName.hashCode());
      int part0 = chosen[0] & 0xFF;
      int part1 = chosen[1] & 0xFF;
      int part2 = chosen[2] & 0xFF;
      String multicastGroup = "239." + part2 + "." + part1 + "." + part0;
      
      MulticastConfig multicast = join.getMulticastConfig();
      multicast.setMulticastGroup(multicastGroup);
      multicast.setMulticastPort(54327);
      
      printf("Using Multicast to discover the cluster members on group " + multicastGroup + " port " + 54327 + "\n");
      join.setMulticastConfig(multicast);
    }
    InterfacesConfig intf = network.getInterfaces();
    List<String> ListOfAvailableInterfaces = getInterface();
    interfaceValuePassedFromSytemProperties = System.getProperty("com.bloom.config.interface");
    if ((interfaceValuePassedFromSytemProperties != null) && (interfaceValuePassedFromSytemProperties.equals("null"))) {
      interfaceValuePassedFromSytemProperties = null;
    }
    if ((interfaceValuePassedFromSytemProperties != null) && (!interfaceValuePassedFromSytemProperties.isEmpty()))
    {
      if (logger.isInfoEnabled()) {
        logger.info("Using interface : " + interfaceValuePassedFromSytemProperties + " , passed in thorugh system properties");
      }
      InterfaceToBind = interfaceValuePassedFromSytemProperties;
      if (passedInterfaces != null)
      {
        String[] pIntf = passedInterfaces.split(",");
        for (int i = 0; i < pIntf.length; i++)
        {
          if (pIntf[i].equals(InterfaceToBind))
          {
            passedInterfacesDidNotMatchAvailableInterfaces = false;
            break;
          }
          passedInterfacesDidNotMatchAvailableInterfaces = true;
        }
      }
    }
    else if (passedInterfaces != null)
    {
      String[] pIntf = passedInterfaces.split(",");
      for (int i = 0; i < pIntf.length; i++) {
        if (ListOfAvailableInterfaces.contains(pIntf[i]))
        {
          if (logger.isInfoEnabled()) {
            logger.info("Using interface :" + pIntf[i] + " to bind");
          }
          InterfaceToBind = pIntf[i];
          break;
        }
      }
      if (InterfaceToBind == null)
      {
        passedInterfacesDidNotMatchAvailableInterfaces = true;
        if (logger.isInfoEnabled()) {
          logger.warn("Interfaces in file did not match available interfaces");
        }
        chooseInterface(ListOfAvailableInterfaces);
      }
    }
    else if (ListOfAvailableInterfaces.isEmpty())
    {
      logger.error("Did not find any interfaces not even Loopback !!!");
      System.exit(0);
    }
    else if (ListOfAvailableInterfaces.size() == 1)
    {
      if (logger.isInfoEnabled()) {
        logger.info("Only 1 interface available, Connecting to Interface(does not take into count Loopback) : " + (String)ListOfAvailableInterfaces.get(0));
      }
      passedInterfacesDidNotMatchAvailableInterfaces = true;
      InterfaceToBind = (String)ListOfAvailableInterfaces.get(0);
    }
    else if (ListOfAvailableInterfaces.size() > 1)
    {
      passedInterfacesDidNotMatchAvailableInterfaces = true;
      chooseInterface(ListOfAvailableInterfaces);
    }
    printf("Using " + InterfaceToBind + "\n");
    intf.addInterface(InterfaceToBind);
    intf.setEnabled(true);
    network.setInterfaces(intf);
    setMacID();
    config.setNetworkConfig(network);
    config.getNetworkConfig().setPort(5701);
    config.getNetworkConfig().setPortAutoIncrement(true);
    if (logger.isInfoEnabled()) {
      logger.info(" Current interface in Configuration " + config.getNetworkConfig().getInterfaces());
    }
    config.getGroupConfig().setName(clusterName);
    String password = "BloomClusterPassword";
    try
    {
      password = WASecurityManager.encrypt(password + clusterName, WASecurityManager.ENCRYPTION_SALT.toEightBytes());
    }
    catch (Throwable e) {}
    config.getGroupConfig().setPassword(password);
    if (logger.isInfoEnabled()) {
      logger.info("Initializing cluster with name: " + clusterName);
    }
    return config;
  }
  
  private static List<String> getMembersFromFile()
  {
    Properties props = null;
    
    String filePath = NodeStartUp.getPlatformHome() + "/" + "conf" + "/" + "members.txt";
    
    File f = new File(filePath);
    if (f.exists()) {
      try
      {
        return FileUtils.readLines(f);
      }
      catch (IOException|NullPointerException e)
      {
        logger.error("Error while reading members file.", e);
      }
    }
    return null;
  }
  
  public static boolean isAvailable()
  {
    return instance != null;
  }
  
  public static void printf(String toPrint)
  {
    synchronized (System.out)
    {
      if (System.console() != null) {
        System.console().printf(toPrint, new Object[0]);
      } else {
        System.out.print(toPrint);
      }
      System.out.flush();
    }
  }
  
  private static MapConfig getMapConfig(String mapName)
  {
    MapConfig mapCfg = new MapConfig();
    mapCfg.setName(mapName);
    mapCfg.setBackupCount(1);
    return mapCfg;
  }
  
  public static void shutdown()
  {
    if (instance != null)
    {
      LifecycleService srv = instance.getLifecycleService();
      srv.shutdown();
      instance = null;
      nodeId = null;
      config = null;
      isFirstMember = -1;
    }
  }
}
