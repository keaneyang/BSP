package com.bloom.proc.events;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.anno.EventType;
import com.bloom.anno.EventTypeData;
import com.bloom.event.SimpleEvent;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import org.pcap4j.packet.ArpPacket;
import org.pcap4j.packet.ArpPacket.ArpHeader;
import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.EthernetPacket.EthernetHeader;
import org.pcap4j.packet.IcmpV4CommonPacket;
import org.pcap4j.packet.IcmpV4CommonPacket.IcmpV4CommonHeader;
import org.pcap4j.packet.IcmpV6CommonPacket;
import org.pcap4j.packet.IcmpV6CommonPacket.IcmpV6CommonHeader;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.IpV4Packet.IpV4Header;
import org.pcap4j.packet.IpV6Packet;
import org.pcap4j.packet.IpV6Packet.IpV6Header;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.TcpPacket;
import org.pcap4j.packet.TcpPacket.TcpHeader;
import org.pcap4j.packet.UdpPacket;
import org.pcap4j.packet.UdpPacket.UdpHeader;
import org.pcap4j.packet.namednumber.TcpPort;
import org.pcap4j.packet.namednumber.UdpPort;

@EventType(schema="Internal", classification="All", uri="com.bloom.proc.events:PCAPPacketEvent:1.0")
public class PCAPPacketEvent
  extends SimpleEvent
{
  private static final long serialVersionUID = -861041374336281417L;
  public long ts;
  public int tsns;
  public int len;
  
  public PCAPPacketEvent() {}
  
  public PCAPPacketEvent(long timestamp)
  {
    super(timestamp);
  }
  
  public PCAPPacketEvent(long ts, int tsns, int len, Packet packet)
  {
    this.ts = ts;
    this.tsns = tsns;
    this.len = len;
    if ((packet instanceof EthernetPacket))
    {
      this.linkType = "ETHER";
      EthernetPacket ep = (EthernetPacket)packet;
      this.ether = ep.getHeader();
      
      Packet network = ep.getPayload();
      if ((network instanceof IpV4Packet))
      {
        this.networkType = "IPV4";
        IpV4Packet ip = (IpV4Packet)network;
        this.ipv4 = ip.getHeader();
        this.srcAddress = this.ipv4.getSrcAddr().getHostAddress();
        this.dstAddress = this.ipv4.getDstAddr().getHostAddress();
      }
      else if ((network instanceof IpV6Packet))
      {
        this.networkType = "IPV6";
        IpV6Packet ip = (IpV6Packet)network;
        this.ipv6 = ip.getHeader();
        this.srcAddress = this.ipv6.getSrcAddr().getHostAddress();
        this.dstAddress = this.ipv6.getDstAddr().getHostAddress();
      }
      else if ((network instanceof ArpPacket))
      {
        this.networkType = "ARP";
        ArpPacket ip = (ArpPacket)network;
        this.arp = ip.getHeader();
        this.srcAddress = this.arp.getSrcProtocolAddr().getHostAddress();
        this.dstAddress = this.arp.getDstProtocolAddr().getHostAddress();
      }
      else
      {
        this.networkType = "UNKNOWN";
      }
      if (network != null)
      {
        Packet transport = network.getPayload();
        if ((transport instanceof TcpPacket))
        {
          this.transportType = "TCP";
          TcpPacket t = (TcpPacket)transport;
          this.tcp = t.getHeader();
          this.srcPort = this.tcp.getSrcPort().valueAsInt();
          this.dstPort = this.tcp.getDstPort().valueAsInt();
        }
        else if ((transport instanceof UdpPacket))
        {
          this.transportType = "UDP";
          UdpPacket t = (UdpPacket)transport;
          this.udp = t.getHeader();
          this.srcPort = this.udp.getSrcPort().valueAsInt();
          this.dstPort = this.udp.getDstPort().valueAsInt();
        }
        else if ((transport instanceof IcmpV4CommonPacket))
        {
          this.transportType = "ICMPV4";
          IcmpV4CommonPacket t = (IcmpV4CommonPacket)transport;
          this.icmpV4 = t.getHeader();
        }
        else if ((transport instanceof IcmpV6CommonPacket))
        {
          this.transportType = "ICMPV6";
          IcmpV6CommonPacket t = (IcmpV6CommonPacket)transport;
          this.icmpV6 = t.getHeader();
        }
        else if (transport == null)
        {
          this.transportType = network.getClass().getSimpleName();
        }
        else
        {
          this.transportType = transport.getClass().getSimpleName();
        }
        if ((transport != null) && 
          (transport.getPayload() != null)) {
          this.appPayload = new String(transport.getPayload().getRawData());
        }
      }
    }
    else
    {
      this.linkType = "UNKNOWN";
      this.packet = packet;
    }
  }
  
  @EventTypeData
  public Packet packet = null;
  public String linkType = null;
  public EthernetPacket.EthernetHeader ether = null;
  public String networkType = null;
  public IpV4Packet.IpV4Header ipv4 = null;
  public IpV6Packet.IpV6Header ipv6 = null;
  public ArpPacket.ArpHeader arp = null;
  public String transportType = null;
  public TcpPacket.TcpHeader tcp = null;
  public UdpPacket.UdpHeader udp = null;
  public IcmpV4CommonPacket.IcmpV4CommonHeader icmpV4 = null;
  public IcmpV6CommonPacket.IcmpV6CommonHeader icmpV6 = null;
  public String srcAddress = null;
  public String dstAddress = null;
  public int srcPort = -1;
  public int dstPort = -1;
  public String appPayload = null;
  
  public void setPayload(Object[] payload)
  {
    this.ts = ((Long)payload[0]).longValue();
    this.tsns = ((Integer)payload[1]).intValue();
    this.len = ((Integer)payload[2]).intValue();
    this.packet = ((Packet)payload[3]);
  }
  
  public Object[] getPayload()
  {
    return new Object[] { Long.valueOf(this.ts), Integer.valueOf(this.tsns), Integer.valueOf(this.len), this.packet };
  }
  
  public void write(Kryo kryo, Output output)
  {
    super.write(kryo, output);
    output.writeLong(this.ts);
    output.writeInt(this.tsns);
    output.writeInt(this.len);
    kryo.writeClassAndObject(output, this.packet);
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    this.ts = input.readLong();
    this.tsns = input.readInt();
    this.len = input.readInt();
    this.packet = ((Packet)kryo.readClassAndObject(input));
  }
}
