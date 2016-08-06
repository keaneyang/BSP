package com.bloom.iplookup;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.net.URL;
import org.apache.log4j.Logger;

public final class IPLookup2
{
  private static final String PALOALTOIPADDRESS = "999";
  private static final float PALOALTOLATITUDE = 37.44F;
  private static final float PALOALTOLONGITUDE = -122.14F;
  private static LookupService service = null;
  private static Logger logger = Logger.getLogger(IPLookup2.class);
  
  private static void invalidIPAddress(String ip)
  {
    logger.warn("IP Address " + ip + " cannot be found");
  }
  
  private static Location getPaloAltoLocation()
  {
    Location location = new Location();
    location.latitude = 37.44F;
    location.longitude = -122.14F;
    location.city = "Palo Alto";
    location.countryName = "USA";
    return location;
  }
  
  static String lastIp = null;
  static Location lastLocation = null;
  
  private static Location getLocation(String ip)
  {
    Location location = null;
    if (ip.startsWith("999"))
    {
      location = getPaloAltoLocation();
    }
    else if ((lastIp != null) && (lastIp.equals(ip)) && (lastLocation != null))
    {
      location = lastLocation;
    }
    else
    {
      if (service == null) {
        try
        {
          URL dir_url = ClassLoader.getSystemResource("GeoLiteCity.dat");
          
          File dir = new File(dir_url.toURI());
          service = new LookupService(dir, 1);
        }
        catch (IOException|URISyntaxException e)
        {
          logger.error(e);
        }
      }
      if (service != null)
      {
        location = service.getLocation(ip);
        if (location == null)
        {
          invalidIPAddress(ip);
        }
        else
        {
          lastLocation = location;
          lastIp = ip;
        }
      }
    }
    return location;
  }
  
  public static String lookupLat(String ip)
  {
    Location loc = getLocation(ip);
    if (loc != null) {
      return Float.toString(loc.latitude);
    }
    return null;
  }
  
  public static String lookupLon(String ip)
  {
    Location loc = getLocation(ip);
    if (loc != null) {
      return Float.toString(loc.longitude);
    }
    return null;
  }
  
  public static String lookupCity(String ip)
  {
    Location loc = getLocation(ip);
    if (loc != null) {
      return loc.city;
    }
    return null;
  }
  
  public static String lookupCountry(String ip)
  {
    Location loc = getLocation(ip);
    if (loc != null) {
      return loc.countryName;
    }
    return null;
  }
  
  public static void clearCache()
  {
    service = null;
  }
  
  public static void main(String[] args)
  {
    System.out.println("City is " + lookupCity("173.11.113.126"));
  }
}
