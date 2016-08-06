package com.bloom.iplookup;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import java.io.IOException;
import org.apache.log4j.Logger;

public final class IPLookup
{
  private static final String PALOALTOIPADDRESS = "999";
  private static final float PALOALTOLATITUDE = 37.44F;
  private static final float PALOALTOLONGITUDE = -122.14F;
  private static LookupService service = null;
  private static Logger logger = Logger.getLogger(IPLookup.class);
  
  private static void invalidIPAddress(String ip, String datafile)
  {
    logger.warn("IP Address " + ip + " cannot be found in datafile '" + datafile + "'");
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
  
  private static Location getLocation(String ip, String datafile)
  {
    Location location = null;
    if (ip.startsWith("999"))
    {
      location = getPaloAltoLocation();
    }
    else
    {
      if (service == null) {
        try
        {
          service = new LookupService(datafile, 1);
        }
        catch (IOException e)
        {
          logger.error(e);
        }
      }
      if (service != null)
      {
        location = service.getLocation(ip);
        if (location == null) {
          invalidIPAddress(ip, datafile);
        }
      }
    }
    return location;
  }
  
  public static String lookupLat(String ip, String datafile)
  {
    Location loc = getLocation(ip, datafile);
    if (loc != null) {
      return Float.toString(loc.latitude);
    }
    return null;
  }
  
  public static String lookupLon(String ip, String datafile)
  {
    Location loc = getLocation(ip, datafile);
    if (loc != null) {
      return Float.toString(loc.longitude);
    }
    return null;
  }
  
  public static String lookupCity(String ip, String datafile)
  {
    Location loc = getLocation(ip, datafile);
    if (loc != null) {
      return loc.city;
    }
    return null;
  }
  
  public static String lookupCountry(String ip, String datafile)
  {
    Location loc = getLocation(ip, datafile);
    if (loc != null) {
      return loc.countryName;
    }
    return null;
  }
  
  public static void clearCache()
  {
    service = null;
  }
}
