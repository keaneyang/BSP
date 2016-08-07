package com.bloom.runtime;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.log4j.Logger;

public class ReleaseNumber
{
  private int major;
  private int minor;
  private int patch;
  private String extras;
  private boolean isDevVersion;
  private static Logger logger = Logger.getLogger(ReleaseNumber.class);
  
  private ReleaseNumber(int major, int minor, int patch, String extras)
  {
    this.major = major;
    this.minor = minor;
    this.patch = patch;
    this.extras = extras;
    if ((major == 1) && (minor == 0) && (patch == 0)) {
      setIsDevVersion(true);
    }
  }
  
  public static ReleaseNumber createReleaseNumber(int major, int minor, int patch)
  {
    return new ReleaseNumber(major, minor, patch, null);
  }
  
  public static ReleaseNumber createReleaseNumber(int major, int minor, String patch)
  {
    int patch_int = Integer.parseInt(Character.toString(patch.charAt(0)));
    String extras = patch.substring(1);
    return new ReleaseNumber(major, minor, patch_int, extras);
  }
  
  public static ReleaseNumber createReleaseNumber(String releaseAsString)
  {
    String[] releaseStringSplit = releaseAsString.split("\\.");
    if (releaseStringSplit.length == 1)
    {
      releaseAsString.concat(".").concat(String.valueOf(0)).concat(".").concat(String.valueOf(0));
    }
    else if (releaseStringSplit.length == 2)
    {
      releaseAsString.concat(".").concat(String.valueOf(0));
    }
    else if (releaseStringSplit.length == 3)
    {
      if (releaseStringSplit[2].length() > 1)
      {
        int patch_int = Integer.parseInt(Character.toString(releaseStringSplit[2].charAt(0)));
        String patch_extra = releaseStringSplit[2].substring(1);
        return new ReleaseNumber(Integer.parseInt(releaseStringSplit[0]), Integer.parseInt(releaseStringSplit[1]), patch_int, patch_extra);
      }
    }
    else if (releaseStringSplit.length > 3)
    {
      logger.error("Unexpected release number: " + releaseAsString);
      System.exit(1);
    }
    releaseStringSplit = releaseAsString.split("\\.");
    return new ReleaseNumber(Integer.parseInt(releaseStringSplit[0]), Integer.parseInt(releaseStringSplit[1]), Integer.parseInt(releaseStringSplit[2]), null);
  }
  
  public boolean isDevVersion()
  {
    return this.isDevVersion;
  }
  
  public void setIsDevVersion(boolean isDevVersion)
  {
    this.isDevVersion = isDevVersion;
  }
  
  public boolean greaterThanEqualTo(ReleaseNumber releaseNumber)
  {
    if (this.isDevVersion) {
      return true;
    }
    if (releaseNumber.isDevVersion) {
      return false;
    }
    if (this.major > releaseNumber.major) {
      return true;
    }
    if ((this.major == releaseNumber.major) && (this.minor > releaseNumber.minor)) {
      return true;
    }
    if ((this.major == releaseNumber.major) && (this.minor == releaseNumber.minor) && (this.patch > releaseNumber.patch)) {
      return true;
    }
    return false;
  }
  
  public boolean lessThan(ReleaseNumber releaseNumber)
  {
    if (this.isDevVersion) {
      return false;
    }
    if (releaseNumber.isDevVersion) {
      return true;
    }
    if (this.major < releaseNumber.major) {
      return true;
    }
    if ((this.major == releaseNumber.major) && (this.minor < releaseNumber.minor)) {
      return true;
    }
    if ((this.major == releaseNumber.major) && (this.minor == releaseNumber.minor) && (this.patch < releaseNumber.patch)) {
      return true;
    }
    return false;
  }
  
  public String toString()
  {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(this.major).append(".").append(this.minor).append(".").append(this.patch);
    if (this.extras != null) {
      stringBuilder.append(this.extras);
    }
    return stringBuilder.toString();
  }
  
  public boolean equals(Object obj)
  {
    ReleaseNumber releaseNumber = (ReleaseNumber)obj;
    return (this.major == releaseNumber.major) && (this.minor == releaseNumber.minor) && (this.patch == releaseNumber.patch);
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(this.major).append(this.minor).append(this.patch).toHashCode();
  }
}
