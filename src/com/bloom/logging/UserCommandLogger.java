package com.bloom.logging;

import org.apache.log4j.Logger;

public class UserCommandLogger
{
  public static final String COMMAND_LOGGER = "command_logger";
  private static Logger logger = Logger.getLogger("command_logger");
  private static final String SEPARATOR = " # ";
  
  public static void logCmd(String userid, String sessionid, String msg)
  {
    if (logger.isDebugEnabled()) {
      logger.debug(userid + " # " + sessionid + " # " + msg);
    }
  }
}
