package com.bloom.runtime;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import jline.History;
import org.apache.log4j.Logger;

public final class ConsoleReader
{
  private static Logger logger = Logger.getLogger(ConsoleReader.class);
  private static final jline.ConsoleReader jlineConsole = getJLineConsole();
  private static final Console systemConsole = System.console();
  protected static final String StdInputAbsentError = "No Standard Input available, hence terminating.";
  
  private static jline.ConsoleReader getJLineConsole()
  {
    jline.ConsoleReader console = null;
    try
    {
      console = new jline.ConsoleReader();
      console.setUseHistory(false);
      if (logger.isDebugEnabled()) {
        logger.debug("Using JLine console");
      }
    }
    catch (NoClassDefFoundError e)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("JLine console not available. Using system console");
      }
    }
    catch (IOException e)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("JLine console IO error. Using system console");
      }
    }
    return console;
  }
  
  public static String readLine(String prompt)
    throws IOException
  {
    if (jlineConsole != null) {
      return jlineConsole.readLine(prompt);
    }
    if ((prompt != null) && (!prompt.isEmpty())) {
      printf(prompt);
    }
    if (systemConsole != null) {
      return systemConsole.readLine();
    }
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    
    return reader.readLine();
  }
  
  public static String readLine()
    throws IOException
  {
    return readLine("");
  }
  
  public static String readLineRespond(String prompt)
    throws IOException
  {
    String read = readLine(prompt);
    if (read == null)
    {
      System.err.println("No Standard Input available, hence terminating.");
      System.exit(0);
    }
    return read;
  }
  
  public static String readPassword(String prompt)
    throws IOException
  {
    if (jlineConsole != null) {
      return jlineConsole.readLine(prompt, new Character('*'));
    }
    printf(prompt);
    if (systemConsole != null) {
      return new String(System.console().readPassword());
    }
    InputStream inputReader = System.in;
    BufferedReader reader = null;
    if (inputReader != null)
    {
      reader = new BufferedReader(new InputStreamReader(System.in));
    }
    else
    {
      System.err.println("No Standard Input available, hence terminating.");
      System.exit(0);
    }
    return reader.readLine();
  }
  
  public static void enableHistory()
  {
    if (jlineConsole != null)
    {
      File historyFile = new File(System.getProperty("user.home"), ".bloom_history");
      
      Path historyPath = historyFile.toPath();
      Boolean hideFile = Boolean.valueOf(true);
      if (logger.isDebugEnabled()) {
        logger.debug("bloom history file is " + historyFile.toString());
      }
      try
      {
        Files.setAttribute(historyPath, "dos:hidden", Boolean.FALSE, new LinkOption[] { LinkOption.NOFOLLOW_LINKS });
        if (logger.isDebugEnabled()) {
          logger.debug("Cleared DOS Hidden attribute successfully");
        }
      }
      catch (NoSuchFileException e)
      {
        hideFile = Boolean.valueOf(true);
      }
      catch (UnsupportedOperationException|IOException e)
      {
        hideFile = Boolean.valueOf(false);
        if (logger.isDebugEnabled()) {
          logger.debug("Failed to unhide history file '" + historyFile + "', " + e);
        }
      }
      try
      {
        jlineConsole.setHistory(new History(historyFile));
        if (logger.isDebugEnabled()) {
          logger.debug("Set history file successfully");
        }
        if (hideFile.booleanValue())
        {
          Files.setAttribute(historyPath, "dos:hidden", Boolean.TRUE, new LinkOption[] { LinkOption.NOFOLLOW_LINKS });
          if (logger.isDebugEnabled()) {
            logger.debug("Set DOS Hidden attribute successfully");
          }
        }
      }
      catch (UnsupportedOperationException|IOException e)
      {
        if (logger.isDebugEnabled()) {
          logger.debug("Problem setting up history file, '" + historyFile + "', " + e);
        }
      }
      jlineConsole.setUseHistory(true);
    }
  }
  
  public static void enableHistoryWithoutFile()
  {
    if (jlineConsole != null)
    {
      jlineConsole.setHistory(new History());
      if (logger.isDebugEnabled()) {
        logger.debug("History is set successfully without file.");
      }
      jlineConsole.setUseHistory(true);
    }
  }
  
  public static void clearHistory()
  {
    jlineConsole.setHistory(new History());
  }
  
  public static void disableHistory()
  {
    if (jlineConsole != null) {
      jlineConsole.setUseHistory(false);
    }
  }
  
  public static void printf(String toPrint)
  {
    if (systemConsole != null) {
      systemConsole.printf(toPrint, new Object[0]);
    } else {
      synchronized (System.out)
      {
        System.out.print(toPrint);
        System.out.flush();
      }
    }
  }
}
