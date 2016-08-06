package com.bloom.preview;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

public class SampleDataFiles
{
  private static Logger logger = Logger.getLogger(SampleDataFiles.class);
  List<DataFile> samples;
  File homeFile;
  boolean samplesListCreated;
  
  public synchronized void init(String home)
    throws IOException
  {
    if (home.endsWith("Platform")) {
      home = home.substring(0, home.lastIndexOf("/"));
    }
    String customerFolder = System.getProperty("com.bloom.config.samplesHome");
    assert (customerFolder != null);
    File homeDir = new File(home);
    
    home = homeDir.getCanonicalPath();
    
    home = home.concat(customerFolder);
    this.samples = new ArrayList();
    for (DataPreviewConstants.DataFileInfo d : DataPreviewConstants.DataFileInfo.values())
    {
      String pathToFile = home.concat(d.getPath());
      File file = new File(pathToFile);
      if (file.exists())
      {
        DataFile dataFile = new DataFile();
        dataFile.size = file.length();
        dataFile.name = file.getName();
        dataFile.type = d.getType();
        dataFile.fqPath = pathToFile;
        this.samples.add(dataFile);
      }
    }
  }
  
  public void searchForSampleDataFiles(File parent, String nameOfDir)
    throws IOException
  {
    if (parent.isDirectory())
    {
      if (logger.isInfoEnabled()) {
        logger.info("Searching " + parent.getAbsolutePath() + " for creating pointers to sample data files in Source Preview");
      }
      if (parent.canRead()) {
        for (File temp : parent.listFiles()) {
          if (temp.isDirectory()) {
            if (temp.getName().equalsIgnoreCase(nameOfDir))
            {
              if (logger.isInfoEnabled()) {
                logger.info("App data found at: " + temp.getAbsolutePath());
              }
              for (File anotherTemp : temp.listFiles())
              {
                DataFile dataFile = new DataFile();
                dataFile.name = anotherTemp.getName();
                dataFile.fqPath = anotherTemp.getCanonicalPath();
                dataFile.size = anotherTemp.length();
                this.samples.add(dataFile);
              }
            }
            else
            {
              searchForSampleDataFiles(temp, nameOfDir);
            }
          }
        }
      }
    }
  }
  
  public List<DataFile> getSamples()
  {
    return this.samples;
  }
}
