package com.bloom.fileSystem;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

import com.bloom.runtime.NodeStartUp;

public class FileSystemBrowserImpl
  implements FileSystemBrowser
{
  private static Logger logger = Logger.getLogger(FileSystemBrowserImpl.class);
  private String defaultRootPath = NodeStartUp.getPlatformHome();
  private File defaultRootDir;
  
  public String getDefaultRootPath()
  {
    return this.defaultRootPath;
  }
  
  public void init(String root)
    throws IOException
  {
    if (root == null)
    {
      if (this.defaultRootPath == null)
      {
        NodeStartUp.setPlatformHome();
        this.defaultRootPath = NodeStartUp.getPlatformHome();
      }
      this.defaultRootDir = new File(Paths.get(this.defaultRootPath, new String[0]).toAbsolutePath().toString());
    }
    else
    {
      this.defaultRootDir = new File(root);
    }
    this.defaultRootPath = this.defaultRootDir.getCanonicalPath();
    if (logger.isDebugEnabled())
    {
      logger.debug("Default root directory: " + this.defaultRootDir);
      logger.debug("Default Root Path: " + this.defaultRootPath);
      logger.debug("Default Parent: " + this.defaultRootDir.getParentFile().getCanonicalPath());
    }
  }
  
  public FileSystemBrowser.DirectoryDetails goToParent(String path)
    throws IOException
  {
    FileSystemBrowser.DirectoryDetails details = new FileSystemBrowser.DirectoryDetails();
    File mainDirectory = null;
    if (logger.isDebugEnabled()) {
      logger.debug("Go To Parent Path: " + path);
    }
    if ((path == null) || (path.isEmpty()))
    {
      String pathToParent = this.defaultRootDir.getCanonicalFile().getParent();
      File parent = new File(pathToParent);
      assert (parent.isDirectory() == true);
      mainDirectory = parent;
    }
    else
    {
      File currentDir = new File(path);
      if (currentDir.exists())
      {
        File absoluteFile = currentDir.getCanonicalFile();
        String pathToParent = absoluteFile.getParent();
        File parentOfCurrentDir = new File(pathToParent);
        assert (parentOfCurrentDir.isDirectory() == true);
        mainDirectory = parentOfCurrentDir;
      }
      else
      {
        if (logger.isDebugEnabled()) {
          logger.debug("Invalid File Path");
        }
        throw new IOException("Invalid File Path.");
      }
    }
    File[] children = mainDirectory.listFiles();
    List<FileSystemBrowser.DirectoryDetails> allChildren = new ArrayList();
    for (File file : children)
    {
      FileSystemBrowser.DirectoryDetails dirDetails = new FileSystemBrowser.DirectoryDetails();
      if (file.isDirectory()) {
        dirDetails.setDirectory(true);
      } else {
        dirDetails.setDirectory(false);
      }
      dirDetails.setName(file.getName());
      dirDetails.setCurrentFilePath(file.getParentFile().getCanonicalPath());
      allChildren.add(dirDetails);
    }
    details.setDirectory(true);
    details.setName(mainDirectory.getName());
    details.setCurrentFilePath(mainDirectory.getParentFile().getCanonicalPath());
    details.setAllFiles(allChildren);
    return details;
  }
  
  public FileSystemBrowser.DirectoryDetails goToChildren(String path)
    throws IOException
  {
    FileSystemBrowser.DirectoryDetails details = new FileSystemBrowser.DirectoryDetails();
    File mainDirectory = null;
    if (logger.isDebugEnabled()) {
      logger.debug("Go To Children Path: " + path);
    }
    if ((path == null) || (path.isEmpty()))
    {
      mainDirectory = this.defaultRootDir;
    }
    else
    {
      File currentDir = new File(path);
      if (currentDir.exists())
      {
        mainDirectory = currentDir;
      }
      else
      {
        if (logger.isDebugEnabled()) {
          logger.debug("Invalid File Path");
        }
        throw new IOException("Invalid File Path.");
      }
    }
    if (mainDirectory.isDirectory())
    {
      File[] children = mainDirectory.listFiles();
      List<FileSystemBrowser.DirectoryDetails> allChildren = new ArrayList();
      for (File file : children)
      {
        FileSystemBrowser.DirectoryDetails dirDetails = new FileSystemBrowser.DirectoryDetails();
        if (file.isDirectory()) {
          dirDetails.setDirectory(true);
        } else {
          dirDetails.setDirectory(false);
        }
        dirDetails.setName(file.getName());
        dirDetails.setCurrentFilePath(file.getParentFile().getCanonicalPath());
        allChildren.add(dirDetails);
      }
      details.setDirectory(true);
      details.setName(mainDirectory.getName());
      details.setCurrentFilePath(mainDirectory.getParentFile().getCanonicalPath());
      details.setAllFiles(allChildren);
    }
    else
    {
      details.setDirectory(false);
      details.setCurrentFilePath(mainDirectory.getParentFile().getCanonicalPath());
      details.setName(mainDirectory.getName());
      details.setAllFiles(null);
    }
    return details;
  }
}
