package com.bloom.fileSystem;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jettison.json.JSONException;

public class HDFSBrowserImpl
  implements FileSystemBrowser
{
  private FileSystem hdfs;
  private Path defaultRootDirectory;
  private String hadoopUrl;
  
  public void init(String root)
    throws IOException
  {
    Configuration defaultFSConfiguration = new Configuration();
    this.hadoopUrl = root;
    defaultFSConfiguration.set("fs.default.name", this.hadoopUrl);
    this.hdfs = FileSystem.get(defaultFSConfiguration);
    this.defaultRootDirectory = new Path(this.hadoopUrl);
  }
  
  public String getDefaultRootPath()
  {
    return this.defaultRootDirectory.getName().trim().isEmpty() ? this.defaultRootDirectory.toUri().getPath() : this.defaultRootDirectory.getName();
  }
  
  public FileSystemBrowser.DirectoryDetails goToParent(String path)
    throws IOException
  {
    path = this.hadoopUrl + path;
    FileSystemBrowser.DirectoryDetails directoryDetails = new FileSystemBrowser.DirectoryDetails();
    Path mainDirectory = this.defaultRootDirectory;
    if ((path == null) || (path.trim().isEmpty()))
    {
      Path pathToParent = this.defaultRootDirectory.getParent();
      if ((pathToParent != null) && 
        (this.hdfs.getFileStatus(pathToParent).isDirectory())) {
        mainDirectory = pathToParent;
      }
    }
    else
    {
      Path currentDirectory = new Path(path);
      if (this.hdfs.exists(currentDirectory))
      {
        Path pathToParent = currentDirectory.getParent();
        if (pathToParent != null) {
          mainDirectory = pathToParent;
        }
      }
      else
      {
        throw new IOException("Path specified is invalid");
      }
    }
    FileStatus[] files = this.hdfs.listStatus(mainDirectory);
    List<FileSystemBrowser.DirectoryDetails> allChildren = new ArrayList();
    for (FileStatus file : files)
    {
      Path thisPath = file.getPath();
      FileSystemBrowser.DirectoryDetails directoryDetail = new FileSystemBrowser.DirectoryDetails();
      if (file.isDirectory()) {
        directoryDetail.setDirectory(true);
      }
      directoryDetail.setName(thisPath.getName());
      directoryDetail.setCurrentFilePath(thisPath.getParent() != null ? thisPath.getParent().toUri().getPath() : null);
      allChildren.add(directoryDetail);
    }
    directoryDetails.setDirectory(true);
    directoryDetails.setName(mainDirectory.getName().trim().isEmpty() ? mainDirectory.toUri().getPath() : mainDirectory.getName());
    directoryDetails.setCurrentFilePath(mainDirectory.getParent() != null ? mainDirectory.getParent().toUri().getPath() : null);
    directoryDetails.setAllFiles(allChildren);
    return directoryDetails;
  }
  
  public FileSystemBrowser.DirectoryDetails goToChildren(String path)
    throws IOException
  {
    path = this.hadoopUrl + path;
    FileSystemBrowser.DirectoryDetails directoryDetails = new FileSystemBrowser.DirectoryDetails();
    Path mainDirectory = this.defaultRootDirectory;
    if ((path != null) && (!path.trim().isEmpty()))
    {
      Path currentDirectory = new Path(path);
      if (this.hdfs.exists(currentDirectory)) {
        mainDirectory = currentDirectory;
      } else {
        throw new IOException("Path specified is invalid");
      }
    }
    String mainDirectoryParentPathName = mainDirectory.getParent() != null ? mainDirectory.getParent().toUri().getPath() : null;
    String mainDirectoryPathName = mainDirectory.getName().trim().isEmpty() ? mainDirectory.toUri().getPath() : mainDirectory.getName();
    if (this.hdfs.getFileStatus(mainDirectory).isDirectory())
    {
      FileStatus[] files = this.hdfs.listStatus(mainDirectory);
      List<FileSystemBrowser.DirectoryDetails> allChildren = new ArrayList();
      for (FileStatus file : files)
      {
        FileSystemBrowser.DirectoryDetails directoryDetail = new FileSystemBrowser.DirectoryDetails();
        Path thisPath = file.getPath();
        if (file.isDirectory()) {
          directoryDetail.setDirectory(true);
        } else {
          directoryDetail.setDirectory(false);
        }
        directoryDetail.setName(thisPath.getName());
        directoryDetail.setCurrentFilePath(thisPath.getParent() != null ? thisPath.getParent().toUri().getPath() : null);
        allChildren.add(directoryDetail);
      }
      directoryDetails.setDirectory(true);
      directoryDetails.setName(mainDirectoryPathName);
      directoryDetails.setCurrentFilePath(mainDirectoryParentPathName);
      directoryDetails.setAllFiles(allChildren);
    }
    else
    {
      directoryDetails.setDirectory(false);
      directoryDetails.setName(mainDirectoryPathName);
      directoryDetails.setCurrentFilePath(mainDirectoryParentPathName);
      directoryDetails.setAllFiles(null);
    }
    return directoryDetails;
  }
  
  public static void main(String[] args)
    throws IOException, JSONException
  {
    HDFSBrowserImpl hbi = new HDFSBrowserImpl();
    hbi.init("hdfs://localhost:9000/");
    
    FileSystemBrowser.DirectoryDetails dd = hbi.goToParent("myData/myData2/myData3");
    System.out.println(dd.getJsonString());
    dd = hbi.goToChildren("myData");
    System.out.println(dd.getJsonString());
  }
}
