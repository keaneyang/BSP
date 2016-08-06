package com.bloom.fileSystem;

import java.io.IOException;
import java.util.List;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public abstract interface FileSystemBrowser
{
  public abstract void init(String paramString)
    throws IOException;
  
  public abstract String getDefaultRootPath();
  
  public abstract DirectoryDetails goToParent(String paramString)
    throws IOException;
  
  public abstract DirectoryDetails goToChildren(String paramString)
    throws IOException;
  
  public static class DirectoryDetails
  {
    private static Logger logger = Logger.getLogger(DirectoryDetails.class);
    private boolean isDirectory;
    private String currentFilePath;
    private List<DirectoryDetails> allFiles;
    private String name;
    
    public List<DirectoryDetails> getAllFiles()
    {
      return this.allFiles;
    }
    
    public void setAllFiles(List<DirectoryDetails> allFiles)
    {
      this.allFiles = allFiles;
    }
    
    public boolean isDirectory()
    {
      return this.isDirectory;
    }
    
    public void setDirectory(boolean isDirectory)
    {
      this.isDirectory = isDirectory;
    }
    
    public String getCurrentFilePath()
    {
      return this.currentFilePath;
    }
    
    public void setCurrentFilePath(String currentFilePath)
    {
      this.currentFilePath = currentFilePath;
    }
    
    public String getName()
    {
      return this.name;
    }
    
    public void setName(String name)
    {
      this.name = name;
    }
    
    public String getJsonString()
      throws JSONException
    {
      JSONObject jsonObject = new JSONObject();
      JSONArray jsonArray = new JSONArray();
      jsonObject.put("isDirectory", this.isDirectory);
      jsonObject.put("currentPath", this.currentFilePath);
      jsonObject.put("name", this.name);
      if (this.allFiles != null)
      {
        for (DirectoryDetails ss : this.allFiles)
        {
          JSONObject detailObject = new JSONObject();
          detailObject.put("isDirectory", ss.isDirectory);
          detailObject.put("currentPath", ss.currentFilePath);
          detailObject.put("name", ss.name);
          jsonArray.put(detailObject);
        }
        jsonObject.put("children", jsonArray);
      }
      else
      {
        jsonObject.put("children", "NULL");
      }
      return jsonObject.toString();
    }
    
    public String toString()
    {
      try
      {
        return getJsonString();
      }
      catch (JSONException e)
      {
        logger.error(e.getMessage());
      }
      return null;
    }
  }
}
