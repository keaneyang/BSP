package com.bloom.preview;

import com.bloom.runtime.NodeStartUp;

public class DataPreviewConstants
{
  public static final String SPLIT_CONSTANT = "parserProperties=";
  public static final String SPACE = " ";
  public static final String SAMPLES = "samples";
  public static final String APPDATA = "appData";
  public static final String HOME = NodeStartUp.getPlatformHome();
  public static final String PLATFORM = "Platform";
  public static final String POSDATA = "/PosApp/appData/PosDataPreview.csv";
  public static final String POSDATATYPE = "CSV";
  public static final String RETAILDATA = "/RetailApp/appData/RetailDataPreview.csv";
  public static final String RETAILDATATYPE = "CSV";
  public static final String MULTILOGDATA = "/MultiLogApp/appData/ApacheAccessLogPreview";
  public static final String MULTILOGTYPE = "LOG";
  
  public static abstract enum DataFileInfo
  {
    posdata,  multilogdata,  retaildata;
    
    private DataFileInfo() {}
    
    public abstract String getPath();
    
    public abstract String getType();
  }
}
