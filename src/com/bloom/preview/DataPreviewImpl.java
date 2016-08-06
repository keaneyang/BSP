package com.bloom.preview;

import com.bloom.classloading.WALoader;
import com.bloom.intf.Analyzer;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.proc.AbstractEventSink;
import com.bloom.proc.BaseProcess;
import com.bloom.proc.events.JsonNodeEvent;
import com.bloom.runtime.BaseServer;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.PropertyTemplateInfo;
import com.bloom.security.WASecurityManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.bloom.common.exc.RecordException;
import com.bloom.common.exc.RecordException.Type;
import com.bloom.event.Event;
import com.bloom.proc.events.WAEvent;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class DataPreviewImpl
  implements DataPreview
{
  private static Logger logger = Logger.getLogger(DataPreviewImpl.class);
  public static String MODULE_NAME = "handler";
  private final int numberOfEventsForRawData = 20;
  private final int numberOfEventsForPreview = 100;
  private MDRepository mdRepo = MetadataRepository.getINSTANCE();
  
  public List<WAEvent> getRawData(String fqPathToFile, ReaderType fType)
    throws Exception
  {
    Class<?> clazz = null;
    Map<String, Object> resultMap = new HashMap();
    switch (fType)
    {
    case HDFSReader: 
      clazz = WALoader.get().loadClass("com.bloom.proc.HDFSReader_1_0");
      break;
    case FileReader: 
      clazz = WALoader.get().loadClass("com.bloom.proc.FileReader_1_0");
    }
    String[] dirAndFileName = getDirectoryAndFileName(fqPathToFile);
    
    BaseProcess baseProcess = (BaseProcess)clazz.newInstance();
    
    Map<String, Object> readerProperties = fType.getDefaultReaderProperties();
    if (fType.equals(ReaderType.HDFSReader))
    {
      dirAndFileName[0] = dirAndFileName[0];
      readerProperties.put("hadoopurl", dirAndFileName[0]);
      readerProperties.put("wildcard", dirAndFileName[1]);
    }
    else if (fType.equals(ReaderType.FileReader))
    {
      readerProperties.put("directory", dirAndFileName[0]);
      readerProperties.put("wildcard", dirAndFileName[1]);
    }
    Map<String, Object> parserProperties = ParserType.DSV.getDefaultParserProperties();
    
    getClass();List<WAEvent> allEvents = runBaseProcess(baseProcess, readerProperties, parserProperties, 20);
    resultMap.put("Events", allEvents);
    return allEvents;
  }
  
  public DataFile getFileInfo(String fqPathToFile, ReaderType fType)
    throws IOException, JSONException
  {
    DataFile dataFile = null;
    switch (fType)
    {
    case FileReader: 
      File file = new File(fqPathToFile);
      if (file.isFile())
      {
        dataFile = new DataFile();
        dataFile.name = file.getName();
        dataFile.fqPath = file.getCanonicalPath();
        dataFile.size = file.length();
      }
      else
      {
        throw new FileNotFoundException("The URI: " + fqPathToFile + " is not a valid path to a file");
      }
      break;
    case HDFSReader: 
      Configuration defaultFSConfiguration = new Configuration();
      String[] dirAndFileName = getDirectoryAndFileName(fqPathToFile);
      defaultFSConfiguration.set("fs.default.name", dirAndFileName[0]);
      FileSystem hdfs = FileSystem.get(defaultFSConfiguration);
      Path path = new Path(fqPathToFile);
      FileStatus fileStatus = hdfs.getFileStatus(path);
      if (fileStatus.isFile())
      {
        dataFile = new DataFile();
        dataFile.name = path.getName();
        dataFile.fqPath = path.toUri().toString();
        dataFile.size = fileStatus.getLen();
      }
      break;
    }
    return dataFile;
  }
  
  public String getFileType(String fqPathToFile)
  {
    return ParserType.DSV.name();
  }
  
  private String[] getDirectoryAndFileName(String fqPathToFile)
  {
    int lastIndex = fqPathToFile.lastIndexOf("/");
    String fileName = fqPathToFile.substring(lastIndex + 1);
    String directory = fqPathToFile.substring(0, lastIndex + 1);
    return new String[] { directory, fileName };
  }
  
  public List<Map<String, Object>> analyze(ReaderType rType, String fqPath, ParserType pType)
    throws Exception
  {
    Map<String, Object> parserProperties = new HashMap();
    
    Class<?> clazz = null;
    String[] dirAndFileName = getDirectoryAndFileName(fqPath);
    Map<String, Object> readerProperties = rType.getDefaultReaderProperties();
    switch (rType)
    {
    case HDFSReader: 
      readerProperties.put("hadoopurl", dirAndFileName[0]);
      readerProperties.put("wildcard", dirAndFileName[1]);
      clazz = WALoader.get().loadClass("com.bloom.proc.HDFSReader_1_0");
      break;
    case FileReader: 
      readerProperties.put("directory", dirAndFileName[0]);
      readerProperties.put("wildcard", dirAndFileName[1]);
      clazz = WALoader.get().loadClass("com.bloom.proc.FileReader_1_0");
    }
    parserProperties.put(MODULE_NAME, "Analyzer");
    if (pType != null) {
      parserProperties.put("docType", pType.toString());
    }
    BaseProcess baseProcess = (BaseProcess)clazz.newInstance();
    
    baseProcess.init(readerProperties, parserProperties, null, BaseServer.getServerName(), null, false, null);
    baseProcess.receiveImpl(0, null);
    List<Map<String, Object>> possibleProperties;
    if (pType != null)
    {
      possibleProperties = ((Analyzer)baseProcess).getProbableProperties();
    }
    else
    {
      possibleProperties = new ArrayList();
      Map<String, Object> fileDetails = ((Analyzer)baseProcess).getFileDetails();
      possibleProperties.add(fileDetails);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Delimiter recommendations: " + possibleProperties);
    }
    baseProcess.close();
    return possibleProperties;
  }
  
  public List<WAEvent> doPreview(ReaderType rType, String fqPath, Map<String, Object> parserProperties)
    throws Exception
  {
    Class<?> clazz = null;
    List<WAEvent> allEvents = null;
    
    String[] dirAndFileName = getDirectoryAndFileName(fqPath);
    
    Map<String, Object> readerProperties = rType.getDefaultReaderProperties();
    switch (rType)
    {
    case FileReader: 
      readerProperties.put("directory", dirAndFileName[0]);
      readerProperties.put("wildcard", dirAndFileName[1]);
      clazz = WALoader.get().loadClass("com.bloom.proc.FileReader_1_0");
      break;
    case HDFSReader: 
      readerProperties.put("hadoopurl", dirAndFileName[0]);
      readerProperties.put("wildcard", dirAndFileName[1]);
      clazz = WALoader.get().loadClass("com.bloom.proc.HDFSReader_1_0");
    }
    if (clazz != null)
    {
      BaseProcess baseProcess = (BaseProcess)clazz.newInstance();
      
      String parserHandler = (String)parserProperties.get("handler");
      if ((parserHandler.equals("XMLParser")) || (parserHandler.equals("JSONParser"))) {
        readerProperties.remove("charset");
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Parser Properties : " + parserProperties.entrySet());
      }
      getClass();allEvents = runBaseProcess(baseProcess, readerProperties, parserProperties, 100);
    }
    return allEvents;
  }
  
  public List<WAEvent> runBaseProcess(BaseProcess baseProcess, Map<String, Object> readerProperties, Map<String, Object> parserProperties, int numberOfEvents)
    throws Exception
  {
    parserProperties.put("breakonnorecord", Boolean.valueOf(true));
    baseProcess.init(readerProperties, parserProperties, null, BaseServer.getServerName(), null, false, null);
    final int totalEventCount = numberOfEvents;
    final List<WAEvent> allEvents = new ArrayList(totalEventCount);
    
    final AtomicBoolean done = new AtomicBoolean(false);
    int cnt = 0;
    
    baseProcess.addEventSink(new AbstractEventSink()
    {
      int count = 0;
      
      public void receive(int channel, Event event)
        throws Exception
      {
        WAEvent waEvent;
        if ((event instanceof JsonNodeEvent))
        {
           waEvent = new WAEvent(1, null);
          waEvent.data = new Object[1];
          JsonNode jNode = ((JsonNodeEvent)event).data;
          waEvent.data[0] = jNode.toString();
        }
        else
        {
          waEvent = (WAEvent)event;
        }
        this.count += 1;
        if (this.count <= totalEventCount)
        {
          if (waEvent != null) {
            allEvents.add(waEvent);
          }
        }
        else {
          done.set(true);
        }
      }
    });
    while (!done.get()) {
      try
      {
        baseProcess.receive(0, null);
      }
      catch (Exception exp)
      {
        if ((exp instanceof RecordException))
        {
          RecordException rExp = (RecordException)exp;
          if (rExp.type() != RecordException.Type.NO_RECORD)
          {
            logger.warn("Got exception while loading file for preview", rExp);
            throw new RuntimeException(rExp);
          }
          if (cnt < 3)
          {
            Thread.sleep(50L);
            cnt++;
          }
          else
          {
            break;
          }
        }
        else
        {
          logger.warn("Got exception: " + exp.getMessage());
        }
      }
    }
    baseProcess.close();
    return allEvents;
  }
  
  public MetaInfo.PropertyTemplateInfo getPropertyTemplate(String name)
    throws MetaDataRepositoryException
  {
    return (MetaInfo.PropertyTemplateInfo)this.mdRepo.getMetaObjectByName(EntityType.PROPERTYTEMPLATE, "Global", name, null, WASecurityManager.TOKEN);
  }
  
  public static void main(String[] args)
  {
    DataPreview dataPreview = new DataPreviewImpl();
    String fqPath = "../Samples/AppData/DataCenterData.csv";
    try
    {
      List<WAEvent> allEvents = dataPreview.getRawData(fqPath, ReaderType.FileReader);
      JSONArray jArray = new JSONArray();
      for (WAEvent waEvent : allEvents) {
        jArray.put(waEvent.toJSON());
      }
      System.out.println("Raw Data: " + allEvents.toString());
      
      DataFile dataFile = dataPreview.getFileInfo(fqPath, ReaderType.FileReader);
      System.out.println("File Info: " + dataFile.toJson().toString());
      
      List<Map<String, Object>> possibleDocType = dataPreview.analyze(ReaderType.FileReader, fqPath, null);
      JSONObject jsonObject = new JSONObject();
      for (int itr = 0; itr < possibleDocType.size(); itr++)
      {
        JSONObject docJSon = new JSONObject();
        for (Map.Entry<String, Object> entrySet : ((Map)possibleDocType.get(itr)).entrySet()) {
          docJSon.put((String)entrySet.getKey(), entrySet.getValue());
        }
        jsonObject.put("" + itr + "", docJSon);
      }
      String docType = (String)((Map)possibleDocType.get(0)).get("docType");
      System.out.println("Possible Doc Type: " + jsonObject.toString());
      
      List<Map<String, Object>> possibleDelims = dataPreview.analyze(ReaderType.FileReader, fqPath, ParserType.valueOf(docType));
      JSONObject jsonObject2 = new JSONObject();
      for (int itr = 0; itr < possibleDelims.size(); itr++)
      {
        JSONObject docJSon = new JSONObject();
        for (Map.Entry<String, Object> entrySet : ((Map)possibleDelims.get(itr)).entrySet()) {
          docJSon.put((String)entrySet.getKey(), entrySet.getValue());
        }
        jsonObject2.put("" + itr + "", docJSon);
      }
      System.out.println("Possible Delimiters: " + jsonObject2.toString());
      
      Map<String, Object> parserProperties = (Map)possibleDelims.get(0);
      List<WAEvent> data = dataPreview.doPreview(ReaderType.FileReader, fqPath, parserProperties);
      JSONArray jArray2 = new JSONArray();
      for (WAEvent waEvent : data) {
        jArray2.put(waEvent.toJSON());
      }
      System.out.println("Data Previewd: " + jArray2.toString());
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
}
