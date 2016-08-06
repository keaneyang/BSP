package com.bloom.preview;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.PropertyTemplateInfo;
import com.bloom.proc.events.WAEvent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.codehaus.jettison.json.JSONException;

public abstract interface DataPreview
{
  public abstract List<WAEvent> getRawData(String paramString, ReaderType paramReaderType)
    throws Exception;
  
  public abstract String getFileType(String paramString);
  
  public abstract DataFile getFileInfo(String paramString, ReaderType paramReaderType)
    throws IOException, JSONException;
  
  public abstract List<Map<String, Object>> analyze(ReaderType paramReaderType, String paramString, ParserType paramParserType)
    throws Exception;
  
  public abstract List<WAEvent> doPreview(ReaderType paramReaderType, String paramString, Map<String, Object> paramMap)
    throws Exception;
  
  public abstract MetaInfo.PropertyTemplateInfo getPropertyTemplate(String paramString)
    throws MetaDataRepositoryException;
}
