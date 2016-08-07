package com.bloom.runtime.compiler.stmts;

import com.bloom.classloading.WALoader;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.proc.BaseProcess;
import com.bloom.proc.SourceProcess;
import com.bloom.runtime.Context;
import com.bloom.runtime.TraceOptions;
import com.bloom.runtime.TypeGenerator;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo.Source;
import com.google.common.base.Joiner;
import com.google.common.base.Joiner.MapJoiner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.log4j.Logger;

public class CreateSourceOrTargetStmt
  extends CreateStmt
{
  private static final Logger logger = Logger.getLogger(CreateSourceOrTargetStmt.class);
  public final AdapterDescription srcOrDest;
  public final AdapterDescription parserOrFormatter;
  public final List<InputOutputSink> ios;
  
  public CreateSourceOrTargetStmt(EntityType what, String name, Boolean doReplace, AdapterDescription srcOrDest, AdapterDescription parserOrFormatter, List<InputOutputSink> ios)
  {
    super(what, name, doReplace.booleanValue());
    checkValidity("One of the params needed is null: EntityType: " + what + ", ObjectName: " + name + ", Replace: " + doReplace + ", SourceAdapter: " + srcOrDest + ", I/O Clause: " + ios + ".\n", new Object[] { what, name, doReplace, srcOrDest, ios });
    
    this.srcOrDest = srcOrDest;
    this.parserOrFormatter = parserOrFormatter;
    this.ios = ios;
  }
  
  public String toString()
  {
    checkValidity("One of the params needed is null: EntityType: " + this.what + ", ObjectName: " + this.name + ", Replace: " + this.doReplace + ", SourceAdapter: " + this.srcOrDest + ", I/O Clause: " + this.ios + ".\n", new Object[] { this.what, this.name, Boolean.valueOf(this.doReplace), this.srcOrDest, this.ios });
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(stmtToString()).append(" OF ").append(this.srcOrDest.getAdapterTypeName()).append(" (").append(this.srcOrDest.getProps()).append(")");
    if (this.parserOrFormatter != null) {
      stringBuilder.append("PARSER ").append(this.parserOrFormatter.getAdapterTypeName()).append(" (").append(this.parserOrFormatter.getProps()).append(")");
    }
    return stringBuilder.toString();
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    checkValidity("Input / Output clause not defined", new Object[] { this.ios });
    checkValidity("Compiler not defined", new Object[] { c });
    
    TraceOptions traceOptions = Compiler.buildTraceOptions(this);
    
    boolean isAnySourceFiltered = isAnySourceFiltered(this.ios);
    if (this.what == EntityType.SOURCE)
    {
      try
      {
        ArrayList<String> selectText = getSelectTexts(this.ios, traceOptions);
        MetaInfo.Source sourceInfo = (MetaInfo.Source)c.createFilteredSource(this);
        sourceInfo.setOutputClauses(this.ios);
        MetadataRepository.getINSTANCE().updateMetaObject(sourceInfo, c.getContext().getAuthToken());
        adapterRelatedCompilationOperations(c, sourceInfo);
      }
      catch (Exception e)
      {
        e.printStackTrace();
        throw new RuntimeException(e.getMessage());
      }
      return null;
    }
    InputOutputSink ios = (InputOutputSink)this.ios.get(0);
    c.compileCreateSourceOrTargetStmt(this);
    if (ios.getGeneratedStream() != null)
    {
      TypeGenerator generator = new TypeGenerator(c, this.name);
      generator.generateStream(c, Collections.singletonList(ios.getGeneratedStream()), ios.getStreamName(), true);
    }
    try
    {
      MetaInfo.Source sourceInfo = c.getContext().getSourceInCurSchema(this.name);
      adapterRelatedCompilationOperations(c, sourceInfo);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    return null;
  }
  
  private boolean isAnySourceFiltered(List<InputOutputSink> outputClauses)
  {
    if (this.what == EntityType.TARGET) {
      return false;
    }
    if (outputClauses == null) {
      throw new RuntimeException("Problem with output clause for the source.");
    }
    for (InputOutputSink inputOutputSink : outputClauses) {
      if (inputOutputSink.isFiltered()) {
        return true;
      }
    }
    return false;
  }
  
  private ArrayList<String> getSelectTexts(List<InputOutputSink> ios, TraceOptions traceOptions)
  {
    ArrayList<String> selectTexts = new ArrayList();
    for (InputOutputSink inputOutputSink : ios) {
      if (inputOutputSink.getFilterText() != null)
      {
        String TQL = inputOutputSink.getFilterText().trim();
        if ((TQL != null) && (TQL.length() >= 1) && (TQL.endsWith(","))) {
          TQL = TQL.substring(0, TQL.length() - 1);
        }
        if (inputOutputSink.getGeneratedStream() == null)
        {
          if (inputOutputSink.getStreamName() != null) {
            if (inputOutputSink.getTypeDefinition() != null) {
              selectTexts.add(inputOutputSink.getStreamName().trim() + " ( " + Joiner.on(", ").join(inputOutputSink.getTypeDefinition()) + " ) " + TQL);
            } else {
              selectTexts.add(inputOutputSink.getStreamName().trim() + " " + TQL);
            }
          }
        }
        else
        {
          MappedStream ms = inputOutputSink.getGeneratedStream();
          if ((ms != null) && (ms.streamName != null) && (ms.mappingProperties != null))
          {
            Joiner.MapJoiner mapJoiner = Joiner.on(',').withKeyValueSeparator(":");
            selectTexts.add(ms.streamName.trim() + " ( " + mapJoiner.join(ms.mappingProperties) + " ) " + TQL);
          }
        }
      }
    }
    if ((isWithDebugEnabled()) && (selectTexts != null)) {
      for (String selectText : selectTexts) {
        logger.warn("Select extracted: " + selectText);
      }
    }
    return selectTexts;
  }
  
  private void adapterRelatedCompilationOperations(Compiler compiler, MetaInfo.Source sourceMetaInfo)
    throws Exception
  {
    if (sourceMetaInfo != null)
    {
      Class<?> adapterFactory = WALoader.get().loadClass(sourceMetaInfo.adapterClassName);
      BaseProcess proc = (BaseProcess)adapterFactory.newInstance();
      if ((proc instanceof SourceProcess)) {
        ((SourceProcess)proc).onCompile(compiler, sourceMetaInfo);
      }
    }
  }
  
  public String getStreamName()
  {
    checkValidity("Input / Output clause not defined", new Object[] { this.ios });
    return ((InputOutputSink)this.ios.get(0)).getStreamName();
  }
  
  public Select getFilter()
  {
    checkValidity("Input / Output clause not defined", new Object[] { this.ios });
    if (((InputOutputSink)this.ios.get(0)).isFiltered()) {
      return ((InputOutputSink)this.ios.get(0)).getFilter();
    }
    return null;
  }
  
  public List<String> getPartitionFields()
  {
    return ((InputOutputSink)this.ios.get(0)).getPartitionFields();
  }
  
  public MappedStream getGeneratedStream()
  {
    return ((InputOutputSink)this.ios.get(0)).getGeneratedStream();
  }
}
