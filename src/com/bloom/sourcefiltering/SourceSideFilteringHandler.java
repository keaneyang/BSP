package com.bloom.sourcefiltering;

import com.bloom.kafkamessaging.StreamPersistencePolicy;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.Context;
import com.bloom.runtime.TypeGenerator;
import com.bloom.runtime.compiler.AST;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.ObjectName;
import com.bloom.runtime.compiler.exprs.ObjectRef;
import com.bloom.runtime.compiler.select.DataSource;
import com.bloom.runtime.compiler.stmts.AdapterDescription;
import com.bloom.runtime.compiler.stmts.CreateCqStmt;
import com.bloom.runtime.compiler.stmts.CreateSourceOrTargetStmt;
import com.bloom.runtime.compiler.stmts.CreateStreamStmt;
import com.bloom.runtime.compiler.stmts.InputOutputSink;
import com.bloom.runtime.compiler.stmts.MappedStream;
import com.bloom.runtime.compiler.stmts.OutputClause;
import com.bloom.runtime.compiler.stmts.Select;
import com.bloom.runtime.compiler.stmts.SelectTarget;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.CQExecutionPlan;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.MetaInfo.CQ;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Source;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.utils.RuntimeUtils;
import com.bloom.uuid.UUID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.apache.log4j.Logger;

public class SourceSideFilteringHandler
  implements Handler
{
  private static final Logger logger = Logger.getLogger(SourceSideFilteringHandler.class);
  
  private void dump(Boolean isMapped, Object... args)
  {
    StringBuilder stringBuffer = new StringBuilder();
    MetaInfo.Source src = (MetaInfo.Source)args[0];
    stringBuffer.append("\n Source name: ").append(src.getFullName()).append(" Outputs to: ").append(MetaInfo.MetaObject.obtainMetaObject(src.getOutputStream()).getFullName()).append(" \n ");
    if (!isMapped.booleanValue())
    {
      List<UUID> UUIDs = (List)args[1];
      UUID cqUUID = (UUID)UUIDs.get(1);
      MetaInfo.CQ firstCQ = (MetaInfo.CQ)MetaInfo.MetaObject.obtainMetaObject(cqUUID);
      
      stringBuffer.append("CQ ").append(firstCQ.getFullName()).append(" FROM: ").append(firstCQ.plan.getDataSourceList()).append(" => ");
      stringBuffer.append("TO Stream: ").append(MetaInfo.MetaObject.obtainMetaObject(firstCQ.stream).getFullName()).append("\n ");
    }
    else
    {
      List<UUID> UUIDs = (List)args[1];
      UUID firstCQUUID = (UUID)UUIDs.get(0);
      UUID firstStreamUUID = (UUID)UUIDs.get(1);
      
      UUID secondCQUUID = (UUID)UUIDs.get(3);
      UUID secondStreamUUID = (UUID)UUIDs.get(4);
      
      MetaInfo.CQ firstCQ = (MetaInfo.CQ)MetaInfo.MetaObject.obtainMetaObject(firstCQUUID);
      stringBuffer.append("CQ ").append(firstCQ.getFullName()).append(" FROM: ").append(firstCQ.plan.getDataSourceList()).append(" => ");
      stringBuffer.append("TO Stream: ").append(MetaInfo.MetaObject.obtainMetaObject(firstStreamUUID).getFullName()).append(" \n ");
      
      MetaInfo.CQ secondCQ = (MetaInfo.CQ)MetaInfo.MetaObject.obtainMetaObject(secondCQUUID);
      stringBuffer.append("CQ ").append(secondCQ.getFullName()).append(" FROM: ").append(secondCQ.plan.getDataSourceList()).append(" => ");
      stringBuffer.append("TO Stream: ").append(MetaInfo.MetaObject.obtainMetaObject(secondStreamUUID).getFullName()).append(" \n ");
    }
    logger.warn(stringBuffer.toString());
  }
  
  public Object handle(Compiler compiler, CreateSourceOrTargetStmt createSourceOrTargetStmt)
    throws MetaDataRepositoryException
  {
    if ((createSourceOrTargetStmt == null) || (createSourceOrTargetStmt.ios == null) || (createSourceOrTargetStmt.ios.isEmpty())) {
      throw new RuntimeException("Null value passed");
    }
    Context ctx = compiler.getContext();
    boolean doReplace = createSourceOrTargetStmt.doReplace;
    ObjectName sourceName = ctx.makeObjectName(createSourceOrTargetStmt.name);
    if (createSourceOrTargetStmt.ios.size() == 1)
    {
      InputOutputSink sink = (InputOutputSink)createSourceOrTargetStmt.ios.get(0);
      if ((sink != null) && (!sink.isFiltered()) && (sink.getGeneratedStream() == null) && (sink.getTypeDefinition() == null)) {
        try
        {
          return createSourceWithImplicitStream(compiler, ctx, createSourceOrTargetStmt, doReplace, sourceName.getFullName(), createSourceOrTargetStmt.getStreamName());
        }
        catch (MetaDataRepositoryException e)
        {
          try
          {
            rollbackOperation(ctx, null, new MetaInfo.MetaObject[0]);
            throw e;
          }
          catch (MetaDataRepositoryException e1)
          {
            log(e1);
            throw e1;
          }
        }
      }
    }
    MetaInfo.Source sourceObject = null;
    String streamName = RuntimeUtils.genRandomName(createSourceOrTargetStmt.getStreamName());
    ObjectName streamObjectNameTier1 = ctx.makeObjectName(streamName);
    
    MetaInfo.Stream implicitStream = null;
    
    List<UUID> coDependentUUIDs = new ArrayList();
    try
    {
      MetaInfo.Type inputType = findTypeOfSourceAdapter(compiler, ctx, createSourceOrTargetStmt);
      implicitStream = createImplicitStreamForRedirect(compiler, ctx, doReplace, streamObjectNameTier1, inputType);
      changeMetaObjectState(ctx, implicitStream, Boolean.valueOf(true), null);
      sourceObject = createSourceWithImplicitStream(compiler, ctx, createSourceOrTargetStmt, doReplace, sourceName.getFullName(), streamObjectNameTier1.getFullName());
      for (InputOutputSink sink : createSourceOrTargetStmt.ios)
      {
        List<UUID> outputClauseImplicitItems = new ArrayList();
        if ((sink.isFiltered()) && (sink.getGeneratedStream() == null) && (sink.getTypeDefinition() != null))
        {
          if (createSourceOrTargetStmt.isWithDebugEnabled()) {
            logger.warn("Filtering with Type defined");
          }
          outputClauseImplicitItems.addAll(handleSimpleFilterWithTypeDef(sourceObject, compiler, ctx, Boolean.valueOf(doReplace), sink, streamObjectNameTier1));
          if (createSourceOrTargetStmt.isWithDebugEnabled()) {
            dump(Boolean.valueOf(false), new Object[] { sourceObject, outputClauseImplicitItems });
          }
        }
        else if ((sink.isFiltered()) && (sink.getGeneratedStream() != null) && (sink.getTypeDefinition() == null))
        {
          if (createSourceOrTargetStmt.isWithDebugEnabled()) {
            logger.warn("Filtering with Map() clause");
          }
          outputClauseImplicitItems.addAll(handleMappedStream(sourceObject, compiler, ctx, Boolean.valueOf(doReplace), sink, streamObjectNameTier1));
          if (createSourceOrTargetStmt.isWithDebugEnabled()) {
            dump(Boolean.valueOf(true), new Object[] { sourceObject, outputClauseImplicitItems });
          }
        }
        else if ((sink.isFiltered()) && (sink.getGeneratedStream() == null) && (sink.getTypeDefinition() == null))
        {
          if (createSourceOrTargetStmt.isWithDebugEnabled()) {
            logger.warn("Filtering with Type not defined");
          }
          outputClauseImplicitItems.addAll(handleSimpleFilterWithOutTypeDef(createSourceOrTargetStmt, sourceObject, compiler, ctx, Boolean.valueOf(doReplace), sink, streamObjectNameTier1));
          if (createSourceOrTargetStmt.isWithDebugEnabled()) {
            dump(Boolean.valueOf(false), new Object[] { sourceObject, outputClauseImplicitItems });
          }
        }
        else if ((!sink.isFiltered()) && (sink.getGeneratedStream() == null) && (sink.getTypeDefinition() == null))
        {
          if (createSourceOrTargetStmt.isWithDebugEnabled()) {
            logger.warn("No filtering with Type not defined");
          }
          outputClauseImplicitItems.addAll(handleSimpleOutput(createSourceOrTargetStmt, sourceObject, compiler, ctx, doReplace, sink, streamObjectNameTier1));
        }
        else if ((!sink.isFiltered()) && (sink.getGeneratedStream() != null) && (sink.getTypeDefinition() == null))
        {
          if (createSourceOrTargetStmt.isWithDebugEnabled()) {
            logger.warn("No filtering with Map() clause");
          }
          outputClauseImplicitItems.addAll(handleSimpleOutputWithMap(createSourceOrTargetStmt, sourceObject, compiler, ctx, doReplace, sink, streamObjectNameTier1));
        }
        else
        {
          throw new RuntimeException("Problem in creating filtered source");
        }
        if ((coDependentUUIDs != null) && (outputClauseImplicitItems != null)) {
          coDependentUUIDs.addAll(outputClauseImplicitItems);
        }
      }
    }
    catch (MetaDataRepositoryException e)
    {
      try
      {
        rollbackOperation(ctx, coDependentUUIDs, new MetaInfo.MetaObject[] { sourceObject, implicitStream });
      }
      catch (MetaDataRepositoryException e1)
      {
        log(e1);
      }
      log(e);
    }
    return sourceObject;
  }
  
  private Collection<UUID> handleSimpleOutputWithMap(CreateSourceOrTargetStmt createSourceOrTargetStmt, MetaInfo.Source sourceObject, Compiler compiler, Context ctx, boolean doReplace, InputOutputSink sink, ObjectName streamObjectNameTier1)
    throws MetaDataRepositoryException
  {
    List<UUID> allObjectsCreated = new ArrayList();
    
    MappedStream mappedStream = sink.getGeneratedStream();
    
    String lastStream = mappedStream.streamName;
    
    TypeGenerator generator = new TypeGenerator(compiler, sourceObject.name);
    logger.warn(sink.getGeneratedStream().streamName);
    List<UUID> UUIDs = generator.generateStream(compiler, Collections.singletonList(sink.getGeneratedStream()), streamObjectNameTier1.getFullName(), true);
    allObjectsCreated.addAll(UUIDs);
    changeMetaObjectState(ctx, allObjectsCreated, Boolean.valueOf(true), null);
    
    return allObjectsCreated;
  }
  
  private Collection<UUID> handleSimpleOutput(CreateSourceOrTargetStmt stmt, MetaInfo.Source sourceObject, Compiler compiler, Context ctx, boolean doReplace, InputOutputSink sink, ObjectName streamObjectNameTier1)
    throws MetaDataRepositoryException
  {
    List<UUID> allObjectsCreated = new ArrayList();
    String cqName = RuntimeUtils.genRandomName(sink.getStreamName());
    MetaInfo.Type t = findTypeOfSourceAdapter(compiler, ctx, stmt);
    MetaInfo.Stream outputStream = createStreamWithGivenType(compiler, ctx, sink.getStreamName(), t.getFullName(), doReplace);
    
    changeMetaObjectState(ctx, outputStream, null, Boolean.valueOf(true));
    allObjectsCreated.add(outputStream.getUuid());
    
    List<DataSource> source_list = new ArrayList();
    source_list.add(AST.SourceStream(streamObjectNameTier1.getFullName(), null, "$all"));
    
    List<SelectTarget> target_list = new ArrayList();
    target_list.add(new SelectTarget(new ObjectRef("$all"), null));
    Select sel = new Select(false, 1, target_list, source_list, null, null, null, null, null, false, null);
    
    CreateCqStmt ccs = new CreateCqStmt(cqName, Boolean.valueOf(doReplace), sink.getStreamName(), new ArrayList(), sel, "select $all;");
    MetaInfo.CQ cqMetaObject = (MetaInfo.CQ)compiler.compileCreateCqStmt(ccs);
    changeMetaObjectState(ctx, cqMetaObject, Boolean.valueOf(true), null);
    
    return allObjectsCreated;
  }
  
  private Collection<UUID> handleMappedStream(MetaInfo.Source sourceMetaObject, Compiler compiler, Context ctx, Boolean doReplace, InputOutputSink sink, ObjectName sourceStreamName)
  {
    List<UUID> allObjectsCreated = new ArrayList();
    try
    {
      MappedStream mappedStream = sink.getGeneratedStream();
      
      String lastStream = mappedStream.streamName;
      String secondStream = RuntimeUtils.genRandomName(lastStream);
      String firstCQ = RuntimeUtils.genRandomName(lastStream);
      
      String oldStream = mappedStream.streamName;
      
      redirectStreamForGeneratedStream(mappedStream, secondStream);
      
      TypeGenerator generator = new TypeGenerator(compiler, sourceMetaObject.name);
      List<UUID> UUIDs = generator.generateStream(compiler, Collections.singletonList(sink.getGeneratedStream()), sourceStreamName.getFullName(), false);
      allObjectsCreated.addAll(UUIDs);
      changeMetaObjectState(ctx, allObjectsCreated, Boolean.valueOf(true), null);
      
      setSelectFromClauseToImplicitStream(compiler, ctx, sink, secondStream, null);
      
      MetaInfo.CQ cqMetaObject = createCQWithImplicitStream(compiler, ctx, doReplace.booleanValue(), sink, firstCQ, lastStream);
      
      changeMetaObjectState(ctx, cqMetaObject, Boolean.valueOf(true), null);
      allObjectsCreated.add(cqMetaObject.getUuid());
      allObjectsCreated.add(cqMetaObject.stream);
      for (UUID uuid : allObjectsCreated) {
        sourceMetaObject.addCoDependentObject(uuid);
      }
      sourceMetaObject.addCoDependentObject(cqMetaObject.getUuid());
      ctx.updateMetaObject(sourceMetaObject);
      redirectStreamForGeneratedStream(mappedStream, oldStream);
    }
    catch (MetaDataRepositoryException e)
    {
      log(e);
    }
    return allObjectsCreated;
  }
  
  private Collection<UUID> handleSimpleFilterWithOutTypeDef(CreateSourceOrTargetStmt stmt, MetaInfo.Source sourceMetaObject, Compiler compiler, Context ctx, Boolean doReplace, InputOutputSink sink, ObjectName sourceStreamName)
  {
    List<UUID> allObjectsCreated = new ArrayList();
    try
    {
      String cqName = RuntimeUtils.genRandomName(sink.getStreamName());
      
      setSelectFromClauseToImplicitStream(compiler, ctx, sink, sourceStreamName.getFullName(), "$all");
      
      MetaInfo.Type t = findTypeOfSourceAdapter(compiler, ctx, stmt);
      MetaInfo.Stream outputStream = createStreamWithGivenType(compiler, ctx, sink.getStreamName(), t.getFullName(), doReplace.booleanValue());
      changeMetaObjectState(ctx, outputStream, null, Boolean.valueOf(true));
      allObjectsCreated.add(outputStream.getUuid());
      
      MetaInfo.CQ filterCQ = createCQWithImplicitStream(compiler, ctx, doReplace.booleanValue(), sink, cqName, sink.getStreamName());
      changeMetaObjectState(ctx, filterCQ, Boolean.valueOf(true), null);
      allObjectsCreated.add(filterCQ.getUuid());
      allObjectsCreated.add(outputStream.getDataType());
      
      sourceMetaObject.addCoDependentObject(filterCQ.getUuid());
      sourceMetaObject.addCoDependentObject(outputStream.getUuid());
      sourceMetaObject.addCoDependentObject(outputStream.getDataType());
      ctx.updateMetaObject(sourceMetaObject);
    }
    catch (MetaDataRepositoryException e)
    {
      log(e);
    }
    return allObjectsCreated;
  }
  
  private Collection<UUID> handleSimpleFilterWithTypeDef(MetaInfo.Source sourceMetaObject, Compiler compiler, Context ctx, Boolean doReplace, InputOutputSink sink, ObjectName sourceStreamName)
  {
    List<UUID> allObjectsCreated = new ArrayList();
    String cqName = RuntimeUtils.genRandomName(sink.getStreamName());
    try
    {
      setSelectFromClauseToImplicitStream(compiler, ctx, sink, sourceStreamName.getFullName(), "$all");
      
      MetaInfo.Stream outputStream = createStreamWithGivenType(compiler, ctx, sink, doReplace.booleanValue());
      changeMetaObjectState(ctx, outputStream, null, Boolean.valueOf(true));
      allObjectsCreated.add(outputStream.getUuid());
      
      MetaInfo.CQ filterCQ = createCQWithImplicitStream(compiler, ctx, doReplace.booleanValue(), sink, cqName, sink.getStreamName());
      changeMetaObjectState(ctx, filterCQ, Boolean.valueOf(true), null);
      allObjectsCreated.add(filterCQ.getUuid());
      allObjectsCreated.add(outputStream.getDataType());
      
      sourceMetaObject.addCoDependentObject(filterCQ.getUuid());
      sourceMetaObject.addCoDependentObject(outputStream.getUuid());
      sourceMetaObject.addCoDependentObject(outputStream.getDataType());
      ctx.updateMetaObject(sourceMetaObject);
    }
    catch (MetaDataRepositoryException e)
    {
      log(e);
    }
    return allObjectsCreated;
  }
  
  private void changeMetaObjectState(@NotNull Context context, @NotNull List<UUID> UUIDs, @Nullable Boolean setAnonymous, @Nullable Boolean setGenerated)
    throws MetaDataRepositoryException
  {
    if (UUIDs == null) {
      throw new RuntimeException("Problem in creating MAP() internal components");
    }
    if (UUIDs != null) {
      for (UUID uuid : UUIDs)
      {
        MetaInfo.MetaObject obj = context.getObject(uuid);
        if (obj != null) {
          changeMetaObjectState(context, obj, setAnonymous, setGenerated);
        }
      }
    }
  }
  
  private void changeMetaObjectState(@NotNull Context context, @NotNull MetaInfo.MetaObject object, @Nullable Boolean setAnonymous, @Nullable Boolean setGenerated)
    throws MetaDataRepositoryException
  {
    if (object == null) {
      throw new RuntimeException("Problem in altering null object with setAnonymous: " + setAnonymous + " setGenerated: " + setGenerated);
    }
    if (setGenerated != null) {
      object.getMetaInfoStatus().setGenerated(setGenerated.booleanValue());
    }
    if (setAnonymous != null) {
      object.getMetaInfoStatus().setAnonymous(setAnonymous.booleanValue());
    }
    context.updateMetaObject(object);
  }
  
  private void rollbackOperation(Context context, @Nullable List<UUID> optionalUUIDs, MetaInfo.MetaObject... objects)
    throws MetaDataRepositoryException
  {
    if ((context == null) || (objects == null)) {
      return;
    }
    for (MetaInfo.MetaObject obj : objects) {
      if (obj != null) {
        context.removeObject(obj);
      }
    }
  }
  
  private MetaInfo.Type findTypeOfSourceAdapter(@NotNull Compiler compiler, @NotNull Context ctx, CreateSourceOrTargetStmt createSourceWithImplicitCQStmt)
    throws MetaDataRepositoryException
  {
    return compiler.prepareSource(createSourceWithImplicitCQStmt.srcOrDest.getAdapterTypeName(), createSourceWithImplicitCQStmt.srcOrDest, createSourceWithImplicitCQStmt.parserOrFormatter);
  }
  
  private MetaInfo.Stream createImplicitStreamForRedirect(@NotNull Compiler compiler, @NotNull Context ctx, boolean doReplace, ObjectName streamObjectNameTier1, MetaInfo.Type inputType)
    throws MetaDataRepositoryException
  {
    return ctx.putStream(doReplace, streamObjectNameTier1, inputType.getUuid(), null, null, null, null, new MetaInfoStatus().setAnonymous(true));
  }
  
  private void setSelectFromClauseToImplicitStream(@NotNull Compiler compiler, @NotNull Context ctx, InputOutputSink sink, String streamName, String alias)
  {
    sink.getFilter().from = new ArrayList();
    sink.getFilter().from.add(AST.SourceStream(streamName, null, alias));
  }
  
  private MetaInfo.Stream createStreamWithGivenType(@NotNull Compiler compiler, @NotNull Context ctx, InputOutputSink sink, boolean doReplace)
    throws MetaDataRepositoryException
  {
    StreamPersistencePolicy spp = new StreamPersistencePolicy(null);
    CreateStreamStmt createStreamStmt = new CreateStreamStmt(sink.getStreamName(), Boolean.valueOf(doReplace), null, null, sink.getTypeDefinition(), null, spp);
    return (MetaInfo.Stream)compiler.compileCreateStreamStmt(createStreamStmt);
  }
  
  private MetaInfo.Stream createStreamWithGivenType(@NotNull Compiler compiler, @NotNull Context ctx, String streamName, String typeName, boolean doReplace)
    throws MetaDataRepositoryException
  {
    StreamPersistencePolicy spp = new StreamPersistencePolicy(null);
    CreateStreamStmt createStreamStmt = new CreateStreamStmt(streamName, Boolean.valueOf(doReplace), null, typeName, null, null, spp);
    return (MetaInfo.Stream)compiler.compileCreateStreamStmt(createStreamStmt);
  }
  
  private MetaInfo.CQ createCQWithImplicitStream(@NotNull Compiler compiler, @NotNull Context ctx, boolean doReplace, InputOutputSink sink, @NotNull String cqName, @NotNull String streamName)
    throws MetaDataRepositoryException
  {
    CreateCqStmt ccs = new CreateCqStmt(cqName, Boolean.valueOf(doReplace), streamName, new ArrayList(), sink.getFilter(), sink.getFilterText());
    return (MetaInfo.CQ)compiler.compileCreateCqStmt(ccs);
  }
  
  private MetaInfo.Source createSourceWithImplicitStream(@NotNull Compiler compiler, @NotNull Context ctx, CreateSourceOrTargetStmt createSourceWithImplicitCQStmt, boolean doReplace, String sourceName, String streamName)
    throws MetaDataRepositoryException
  {
    ArrayList<InputOutputSink> sinks = new ArrayList();
    sinks.add(new OutputClause(streamName, null, new ArrayList(), null, null, streamName));
    CreateSourceOrTargetStmt css = new CreateSourceOrTargetStmt(EntityType.SOURCE, sourceName, Boolean.valueOf(doReplace), createSourceWithImplicitCQStmt.srcOrDest, createSourceWithImplicitCQStmt.parserOrFormatter, sinks);
    return (MetaInfo.Source)compiler.compileCreateSourceOrTargetStmt(css);
  }
  
  private void redirectStreamForGeneratedStream(@Nullable MappedStream getGeneratedStream, @NotNull String toStream)
  {
    if (toStream == null) {
      throw new RuntimeException("Mapped stream redirection failed");
    }
    if (getGeneratedStream != null) {
      getGeneratedStream.streamName = toStream;
    }
  }
  
  private void log(Exception e)
  {
    if ((e != null) && 
      (logger.isDebugEnabled())) {
      logger.debug(e.getMessage(), e);
    }
  }
}
