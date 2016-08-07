package com.bloom.runtime;

import com.bloom.classloading.WALoader;
import com.bloom.exception.CompilationException;
import com.bloom.intf.SourceMetadataProvider;
import com.bloom.kafkamessaging.StreamPersistencePolicy;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.proc.BaseProcess;
import com.bloom.runtime.compiler.AST;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.TypeDefOrName;
import com.bloom.runtime.compiler.TypeField;
import com.bloom.runtime.compiler.exprs.ComparePredicate;
import com.bloom.runtime.compiler.exprs.Constant;
import com.bloom.runtime.compiler.exprs.ExprCmd;
import com.bloom.runtime.compiler.exprs.FuncCall;
import com.bloom.runtime.compiler.exprs.MethodCall;
import com.bloom.runtime.compiler.exprs.ObjectRef;
import com.bloom.runtime.compiler.exprs.Predicate;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.select.DataSource;
import com.bloom.runtime.compiler.select.DataSourceStream;
import com.bloom.runtime.compiler.stmts.CreateCqStmt;
import com.bloom.runtime.compiler.stmts.CreatePropertySetStmt;
import com.bloom.runtime.compiler.stmts.CreateStreamStmt;
import com.bloom.runtime.compiler.stmts.CreateTypeStmt;
import com.bloom.runtime.compiler.stmts.MappedStream;
import com.bloom.runtime.compiler.stmts.Select;
import com.bloom.runtime.compiler.stmts.SelectTarget;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.MetaInfo.CQ;
import com.bloom.runtime.meta.MetaInfo.Source;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.UUID;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.log4j.Logger;

public class TypeGenerator
{
  private static Logger logger = Logger.getLogger(TypeGenerator.class);
  public static final String TYPE_SUFFIX = "Type";
  public static final String CQ_SUFFIX = "Cq";
  private Map<String, TypeDefOrName> metadataMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
  private final String metadataKey;
  
  public TypeGenerator(Compiler c, String sourceName)
  {
    Context ctx = c.getContext();
    try
    {
      MetaInfo.Source sourceInfo = ctx.getSourceInCurSchema(sourceName);
      
      Class<?> adapterFactory = WALoader.get().loadClass(sourceInfo.adapterClassName);
      BaseProcess proc = (BaseProcess)adapterFactory.newInstance();
      if (!(proc instanceof SourceMetadataProvider)) {
        throw new RuntimeException("Adapter " + proc.toString() + " does not support providing Metadata.");
      }
      String defaultMode = (String)sourceInfo.properties.get("SessionType");
      sourceInfo.properties.put("SessionType", "METADATA");
      proc.init(sourceInfo.properties, sourceInfo.parserProperties, sourceInfo.uuid, BaseServer.getServerName(), null, false, null);
      
      this.metadataKey = ((SourceMetadataProvider)proc).getMetadataKey();
      this.metadataMap = ((SourceMetadataProvider)proc).getMetadata();
      sourceInfo.properties.put("SessionType", defaultMode);
      proc.close();
    }
    catch (Exception e)
    {
      throw new CompilationException(e);
    }
  }
  
  public List<UUID> generateStream(Compiler c, List<MappedStream> mapped_streams, String sourceStreamName, boolean isStreamEndpoint)
    throws MetaDataRepositoryException
  {
    List<UUID> metaObjects = new ArrayList();
    for (MappedStream mappedStream : mapped_streams)
    {
      String typeName = mappedStream.streamName + "Type";
      String targetStreamName = mappedStream.streamName;
      String cqName = mappedStream.streamName + "Cq";
      String sourceTable = mappedStream.mappingProperties.get("table").toString();
      
      TypeDefOrName tableDef = (TypeDefOrName)this.metadataMap.get(sourceTable.replace(".", "_"));
      if (tableDef == null) {
        throw new CompilationException("No metadata found for " + sourceTable);
      }
      List<TypeField> fields = tableDef.typeDef;
      
      CreateTypeStmt ctStmt = new CreateTypeStmt(typeName, Boolean.valueOf(true), tableDef);
      
      UUID typeUUID = c.compileCreateTypeStmt(ctStmt);
      
      StreamPersistencePolicy spp = new StreamPersistencePolicy(null);
      CreateStreamStmt csStmt = new CreateStreamStmt(targetStreamName, Boolean.valueOf(true), null, typeName, fields, null, spp);
      MetaInfo.Stream streamMetaObject = (MetaInfo.Stream)c.compileCreateStreamStmt(csStmt);
      if (isStreamEndpoint)
      {
        streamMetaObject.getMetaInfoStatus().setGenerated(true);
        MetadataRepository.getINSTANCE().updateMetaObject(streamMetaObject, WASecurityManager.TOKEN);
      }
      DataSource sourceStream = new DataSourceStream(sourceStreamName, null, null);
      List<DataSource> source_list = new ArrayList();
      source_list.add(sourceStream);
      List<SelectTarget> target_list = convertFieldsToTarget(fields);
      
      sourceTable = sourceTable.replaceAll("\\\\", "\\\\\\\\");
      
      Predicate where = null;
      if (this.metadataKey != null)
      {
        List<ValueExpr> whereArgs = new ArrayList();
        List<ValueExpr> toStringArgs = new ArrayList();
        List<ValueExpr> metaArgs = new ArrayList();
        metaArgs.add(new ObjectRef(sourceStreamName));
        metaArgs.add(new Constant(ExprCmd.STRING, this.metadataKey, String.class));
        toStringArgs.add(new FuncCall("META", metaArgs, 0));
        whereArgs.add(new MethodCall("toString", toStringArgs, 0));
        whereArgs.add(new Constant(ExprCmd.STRING, sourceTable, String.class));
        where = new ComparePredicate(ExprCmd.EQ, whereArgs);
      }
      Select sel = new Select(false, 1, target_list, source_list, where, null, null, null, null, false, null);
      
      List<String> field_list = convertFieldsToName(fields);
      CreateCqStmt ccqStmt = new CreateCqStmt(cqName, Boolean.valueOf(true), targetStreamName, field_list, sel, null);
      MetaInfo.CQ cqMetaObject = (MetaInfo.CQ)c.compileCreateCqStmt(ccqStmt);
      
      metaObjects.add(cqMetaObject.getUuid());
      metaObjects.add(streamMetaObject.getUuid());
      metaObjects.add(typeUUID);
    }
    return metaObjects;
  }
  
  public static UUID generatePropertySet(Compiler c, String propertySetName, List<Property> properties)
    throws Exception
  {
    CreatePropertySetStmt cpsStmt = new CreatePropertySetStmt(propertySetName, Boolean.valueOf(true), properties);
    return c.compileCreatePropertySetStmt(cpsStmt);
  }
  
  public static List<String> convertFieldsToName(List<TypeField> field_list)
  {
    List<String> name_list = new ArrayList();
    for (TypeField field : field_list) {
      name_list.add(field.fieldName);
    }
    return name_list;
  }
  
  public static List<SelectTarget> convertFieldsToTarget(List<TypeField> field_list)
  {
    List<SelectTarget> target_list = new ArrayList();
    
    int i = 0;
    for (TypeField field : field_list)
    {
      List<ValueExpr> exprArgs = new ArrayList();
      exprArgs.add(AST.NewIntegerConstant(Integer.valueOf(i)));
      ValueExpr indexExpr = AST.NewIndexExpr(AST.NewIdentifierRef("data"), exprArgs);
      SelectTarget target = new SelectTarget(indexExpr, "data[" + i + "]");
      target_list.add(target);
      i++;
    }
    return target_list;
  }
  
  public static String getValidJavaIdentifierName(String name)
  {
    String modified = "";
    
    Character fc = Character.valueOf(name.charAt(0));
    if (Character.isJavaIdentifierStart(fc.charValue())) {
      modified = modified + fc;
    } else {
      modified = "_" + getNameOfCharacter(fc) + "_";
    }
    for (int i = 1; i < name.length(); i++)
    {
      Character c = Character.valueOf(name.charAt(i));
      if (Character.isJavaIdentifierPart(c.charValue())) {
        modified = modified + c;
      } else {
        modified = modified + "_" + getNameOfCharacter(c) + "_";
      }
    }
    if (logger.isInfoEnabled()) {
      logger.info("original " + name + " Modified " + modified);
    }
    return modified;
  }
  
  public static String getNameOfCharacter(Character c)
  {
    switch (c.charValue())
    {
    case '!': 
      return "EXCLAMATION_MARK";
    case '@': 
      return "COMMERCIAL_AT";
    case '#': 
      return "HASH";
    case '%': 
      return "PERCENT_SIGN";
    case '^': 
      return "CIRCUMFLEX_ACCENT";
    case '&': 
      return "AMPERSAND";
    case '*': 
      return "ASTERISK";
    case '(': 
      return "LEFT_PARENTHESIS";
    case ')': 
      return "RIGHT_PARENTHESIS";
    case '+': 
      return "PLUS_SIGN";
    case '{': 
      return "LEFT_CURLY_BRACKET";
    case '}': 
      return "RIGHT_CURLY_BRACKET";
    case '[': 
      return "LEFT_SQUARE_BRACKET";
    case ']': 
      return "RIGHT_SQUARE_BRACKET";
    case '/': 
      return "SOLIDUS";
    case '|': 
      return "VERTICAL_LINE";
    case '\\': 
      return "REVERSE_SOLIDUS";
    case '-': 
      return "HYPHEN_MINUS";
    case '=': 
      return "EQUALS_SIGN";
    case ':': 
      return "COLON";
    case ';': 
      return "SEMICOLON";
    case '\'': 
      return "APOSTROPHE";
    case '<': 
      return "LESS_THAN_SIGN";
    case '>': 
      return "GREATER_THAN_SIGN";
    case '?': 
      return "QUESTION_MARK";
    case '"': 
      return "QUOTATION_MARK";
    case ' ': 
      return "SPACE";
    }
    return Character.getName(c.charValue()).replace(' ', '_').replace('-', '_');
  }
  
  public static String getTypeName(String namespace, String sourceName, String tableName)
  {
    tableName = tableName.replace(".", "_");
    
    String typeName = namespace + "." + sourceName + "_" + getValidJavaIdentifierName(tableName) + "_" + "Type";
    if (logger.isInfoEnabled()) {
      logger.info("Namespace name - " + namespace + ", Source name - " + sourceName + ", Table Name - " + tableName + " - Generated TypeName - " + typeName);
    }
    return typeName;
  }
}
