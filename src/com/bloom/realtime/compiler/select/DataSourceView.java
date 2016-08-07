package com.bloom.runtime.compiler.select;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.runtime.Context;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.stmts.Select;
import com.bloom.runtime.meta.CQExecutionPlan;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.MetaInfo.CQ;
import com.bloom.runtime.utils.RuntimeUtils;

import java.util.Collections;

public class DataSourceView
  extends DataSourcePrimary
{
  private final Select subSelet;
  private final String alias;
  private final String selectText;
  
  public DataSourceView(Select stmt, String alias, String selectText)
  {
    this.subSelet = stmt;
    this.alias = alias;
    this.selectText = selectText;
  }
  
  public String toString()
  {
    return "(" + this.subSelet + ") AS " + this.alias;
  }
  
  public DataSet resolveDataSourceImpl(DataSource.Resolver r)
    throws MetaDataRepositoryException
  {
    Compiler co = r.getCompiler();
    Context ctx = co.getContext();
    
    String cqName = RuntimeUtils.genRandomName("view");
    Select select = this.subSelet;
    try
    {
      CQExecutionPlan plan = SelectCompiler.compileSelect(cqName, ctx.getCurNamespaceName(), co, select, r.getTraceOptions());
      
      MetaInfo.CQ cq = ctx.putCQ(false, ctx.makeObjectName(cqName), null, plan, this.selectText, Collections.emptyList(), null);
      
      cq.getMetaInfoStatus().setAnonymous(true);
      MetadataRepository.getINSTANCE().updateMetaObject(cq, ctx.getSessionID());
      return DataSetFactory.createSelectViewDS(r.getNextDataSetID(), this.alias, cq.uuid, plan.resultSetDesc, plan.isStateful());
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }
}
