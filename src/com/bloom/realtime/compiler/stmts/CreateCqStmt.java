package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

import java.util.List;

public class CreateCqStmt
  extends CreateStmt
{
  public final String stream_name;
  public final List<String> field_list;
  private List<String> partitionFieldList;
  public final Select select;
  public final String select_text;
  
  public CreateCqStmt(String cq_name, Boolean doReplace, String dest_stream_name, List<String> field_name_list, Select sel, String select_text)
  {
    super(EntityType.CQ, cq_name, doReplace.booleanValue());
    this.stream_name = dest_stream_name;
    this.field_list = field_name_list;
    this.select = sel;
    this.select_text = select_text;
  }
  
  public String toString()
  {
    return stmtToString() + " AS INSERT INTO " + this.stream_name + "(" + this.field_list + ")\n" + this.select;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileCreateCqStmt(this);
  }
  
  public List<String> getPartitionFieldList()
  {
    return this.partitionFieldList;
  }
  
  public void setPartitionFieldList(List<String> partitionFieldList)
  {
    this.partitionFieldList = partitionFieldList;
  }
}
