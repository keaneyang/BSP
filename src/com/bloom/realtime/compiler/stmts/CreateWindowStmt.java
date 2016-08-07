package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.Pair;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.IntervalPolicy;

import java.util.List;

public class CreateWindowStmt
  extends CreateStmt
{
  public final String stream_name;
  public final Pair<IntervalPolicy, IntervalPolicy> window_len;
  public final boolean jumping;
  public final List<String> partitioning_fields;
  public final Pair<IntervalPolicy, IntervalPolicy> slidePolicy;
  
  public CreateWindowStmt(String window_name, Boolean doReplace, String stream_name, Pair<IntervalPolicy, IntervalPolicy> window_len, boolean isJumping, List<String> part, Pair<IntervalPolicy, IntervalPolicy> slidePolicy)
  {
    super(EntityType.WINDOW, window_name, doReplace.booleanValue());
    this.stream_name = stream_name;
    this.window_len = window_len;
    this.jumping = isJumping;
    this.partitioning_fields = part;
    this.slidePolicy = slidePolicy;
  }
  
  public String toString()
  {
    return stmtToString() + " OVER " + this.stream_name + " KEEP " + this.window_len + (this.jumping ? " JUMPING " : "") + (this.partitioning_fields == null ? "" : new StringBuilder().append("PARTITION ").append(this.partitioning_fields).toString());
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileCreateWindowStmt(this);
  }
}
