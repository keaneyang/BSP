package com.bloom.runtime.compiler.select;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.TypeField;
import com.bloom.runtime.compiler.TypeName;
import com.bloom.runtime.compiler.exprs.FieldRef;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.exprs.VirtFieldRef;
import com.bloom.runtime.utils.Factory;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataSourceNestedCollection
  extends DataSourcePrimary
{
  private final String fieldName;
  private final String alias;
  private final TypeName itemTypeName;
  private final List<TypeField> expectedFieldsDesc;
  
  public DataSourceNestedCollection(String fieldName, String alias, TypeName typeName, List<TypeField> fields)
  {
    this.fieldName = fieldName;
    this.alias = alias;
    this.itemTypeName = typeName;
    this.expectedFieldsDesc = fields;
  }
  
  public String toString()
  {
    return "ITERATOR(" + this.fieldName + ") AS " + this.alias;
  }
  
  public DataSet resolveDataSourceImpl(DataSource.Resolver r)
  {
    throw new UnsupportedOperationException();
  }
  
  private FieldRef getRowAsField(DataSet ds)
  {
    List<FieldRef> flds = ds.makeListOfAllFields();
    if (flds.size() != 1) {
      return null;
    }
    FieldRef fld = (FieldRef)flds.get(0);
    if (!(fld instanceof VirtFieldRef)) {
      return null;
    }
    if (!DataSetBuilder.isRowAsField((VirtFieldRef)fld)) {
      return null;
    }
    return fld;
  }
  
  private FieldRef resolveField(DataSet ds, String fieldName)
  {
    FieldRef fld = null;
    try
    {
      ValueExpr dsref = ds.makeRef();
      for (String namePart : fieldName.split("\\."))
      {
        fld = dsref.getField(namePart);
        dsref = fld;
      }
    }
    catch (NoSuchFieldException|SecurityException e)
    {
      return null;
    }
    return fld;
  }
  
  public DataSet resolveDataSource(DataSource.Resolver r, Map<String, DataSet> resolved, boolean onlyPrimary)
    throws MetaDataRepositoryException
  {
    if (onlyPrimary) {
      return null;
    }
    String dsAndFieldName = this.fieldName;
    int pos = dsAndFieldName.indexOf('.');
    String datasetName = pos == -1 ? dsAndFieldName : dsAndFieldName.substring(0, pos);
    
    String fieldName = pos == -1 ? null : dsAndFieldName.substring(pos + 1);
    
    DataSet ds = (DataSet)resolved.get(datasetName);
    if (ds == null) {
      return null;
    }
    FieldRef fld = fieldName != null ? resolveField(ds, fieldName) : getRowAsField(ds);
    if (fld == null) {
      r.error("no such field", dsAndFieldName);
    }
    Class<?> type = fld.getType();
    if (!Iterable.class.isAssignableFrom(type)) {
      r.error("has not iterable type", dsAndFieldName);
    }
    if (this.itemTypeName != null)
    {
      Class<?> itemType = r.getTypeInfo(this.itemTypeName);
      return DataSetFactory.createNestedJavaObjects(r.getNextDataSetID(), ds, fld, this.alias, itemType);
    }
    if (this.expectedFieldsDesc != null)
    {
      List<RSFieldDesc> flds = makeRSDesc(r, this.expectedFieldsDesc);
      return DataSetFactory.createNestedJsonTyped(r.getNextDataSetID(), ds, fld, this.alias, flds);
    }
    if (JsonNode.class.isAssignableFrom(type)) {
      return DataSetFactory.createNestedJsonUnstruct(r.getNextDataSetID(), ds, fld, this.alias);
    }
    return DataSetFactory.createNestedJavaReflect(r.getNextDataSetID(), ds, fld, this.alias);
  }
  
  private List<RSFieldDesc> makeRSDesc(DataSource.Resolver r, List<TypeField> fields)
  {
    Set<String> names = Factory.makeNameSet();
    List<RSFieldDesc> ret = new ArrayList();
    for (TypeField tf : fields)
    {
      String fieldName = tf.fieldName;
      if (names.contains(fieldName))
      {
        r.error("duplicated field name", fieldName);
      }
      else
      {
        names.add(fieldName);
        Class<?> type = r.getTypeInfo(tf.fieldType);
        ret.add(new RSFieldDesc(fieldName, type));
      }
    }
    return ret;
  }
}
