package com.bloom.waction;

import com.bloom.exception.NoOperatorFoundException;
import com.bloom.persistence.WactionQuery;
import com.bloom.persistence.WactionQuery.OpType;
import com.bloom.runtime.exceptions.InvalidFormatException;

import java.util.Map;
import java.util.Map.Entry;

public class WactionApiRestCompiler
{
  StringBuilder selectString;
  
  public WactionApiRestCompiler()
  {
    this.selectString = new StringBuilder();
  }
  
  public String createSelectString(String dataStore, String[] fields, Map<String, Object> filters, String keyField, String[] implicitFields, boolean getEventList)
    throws NoOperatorFoundException, InvalidFormatException
  {
    this.selectString.setLength(0);
    this.selectString.append("SELECT ");
    if (((fields == null) && (!getEventList)) || ((fields.length == 0) && (!getEventList))) {
      throw new InvalidFormatException("SELECT expression need atleast 1 field or event list");
    }
    if ((fields != null) && (fields.length > 0)) {
      for (int ii = 0; ii < fields.length; ii++) {
        if (ii != fields.length - 1)
        {
          this.selectString.append(fields[ii]);
          this.selectString.append(", ");
        }
        else
        {
          this.selectString.append(fields[ii]);
        }
      }
    }
    if (getEventList)
    {
      this.selectString.append(", ");
      this.selectString.append("eventList(w)");
    }
    this.selectString.append(" ");
    this.selectString.append("FROM ");
    this.selectString.append(dataStore);
    if (getEventList)
    {
      this.selectString.append(" ");
      this.selectString.append("w");
    }
    this.selectString.append(" ");
    if (filters != null) {
      addFilters(filters, null, null);
    }
    this.selectString.append(";");
    return this.selectString.toString();
  }
  
  public void addFilters(Map<String, Object> filters, String groupByVal, String orderByVal)
  {
    Map<String, Object> ctxFilter = (Map)filters.get("context");
    int totalConditions;
    if ((ctxFilter != null) && (!ctxFilter.isEmpty()))
    {
      this.selectString.append("WHERE ");
      totalConditions = ctxFilter.size();
      for (Map.Entry<String, Object> conditions : ctxFilter.entrySet())
      {
        String ctxField = (String)conditions.getKey();
        String ctxCondition = (String)conditions.getValue();
        OperatorOnField operatorOnField = stripOperator(ctxCondition);
        switch (operatorOnField.operator)
        {
        case IN: 
          String[] inValues = operatorOnField.data.split("[~]");
          this.selectString.append(ctxField + " " + operatorOnField.operator.name() + "(");
          for (int ii = 0; ii < inValues.length; ii++) {
            if (ii != inValues.length - 1)
            {
              this.selectString.append(inValues[ii]);
              this.selectString.append(", ");
            }
            else
            {
              this.selectString.append(inValues[ii]);
            }
          }
          this.selectString.append(")");
          break;
        case BTWN: 
          String[] btwnValues = operatorOnField.data.split("[~]");
          if (btwnValues.length != 2) {
            throw new InvalidFormatException(operatorOnField.operator.name() + " expects 2 values");
          }
          this.selectString.append(ctxField);
          this.selectString.append(" ");
          this.selectString.append("BETWEEN ");
          this.selectString.append(btwnValues[0]);
          this.selectString.append(" ");
          this.selectString.append(" AND ");
          this.selectString.append(" ");
          this.selectString.append(btwnValues[1]);
          break;
        case LIKE: 
          this.selectString.append(ctxField);
          this.selectString.append(" ");
          this.selectString.append(operatorOnField.operator.name());
          this.selectString.append("'%");
          this.selectString.append(operatorOnField.data);
          this.selectString.append("%'");
          break;
        case GT: 
          this.selectString.append(ctxField);
          this.selectString.append(">");
          this.selectString.append(operatorOnField.data);
          break;
        case LT: 
          this.selectString.append(ctxField);
          this.selectString.append("<");
          this.selectString.append(operatorOnField.data);
          break;
        case GTE: 
          this.selectString.append(ctxField);
          this.selectString.append(">=");
          this.selectString.append(operatorOnField.data);
          break;
        case LTE: 
          this.selectString.append(ctxField);
          this.selectString.append("<=");
          this.selectString.append(operatorOnField.data);
          break;
        case EQ: 
          this.selectString.append(ctxField);
          this.selectString.append("=");
          this.selectString.append(operatorOnField.data);
          break;
        case NE: 
          this.selectString.append(ctxField);
          this.selectString.append("<>");
          this.selectString.append(operatorOnField.data);
        }
        totalConditions--;
        if (totalConditions != 0) {
          this.selectString.append(" AND ");
        }
      }
    }
    if (filters.containsKey("starttime"))
    {
      if (filters.containsKey("endtime"))
      {
        this.selectString.append(" AND ");
        this.selectString.append("$timestamp");
        this.selectString.append(" ");
        this.selectString.append("BETWEEN ");
        this.selectString.append(filters.get("starttime"));
        this.selectString.append(" AND ");
        this.selectString.append(filters.get("endtime"));
      }
      else
      {
        this.selectString.append(" AND ");
        this.selectString.append("$timestamp");
        this.selectString.append(">");
        this.selectString.append(filters.get("starttime"));
      }
    }
    else if (filters.containsKey("endtime"))
    {
      this.selectString.append(" AND ");
      this.selectString.append("$timestamp");
      this.selectString.append("<");
      this.selectString.append(filters.get("endtime"));
    }
    if (filters.containsKey("groupby"))
    {
      this.selectString.append(" ");
      this.selectString.append("GROUP BY ");
      if (groupByVal != null)
      {
        this.selectString.append(groupByVal);
      }
      else
      {
        String gBy = (String)filters.get("groupby");
        String[] gByArray = gBy.split("~");
        for (int ii = 0; ii < gByArray.length; ii++) {
          if (ii != gByArray.length - 1)
          {
            this.selectString.append(gByArray[ii]);
            this.selectString.append(", ");
          }
          else
          {
            this.selectString.append(gByArray[ii]);
          }
        }
      }
    }
    if (filters.containsKey("sortby"))
    {
      this.selectString.append(" ");
      this.selectString.append("ORDER BY ");
      String oBy = (String)filters.get("sortby");
      String[] oByArray = oBy.split("~");
      for (int ii = 0; ii < oByArray.length; ii++) {
        if (ii != oByArray.length - 1)
        {
          this.selectString.append(oByArray[ii]);
          this.selectString.append(", ");
        }
        else
        {
          this.selectString.append(oByArray[ii]);
        }
      }
      if (orderByVal != null)
      {
        this.selectString.append(", ");
        this.selectString.append(orderByVal);
      }
      if (filters.containsKey("sortdir"))
      {
        this.selectString.append(" ");
        this.selectString.append(filters.get("sortdir"));
      }
    }
    if (filters.containsKey("limit"))
    {
      this.selectString.append(" ");
      this.selectString.append("LIMIT ");
      this.selectString.append(filters.get("limit"));
    }
  }
  
  public String createQueryToGetLatestPerKey(String dataStore, String[] fields, Map<String, Object> filters, String keyField, String[] implicitFields, boolean getEventList)
  {
    if (((fields == null) && (!getEventList)) || ((fields.length == 0) && (!getEventList))) {
      throw new InvalidFormatException("SELECT expression need atleast 1 field or event list");
    }
    this.selectString.setLength(0);
    this.selectString.append("SELECT ");
    if ((fields != null) && (fields.length > 0)) {
      for (int ii = 0; ii < fields.length; ii++) {
        if (ii != fields.length - 1)
        {
          this.selectString = addField(fields[ii]);
          this.selectString.append(", ");
        }
        else
        {
          this.selectString = addField(fields[ii]);
        }
      }
    }
    if (getEventList)
    {
      this.selectString.append(", ");
      this.selectString.append("eventList(w)");
    }
    this.selectString.append(" ");
    this.selectString.append("FROM ");
    this.selectString.append(dataStore);
    this.selectString.append(" ");
    this.selectString.append("w");
    this.selectString.append(" ");
    if (filters.containsKey("singlewactions"))
    {
      Object removed = filters.remove("singlewactions");
      filters.put("groupby", removed);
    }
    addFilters(filters, keyField, "$timestamp");
    this.selectString.append(";");
    return this.selectString.toString();
  }
  
  public StringBuilder addField(String field)
  {
    this.selectString.append("last");
    this.selectString.append("(");
    this.selectString.append("w." + field);
    this.selectString.append(")");
    return this.selectString;
  }
  
  public OperatorOnField stripOperator(String ctxCondition)
    throws NoOperatorFoundException, InvalidFormatException
  {
    String[] conditionSplit = ctxCondition.split("\\$");
    if (conditionSplit.length == 3)
    {
      if ((conditionSplit[1] != null) && (!conditionSplit[1].isEmpty()))
      {
        if ((conditionSplit[2] != null) && (!conditionSplit[2].isEmpty()))
        {
          WactionQuery.OpType opType = WactionQuery.OpType.valueOf(conditionSplit[1]);
          OperatorOnField operatorOnField = new OperatorOnField(opType, conditionSplit[2]);
          return operatorOnField;
        }
        throw new InvalidFormatException("Can't apply operator " + conditionSplit[1] + " on Null or Empty Data Set");
      }
      throw new NoOperatorFoundException("No Operator defined for to run a Query.");
    }
    throw new InvalidFormatException("Format is $OPERATOR$FIELD");
  }
  
  public class OperatorOnField
  {
    public WactionQuery.OpType operator;
    public String data;
    
    public OperatorOnField(WactionQuery.OpType operator, String field)
    {
      this.operator = operator;
      this.data = field;
    }
  }
}
