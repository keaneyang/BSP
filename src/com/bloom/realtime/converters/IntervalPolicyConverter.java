package com.bloom.runtime.converters;

import com.bloom.runtime.Interval;
import com.bloom.runtime.meta.IntervalPolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.bloom.event.ObjectMapperFactory;

import java.util.HashMap;
import org.apache.log4j.Logger;
import org.eclipse.persistence.mappings.DatabaseMapping;
import org.eclipse.persistence.mappings.converters.Converter;
import org.eclipse.persistence.sessions.Session;

public class IntervalPolicyConverter
  implements Converter
{
  private static final long serialVersionUID = -8049064931525422511L;
  private static Logger logger = Logger.getLogger(IntervalPolicyConverter.class);
  ObjectMapper mapper = ObjectMapperFactory.newInstance();
  
  public Object convertDataValueToObjectValue(Object dataValue, Session arg1)
  {
    if (dataValue == null) {
      return dataValue;
    }
    if ((dataValue instanceof String)) {
      try
      {
        HashMap<String, Object> iPolicyMap = (HashMap)this.mapper.readValue((String)dataValue, HashMap.class);
        int countVal = -1;
        long timeVal = -1L;
        String attrNameVal = null;
        long attrRangeVal = -1L;
        IntervalPolicy objVal = null;
        
        Object attrPolicy = iPolicyMap.get("attrBasedPolicy");
        if (attrPolicy != null)
        {
          HashMap<String, Object> attrPolicyHash = (HashMap)attrPolicy;
          attrNameVal = (String)attrPolicyHash.get("name");
          Object attrRangeValObj = attrPolicyHash.get("range");
          if ((attrRangeValObj instanceof Long)) {
            attrRangeVal = ((Long)attrRangeValObj).longValue();
          } else {
            attrRangeVal = ((Integer)attrRangeValObj).intValue();
          }
        }
        Object countPolicy = iPolicyMap.get("countBasedPolicy");
        if (countPolicy != null)
        {
          HashMap<String, Integer> countPolicyHash = (HashMap)countPolicy;
          countVal = ((Integer)countPolicyHash.get("count")).intValue();
        }
        Object timePolicy = iPolicyMap.get("timeBasedPolicy");
        if (timePolicy != null)
        {
          HashMap<String, Object> timePolicyHash = (HashMap)timePolicy;
          HashMap<String, Object> timeValHash = (HashMap)timePolicyHash.get("time");
          Object timeValObj = timeValHash.get("value");
          if ((timeValObj instanceof Long)) {
            timeVal = ((Long)timeValObj).longValue();
          } else {
            timeVal = ((Integer)timeValObj).intValue();
          }
        }
        if ((attrPolicy != null) && (timePolicy != null)) {
          objVal = IntervalPolicy.createTimeAttrPolicy(new Interval(timeVal), attrNameVal, attrRangeVal);
        } else if ((countPolicy != null) && (timePolicy != null)) {
          objVal = IntervalPolicy.createTimeCountPolicy(new Interval(timeVal), countVal);
        } else if (countPolicy != null) {
          objVal = IntervalPolicy.createCountPolicy(countVal);
        } else if (timePolicy != null) {
          objVal = IntervalPolicy.createTimePolicy(new Interval(timeVal));
        } else if (attrPolicy == null) {}
        return IntervalPolicy.createAttrPolicy(attrNameVal, attrRangeVal);
      }
      catch (Exception e)
      {
        logger.error("Problem reading Interval policy " + dataValue + " from JSON");
      }
    }
    return null;
  }
  
  public Object convertObjectValueToDataValue(Object iPolicy, Session arg1)
  {
    if (iPolicy == null) {
      return null;
    }
    if ((iPolicy instanceof IntervalPolicy)) {
      try
      {
        return this.mapper.writeValueAsString(iPolicy);
      }
      catch (Exception e)
      {
        logger.error("Problem writing Interval policy " + iPolicy + " as JSON");
      }
    }
    return null;
  }
  
  public void initialize(DatabaseMapping arg0, Session arg1) {}
  
  public boolean isMutable()
  {
    return false;
  }
}
