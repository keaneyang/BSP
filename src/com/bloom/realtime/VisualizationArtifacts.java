package com.bloom.runtime;

import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Dashboard;
import com.bloom.runtime.meta.MetaInfo.Page;
import com.bloom.runtime.meta.MetaInfo.Query;
import com.bloom.runtime.meta.MetaInfo.QueryVisualization;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.codehaus.jackson.JsonGenerationException;

public class VisualizationArtifacts
{
  private MetaInfo.Dashboard dashboard = new MetaInfo.Dashboard();
  private List<MetaInfo.Page> pages = new ArrayList();
  private List<MetaInfo.QueryVisualization> queryVisualizations = new ArrayList();
  private List<MetaInfo.Query> parametrizedQuery = new ArrayList();
  
  public void setDashboard(MetaInfo.Dashboard dashboard)
  {
    this.dashboard = dashboard;
  }
  
  public void addPages(MetaInfo.Page page)
  {
    this.pages.add(page);
  }
  
  public void addQueryVisualization(MetaInfo.QueryVisualization qv)
  {
    this.queryVisualizations.add(qv);
  }
  
  public void addParametrizedQuery(MetaInfo.Query query)
  {
    this.parametrizedQuery.add(query);
  }
  
  public String convertToJSON()
    throws JsonGenerationException, JsonMappingException, IOException
  {
    return new ObjectMapper().writeValueAsString(this);
  }
  
  public MetaInfo.Dashboard getDashboard()
  {
    return this.dashboard;
  }
  
  public List<MetaInfo.Page> getPages()
  {
    return this.pages;
  }
  
  public List<MetaInfo.QueryVisualization> getQueryVisualizations()
  {
    return this.queryVisualizations;
  }
  
  public String toString()
  {
    return "DASHBOARD : " + this.dashboard + " PAGES : " + this.pages + " QUERYVISUALIZATIONS: " + this.queryVisualizations + " PARAMETRIZEDQUERIES: " + this.parametrizedQuery;
  }
  
  public List<MetaInfo.Query> getParametrizedQuery()
  {
    return this.parametrizedQuery;
  }
}
