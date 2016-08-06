package com.bloom.waction;

import com.bloom.exception.ExpiredSessionException;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.uuid.AuthToken;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;

public class WactionApiServlet
  extends HttpServlet
{
  private static Logger logger = Logger.getLogger(WactionApiServlet.class);
  private static final long serialVersionUID = 8628625019674604142L;
  private WactionApi wap;
  
  public WactionApiServlet()
  {
    this.wap = WactionApi.get();
  }
  
  private static Set<String> predefined = new HashSet() {};
  
  public String getWactionStoreDefinition(HttpServletRequest request, AuthToken token)
    throws Exception
  {
    String storename = request.getParameter("name");
    Object[] storeDef = this.wap.getWactionStoreDef(storename);
    if (storeDef != null)
    {
      StringBuilder def = new StringBuilder();
      def.append("CONTEXT DEFINITION : \n");
      def.append(((MetaInfo.Type)storeDef[0]).describe(token));
      def.append("\n");
      def.append("EVENT LIST DEFINITION : \n");
      List<MetaInfo.Type> eventDefs = (List)storeDef[1];
      for (MetaInfo.Type type : eventDefs)
      {
        def.append(type.describe(token));
        def.append("\n");
      }
      return def.toString();
    }
    return "No Definition found for : " + storename;
  }
  
  private String getWactions(HttpServletRequest request, AuthToken token)
    throws Exception
  {
    Object queryResult = null;
    String storename = request.getParameter("name");
    if (storename == null) {
      throw new NullPointerException("WAction Store NULL not allowed");
    }
    String query = request.getParameter("query");
    if (query == null)
    {
      String[] fields = fieldParser(request.getParameter("fields"));
      Map<String, Object> filters = filterParser((String[])request.getParameterMap().get("filter"));
      if (logger.isDebugEnabled())
      {
        logger.debug("INPUT REQUEST PARAMS : ");
        logger.debug("WAction Store : " + storename);
        if (fields != null)
        {
          logger.debug("Fields : ");
          for (int ii = 0; ii < fields.length; ii++) {
            logger.debug("\t\t" + fields[ii]);
          }
        }
        if (filters != null)
        {
          logger.debug("Filter Criteria : ");
          logger.debug(filters.entrySet());
        }
      }
      queryResult = this.wap.get(storename, null, fields, filters, token);
    }
    else
    {
      Map<String, Object> queryParams = getParamsForQuery(request.getParameter("params"));
      queryResult = this.wap.executeQuery(storename, query, queryParams, token);
    }
    if (queryResult == null) {
      return null;
    }
    return this.wap.toJSON(queryResult);
  }
  
  public Map<String, Object> getParamsForQuery(String params)
    throws IllegalArgumentException
  {
    Map<String, Object> queryParams = null;
    if (params != null)
    {
      String[] allParams = params.split(",");
      for (String aParam : allParams)
      {
        String[] param = aParam.split(":");
        if (param.length != 2) {
          throw new IllegalArgumentException("Expected format is paramName:paramValue, but got " + param[0] + ":" + param[1]);
        }
        queryParams = new HashMap();
        queryParams.put(param[0], param[1]);
      }
    }
    return queryParams;
  }
  
  public Map<String, Object> filterParser(String[] filter)
  {
    if (filter != null)
    {
      Map<String, Object> map = new HashMap();
      Map<String, Object> ctx = new HashMap();
      
      String[] fltrs = filter[0].split(",");
      if (logger.isDebugEnabled()) {
        for (int flt = 0; flt < fltrs.length; flt++) {
          logger.debug("Filter : " + fltrs[flt]);
        }
      }
      for (String s : fltrs)
      {
        String[] fltr = s.split(":");
        if ((fltr.length == 2) && (!fltr[1].isEmpty()))
        {
          if (logger.isDebugEnabled()) {
            logger.debug(fltr[0] + " : " + fltr[1]);
          }
          String lCase = fltr[0].toLowerCase();
          if (predefined.contains(lCase))
          {
            if (lCase.equalsIgnoreCase("orderby")) {
              lCase = "sortby";
            }
            map.put(lCase, fltr[1]);
          }
          else
          {
            ctx.put(fltr[0], fltr[1]);
          }
        }
      }
      if ((map.isEmpty()) && (ctx.isEmpty()))
      {
        map = null;
      }
      else
      {
        if (!ctx.isEmpty()) {
          map.put("context", ctx);
        }
        if (logger.isDebugEnabled())
        {
          logger.debug("Filter ");
          logger.debug(map);
        }
      }
      return map;
    }
    return null;
  }
  
  public String[] fieldParser(String fields)
  {
    if (fields != null) {
      return fields.split(",");
    }
    return null;
  }
  
  private AuthToken getToken(HttpServletRequest request)
  {
    String rawToken = request.getParameter("token");
    if (rawToken == null)
    {
      Cookie[] cookies = request.getCookies();
      for (int i = 0; i < cookies.length; i++) {
        if (cookies[i].getName().equals("token")) {
          rawToken = cookies[i].getValue();
        }
      }
    }
    if (rawToken != null) {
      return new AuthToken(rawToken);
    }
    return null;
  }
  
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException
  {
    response.addHeader("Access-Control-Allow-Origin", "*");
    try
    {
      AuthToken token = getToken(request);
      if (token == null)
      {
        response.setStatus(401);
        return;
      }
      String servletPath = request.getPathInfo();
      String resultOfMethodCall = null;
      switch (servletPath)
      {
      case "/def": 
        resultOfMethodCall = getWactionStoreDefinition(request, token);
        break;
      case "/search": 
        resultOfMethodCall = getWactions(request, token);
      }
      if (resultOfMethodCall == null)
      {
        response.setStatus(403);
        return;
      }
      response.setStatus(200);
      response.getWriter().write(resultOfMethodCall);
    }
    catch (ExpiredSessionException|ServletException|NullPointerException|MetaDataRepositoryException e)
    {
      if ((e instanceof ExpiredSessionException)) {
        response.setStatus(403);
      } else if ((e instanceof ServletException)) {
        response.setStatus(400);
      } else if ((e instanceof NullPointerException)) {
        response.setStatus(400);
      } else if ((e instanceof MetaDataRepositoryException)) {
        response.setStatus(400);
      }
      logger.error(e.getMessage(), e);
      response.resetBuffer();
      response.getWriter().write(e.getMessage());
    }
    catch (Exception e)
    {
      logger.error(e.getMessage(), e);
      response.setStatus(400);
      response.resetBuffer();
      response.getWriter().write(e.getMessage());
    }
  }
  
  private static class Paths
  {
    public static final String DEFINITION = "/def";
    public static final String SEARCH = "/search";
  }
}
