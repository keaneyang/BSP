package com.bloom.rest;

import com.bloom.security.WASecurityManager;
import com.bloom.uuid.AuthToken;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.codehaus.jettison.json.JSONObject;

public class SecurityManagerServlet
  extends HttpServlet
{
  private static final long serialVersionUID = 1L;
  private Pattern authenticatePattern = Pattern.compile("/authenticate");
  private static final String UI_TYPE = "Bloom UI";
  
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException
  {
    Matcher authenticateMatcher = this.authenticatePattern.matcher(request.getPathInfo());
    if (authenticateMatcher.find())
    {
      String username = request.getParameter("username");
      String password = request.getParameter("password");
      String clientId = request.getParameter("clientId");
      if ((username == null) || (password == null))
      {
        response.setStatus(400);
        return;
      }
      try
      {
        AuthToken token = WASecurityManager.get().authenticate(username, password, clientId, "Bloom UI");
        
        Cookie cookie = new Cookie("token", token.getUUIDString());
        response.addCookie(cookie);
        
        response.setStatus(200);
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        
        JSONObject jo = new JSONObject();
        jo.put("token", token.getUUIDString());
        response.getWriter().write(jo.toString());
        
        return;
      }
      catch (Exception e)
      {
        response.setStatus(500);
        return;
      }
    }
    response.setStatus(404);
  }
}
