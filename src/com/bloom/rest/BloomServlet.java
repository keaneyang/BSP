package com.bloom.rest;

import com.bloom.exception.ExpiredSessionException;
import com.bloom.exception.InvalidUriException;
import com.bloom.uuid.AuthToken;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;

public class BloomServlet
  extends HttpServlet
{
  protected AuthToken getToken(HttpServletRequest request)
  {
    AuthToken token = null;
    String rawToken = request.getParameter("token");
    if (rawToken == null)
    {
      Cookie[] cookies = request.getCookies();
      for (Cookie cookie : cookies) {
        if (cookie.getName().equals("token")) {
          rawToken = cookie.getValue();
        }
      }
    }
    try
    {
      if (rawToken != null) {
        token = new AuthToken(rawToken);
      } else {
        throw new InvalidUriException("The URI is invalid, please provide an authentication token with the request\nTo get an authentication token do a POST Request as curl -X POST -d'username=Bloom user name&password=Bloom password' http://IP address of a Bloom Server:port/security/authenticate from terminal");
      }
    }
    catch (ExpiredSessionException e) {}
    return token;
  }
}
