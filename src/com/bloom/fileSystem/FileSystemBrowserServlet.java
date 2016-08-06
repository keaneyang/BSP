package com.bloom.fileSystem;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;

public class FileSystemBrowserServlet
  extends HttpServlet
{
  private static final long serialVersionUID = 4849599346722979744L;
  private static Logger logger = Logger.getLogger(FileSystemBrowserServlet.class);
  FileSystemBrowser fileSystemBrowser;
  
  private static enum ResultWordsForGetRequest
  {
    path,  children,  hdfsurl;
    
    private ResultWordsForGetRequest() {}
  }
  
  public void init()
    throws ServletException
  {
    super.init();
  }
  
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException
  {
    String pathInFileSystem = request.getParameter(ResultWordsForGetRequest.path.name());
    
    String children = request.getParameter(ResultWordsForGetRequest.children.name());
    
    String hadoopUrl = request.getParameter(ResultWordsForGetRequest.hdfsurl.name());
    
    int responseStatus = 200;
    String responseData = null;
    try
    {
      if (hadoopUrl != null)
      {
        this.fileSystemBrowser = new HDFSBrowserImpl();
        this.fileSystemBrowser.init(hadoopUrl);
      }
      else
      {
        this.fileSystemBrowser = new FileSystemBrowserImpl();
        this.fileSystemBrowser.init(null);
      }
      if ((children == null) || (children.isEmpty()) || (children.equalsIgnoreCase("true")))
      {
        FileSystemBrowser.DirectoryDetails details = this.fileSystemBrowser.goToChildren(pathInFileSystem);
        if (details.isDirectory()) {
          responseData = details.getJsonString();
        }
      }
      else
      {
        responseData = this.fileSystemBrowser.goToParent(pathInFileSystem).getJsonString();
      }
    }
    catch (JSONException|IOException e)
    {
      responseStatus = 500;
      responseData = e.getMessage();
      logger.error(responseData);
    }
    catch (Exception e)
    {
      responseStatus = 500;
      logger.error(e.getMessage(), e);
    }
    response.setStatus(responseStatus);
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    try
    {
      if (responseData != null) {
        response.getWriter().write(responseData);
      } else {
        response.getWriter().write("Null result");
      }
    }
    catch (IOException e)
    {
      logger.error(e.getMessage());
    }
  }
}
