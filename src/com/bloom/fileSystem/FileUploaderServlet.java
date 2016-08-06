package com.bloom.fileSystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.ListIterator;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.bloom.runtime.NodeStartUp;

@WebServlet(name="FS")
public class FileUploaderServlet
  extends HttpServlet
{
  private final long serialVersionUID = -2045199313944348406L;
  private static Logger logger = Logger.getLogger(FileUploaderServlet.class);
  private final String DESTINATION_DIR_PATH = NodeStartUp.getPlatformHome() + "/UploadedFiles/";
  private int sizeThreshold = 102400;
  boolean writeToFile = true;
  
  public void init()
    throws ServletException
  {
    super.init();
    File destFile = new File(this.DESTINATION_DIR_PATH);
    if (!destFile.exists()) {
      if (!destFile.mkdir()) {
        logger.warn("Unable to create directory at: " + destFile.getAbsolutePath());
      }
    }
    getServletConfig().getServletContext().setAttribute("javax.servlet.context.tempdir", destFile);
  }
  
  public void doPost(HttpServletRequest request, HttpServletResponse response)
  {
    PrintWriter writer = null;
    InputStream is = null;
    FileOutputStream fos = null;
    try
    {
      writer = response.getWriter();
    }
    catch (IOException e)
    {
      logger.error(e);
    }
    boolean isMultiPart = ServletFileUpload.isMultipartContent(request);
    if (isMultiPart)
    {
      DiskFileItemFactory diskFileItemFactory = new DiskFileItemFactory();
      
      ServletContext servletContext = getServletConfig().getServletContext();
      File repository = (File)servletContext.getAttribute("javax.servlet.context.tempdir");
      diskFileItemFactory.setRepository(repository);
      
      diskFileItemFactory.setSizeThreshold(this.sizeThreshold);
      
      ServletFileUpload fileUpload = new ServletFileUpload(diskFileItemFactory);
      
      List items = null;
      try
      {
        items = fileUpload.parseRequest(request);
      }
      catch (FileUploadException e) {}
      String statusOfUpload = processUploadedFileItem(items);
      
      writer.print(statusOfUpload);
    }
    else
    {
      String fileName = request.getHeader("X-fileName");
      if (fileName == null) {
        fileName = "myFile";
      }
      try
      {
        String fqp = this.DESTINATION_DIR_PATH + fileName;
        is = request.getInputStream();
        fos = new FileOutputStream(new File(fqp));
        IOUtils.copy(is, fos);
        if (logger.isDebugEnabled()) {
          logger.debug("Input Stream: " + is);
        }
        response.setStatus(200);
        response.addHeader("Access-Control-Allow-Origin", "*");
        writer.print(fqp);
        try
        {
          fos.close();
          is.close();
        }
        catch (IOException ignored) {}
        writer.flush();
      }
      catch (FileNotFoundException ex)
      {
        response.setStatus(500);
        writer.print("{success: false}");
      }
      catch (IOException ex)
      {
        response.setStatus(500);
        writer.print("{success: false}");
      }
      finally
      {
        try
        {
          fos.close();
          is.close();
        }
        catch (IOException ignored) {}
      }
      writer.close();
    }
  }
  
  private String processUploadedFileItem(List items)
  {
    ListIterator listIterator = items.listIterator();
    String status = "{success:false}";
    while (listIterator.hasNext())
    {
      FileItem fileItem = (FileItem)listIterator.next();
      if (fileItem.isFormField()) {
        status = processFormField(fileItem);
      } else {
        status = processUploadedFile(fileItem);
      }
    }
    return status;
  }
  
  private String processFormField(FileItem fileItem)
  {
    return "{success:false}";
  }
  
  private String processUploadedFile(FileItem item)
  {
    String itemName = item.getName();
    String statusOfProcessing = "{success:false}";
    String path = this.DESTINATION_DIR_PATH + itemName;
    if (this.writeToFile) {
      try
      {
        item.write(new File(path));
        statusOfProcessing = path;
      }
      catch (Exception e)
      {
        logger.error(e);
        statusOfProcessing = "{success:false}";
      }
    }
    return statusOfProcessing;
  }
  
  public String getServletInfo()
  {
    return "Handles file upload data from multipart/form-data.";
  }
}
