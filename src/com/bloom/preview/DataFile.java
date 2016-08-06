package com.bloom.preview;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class DataFile
{
  long size;
  String name;
  String fqPath;
  String type;
  
  public JSONObject toJson()
    throws JSONException
  {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("Size", this.size);
    jsonObject.put("Name", this.name);
    jsonObject.put("Path", this.fqPath);
    jsonObject.put("Type", this.type);
    return jsonObject;
  }
  
  public String toString()
  {
    return "FQP of " + this.name + " is " + this.fqPath + " with a size: " + this.size;
  }
}
