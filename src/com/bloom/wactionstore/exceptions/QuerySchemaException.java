package com.bloom.wactionstore.exceptions;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonschema.core.report.ProcessingMessage;
import java.util.ArrayList;
import java.util.List;

public class QuerySchemaException
  extends WActionStoreException
{
  private static final long serialVersionUID = -6132114902661917869L;
  private static final String MSG_EXC_MESSAGE = "WActionStore query schema violation: %s";
  public final String queryString;
  public final List<String> errorMessages = new ArrayList();
  
  public QuerySchemaException(JsonNode queryJson, Iterable<ProcessingMessage> report)
  {
    super(String.format("WActionStore query schema violation: %s", new Object[] { getErrorMessages(report).get(0) }));
    this.queryString = queryJson.toString();
    this.errorMessages.addAll(getErrorMessages(report));
  }
  
  public QuerySchemaException(JsonNode queryJson, String errorMessage)
  {
    super(String.format("WActionStore query schema violation: %s", new Object[] { errorMessage }));
    this.queryString = queryJson.toString();
    this.errorMessages.add(errorMessage);
  }
  
  private static List<String> getErrorMessages(Iterable<ProcessingMessage> processingReport)
  {
    List<String> result = new ArrayList();
    for (ProcessingMessage message : processingReport)
    {
      String messageText = message.getMessage();
      if (!messageText.contains("will be ignored")) {
        result.add(messageText);
      }
    }
    return result;
  }
}
