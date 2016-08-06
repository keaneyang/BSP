 package com.bloom.Wizard;
 
 import com.fasterxml.jackson.core.JsonProcessingException;
 import com.fasterxml.jackson.databind.ObjectMapper;
 import com.bloom.event.ObjectMapperFactory;
 import org.apache.log4j.Logger;
 
 public class ValidationResult
 {
   public static final ObjectMapper mapper = ObjectMapperFactory.getInstance();
   private static Logger logger = Logger.getLogger(ValidationResult.class);
   
 
   public boolean result = true;
   public String message = null;
   
   public ValidationResult() {}
   
   public ValidationResult(boolean result, String message) {
     this.result = result;
     this.message = message;
   }
   
   public String toJson() {
     try {
       return mapper.writeValueAsString(this);
     } catch (JsonProcessingException jex) {
       logger.warn("error converting json " + jex.getMessage());
     }
     return "<error converting json>";
   }
 }

