 package com.bloom.Wizard;
 
 public abstract interface ICDCWizard
 {
   public abstract String validateConnection(String paramString1, String paramString2, String paramString3);
   
   public abstract String checkPrivileges(String paramString1, String paramString2, String paramString3);
   
   public abstract String getTables(String paramString1, String paramString2, String paramString3, String paramString4) throws Exception;
   
   public static enum DBTYPE
   {
     LOGMINER,  XSTREAM,  MSSQL,  MYSQL,  ORACLE;
     
     private DBTYPE() {}
   }
   
   public abstract String getTableColumns(String paramString1, String paramString2, String paramString3, String paramString4)
     throws Exception;
   
   public abstract String checkCDCConfigurations(String paramString1, String paramString2, String paramString3);
   
   public abstract String checkVersion(String paramString1, String paramString2, String paramString3);
 }

