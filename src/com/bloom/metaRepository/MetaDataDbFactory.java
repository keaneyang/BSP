package com.bloom.metaRepository;

import org.apache.log4j.Logger;

public class MetaDataDbFactory
{
  public static final String DERBY = "derby";
  public static final String ORACLE = "oracle";
  private static Logger logger = Logger.getLogger(MetaDataDbFactory.class);
  private static MetaDataDbProvider ourMetaDataDbProvider = null;
  
  public static MetaDataDbProvider getOurMetaDataDb(String dataBaseName)
  {
    switch (dataBaseName)
    {
    case "derby": 
      if (ourMetaDataDbProvider == null) {
        ourMetaDataDbProvider = new DerbyMD();
      }
      break;
    case "oracle": 
      if (ourMetaDataDbProvider == null) {
        ourMetaDataDbProvider = new OracleMD();
      }
      break;
    }
    return ourMetaDataDbProvider;
  }
}
