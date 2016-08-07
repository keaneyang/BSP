package com.bloom.runtime.deployment;

public class Constant
{
  public static String REMOTE_CALL_MSG = "%s call on object %s received with %d parameters from client with session ID %s";
  public static String DEPLOY_START_MSG = "\nDEPLOY APPLICATION %s; \nSTART APPLICATION %s;";
  public static String ALREAD_LOADED_MSG = "%s %s failed, it is already loaded.";
  public static String ALREAD_UNLOADED_MSG = "%s %s failed, it is already unloaded.";
  public static String OPERATION_NOT_SUPPORTED = "%s operation not supported.";
  public static String LOAD_FAILED_WITH_NO_IMPLICIT_APP = "There was a problem in creating implicit application for object: %s";
}
