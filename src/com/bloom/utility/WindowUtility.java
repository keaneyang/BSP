package com.bloom.utility;

import java.util.Map;

public class WindowUtility
{
  public static void validateIntervalPolicies(boolean isJumping, Map<String, Object> policy, Map<String, Object> slidePolicy)
  {
    boolean valid = (validateCountBasedPolicy(isJumping, policy, slidePolicy)) || (validateTimeBasedPolicy(isJumping, policy, slidePolicy)) || (validateAttributeBasedPolicy(isJumping, policy, slidePolicy)) || (validateCountTimeBasedPolicy(isJumping, policy, slidePolicy)) || (validateAttributeTimeBasedPolicy(isJumping, policy, slidePolicy));
    if (!valid) {
      throw new RuntimeException(prepareMessage(isJumping, policy, slidePolicy));
    }
  }
  
  public static boolean validateCountBasedPolicy(boolean isJumping, Map<String, Object> policy, Map<String, Object> slidePolicy)
  {
    if ((!isJumping) && (policy.get("count") != null) && (policy.get("time") == null) && (policy.get("onField") == null) && (policy.get("timeout") == null) && ((slidePolicy == null) || (slidePolicy.isEmpty()))) {
      return true;
    }
    if ((isJumping) && (policy.get("count") != null) && (policy.get("time") == null) && (policy.get("onField") == null) && (policy.get("timeout") == null) && ((slidePolicy == null) || (slidePolicy.isEmpty()))) {
      return true;
    }
    if ((!isJumping) && (policy.get("count") != null) && (policy.get("time") == null) && (policy.get("onField") == null) && (policy.get("timeout") == null) && (slidePolicy != null) && (slidePolicy.get("count") != null) && (slidePolicy.get("time") == null)) {
      return true;
    }
    return false;
  }
  
  public static boolean validateTimeBasedPolicy(boolean isJumping, Map<String, Object> policy, Map<String, Object> slidePolicy)
  {
    if ((!isJumping) && (policy.get("time") != null) && (policy.get("count") == null) && (policy.get("onField") == null) && (policy.get("timeout") == null) && ((slidePolicy == null) || (slidePolicy.isEmpty()))) {
      return true;
    }
    if ((isJumping) && (policy.get("time") != null) && (policy.get("count") == null) && (policy.get("onField") == null) && (policy.get("timeout") == null) && ((slidePolicy == null) || (slidePolicy.isEmpty()))) {
      return true;
    }
    if ((!isJumping) && (policy.get("time") != null) && (policy.get("count") == null) && (policy.get("onField") == null) && (policy.get("timeout") == null) && (slidePolicy != null) && (slidePolicy.get("time") != null) && (slidePolicy.get("count") == null)) {
      return true;
    }
    return false;
  }
  
  public static boolean validateAttributeBasedPolicy(boolean isJumping, Map<String, Object> policy, Map<String, Object> slidePolicy)
  {
    if ((!isJumping) && (policy.get("time") != null) && (policy.get("count") == null) && (policy.get("onField") != null) && (policy.get("timeout") == null) && ((slidePolicy == null) || (slidePolicy.isEmpty()))) {
      return true;
    }
    if ((isJumping) && (policy.get("time") != null) && (policy.get("count") == null) && (policy.get("onField") != null) && (policy.get("timeout") == null) && ((slidePolicy == null) || (slidePolicy.isEmpty()))) {
      return true;
    }
    if ((!isJumping) && (policy.get("time") != null) && (policy.get("count") == null) && (policy.get("onField") != null) && (policy.get("timeout") == null) && (slidePolicy != null) && (slidePolicy.get("time") != null) && (slidePolicy.get("count") == null)) {
      return true;
    }
    return false;
  }
  
  public static boolean validateCountTimeBasedPolicy(boolean isJumping, Map<String, Object> policy, Map<String, Object> slidePolicy)
  {
    if ((!isJumping) && (policy.get("count") != null) && (policy.get("timeout") != null) && (policy.get("onField") == null) && (policy.get("time") == null) && ((slidePolicy == null) || (slidePolicy.isEmpty()))) {
      return true;
    }
    if ((isJumping) && (policy.get("count") != null) && (policy.get("timeout") != null) && (policy.get("onField") == null) && (policy.get("time") == null) && ((slidePolicy == null) || (slidePolicy.isEmpty()))) {
      return true;
    }
    if ((!isJumping) && (policy.get("count") != null) && (policy.get("timeout") != null) && (policy.get("onField") == null) && (policy.get("time") == null) && (slidePolicy != null) && (slidePolicy.get("count") != null) && (slidePolicy.get("time") == null)) {
      return true;
    }
    return false;
  }
  
  public static boolean validateAttributeTimeBasedPolicy(boolean isJumping, Map<String, Object> policy, Map<String, Object> slidePolicy)
  {
    if ((!isJumping) && (policy.get("time") != null) && (policy.get("count") == null) && (policy.get("onField") != null) && (policy.get("timeout") != null) && ((slidePolicy == null) || (slidePolicy.isEmpty()))) {
      return true;
    }
    if ((isJumping) && (policy.get("time") != null) && (policy.get("count") == null) && (policy.get("onField") != null) && (policy.get("timeout") != null) && ((slidePolicy == null) || (slidePolicy.isEmpty()))) {
      return true;
    }
    if ((!isJumping) && (policy.get("time") != null) && (policy.get("count") == null) && (policy.get("onField") != null) && (policy.get("timeout") != null) && (slidePolicy != null) && (slidePolicy.get("time") != null) && (slidePolicy.get("count") == null)) {
      return true;
    }
    return false;
  }
  
  private static String prepareMessage(boolean isJumping, Map<String, Object> policy, Map<String, Object> slidePolicy)
  {
    StringBuilder builder = new StringBuilder("Invalid policy specified. ");
    builder.append("Jumping window: ").append(isJumping).append("\n");
    builder.append("Window policy: ").append(policy).append("\n");
    builder.append("Sliding policy: ").append(slidePolicy).append("\n");
    return builder.toString();
  }
}
