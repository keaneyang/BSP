package com.bloom.intf;

import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;

public abstract interface DashboardOperations
{
  public abstract String exportDashboard(AuthToken paramAuthToken, String paramString)
    throws Exception;
  
  public abstract UUID importDashboard(AuthToken paramAuthToken, Boolean paramBoolean, String paramString1, String paramString2)
    throws Exception;
  
  public abstract UUID importDashboard(AuthToken paramAuthToken, String paramString1, Boolean paramBoolean, String paramString2, String paramString3)
    throws Exception;
}
