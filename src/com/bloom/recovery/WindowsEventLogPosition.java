package com.bloom.recovery;

import com.bloom.recovery.SourcePosition;

public class WindowsEventLogPosition
  extends SourcePosition
{
  private static final long serialVersionUID = -5432586375077517449L;
  private int eventRecordNumberOffset;
  
  public WindowsEventLogPosition(int offset)
  {
    this.eventRecordNumberOffset = offset;
  }
  
  public int getOffset()
  {
    return this.eventRecordNumberOffset;
  }
  
  public int compareTo(SourcePosition otherPosition)
  {
    if ((otherPosition != null) && ((otherPosition instanceof WindowsEventLogPosition)))
    {
      WindowsEventLogPosition otherWindowsPosition = (WindowsEventLogPosition)otherPosition;
      int otherOffset = otherWindowsPosition.getOffset();
      int cmp = this.eventRecordNumberOffset < otherOffset ? -1 : this.eventRecordNumberOffset > otherOffset ? 1 : 0;
      return cmp;
    }
    return 1;
  }
}
