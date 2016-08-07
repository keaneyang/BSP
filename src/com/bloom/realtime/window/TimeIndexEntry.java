package com.bloom.runtime.window;

class TimeIndexEntry
{
  final long createdTimestamp;
  int count;
  
  TimeIndexEntry(int count, long now)
  {
    this.count = count;
    this.createdTimestamp = now;
  }
}
