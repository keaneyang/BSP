package com.bloom.runtime.window;

abstract class JumpingWindow
  extends BufWindow
{
  Long nextHead = null;
  
  JumpingWindow(BufferManager owner)
  {
    super(owner);
  }
}
