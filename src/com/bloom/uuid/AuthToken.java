 package com.bloom.uuid;

import com.bloom.uuid.UUID;

public class AuthToken
   extends UUID
 {
   private static final long serialVersionUID = 4392763777100817312L;
   
 
 
 
 
 
   public AuthToken()
   {
     this(System.currentTimeMillis());
   }
   
 
 
   public AuthToken(long currentTimeMillis)
   {
     super(currentTimeMillis);
   }
   
 
 
 
   public AuthToken(long time, long clockSeqAndNode)
   {
     super(time, clockSeqAndNode);
   }
   
 
 
   public AuthToken(UUID u)
   {
     super(u);
   }
   
 
 
   public AuthToken(String uidstr)
   {
     super(uidstr);
   }
   
 
 
   public AuthToken(CharSequence s)
   {
     super(s);
   }
 }

