 package com.bloom.uuid;
 
 import java.io.IOException;
 

 
 public final class Hex
 {
   private static final char[] DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
   
 
 
   public static Appendable append(Appendable a, short in)
   {
     return append(a, in, 4);
   }
   
 
 
   public static Appendable append(Appendable a, short in, int length)
   {
     return append(a, in, length);
   }
   
 
 
   public static Appendable append(Appendable a, int in)
   {
     return append(a, in, 8);
   }
   
 
 
   public static Appendable append(Appendable a, int in, int length)
   {
     return append(a, in, length);
   }
   
  
   public static Appendable append(Appendable a, long in)
   {
     return append(a, in, 16);
   }
   
 
   public static Appendable append(Appendable a, long in, int length)
   {
     try
     {
       int lim = (length << 2) - 4;
       while (lim >= 0) {
         a.append(DIGITS[((byte)(int)(in >> lim) & 0xF)]);
         lim -= 4;
       }
     }
     catch (IOException ex) {}
     
 
     return a;
   }
   
 
   public static Appendable append(Appendable a, byte[] bytes)
   {
     try
     {
       for (byte b : bytes) {
         a.append(DIGITS[((byte)((b & 0xF0) >> 4))]);
         a.append(DIGITS[((byte)(b & 0xF))]);
       }
     }
     catch (IOException ex) {}
     
 
     return a;
   }
   
 
 
   public static long parseLong(CharSequence s)
   {
     long out = 0L;
     byte shifts = 0;
     
     for (int i = 0; (i < s.length()) && (shifts < 16); i++) {
       char c = s.charAt(i);
       if ((c > '/') && (c < ':')) {
         shifts = (byte)(shifts + 1);
         out <<= 4;
         out |= c - '0';
       }
       else if ((c > '@') && (c < 'G')) {
         shifts = (byte)(shifts + 1);
         out <<= 4;
         out |= c - '7';
       }
       else if ((c > '`') && (c < 'g')) {
         shifts = (byte)(shifts + 1);
         out <<= 4;
         out |= c - 'W';
       }
     }
     return out;
   }
   
 
 
 
 
 
 
 
 
 
   public static short parseShort(String s)
   {
     short out = 0;
     byte shifts = 0;
     
     for (int i = 0; (i < s.length()) && (shifts < 4); i++) {
       char c = s.charAt(i);
       if ((c > '/') && (c < ':')) {
         shifts = (byte)(shifts + 1);
         out = (short)(out << 4);
         out = (short)(out | c - '0');
       }
       else if ((c > '@') && (c < 'G')) {
         shifts = (byte)(shifts + 1);
         out = (short)(out << 4);
         out = (short)(out | c - '7');
       }
       else if ((c > '`') && (c < 'g')) {
         shifts = (byte)(shifts + 1);
         out = (short)(out << 4);
         out = (short)(out | c - 'W');
       }
     }
     return out;
   }
 }

