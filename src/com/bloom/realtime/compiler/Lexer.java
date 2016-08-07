package com.bloom.runtime.compiler;

import com.bloom.exception.CompilationException;
import com.bloom.runtime.Pair;
import com.bloom.runtime.compiler.exprs.Expr;
import com.bloom.runtime.exceptions.UnterminatedTokenException;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import net.sourceforge.czt.java_cup.runtime.DefaultSymbolFactory;
import net.sourceforge.czt.java_cup.runtime.Scanner;
import net.sourceforge.czt.java_cup.runtime.Symbol;
import net.sourceforge.czt.java_cup.runtime.SymbolFactory;
import org.apache.log4j.Logger;

public class Lexer
  implements Scanner
{
  private static Logger logger;
  private final Reader reader;
  private StringBuilder buf = new StringBuilder();
  private Symbol last = null;
  private FIFO<Symbol> lookahead = null;
  private int pos = 0;
  private int lastBeginPos = 0;
  private Map<Object, Symbol> index = new IdentityHashMap();
  private boolean acceptNewLineInStringLiteral = true;
  private static Map<String, Integer> keywordsMap;
  
  private Symbol add2index(Symbol sym)
  {
    return add2index(sym, false);
  }
  
  private Symbol add2indexLeaf(Symbol sym)
  {
    return add2index(sym, true);
  }
  
  private Symbol add2index(Symbol sym, boolean mustbeunique)
  {
    if (sym.value != null) {
      if (this.index.containsKey(sym.value))
      {
        if (mustbeunique) {
          throw new CompilationException("non-unique object in the stream!!!");
        }
      }
      else {
        this.index.put(sym.value, sym);
      }
    }
    return sym;
  }
  
  public class LexerSymbolFactory
    extends DefaultSymbolFactory
  {
    public LexerSymbolFactory() {}
    
    public Symbol newSymbol(String name, int id, Symbol left, Symbol right, Object value)
    {
      return Lexer.this.add2index(new Symbol(id, left, right, value));
    }
    
    public Symbol newSymbol(String name, int id, Symbol left, Symbol right)
    {
      return Lexer.this.add2index(new Symbol(id, left, right));
    }
    
    public Symbol newSymbol(String name, int id, Object value)
    {
      return Lexer.this.add2index(new Symbol(id, value));
    }
    
    public Symbol newSymbol(String name, int id)
    {
      return Lexer.this.add2index(new Symbol(id));
    }
    
    public Symbol startSymbol(String name, int id, int state)
    {
      Symbol s = new Symbol(id);
      s.parse_state = state;
      return Lexer.this.add2index(s);
    }
  }
  
  public Lexer(String src)
  {
    this(new StringReader(src));
  }
  
  public Lexer(Reader reader)
  {
    this.reader = reader;
  }
  
  public void setAcceptNewLineInStringLiteral(boolean yes)
  {
    this.acceptNewLineInStringLiteral = yes;
  }
  
  public SymbolFactory getSymbolFactory()
  {
    return new LexerSymbolFactory();
  }
  
  public Pair<Integer, Integer> consumeAll()
  {
    Pair<Integer, Integer> ret = Pair.make(Integer.valueOf(this.pos), Integer.valueOf(this.buf.length()));
    this.pos = this.buf.length();
    return ret;
  }
  
  private int peek(int index)
    throws IOException
  {
    int p = this.pos + index;
    int blen = this.buf.length();
    if (p >= blen)
    {
      for (;;)
      {
        int c = this.reader.read();
        if (c == -1) {
          break;
        }
        blen = this.buf.append((char)c).length();
        if ((p < blen) && (c == 10)) {
          break;
        }
      }
      if (p >= blen) {
        return -1;
      }
    }
    return this.buf.charAt(p);
  }
  
  private int peek()
    throws IOException
  {
    return peek(0);
  }
  
  private void consume(int k)
  {
    this.pos += k;
    assert (this.pos <= this.buf.length());
  }
  
  private void consume()
    throws IOException
  {
    consume(1);
  }
  
  public Symbol nextToken()
    throws IOException
  {
    Symbol sym = this.lookahead == null ? _nextToken() : (Symbol)this.lookahead.get();
    this.last = sym;
    return this.last;
  }
  
  private Symbol _nextToken()
    throws IOException
  {
    Symbol sym = getNextToken();
    sym.left = this.lastBeginPos;
    sym.right = this.pos;
    return sym;
  }
  
  public boolean debug_lex()
    throws IOException
  {
    Symbol sym = nextToken();
    System.out.println(sym);
    if (sym.value != null) {
      System.out.println(sym.value.toString());
    }
    return sym.sym != 0;
  }
  
  private Symbol getNextToken()
    throws IOException
  {
    int c;
    for (;;)
    {
      this.lastBeginPos = this.pos;
      c = peek();
      switch (c)
      {
      case -1: 
        return newSymbol(0, "EOF");
      case 9: 
      case 10: 
      case 12: 
      case 13: 
      case 32: 
        consume();
        break;
      case 26: 
        consume();
        return newSymbol(0, "EOF_CTRL_Z");
      case 45: 
        if (peek(1) == 45)
        {
          consume(2);
          singleLineComment();
        }
        break;
      case 47: 
        if (peek(1) != 42) {
          break label167;
        }
        consume(2);
        consumeMultiLineComment();
      }
    }
    label167:
    return getToken(c);
    return getToken(c);
  }
  
  private void consumeMultiLineComment()
    throws IOException
  {
    for (;;)
    {
      switch (peek())
      {
      case -1: 
        throw unterminatedTokenError("comment", "/*");
      case 42: 
        if (peek(1) == 47) {
          consume(2);
        }
        break;
      }
      consume();
    }
  }
  
  private void singleLineComment()
    throws IOException
  {
    for (;;)
    {
      switch (peek())
      {
      case -1: 
      case 10: 
        break;
      default: 
        consume();
      }
    }
  }
  
  private Symbol getToken(int c)
    throws IOException
  {
    switch (c)
    {
    case 64: 
      consume();return getFileName(64, 214, "string literal", ';');
    case 40: 
      consume();return newSymbol(199, "(");
    case 41: 
      consume();return newSymbol(200, ")");
    case 123: 
      consume();return newSymbol(195, "{");
    case 125: 
      consume();return newSymbol(196, "}");
    case 91: 
      consume();return newSymbol(197, "[");
    case 93: 
      consume();return newSymbol(198, "]");
    case 59: 
      consume();return newSymbol(192, ";");
    case 44: 
      consume();return newSymbol(194, ",");
    case 126: 
      consume();return newSymbol(188, "~");
    case 35: 
      consume();return newSymbol(190, "#");
    case 63: 
      consume();return newSymbol(189, "?");
    case 58: 
      consume();return newSymbol(193, ":");
    case 38: 
      consume();return newSymbol(181, "&");
    case 124: 
      consume();return newSymbol(179, "|");
    case 43: 
      consume();return newSymbol(183, "+");
    case 45: 
      consume();return newSymbol(184, "-");
    case 42: 
      consume();return newSymbol(185, "*");
    case 47: 
      consume();return newSymbol(186, "/");
    case 94: 
      consume();return newSymbol(180, "^");
    case 37: 
      consume();return newSymbol(187, "%");
    case 33: 
      if (peek(1) == 61)
      {
        consume(2);
        return newSymbol(169, "!=");
      }
      break;
    case 61: 
      consume();
      if (peek() == 61)
      {
        consume();
        return newSymbol(172, "==");
      }
      return newSymbol(168, "=");
    case 62: 
      consume();
      int c2 = peek();
      switch (c2)
      {
      case 61: 
        consume();
        return newSymbol(174, ">=");
      case 62: 
        consume();
        if (peek() == 62)
        {
          consume();
          return newSymbol(178, ">>>");
        }
        return newSymbol(177, ">>");
      }
      return newSymbol(171, ">");
    case 60: 
      consume();
      int c2 = peek();
      switch (c2)
      {
      case 61: 
        consume();
        return newSymbol(173, "<=");
      case 60: 
        consume();
        return newSymbol(176, "<<");
      case 62: 
        consume();
        return newSymbol(175, "<>");
      }
      return newSymbol(170, "<");
    case 39: 
      return getStringLiteral(c, 211, "string literal", "'");
    case 34: 
      return getStringLiteral(c, 212, "string literal", "\"");
    case 46: 
      if (Character.digit(peek(1), 10) != -1)
      {
        Symbol num = getNumericLiteral(c);
        return num;
      }
      consume();
      return newSymbol(191, "DOT");
    }
    if (Character.isJavaIdentifierStart(c)) {
      return getIdentifier(c);
    }
    if (Character.isDigit(c))
    {
      Symbol num = getNumericLiteral(c);
      return num;
    }
    throw scanError("Illegal character <" + (char)c + "> code [" + c + "]");
  }
  
  static
  {
    logger = Logger.getLogger(Lexer.class);
    
    keywordsMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    for (Field f : Sym.class.getDeclaredFields()) {
      try
      {
        String name = f.getName();
        if (!name.startsWith("$")) {
          keywordsMap.put(name, Integer.valueOf(f.getInt(null)));
        }
      }
      catch (IllegalArgumentException|IllegalAccessException e)
      {
        logger.error(e);
      }
    }
  }
  
  private Symbol getIdentifier(int c)
    throws IOException
  {
    StringBuffer sb = new StringBuffer().append((char)c);
    consume();
    for (;;)
    {
      c = peek();
      if (!Character.isJavaIdentifierPart(c)) {
        break;
      }
      sb.append((char)c);
      consume();
    }
    String s = sb.toString();
    Integer index = (Integer)keywordsMap.get(s);
    if (index == null) {
      return add2indexLeaf(new Symbol(213, s));
    }
    return newSymbol(index.intValue(), s);
  }
  
  private Symbol getNumericLiteral(int c)
    throws IOException
  {
    if (c == 48)
    {
      int c2 = peek(1);
      if (".eEfFdD".indexOf(c2) == -1)
      {
        if ((c2 == 120) || (c2 == 88))
        {
          consume(2);
          
          String num = getDigits(16);
          return getIntegerLiteral(num, 16);
        }
        if ((c2 == 98) || (c2 == 66))
        {
          consume(2);
          
          String num = getDigits(2);
          return getIntegerLiteral(num, 2);
        }
        String num = getDigits(8);
        return getIntegerLiteral(num, 8);
      }
    }
    StringBuffer sb = new StringBuffer(getDigits(10));
    boolean hasDot = false;
    c = peek();
    if (c == 46)
    {
      hasDot = true;
      consume();
      sb.append((char)c).append(getDigits(10));
    }
    boolean hasExp = false;
    c = peek();
    if ((c == 101) || (c == 69))
    {
      hasExp = true;
      consume();
      sb.append((char)c);
      c = peek();
      if ((c == 43) || (c == 45))
      {
        consume();
        sb.append((char)c);
      }
      sb.append(getDigits(10));
    }
    String num = sb.toString();
    try
    {
      switch (peek())
      {
      case 70: 
      case 102: 
        consume();
        Float f = new Float(Float.parseFloat(num));
        return add2indexLeaf(new Symbol(217, f));
      case 68: 
      case 100: 
        consume();
        Double d = new Double(Double.parseDouble(num));
        return add2indexLeaf(new Symbol(218, d));
      }
      if ((hasDot) || (hasExp))
      {
        Double d = new Double(Double.parseDouble(num));
        return add2indexLeaf(new Symbol(218, d));
      }
      return getIntegerLiteral(num, 10);
    }
    catch (NumberFormatException e)
    {
      throw scanError("Illegal floating-point number");
    }
  }
  
  private Symbol getIntegerLiteral(String num, int radix)
    throws IOException
  {
    int c = peek();
    if ((c == 108) || (c == 76))
    {
      consume();
      try
      {
        Long l = new Long(Long.parseLong(num, radix));
        return add2indexLeaf(new Symbol(216, l));
      }
      catch (NumberFormatException e)
      {
        throw scanError("Too big long constant");
      }
    }
    try
    {
      Integer i = new Integer(Integer.parseInt(num, radix));
      return add2indexLeaf(new Symbol(215, i));
    }
    catch (NumberFormatException e)
    {
      throw scanError("Too big integer constant");
    }
  }
  
  private String getDigits(int radix)
    throws IOException
  {
    StringBuffer sb = new StringBuffer();
    for (;;)
    {
      int c = peek();
      if ((c == -1) || (Character.digit(c, radix) == -1)) {
        break;
      }
      consume();
      sb.append((char)c);
    }
    return sb.toString();
  }
  
  private Symbol getFileName(int openquote, int kind, String what, char term)
    throws IOException
  {
    StringBuffer val = new StringBuffer();
    for (;;)
    {
      int c = peek();
      if (term == c) {
        break;
      }
      if (c == openquote)
      {
        consume();
        break;
      }
      switch (c)
      {
      case -1: 
        throw unterminatedTokenError(what, "" + term);
      case 92: 
        consume();
        val.append(getEscapeSequence());
        break;
      default: 
        consume();
        val.append((char)c);
      }
    }
    return add2indexLeaf(new Symbol(kind, val.toString()));
  }
  
  private Symbol getTripleQuoteStringLiteral(int openquote, int kind, String what, String term)
    throws IOException
  {
    StringBuffer val = new StringBuffer();
    boolean firstLine = true;
    for (;;)
    {
      int c = peek();
      if ((c == openquote) && (peek(1) == openquote) && (peek(2) == openquote))
      {
        consume(3);
        break;
      }
      if (c == -1) {
        throw unterminatedTokenError(what, term);
      }
      consume();
      val.append((char)c);
      if ((firstLine) && (c == 10))
      {
        firstLine = false;
        if (val.toString().trim().isEmpty()) {
          val.setLength(0);
        }
      }
    }
    return add2indexLeaf(new Symbol(kind, val.toString()));
  }
  
  private Symbol getStringLiteral(int openquote, int kind, String what, String term)
    throws IOException
  {
    consume();
    if ((peek() == openquote) && (peek(1) == openquote))
    {
      consume(2);
      return getTripleQuoteStringLiteral(openquote, kind, what, term);
    }
    StringBuffer val = new StringBuffer();
    for (;;)
    {
      int c = peek();
      if (c == openquote)
      {
        consume();
        break;
      }
      switch (c)
      {
      case -1: 
        throw unterminatedTokenError(what, term);
      case 92: 
        consume();
        val.append(getEscapeSequence());
        break;
      case 10: 
        if (!this.acceptNewLineInStringLiteral) {
          throw scanError("Invalid " + what);
        }
        break;
      }
      consume();
      val.append((char)c);
    }
    return add2indexLeaf(new Symbol(kind, val.toString()));
  }
  
  private char getEscapeSequence()
    throws IOException
  {
    int c = peek();
    switch (c)
    {
    case 98: 
      consume();
      return '\b';
    case 116: 
      consume();
      return '\t';
    case 110: 
      consume();
      return '\n';
    case 102: 
      consume();
      return '\f';
    case 114: 
      consume();
      return '\r';
    case 34: 
      consume();
      return '"';
    case 39: 
      consume();
      return '\'';
    case 92: 
      consume();
      return '\\';
    case 48: 
    case 49: 
    case 50: 
    case 51: 
    case 52: 
    case 53: 
    case 54: 
    case 55: 
      consume();
      return (char)getOctal(c);
    }
    throw scanError("Invalid escape sequence");
  }
  
  private int getOctal(int first_digit)
    throws IOException
  {
    int val = Character.digit(first_digit, 8);
    for (;;)
    {
      int c = peek();
      int d;
      if ((c == -1) || ((d = Character.digit(c, 8)) == -1)) {
        break;
      }
      consume();
      val = (val << 3) + d;
      if (val > 255) {
        throw scanError("Invalid octal escape sequence");
      }
    }
    return val;
  }
  
  static class FIFO<T>
  {
    private final LinkedList<T> backing;
    private final Getter<T> getter;
    
    public FIFO(Getter<T> getter)
    {
      this.backing = new LinkedList();
      this.getter = getter;
    }
    
    public void put(T o)
    {
      this.backing.add(o);
    }
    
    public T get()
      throws IOException
    {
      if (this.backing.isEmpty()) {
        put(this.getter.next());
      }
      return (T)this.backing.remove();
    }
    
    public T peek(int i)
      throws IOException
    {
      while (i >= this.backing.size()) {
        put(this.getter.next());
      }
      return (T)this.backing.get(i);
    }
    
    static abstract class Getter<T>
    {
      abstract T next()
        throws IOException;
    }
  }
  
  private Symbol newSymbol(int id, String sym)
  {
    return new Symbol(id, sym);
  }
  
  public Symbol next_token()
    throws IOException
  {
    return nextToken();
  }
  
  private int getLineNumber(int pos)
  {
    int buflen = this.buf.length();
    int p = pos >= buflen ? buflen : pos;
    int newlinecount = 0;
    char[] chars = new char[p];
    this.buf.getChars(0, p, chars, 0);
    for (int i = 0; i < p; i++) {
      if (chars[i] == '\n') {
        newlinecount++;
      }
    }
    return newlinecount + 1;
  }
  
  private int indexOfNewLineBeforeBegin(int begin)
  {
    if (begin == 0) {
      return 0;
    }
    int newlinepos1 = this.buf.lastIndexOf("\n", begin - 1);
    int newlinepos2 = this.buf.lastIndexOf("\r", begin - 1);
    int newlinepos = Math.max(newlinepos1, newlinepos2);
    return newlinepos + 1;
  }
  
  private int indexOfNewLineAfterEnd(int end)
  {
    int newlinepos1 = this.buf.indexOf("\n", end);
    int newlinepos2 = this.buf.indexOf("\r", end);
    int newline = Math.max(newlinepos1, newlinepos2);
    if (newline == -1) {
      return this.buf.length();
    }
    return newline;
  }
  
  private String makeErrorText(int begin, int end)
  {
    int b = indexOfNewLineBeforeBegin(begin);
    int e = indexOfNewLineAfterEnd(end);
    int errorlinenum = getLineNumber(begin);
    String linenum = "line:" + errorlinenum + " ";
    String lineWithError = this.buf.substring(b, e);
    StringBuilder text = new StringBuilder(linenum + lineWithError).append('\n');
    for (int i = 0; i < linenum.length(); i++) {
      text.append(' ');
    }
    for (; b < begin; b++) {
      text.append(' ');
    }
    for (; b < end; b++) {
      text.append('^');
    }
    if (begin == end) {
      text.append('^');
    }
    text.append('\n');
    return text.toString();
  }
  
  private CompilationException unterminatedTokenError(String s, String term)
  {
    int e = this.lastBeginPos == this.pos ? this.buf.length() : this.pos;
    return new UnterminatedTokenException("Unterminated " + s + ":\n" + makeErrorText(this.lastBeginPos, e), term);
  }
  
  private CompilationException scanError(String s)
  {
    int e = this.lastBeginPos == this.pos ? this.buf.length() : this.pos;
    return new CompilationException(s + ":\n" + makeErrorText(this.lastBeginPos, e));
  }
  
  public void report_error(String message, Object obj)
  {
    if ((obj instanceof Symbol))
    {
      Symbol s = (Symbol)obj;
      String text = s.left != -1 ? makeErrorText(s.left, s.right) : "";
      throw new CompilationException(message + " at:\n" + text);
    }
    throw new CompilationException(message);
  }
  
  public Symbol getSymbol(Object obj)
  {
    Symbol sym = (Symbol)this.index.get(obj);
    while ((sym == null) && ((obj instanceof Expr)))
    {
      Expr e = (Expr)obj;
      obj = e.originalExpr;
      if (obj == null) {
        break;
      }
      sym = (Symbol)this.index.get(obj);
    }
    return sym;
  }
  
  public String getExprText(Object obj)
  {
    Symbol s = getSymbol(obj);
    if (s == null) {
      return null;
    }
    try
    {
      return this.buf.substring(s.left, s.right);
    }
    catch (StringIndexOutOfBoundsException e) {}
    return "";
  }
  
  public void parseError(String message, Object obj)
  {
    if (obj == null) {
      throw new CompilationException(message);
    }
    Symbol s = getSymbol(obj);
    String text = s != null ? makeErrorText(s.left, s.right) : "(null)";
    throw new CompilationException(message + ":\n" + text);
  }
  
  public String getBufSubstring(int beg, int end)
  {
    return this.buf.substring(beg, end);
  }
  
  public void syntaxError(Grammar parser, Symbol sym)
  {
    if ((sym.left == sym.right) && (sym.left >= this.buf.length()))
    {
      int p = this.buf.length();
      while (p > 0)
      {
        char c = this.buf.charAt(p - 1);
        if (!Character.isWhitespace(c)) {
          break;
        }
        p--;
      }
      Symbol s = new Symbol(sym.sym, p, p, sym.value);
      report_error("Syntax error", s);
    }
    else
    {
      report_error("Syntax error", sym);
    }
  }
  
  public static void process(String text)
  {
    Lexer l = new Lexer(new StringReader(text));
    try
    {
      while (l.debug_lex()) {}
    }
    catch (Throwable e)
    {
      logger.error(e);
    }
  }
  
  public static boolean isKeyword(String s)
  {
    return keywordsMap.containsKey(s);
  }
  
  public static void main(String[] args)
    throws IOException
  {
    Lexer l = new Lexer(new StringReader("123456789\n00000000\n"));
    l.peek(37);
    process("aaaa bbbb\ncccc\nfjdhjgf !!!! fhgdjfg\n");
    process("catch if else break\n");
    process("aaaa 11123456789012345678912345666 skuzi\n");
  }
  
  public void scanErrorMsg(String msg, Symbol info) {}
}
