package java_cup;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sourceforge.czt.java_cup.ErrorManager;
import net.sourceforge.czt.java_cup.Lexer;
import net.sourceforge.czt.java_cup.action_production;
import net.sourceforge.czt.java_cup.emit;
import net.sourceforge.czt.java_cup.internal_error;
import net.sourceforge.czt.java_cup.lalr_state;
import net.sourceforge.czt.java_cup.non_terminal;
import net.sourceforge.czt.java_cup.parse_action_row;
import net.sourceforge.czt.java_cup.parse_action_table;
import net.sourceforge.czt.java_cup.parse_reduce_row;
import net.sourceforge.czt.java_cup.parse_reduce_table;
import net.sourceforge.czt.java_cup.parser;
import net.sourceforge.czt.java_cup.production;
import net.sourceforge.czt.java_cup.runtime.ComplexSymbolFactory;
import net.sourceforge.czt.java_cup.symbol;
import net.sourceforge.czt.java_cup.symbol_part;
import net.sourceforge.czt.java_cup.terminal;

public class ProduceDoc
{
  protected static boolean opt_dump_grammar = true;
  protected static BufferedInputStream input_file;
  protected static PrintWriter parser_class_file;
  protected static PrintWriter symbol_class_file;
  
  public static void main(String[] argv)
    throws internal_error, IOException, Exception
  {
    boolean did_output = false;
    
    terminal.clear();
    production.clear();
    action_production.clear();
    emit.clear();
    non_terminal.clear();
    parse_reduce_row.clear();
    parse_action_row.clear();
    lalr_state.clear();
    
    File f = new File("./src/main/java/com/bloom/runtime/compiler/Grammar.cup");
    if (!f.exists())
    {
      System.err.println("input file " + f.getCanonicalPath() + " does not exist");
      
      System.exit(99);
    }
    FileInputStream in = new FileInputStream(f);
    System.setIn(in);
    input_file = new BufferedInputStream(in);
    
    parse_grammar_spec();
    if (ErrorManager.getManager().getErrorCount() == 0)
    {
      check_unused();
      build_parser();
    }
    dump_grammar();
    if (ErrorManager.getManager().getErrorCount() != 0) {
      System.exit(100);
    }
    close_files();
  }
  
  protected static File dest_dir = null;
  protected static lalr_state start_state;
  protected static parse_action_table action_table;
  protected static parse_reduce_table reduce_table;
  
  protected static void close_files()
    throws IOException
  {
    if (input_file != null) {
      input_file.close();
    }
  }
  
  protected static void parse_grammar_spec()
    throws Exception
  {
    ComplexSymbolFactory csf = new ComplexSymbolFactory();
    parser parser_obj = new parser(new Lexer(csf), csf);
    try
    {
      parser_obj.parse();
    }
    catch (Exception e)
    {
      ErrorManager.getManager().emit_error("Internal error: Unexpected exception");
      
      throw e;
    }
  }
  
  protected static void check_unused()
  {
    for (Enumeration<?> t = terminal.all(); t.hasMoreElements();)
    {
      terminal term = (terminal)t.nextElement();
      if ((term != terminal.EOF) && 
      
        (term != terminal.error) && 
        
        (term.use_count() == 0)) {
        emit.unused_term += 1;
      }
    }
    for (Enumeration<?> n = non_terminal.all(); n.hasMoreElements();)
    {
      non_terminal nt = (non_terminal)n.nextElement();
      if (nt.use_count() == 0) {
        emit.unused_term += 1;
      }
    }
  }
  
  protected static void build_parser()
    throws internal_error
  {
    non_terminal.compute_nullability();
    
    non_terminal.compute_first_sets();
    
    start_state = lalr_state.build_machine(emit.start_production);
    
    action_table = new parse_action_table();
    reduce_table = new parse_reduce_table();
    for (Enumeration<?> st = lalr_state.all(); st.hasMoreElements();)
    {
      lalr_state lst = (lalr_state)st.nextElement();
      lst.build_table_entries(action_table, reduce_table);
    }
    action_table.check_reductions();
  }
  
  protected static String plural(int val)
  {
    if (val == 1) {
      return "";
    }
    return "s";
  }
  
  protected static String timestr(long time_val, long total_time)
  {
    long ms = 0L;
    long sec = 0L;
    
    boolean neg = time_val < 0L;
    if (neg) {
      time_val = -time_val;
    }
    ms = time_val % 1000L;
    sec = time_val / 1000L;
    String pad;
    if (sec < 10L)
    {
      pad = "   ";
    }
    else
    {
      if (sec < 100L)
      {
        pad = "  ";
      }
      else
      {
        if (sec < 1000L) {
          pad = " ";
        } else {
          pad = "";
        }
      }
    }
    long percent10 = time_val * 1000L / total_time;
    
    return (neg ? "-" : "") + pad + sec + "." + ms % 1000L / 100L + ms % 100L / 10L + ms % 10L + "sec" + " (" + percent10 / 10L + "." + percent10 % 10L + "%)";
  }
  
  static Map<String, List<production>> productionMap = new HashMap();
  static List<String> terminalList = new ArrayList();
  static Map<String, String> lookupMap = new HashMap();
  
  public static void dump_grammar()
    throws internal_error
  {
    for (String[] l : lookups) {
      lookupMap.put(l[0], l[1]);
    }
    for (int tidx = 0; tidx < terminal.number(); tidx++) {
      terminalList.add(terminal.find(tidx).name());
    }
    for (int pidx = 0; pidx < production.number(); pidx++)
    {
      production prod = production.find(pidx);
      String name = prod.lhs().the_symbol().name();
      List<production> ps = (List)productionMap.get(name);
      if (ps == null)
      {
        ps = new ArrayList();
        productionMap.put(name, ps);
      }
      ps.add(prod);
    }
    Set<String> done = new HashSet();
    
    System.out.println("<html>\n  <body>");
    outputRecursively("stmt", done);
    System.out.println("  </body>\n</html>");
  }
  
  static String[][] lookups = { { "$EQ", "=" }, { "$NOTEQ", "!=" }, { "$LT", "&lt;" }, { "$GT", "&gt;" }, { "$EQEQ", "==" }, { "$LTEQ", "&lt;=" }, { "$GTEQ", "&gt;=" }, { "$LTGT", "&lt;&gt;" }, { "$LSHIFT", "&lt;&lt;" }, { "$RSHIFT", "&gt;&gt;" }, { "$URSHIFT", "&gt;U&gt;" }, { "$BITOR", "|" }, { "$BITXOR", "~" }, { "$BITAND", "&amp;" }, { "$UNARY", "$" }, { "$PLUS", "+" }, { "$MINUS", "-" }, { "$MULT", "*" }, { "$DIV", "/" }, { "$MOD", "%" }, { "$TILDE", "~" }, { "$COMMA", "," }, { "$DOT", "." }, { "$SEMICOLON", ";" }, { "$COLON", ":" }, { "$LBRACE", "{" }, { "$RBRACE", "}" }, { "$LBRACK", "[" }, { "$RBRACK", "]" }, { "$LPAREN", "(" }, { "$RPAREN", ")" }, { "$SQ_STRING_LITERAL", "'{string}'" }, { "$DQ_STRING_LITERAL", "\"{string}\"" }, { "$IDENTIFIER", "{ident}" }, { "$INTEGER_LITERAL", "{int}" }, { "$LONG_LITERAL", "{long}" }, { "$FLOAT_LITERAL", "{float}" }, { "$DOUBLE_LITERAL", "{double}" }, { "ident", "{ident}" }, { "name", "{name}" } };
  
  public static boolean empty(production p)
    throws internal_error
  {
    boolean empty = true;
    for (int i = 0; i < p.rhs_length(); i++)
    {
      String name = ((symbol_part)p.rhs(i)).the_symbol().name();
      if (name.trim().length() > 0)
      {
        empty = false;
        break;
      }
    }
    return empty;
  }
  
  public static void outputRecursively(String what, Set<String> done)
    throws internal_error
  {
    if (!done.contains(what))
    {
      System.out.println("    <a id=" + what + "><h3>" + what + "</h3></a>");
      List<production> ps = (List)productionMap.get(what);
      done.add(what);
      System.out.println("    <ol>");
      for (production p : ps)
      {
        System.out.print("      <li>");
        if (empty(p)) {
          System.out.print(" <BLANK> ");
        } else {
          for (int i = 0; i < p.rhs_length(); i++)
          {
            String name = ((symbol_part)p.rhs(i)).the_symbol().name();
            boolean terminal = terminalList.contains(name);
            if (i > 0) {
              System.out.print(" ");
            }
            String val = (String)lookupMap.get(name);
            if (val != null) {
              System.out.print(val);
            } else if (terminal) {
              System.out.print(name);
            } else {
              System.out.print("<a href='#" + name + "'>" + name + "</a>");
            }
          }
        }
        System.out.println("</li>");
      }
      System.out.println("    </ol>");
      for (production p : ps) {
        if (!empty(p)) {
          for (int i = 0; i < p.rhs_length(); i++)
          {
            String name = ((symbol_part)p.rhs(i)).the_symbol().name();
            boolean terminal = terminalList.contains(name);
            if (!terminal) {
              outputRecursively(name, done);
            }
          }
        }
      }
    }
  }
}
