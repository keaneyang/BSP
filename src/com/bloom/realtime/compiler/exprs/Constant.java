package com.bloom.runtime.compiler.exprs;

import com.bloom.runtime.Interval;
import com.bloom.runtime.compiler.CompilerUtils.NullType;
import com.bloom.runtime.compiler.visitors.ExpressionVisitor;
import java.sql.Timestamp;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.ISODateTimeFormat;

public class Constant
  extends ValueExpr
{
  private Class<?> valueType;
  public final Object value;
  
  public Constant(ExprCmd kind, Object val, Class<?> type)
  {
    super(kind);
    this.value = val;
    this.valueType = type;
  }
  
  public String toString()
  {
    return exprToString() + this.value;
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitConstant(this, params);
  }
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Constant)) {
      return false;
    }
    Constant o = (Constant)other;
    if ((this.value == null) && (o.value == null)) {
      return true;
    }
    return (super.equals(o)) && (this.value.equals(o.value));
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(this.value).append(super.hashCode()).toHashCode();
  }
  
  public static Constant newString(String val)
  {
    return new Constant(ExprCmd.STRING, val, String.class);
  }
  
  public static Constant newInt(int val)
  {
    return new Constant(ExprCmd.INT, Integer.valueOf(val), Integer.TYPE);
  }
  
  public static Constant newLong(Long val)
  {
    return new Constant(ExprCmd.LONG, val, Long.TYPE);
  }
  
  public static Constant newFloat(Float val)
  {
    return new Constant(ExprCmd.FLOAT, val, Float.TYPE);
  }
  
  public static Constant newDouble(Double val)
  {
    return new Constant(ExprCmd.DOUBLE, val, Double.TYPE);
  }
  
  public static Constant newBool(Boolean val)
  {
    return new Constant(ExprCmd.BOOL, val, Boolean.TYPE);
  }
  
  public static Constant newNull()
  {
    return new Constant(ExprCmd.NULL, null, CompilerUtils.NullType.class);
  }
  
  public static Constant newDate(String literal)
  {
    Timestamp t = Timestamp.valueOf(literal);
    return new Constant(ExprCmd.TIMESTAMP, t, Timestamp.class);
  }
  
  private static DateTimeFormatter makeDefaultFormatter()
  {
    DateTimeParser tp = ISODateTimeFormat.timeElementParser().getParser();
    DateTimeParser otp = new DateTimeFormatterBuilder().appendLiteral(' ').appendOptional(tp).toParser();
    
    DateTimeParser[] parsers = { new DateTimeFormatterBuilder().append(ISODateTimeFormat.dateElementParser().getParser()).appendOptional(otp).toParser(), new DateTimeFormatterBuilder().append(DateTimeFormat.forPattern("yyyy/MM/dd")).appendOptional(otp).toParser(), ISODateTimeFormat.dateOptionalTimeParser().getParser(), tp };
    
    DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(null, parsers).toFormatter();
    
    return formatter;
  }
  
  private static DateTimeFormatter defaultDateTimeFormatter = ;
  
  public static DateTime parseDateTime(String literal)
  {
    return DateTime.parse(literal, defaultDateTimeFormatter);
  }
  
  public static Constant newDateTime(String literal)
  {
    DateTime d = parseDateTime(literal);
    return new Constant(ExprCmd.DATETIME, d, DateTime.class);
  }
  
  public static Constant newYMInterval(Interval value)
  {
    return new Constant(ExprCmd.INTERVAL, Long.valueOf(value.value), Long.TYPE);
  }
  
  public static Constant newDSInterval(Interval value)
  {
    return new Constant(ExprCmd.INTERVAL, Long.valueOf(value.value), Long.TYPE);
  }
  
  public static Constant newClassRef(Class<?> value)
  {
    return new Constant(ExprCmd.CLASS, value, Class.class);
  }
  
  public Class<?> getType()
  {
    return this.valueType;
  }
  
  public boolean isConst()
  {
    return true;
  }
}
