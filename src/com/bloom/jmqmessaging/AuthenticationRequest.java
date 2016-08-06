package com.bloom.jmqmessaging;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.Serializable;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class AuthenticationRequest
  implements KryoSerializable, Serializable
{
  private static final long serialVersionUID = -8399218291499574416L;
  private String username;
  private String password;
  
  public AuthenticationRequest() {}
  
  public AuthenticationRequest(String user, String pwd)
  {
    this.username = user;
    this.password = pwd;
  }
  
  public String getUsername()
  {
    return this.username;
  }
  
  public String getPassword()
  {
    return this.password;
  }
  
  public void write(Kryo kryo, Output output)
  {
    if (this.username != null)
    {
      output.writeByte(0);
      output.writeString(this.username);
    }
    else
    {
      output.writeByte(1);
    }
    if (this.password != null)
    {
      output.writeByte(0);
      output.writeString(this.password);
    }
    else
    {
      output.writeByte(1);
    }
  }
  
  public void read(Kryo kryo, Input input)
  {
    byte hasUsername = input.readByte();
    if (hasUsername == 0) {
      this.username = input.readString();
    } else {
      this.username = null;
    }
    byte hasPassword = input.readByte();
    if (hasPassword == 0) {
      this.password = input.readString();
    } else {
      this.password = null;
    }
  }
  
  public String toString()
  {
    return "AuthenticationRequest(" + this.username + ":" + this.password + ")";
  }
  
  public boolean equals(Object o)
  {
    if ((o instanceof AuthenticationRequest))
    {
      AuthenticationRequest req = (AuthenticationRequest)o;
      return (this.username.equals(req.getUsername())) && (this.password.equals(req.getPassword()));
    }
    return false;
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(this.username).append(this.password).toHashCode();
  }
}
