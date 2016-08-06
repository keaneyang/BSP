package com.bloom.jmqmessaging;

import com.bloom.uuid.AuthToken;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class AuthenticationResponse
  implements KryoSerializable, Serializable
{
  private static final long serialVersionUID = -6711064046717166349L;
  private boolean isAuthenticated;
  private AuthToken authToken;
  
  public AuthenticationResponse()
  {
    this.isAuthenticated = false;
  }
  
  public AuthenticationResponse(AuthToken token)
  {
    this.authToken = token;
    if (token != null) {
      this.isAuthenticated = true;
    } else {
      this.isAuthenticated = false;
    }
  }
  
  public AuthToken getAuthToken()
  {
    return this.authToken;
  }
  
  public boolean isAuthenticated()
  {
    return this.isAuthenticated;
  }
  
  public void write(Kryo kryo, Output output)
  {
    if (this.isAuthenticated)
    {
      output.writeBoolean(true);
      this.authToken.write(kryo, output);
    }
    else
    {
      output.writeBoolean(false);
    }
  }
  
  public void read(Kryo kryo, Input input)
  {
    this.isAuthenticated = input.readBoolean();
    if (this.isAuthenticated)
    {
      if (this.authToken == null) {
        this.authToken = new AuthToken();
      }
      this.authToken.read(kryo, input);
    }
  }
  
  public String toString()
  {
    return "AuthenticationResponse(" + (this.isAuthenticated ? this.authToken.toString() : "NonAuthenticated") + ")";
  }
  
  public boolean equals(Object o)
  {
    if ((o instanceof AuthenticationResponse))
    {
      AuthenticationResponse res = (AuthenticationResponse)o;
      boolean ret = this.isAuthenticated == res.isAuthenticated();
      if ((ret) && (this.isAuthenticated)) {
        return this.authToken.equals(res.getAuthToken());
      }
      return true;
    }
    return false;
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(this.isAuthenticated).append(this.authToken.toString()).toHashCode();
  }
}
