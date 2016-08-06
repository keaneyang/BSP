package com.bloom.distribution;

import com.bloom.metaRepository.HazelcastSingleton;
import com.esotericsoftware.kryo.ClassResolver;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.Util;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.io.PrintStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class ReloadableClassResolver
  implements ClassResolver
{
  public static final byte NAME = -1;
  protected Kryo kryo = null;
  protected Map<Integer, Registration> idToRegistration = new ConcurrentHashMap();
  protected Map<Class<?>, Registration> classToRegistration = new ConcurrentHashMap();
  protected Map<String, Integer> classNameToId = new ConcurrentHashMap();
  protected Map<String, Class<?>> nameToClass = new ConcurrentHashMap();
  private Registration[] memoizedClassIdArray = new Registration[65536];
  
  public void setKryo(Kryo kryo)
  {
    if (this.kryo == null) {
      this.kryo = kryo;
    }
  }
  
  public void removeClassRegistration(Class<?> clazz)
  {
    Registration r = (Registration)this.classToRegistration.get(clazz);
    if (r != null) {
      this.classToRegistration.remove(clazz);
    }
    if (r != null)
    {
      if (r.getId() != -1)
      {
        Registration rId = (Registration)this.idToRegistration.get(Integer.valueOf(r.getId()));
        if (rId != null) {
          this.idToRegistration.remove(Integer.valueOf(r.getId()));
        }
      }
      if ((r.getId() >= 0) && (r.getId() < this.memoizedClassIdArray.length)) {
        this.memoizedClassIdArray[r.getId()] = null;
      }
    }
    Class<?> clazzName = (Class)this.nameToClass.get(Util.className(clazz));
    if (clazzName != null) {
      this.nameToClass.remove(Util.className(clazz));
    }
  }
  
  public Registration register(Registration registration)
  {
    if (registration == null) {
      throw new IllegalArgumentException("registration cannot be null.");
    }
    this.classToRegistration.put(registration.getType(), registration);
    if (registration.getId() != -1) {
      this.idToRegistration.put(Integer.valueOf(registration.getId()), registration);
    }
    this.classNameToId.put(Util.className(registration.getType()), Integer.valueOf(registration.getId()));
    if (registration.getType().isPrimitive()) {
      this.classToRegistration.put(Util.getWrapperClass(registration.getType()), registration);
    }
    return registration;
  }
  
  public Registration registerImplicit(Class type)
  {
    return register(new Registration(type, this.kryo.getDefaultSerializer(type), -1));
  }
  
  public Registration getRegistration(Class type)
  {
    return (Registration)this.classToRegistration.get(type);
  }
  
  public Registration getRegistration(int classID)
  {
    return (Registration)this.idToRegistration.get(Integer.valueOf(classID));
  }
  
  public Registration writeClass(Output output, Class type)
  {
    if (type == null)
    {
      output.writeByte((byte)0);
      return null;
    }
    Registration r = (Registration)this.classToRegistration.get(type);
    if (r == null) {
      synchronized (this.kryo)
      {
        r = this.kryo.getRegistration(type);
        this.classToRegistration.put(type, r);
      }
    }
    if (r.getId() == -1) {
      writeName(output, type, r);
    } else {
      output.writeInt(r.getId() + 2, true);
    }
    return r;
  }
  
  protected void writeName(Output output, Class<?> type, Registration registration)
  {
    output.writeByte(1);
    
    output.write(0);
    output.writeString(type.getName());
  }
  
  public Registration readClass(Input input)
  {
    int classID = input.readInt(true);
    Registration r = null;
    if (classID == 0) {
      return null;
    }
    if (classID == 1)
    {
      r = readName(input);
    }
    else
    {
      int regId = classID - 2;
      if ((regId >= 0) && (regId < this.memoizedClassIdArray.length) && (this.memoizedClassIdArray[regId] != null)) {
        r = this.memoizedClassIdArray[regId];
      }
      if (r == null)
      {
        r = (Registration)this.idToRegistration.get(Integer.valueOf(regId));
        if (r == null)
        {
          String className = null;
          IMap<String, Integer> idMap = HazelcastSingleton.get().getMap("#classIds");
          for (Map.Entry<String, Integer> entry : idMap.entrySet()) {
            if ((((Integer)entry.getValue()).intValue() == regId) && (!((String)entry.getKey()).equals("#allIds")))
            {
              className = (String)entry.getKey();
              break;
            }
          }
          if (className == null) {
            throw new KryoException("Encountered unregistered class ID: " + regId);
          }
          try
          {
            synchronized (this.kryo)
            {
              Class<?> clazz = this.kryo.getClassLoader().loadClass(className);
              r = this.kryo.register(clazz, regId);
            }
          }
          catch (ClassNotFoundException cnfe)
          {
            throw new KryoException("Encountered unregistered class ID with no matching synthetic class: " + regId, cnfe);
          }
        }
        if ((regId >= 0) && (regId < this.memoizedClassIdArray.length)) {
          this.memoizedClassIdArray[regId] = r;
        }
      }
    }
    return r;
  }
  
  protected Registration readName(Input input)
  {
    input.readInt(true);
    Class<?> type = null;
    String className = input.readString();
    type = (Class)this.nameToClass.get(className);
    if (type == null)
    {
      try
      {
        type = this.kryo.getClassLoader().loadClass(className);
      }
      catch (ClassNotFoundException ex)
      {
        throw new KryoException("Unable to find class: " + className, ex);
      }
      this.nameToClass.put(className, type);
    }
    Registration r = (Registration)this.classToRegistration.get(type);
    if (r == null) {
      synchronized (this.kryo)
      {
        r = this.kryo.getRegistration(type);
        this.classToRegistration.put(type, r);
      }
    }
    return r;
  }
  
  public void reset()
  {
    if (!this.kryo.isRegistrationRequired()) {}
  }
  
  public void dumpClassIds()
  {
    System.out.println("**** ALL KRYO REGISTERED CLASSES ****");
    for (Map.Entry<Class<?>, Registration> entry : this.classToRegistration.entrySet()) {
      System.out.println("**  " + ((Class)entry.getKey()).getName() + " - " + ((Registration)entry.getValue()).getId() + " - " + ((Registration)entry.getValue()).getSerializer().getClass().getName());
    }
    System.out.println("*************************************");
    System.out.println("**** ALL KRYO REGISTERED CLASSES BY ID****");
    for (Map.Entry<String, Integer> entry : this.classNameToId.entrySet()) {
      System.out.println("**  " + entry.getValue() + " - " + (String)entry.getKey());
    }
    System.out.println("*************************************");
  }
}
