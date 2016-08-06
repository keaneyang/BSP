package com.bloom.ser;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

public class AbstractAvroSerializer
  extends Serializer<GenericContainer>
{
  public void write(Kryo kryo, Output output, GenericContainer record)
  {
    String fingerprint = record.getSchema().toString();
    output.writeString(fingerprint);
    GenericDatumWriter<GenericContainer> writer = new GenericDatumWriter(record.getSchema());
    
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(output, null);
    try
    {
      writer.write(record, encoder);
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }
  }
  
  public GenericContainer read(Kryo kryo, Input input, Class<GenericContainer> aClass)
  {
    Schema theSchema = new Schema.Parser().parse(input.readString());
    GenericDatumReader<GenericContainer> reader = new GenericDatumReader(theSchema);
    Decoder decoder = DecoderFactory.get().directBinaryDecoder(input, null);
    GenericContainer foo;
    try
    {
      foo = (GenericContainer)reader.read(null, decoder);
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }
    return foo;
  }
}
