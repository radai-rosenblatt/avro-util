/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.compatibility;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;


/**
 * an interface for performing various avro operations that are avro-version-dependant.
 * this class is in the "org.apache.avro.io" package because some
 * classes used here (like {@link JsonEncoder}) are package-private
 * under some versions of avro
 */
public interface AvroAdapter {

  BinaryEncoder newBinaryEncoder(OutputStream out);

  JsonEncoder newJsonEncoder(Schema schema, OutputStream out) throws IOException;

  JsonEncoder newJsonEncoder(Schema schema, JsonGenerator jsonGenerator) throws IOException;

  JsonDecoder newJsonDecoder(Schema schema, InputStream input) throws IOException;

  JsonDecoder newJsonDecoder(Schema schema, String input) throws IOException;

  GenericData.EnumSymbol newEnumSymbol(Schema avroSchema, String enumValue);

  GenericData.Fixed newFixedField(Schema ofType);

  GenericData.Fixed newFixedField(Schema ofType, byte[] contents);

  String toParsingForm(Schema s);

  SchemaParseResult parse(String schemaJson, Collection<Schema> known);

  /**
   * invokes the avro {@link org.apache.avro.specific.SpecificCompiler} to generate java code
   * and then (optionally) post-processes that code to make it compatible with a wider range
   * of avro versions at runtime.
   * @param toCompile schemas to generate java code out of
   * @param minSupportedRuntimeVersion minimum avro version the resulting code should work under.
   *                                   null means no post-processing will be done at all
   *                                   (resulting in "raw" avro output)
   * @return generated java code
   * @throws UnsupportedOperationException if the {@link org.apache.avro.specific.SpecificCompiler} class
   *                                       is not found on the classpath (which is possible with avro 1.5+)
   */
  Collection<AvroGeneratedSourceCode> compile(Collection<Schema> toCompile, AvroVersion minSupportedRuntimeVersion);
}
