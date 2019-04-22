/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.compatibility.avro14;

import com.linkedin.avro.compatibility.*;
import org.apache.avro.Avro14SchemaAccessHelper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificCompiler;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;


public class Avro14Adapter implements AvroAdapter {

  private final Method _compilerEnqueueMethod;
  private final Method _compilerCompileMethod;
  private final Field _outputFilePathField;
  private final Field _outputFileContentsField;

  public Avro14Adapter() throws Exception {
    _compilerEnqueueMethod = SpecificCompiler.class.getDeclaredMethod("enqueue", Schema.class);
    _compilerEnqueueMethod.setAccessible(true); //private
    _compilerCompileMethod = SpecificCompiler.class.getDeclaredMethod("compile");
    _compilerCompileMethod.setAccessible(true); //package-protected
    Class<?> outputFileClass = Class.forName("org.apache.avro.specific.SpecificCompiler$OutputFile");
    _outputFilePathField = outputFileClass.getDeclaredField("path");
    _outputFilePathField.setAccessible(true);
    _outputFileContentsField = outputFileClass.getDeclaredField("contents");
    _outputFileContentsField.setAccessible(true);
  }

  @Override
  public BinaryEncoder newBinaryEncoder(OutputStream out) {
    try {
      return new BinaryEncoder(out);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public JsonEncoder newJsonEncoder(Schema schema, OutputStream out) throws IOException {
    return new JsonEncoder(schema, out);
  }

  @Override
  public JsonEncoder newJsonEncoder(Schema schema, JsonGenerator jsonGenerator) throws IOException {
    return new JsonEncoder(schema, jsonGenerator);
  }

  @Override
  public JsonDecoder newJsonDecoder(Schema schema, InputStream input) throws IOException {
    return new JsonDecoder(schema, input);
  }

  @Override
  public JsonDecoder newJsonDecoder(Schema schema, String input) throws IOException {
    return new JsonDecoder(schema, input);
  }

  @Override
  public GenericData.EnumSymbol newEnumSymbol(Schema avroSchema, String enumValue) {
    return new GenericData.EnumSymbol(enumValue);
  }

  @Override
  public GenericData.Fixed newFixedField(Schema ofType) {
    return new GenericData.Fixed(ofType);
  }

  @Override
  public GenericData.Fixed newFixedField(Schema ofType, byte[] contents) {
    return new GenericData.Fixed(contents);
  }

  @Override
  public String toParsingForm(Schema s) {
    return SchemaNormalization.toParsingForm(s);
  }

  @Override
  public SchemaParseResult parse(String schemaJson, Collection<Schema> known) {
    return Avro14SchemaAccessHelper.parse(schemaJson, known);
  }

  @Override
  public Collection<AvroGeneratedSourceCode> compile(Collection<Schema> toCompile, AvroVersion minSupportedRuntimeVersion) {
    if (toCompile == null || toCompile.isEmpty()) {
      return Collections.emptyList();
    }
    Iterator<Schema> schemaIter = toCompile.iterator();
    Schema first = schemaIter.next();
    try {
      SpecificCompiler compilerInstance = new SpecificCompiler(first);

      while (schemaIter.hasNext()) {
        _compilerEnqueueMethod.invoke(compilerInstance, schemaIter.next());
      }

      Collection<?> outputFiles = (Collection<?>) _compilerCompileMethod.invoke(compilerInstance);
      List<AvroGeneratedSourceCode> translated = outputFiles.stream()
              .map(o -> new AvroGeneratedSourceCode(getPath(o), getContents(o)))
              .collect(Collectors.toList());

      if (minSupportedRuntimeVersion != null) {
        return transform(translated, minSupportedRuntimeVersion);
      } else {
        return translated;
      }
    } catch (UnsupportedOperationException e) {
      throw e; //as-is
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  protected List<AvroGeneratedSourceCode> transform(List<AvroGeneratedSourceCode> avroCodegenOutput, AvroVersion minRuntimeVersion) {
    List<AvroGeneratedSourceCode> transformed = new ArrayList<>(avroCodegenOutput.size());
    String fixed;
    for (AvroGeneratedSourceCode generated : avroCodegenOutput) {
      fixed = generated.getContents();
      fixed = GeneratedCodeOps.addMissingMethodsToFixedClass(fixed);
      fixed = GeneratedCodeOps.addSchemaStringToEnumClass(fixed);
      fixed = GeneratedCodeOps.splitUpBigParseCalls(fixed);
      transformed.add(new AvroGeneratedSourceCode(generated.getPath(), fixed));
    }
    return transformed;
  }

  protected String getPath(Object shouldBeOutputFile) {
    try {
      return (String) _outputFilePathField.get(shouldBeOutputFile);
    } catch (Exception e) {
      throw new IllegalStateException("cant extract path from avro OutputFile", e);
    }
  }

  protected String getContents(Object shouldBeOutputFile) {
    try {
      return (String) _outputFileContentsField.get(shouldBeOutputFile);
    } catch (Exception e) {
      throw new IllegalStateException("cant extract contents from avro OutputFile", e);
    }
  }
}
