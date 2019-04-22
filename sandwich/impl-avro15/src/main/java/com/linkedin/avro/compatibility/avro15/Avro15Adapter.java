package com.linkedin.avro.compatibility.avro15;

import com.linkedin.avro.compatibility.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Avro15Adapter implements AvroAdapter {
    private static final Logger LOG = LogManager.getLogger(Avro15Adapter.class);

    private Avro15CompilerOperations compilerOperations = null;

    public Avro15Adapter() {
        try {
            compilerOperations = new Avro15CompilerOperations();
        } catch (Exception | LinkageError oops) {
            LOG.warn("compiler operations will be unavailable since avro-compiler could not be initialized", oops);
        }
    }

    @Override
    public BinaryEncoder newBinaryEncoder(OutputStream out) {
        return EncoderFactory.get().binaryEncoder(out, null);
    }

    @Override
    public JsonEncoder newJsonEncoder(Schema schema, OutputStream out) throws IOException {
        return EncoderFactory.get().jsonEncoder(schema, out);
    }

    @Override
    public JsonEncoder newJsonEncoder(Schema schema, JsonGenerator jsonGenerator) throws IOException {
        return EncoderFactory.get().jsonEncoder(schema, jsonGenerator);
    }

    @Override
    public JsonDecoder newJsonDecoder(Schema schema, InputStream input) throws IOException {
        return DecoderFactory.get().jsonDecoder(schema, input);
    }

    @Override
    public JsonDecoder newJsonDecoder(Schema schema, String input) throws IOException {
        return DecoderFactory.get().jsonDecoder(schema, input);
    }

    @Override
    public GenericData.EnumSymbol newEnumSymbol(Schema avroSchema, String enumValue) {
        return new GenericData.EnumSymbol(avroSchema, enumValue);
    }

    @Override
    public GenericData.Fixed newFixedField(Schema ofType) {
        return new GenericData.Fixed(ofType);
    }

    @Override
    public GenericData.Fixed newFixedField(Schema ofType, byte[] contents) {
        return new GenericData.Fixed(ofType, contents);
    }

    @Override
    public String toParsingForm(Schema s) {
        return SchemaNormalization.toParsingForm(s);
    }

    @Override
    public SchemaParseResult parse(String schemaJson, Collection<Schema> known) {
        Schema.Parser parser = new Schema.Parser();
        parser.setValidate(true);
        if (known != null) {
            Map<String, Schema> types = new HashMap<>(known.size());
            for (Schema s : known) {
                types.put(s.getFullName(), s);
            }
            parser.addTypes(types);
        }
        Schema mainSchema = parser.parse(schemaJson);
        Map<String, Schema> allChemas = parser.getTypes();
        return new SchemaParseResult(mainSchema, allChemas);
    }

    @Override
    public Collection<AvroGeneratedSourceCode> compile(Collection<Schema> toCompile, AvroVersion minSupportedRuntimeVersion) {
        if (compilerOperations == null){
            throw new UnsupportedOperationException("compiler operations unavailable. see previous warning");
        }
        return compilerOperations.compile(toCompile, minSupportedRuntimeVersion);
    }
}
