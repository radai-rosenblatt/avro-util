package com.linkedin.avro.compatibility.avro17;

import com.linkedin.avro.compatibility.AvroGeneratedSourceCode;
import com.linkedin.avro.compatibility.AvroVersion;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Avro17CompilerOperations {
    private static final Pattern AVROGENERATED_PATTERN = Pattern.compile(Pattern.quote("@org.apache.avro.specific.AvroGenerated"));
    private static final String COMMENTED_OUT_AVROGENERATED = Matcher.quoteReplacement("// @org.apache.avro.specific.AvroGenerated");
    private static final String PARSER_INVOCATION_START = "new org.apache.avro.Schema.Parser().parse(";
    private static final Pattern PARSER_INVOCATION_PATTERN = Pattern.compile(Pattern.quote(PARSER_INVOCATION_START) + "\"(.*)\"\\);([\r\n]+)");
    private static final String CSV_SEPARATOR = "(?<!\\\\)\",\""; //require the 1st " not be preceded by a \
    private static final Pattern BUILDER_START_PATTERN = Pattern.compile("/\\*\\*([\\s*])*Creates a new \\w+ RecordBuilder");
    private static final Pattern SUPER_CTR_BYTES_PATTERN = Pattern.compile(Pattern.quote("super(bytes);"));
    private static final String BYTES_INVOCATION = Matcher.quoteReplacement("super();\n    bytes(bytes);");
    private static final Pattern EXTERNALIZABLE_SUPPORT_START_PATTERN = Pattern.compile(Pattern.quote("private static final org.apache.avro.io.DatumWriter"));
    private static final Pattern WRITE_EXTERNAL_SIGNATURE = Pattern.compile(Pattern.quote("@Override public void writeExternal(java.io.ObjectOutput out)"));
    private static final String WRITE_EXTERNAL_WITHOUT_OVERRIDE = Matcher.quoteReplacement("public void writeExternal(java.io.ObjectOutput out)");
    private static final Pattern READ_EXTERNAL_SIGNATURE = Pattern.compile(Pattern.quote("@Override public void readExternal(java.io.ObjectInput in)"));
    private static final String READ_EXTERNAL_WITHOUT_OVERRIDE = Matcher.quoteReplacement("public void readExternal(java.io.ObjectInput in)");
    private static final Pattern CREATE_ENCODER_INVOCATION = Pattern.compile(Pattern.quote("org.apache.avro.specific.SpecificData.getEncoder(out)"));
    private static final String CREATE_ENCODER_VIA_HELPER = Matcher.quoteReplacement("com.linkedin.avro.compatibility.AvroCompatibilityHelper.newBinaryEncoder(out)");
    private static final Pattern CREATE_DECODER_INVOCATION = Pattern.compile(Pattern.quote("org.apache.avro.specific.SpecificData.getDecoder(in)"));
    private static final String CREATE_DECODER_VIA_HELPER = Matcher.quoteReplacement("com.linkedin.avro.compatibility.AvroCompatibilityHelper.newBinaryDecoder(in)");
    private static final Pattern CATCH_EXCEPTION_PATTERN = Pattern.compile(Pattern.quote("catch (Exception e)"));
    private static final String CATCH_FULLY_QUALIFIED_EXCEPTION = Matcher.quoteReplacement("catch (java.lang.Exception e)");

    private final Method _compilerEnqueueMethod;
    private final Method _compilerCompileMethod;
    private final Field _outputFilePathField;
    private final Field _outputFileContentsField;

    public Avro17CompilerOperations() throws Exception {
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
            if (minRuntimeVersion.earlierThan(AvroVersion.AVRO_1_7)) {
                fixed = commentOutAvroGeneratedAnnotation(fixed);
            }
            transformed.add(new AvroGeneratedSourceCode(generated.getPath(), fixed));
        }
        return transformed;
    }

    /**
     * comment out annotation that only exists in avro 1.7
     * @param code specific record code generated by avro
     * @return generated code with the AvroGenerated annotation commented out
     */
    static String commentOutAvroGeneratedAnnotation(String code) {
        return AVROGENERATED_PATTERN.matcher(code).replaceAll(COMMENTED_OUT_AVROGENERATED);
    }

    /**
     * makes changes to the SCHEMA$ field in generated specific classes to make them avro-1.4 compatible
     * @param code specific record code generated by avro
     * @return generated code with SCHEMA$ declaration made avro-1.4 compatible
     */
    static String makeSchemaDollarBackwardsCompatible(String code) {
        String result = code;

        //issues we have with avro's generated SCHEMA$ field:
        //1. modern avro uses Schema.Parser().parse() whereas 1.4 doesnt have it. 1.7 /1.8 still has Schema.parse() (just deprecated)
        //   so we resort to using that for compatibility
        //2. the java language has a limitation where string literals cannot be over 64K long (this is way less than 64k
        //   characters for complicated unicode) - see AVRO-1316. modern avro 1.7+ has a vararg parse(String...) to get around this
        //   we will need to convert that into new StringBuilder().append().append()...toString()
        Matcher matcher = PARSER_INVOCATION_PATTERN.matcher(result); //group 1 would be the args to parse(), group 2 would be some line break
        if (matcher.find()) {
            String argsStr = result.substring(matcher.start(1), matcher.end(1));
            String lineBreak = matcher.group(2);
            String[] varArgs = argsStr.split(CSV_SEPARATOR);
            String singleArg;
            if (varArgs.length > 1) {
                StringBuilder argBuilder = new StringBuilder(argsStr.length());
                argBuilder.append("new StringBuilder()");
                Arrays.stream(varArgs).forEach(literal -> {
                    argBuilder.append(".append(\"").append(literal).append("\")");
                });
                argBuilder.append(".toString()");
                singleArg = argBuilder.toString();
            } else {
                singleArg = "\"" + varArgs[0] + "\"";
            }
            result = matcher.replaceFirst(Matcher.quoteReplacement("org.apache.avro.Schema.parse(" + singleArg + ");") + lineBreak);
        }

        return result;
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
