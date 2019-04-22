package com.linkedin.avro.compatibility.avro16;

import com.linkedin.avro.compatibility.AvroGeneratedSourceCode;
import com.linkedin.avro.compatibility.AvroVersion;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

public class    Avro16CompilerOperations {

    private final Method _compilerEnqueueMethod;
    private final Method _compilerCompileMethod;
    private final Field _outputFilePathField;
    private final Field _outputFileContentsField;

    public Avro16CompilerOperations() throws Exception {
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
            //TODO - figure out what transforms are required
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
