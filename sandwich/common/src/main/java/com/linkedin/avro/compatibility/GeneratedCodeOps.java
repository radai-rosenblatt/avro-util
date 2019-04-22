package com.linkedin.avro.compatibility;

import com.linkedin.avro.util.TemplateUtil;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * utility class for performing various operations on avro-generated java source code
 */
public class GeneratedCodeOps {
    private static final Pattern PACKAGE_PATTERN = Pattern.compile("package\\s+(.*);");
    private static final Pattern FIXED_SIZE_ANNOTATION_PATTERN = Pattern.compile("@org.apache.avro.specific.FixedSize\\((.*)\\)");
    private static final Pattern FIXED_CLASS_DECL_PATTERN = Pattern.compile("public class (\\w+) extends org.apache\\.avro\\.specific\\.SpecificFixed ");
    private static final Pattern ENUM_CLASS_ANNOTATION_PATTERN = Pattern.compile("public enum (\\w+) ");
    private static final Pattern ENUM_CLASS_DECL_PATTERN = Pattern.compile("public enum (\\w+) \\{\\s*[\\n\\r]\\s*(.*)\\s*[\\n\\r]}");
    private static final Pattern COMMENT_PATTERN = Pattern.compile("(//([/\\s]*).*?\\s*$)|(/\\*+\\s*(.*?)\\s*\\*+/)", Pattern.MULTILINE | Pattern.DOTALL);
    private static final String FIXED_CLASS_BODY_TEMPLATE = TemplateUtil.loadTemplate("SpecificFixedBody.template");
    private static final String FIXED_CLASS_NO_NAMESPACE_BODY_TEMPLATE = TemplateUtil.loadTemplate("SpecificFixedBodyNoNamespace.template");
    private static final String ENUM_CLASS_BODY_TEMPLATE = TemplateUtil.loadTemplate("Enum.template");
    private static final String ENUM_CLASS_NO_NAMESPACE_BODY_TEMPLATE = TemplateUtil.loadTemplate("EnumNoNamespace.template");
    private static final String PARSE_INVOCATION_START = "org.apache.avro.Schema.parse(";
    private static final Pattern PARSE_INVOCATION_PATTERN = Pattern.compile(Pattern.quote(PARSE_INVOCATION_START) + "\"(.*)\"\\);");
    private static final Pattern WRITE_EXTERNAL_SIGNATURE = Pattern.compile(Pattern.quote("@Override public void writeExternal(java.io.ObjectOutput out)"));
    private static final String WRITE_EXTERNAL_WITHOUT_OVERRIDE = Matcher.quoteReplacement("public void writeExternal(java.io.ObjectOutput out)");
    private static final Pattern READ_EXTERNAL_SIGNATURE = Pattern.compile(Pattern.quote("@Override public void readExternal(java.io.ObjectInput in)"));
    private static final String READ_EXTERNAL_WITHOUT_OVERRIDE = Matcher.quoteReplacement("public void readExternal(java.io.ObjectInput in)");
    private static final Pattern CREATE_ENCODER_INVOCATION = Pattern.compile(Pattern.quote("org.apache.avro.specific.SpecificData.getEncoder(out)"));
    private static final String CREATE_ENCODER_VIA_HELPER = Matcher.quoteReplacement("com.linkedin.avro.compatibility.AvroCompatibilityHelper.newBinaryEncoder(out)");
    private static final Pattern CREATE_DECODER_INVOCATION = Pattern.compile(Pattern.quote("org.apache.avro.specific.SpecificData.getDecoder(in)"));
    private static final String CREATE_DECODER_VIA_HELPER = Matcher.quoteReplacement("com.linkedin.avro.compatibility.AvroCompatibilityHelper.newBinaryDecoder(in)");
    private static final Pattern AVROGENERATED_PATTERN = Pattern.compile(Pattern.quote("@org.apache.avro.specific.AvroGenerated"));
    private static final String COMMENTED_OUT_AVROGENERATED = Matcher.quoteReplacement("// @org.apache.avro.specific.AvroGenerated");
    private static final String PARSER_INVOCATION_START = "new org.apache.avro.Schema.Parser().parse(";
    private static final Pattern PARSER_INVOCATION_PATTERN = Pattern.compile(Pattern.quote(PARSER_INVOCATION_START) + "\"(.*)\"\\);([\r\n]+)");
    private static final String CSV_SEPARATOR = "(?<!\\\\)\",\""; //require the 1st " not be preceded by a \
    private static final Pattern BUILDER_START_PATTERN = Pattern.compile("/\\*\\*([\\s*])*Creates a new \\w+ RecordBuilder");
    private static final Pattern EXTERNALIZABLE_SUPPORT_START_PATTERN = Pattern.compile(Pattern.quote("private static final org.apache.avro.io.DatumWriter"));
    private static final Pattern SUPER_CTR_BYTES_PATTERN = Pattern.compile(Pattern.quote("super(bytes);"));
    private static final String BYTES_INVOCATION = Matcher.quoteReplacement("super();\n    bytes(bytes);");
    private static final int MAX_STRING_LITERAL_SIZE = 65000; //just under 64k

    private GeneratedCodeOps() {
        //utility class
    }

    /**
     * avro 1.4 generates bare-bone Fixed classes. modern avro expects Fixed classes (those that extend SpecificFixed):
     * <ul>
     *   <li>to have a public static final org.apache.avro.Schema SCHEMA$ field (at least avro 1.7)</li>
     *   <li>to have a public Schema getSchema() method (in avro 1.8) that returns the above SCHEMA$</li>
     *   <li>to have an implementation of the externalizable interface methods</li>
     * </ul>
     * some extra modern avro amenities that users may expect:
     * <ul>
     *   <li>a constructor that accepts a byte[] argument</li>
     * </ul>
     * this method introduces these into generated code for fixed classes
     * @param code generated code
     * @return if not a fixed class returns input. otherwise returns transformed code.
     */
    public static String addMissingMethodsToFixedClass(String code) {
        Matcher fixedSizeMatcher = FIXED_SIZE_ANNOTATION_PATTERN.matcher(code);
        if (!fixedSizeMatcher.find()) {
            return code; //not a fixed record
        }

        int size;
        String packageName = null;
        String className;
        String doc = "auto-generated for avro compatibility";

        Matcher classMatcher = FIXED_CLASS_DECL_PATTERN.matcher(code);
        if (!classMatcher.find()) {
            throw new IllegalStateException("unable to find class declaration in " + code);
        }
        className = classMatcher.group(1);

        Matcher packageMatcher = PACKAGE_PATTERN.matcher(code);
        if (packageMatcher.find()) { //optional
            packageName = packageMatcher.group(1);
        }

        Matcher commentMatcher = COMMENT_PATTERN.matcher(code);
        if (commentMatcher.find() && commentMatcher.start() < classMatcher.start()) {
            //avro turns the doc property into a class-level comment
            String realDoc = commentMatcher.group(4);
            //remove anything that would otherwise require complicated escaping
            doc = realDoc.replaceAll("[\"'\\t\\n\\r]", "") + " (" + doc + ")"; //retain the "auto-gen" bit
        }

        try {
            size = Integer.parseInt(fixedSizeMatcher.group(1));
        } catch (NumberFormatException e) {
            throw new IllegalStateException("unable to parse size out of " + fixedSizeMatcher.group(0));
        }

        Map<String, String> templateParams = new HashMap<>();
        templateParams.put("name", className);
        templateParams.put("size", String.valueOf(size));
        templateParams.put("doc", doc);
        templateParams.put("namespace", packageName); //might be null
        String template = packageName == null ? FIXED_CLASS_NO_NAMESPACE_BODY_TEMPLATE : FIXED_CLASS_BODY_TEMPLATE;
        String body = TemplateUtil.populateTemplate(template, templateParams);

        return code.substring(0, classMatcher.end(0)) + body;
    }

    /**
     * avro 1.4 generates enum classes are incompatible with modern avro. modern avro expects enums to:
     * <ul>
     *   <li>to have a public static final org.apache.avro.Schema SCHEMA$ field (at least avro 1.7)</li>
     *   <li>to have a public Schema getSchema() method (in avro 1.8) that returns the above SCHEMA$</li>
     * </ul>
     * this method introduces these into generated code for enum classes
     * @param code generated code
     * @return if not an enum class returns input. otherwise returns transformed code.
     */
    public static String addSchemaStringToEnumClass(String code) {
        Matcher enumMatcher = ENUM_CLASS_ANNOTATION_PATTERN.matcher(code);
        if (!enumMatcher.find()) {
            return code; // not a enum class
        }

        String packageName = null;
        String enumClassName;
        String enumSymbols;
        String doc = "auto-generated for avro compatibility";

        Matcher enumClassMatcher = ENUM_CLASS_DECL_PATTERN.matcher(code);
        if (!enumClassMatcher.find()) {
            throw new IllegalStateException("unable to find the enum declaration in " + code);
        }
        enumClassName = enumClassMatcher.group(1);
        enumSymbols = enumClassMatcher.group(2);

        Matcher packageMatcher = PACKAGE_PATTERN.matcher(code);
        if (packageMatcher.find()) { //optional
            packageName = packageMatcher.group(1);
        }

        Matcher commentMatcher = COMMENT_PATTERN.matcher(code);
        if (commentMatcher.find() && commentMatcher.start() < enumClassMatcher.start()) {
            //avro turns the doc property into a class-level comment
            String realDoc = commentMatcher.group(4);
            //remove anything that would otherwise require complicated escaping
            doc = realDoc.replaceAll("[\"'\\t\\n\\r]", " ") + " (auto-generated for avro compatibility)"; //retain the "auto-gen" bit
        }

        Map<String, String> templateParams = new HashMap<>();
        templateParams.put("name", enumClassName);
        templateParams.put("doc", doc);
        templateParams.put("namespace", packageName); //might be null
        templateParams.put("symbols", enumSymbols);
        StringBuilder sb = new StringBuilder();
        for (String enumSymbol : enumSymbols.split("\\s*,\\s*")) {
            sb.append("\\\\\"");
            sb.append(enumSymbol);
            sb.append("\\\\\",");
        }
        sb.deleteCharAt(sb.length() - 1);
        templateParams.put("symbol_string", sb.toString()); // drop the last comma

        String template = packageName == null ? ENUM_CLASS_NO_NAMESPACE_BODY_TEMPLATE : ENUM_CLASS_BODY_TEMPLATE;
        String body = TemplateUtil.populateTemplate(template, templateParams);

        return code.substring(0, enumMatcher.end(0)) + body;
    }

    /**
     * java has a maximum size limit on string _LITERALS_, which generated schemas may go over,
     * producing uncompilable code (see see AVRO-1316).
     * this method replaces giant string literals in parse() invocations with a chain of
     * StringBuilder calls to build the giant String at runtime from smaller pieces.
     * @param code source code generated by avro 1.4
     * @return source code that wont have giant string literals in SCHEMA$
     */
    public static String splitUpBigParseCalls(String code) {
        Matcher matcher = PARSE_INVOCATION_PATTERN.matcher(code); //group 1 would be the args to parse()
        if (!matcher.find()) {
            return code;
        }
        String stringLiteral = matcher.group(1);
        if (stringLiteral.length() < MAX_STRING_LITERAL_SIZE) {
            return code;
        }
        List<String> pieces = safeSplit(stringLiteral, MAX_STRING_LITERAL_SIZE);
        StringBuilder argBuilder = new StringBuilder(stringLiteral.length()); //at least
        argBuilder.append("new StringBuilder()");
        for (String piece : pieces) {
            argBuilder.append(".append(\"").append(piece).append("\")");
        }
        argBuilder.append(".toString()");
        //TODO - optionally use Schema.Parser is avro target is >1.4
        return matcher.replaceFirst(Matcher.quoteReplacement("org.apache.avro.Schema.parse(" + argBuilder.toString() + ");"));
    }

    /**
     * makes the externalizable support code generated by avro 1.8 compile/work with older avro
     * @param code specific record generated by avro
     * @return generated code with externalizable support made backwards compatible
     */
    static String makeExternalizableSupportBackwardsCompatible(String code) {
        String result = code;

        //strip out the "@Override" annotations from Externalizable methods because the parent class
        //(SpecificFixed) is not Externalizable in older avro
        result = WRITE_EXTERNAL_SIGNATURE.matcher(result).replaceAll(WRITE_EXTERNAL_WITHOUT_OVERRIDE);
        result = READ_EXTERNAL_SIGNATURE.matcher(result).replaceAll(READ_EXTERNAL_WITHOUT_OVERRIDE);

        //next up - 1.8 Externalizable support relies on utility code that doesnt exist in <= 1.7
        //so we switch it to use the helper
        result = CREATE_ENCODER_INVOCATION.matcher(result).replaceAll(CREATE_ENCODER_VIA_HELPER);
        result = CREATE_DECODER_INVOCATION.matcher(result).replaceAll(CREATE_DECODER_VIA_HELPER);

        return result;
    }

    /**
     * comment out annotation that only exists in avro 1.7+
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
        //   characters for complicated unicode) - see AVRO-1316. modern avro has a vararg parse(String...) to get around this
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

    /**
     * removes the generated builder class and related methods from an avro-generated specific record.
     * the builder relies on a base class that does not exist in avro 1.4
     * @param code specific record code generated by avro
     * @return generated code without builder support
     */
    static String removeBuilderSupport(String code) {
        String result = code;

        Matcher matcher = BUILDER_START_PATTERN.matcher(result);
        if (matcher.find()) {
            int builderSupportStart = matcher.start();
            Matcher endMatcher = EXTERNALIZABLE_SUPPORT_START_PATTERN.matcher(result);
            if (endMatcher.find()) {
                //avro 1.8 has externalizable support (which we must keep) after the builder support section
                int externalizableSupportStart = endMatcher.start();
                if (externalizableSupportStart <= builderSupportStart) {
                    throw new IllegalStateException("unable to properly locate builder vs externalizable support code");
                }
                result = result.substring(0, builderSupportStart) + result.substring(externalizableSupportStart);
            } else {
                //builder support is at the end of the file
                result = result.substring(0, builderSupportStart) + "\n}";
            }
        }

        return result;
    }

    /**
     * fixes the byte[]-accepting constructor in some avro-generated classes to work under avro 1.4
     * @param code specific record code generated by avro
     * @return generated code with avro-1.4 compatible byte[] handling
     */
    static String fixByteArrayConstructor(String code) {
        return SUPER_CTR_BYTES_PATTERN.matcher(code).replaceAll(BYTES_INVOCATION);
    }

    /**
     * splits a large java string literal into smaller pieces in a safe way.
     * by safe we mean avoids splitting anywhere near an escape sequence
     * @param javaStringLiteral large string literal
     * @return smaller string literals that can be joined to reform the argument
     */
    static List<String> safeSplit(String javaStringLiteral, int maxChunkSize) {
        String remainder = javaStringLiteral;
        List<String> results = new ArrayList<>(remainder.length() / maxChunkSize);
        while (remainder.length() > maxChunkSize) {
            int cutIndex = maxChunkSize;
            while (cutIndex > 0 && escapesNear(remainder, cutIndex)) {
                cutIndex--;
            }
            if (cutIndex <= 0) {
                //should never happen ...
                throw new IllegalStateException("unable to split " + javaStringLiteral);
            }
            String piece = remainder.substring(0, cutIndex);
            results.add(piece);
            remainder = remainder.substring(cutIndex);
        }
        if (!remainder.isEmpty()) {
            results.add(remainder);
        }
        return results;
    }

    /**
     * returns true is there's a string escape sequence starting anywhere
     * near a given index in a given string literal. since the longest escape
     * sequences in java are ~5-6 characters (unicode escapes) a safety margin
     * of 10 characters is used.
     * @param literal string literal to look for escape sequences in
     * @param index index around (before) which to look for escapes
     * @return true if any escape sequence found
     */
    static boolean escapesNear(String literal, int index) {
        //we start at index because we dont want the char at the start of the next fragment
        //to be an "interesting" character either
        for (int i = index; i > Math.max(0, index - 6); i--) {
            char c = literal.charAt(i);
            if (c == '\\' || c == '"' || c == '\'') {
                return true;
            }
        }
        return false;
    }
}
