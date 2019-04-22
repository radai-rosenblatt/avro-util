package com.linkedin.avro.compatibility.avro14;

import com.linkedin.avro.test.TestUtil;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

public class Avro14AdapterTest {
//    private final static String FIXED_TYPE_SCHEMA_JSON =
//            "{\n"
//                    + "  \"type\" : \"fixed\",\n"
//                    + "  \"name\" : \"Whatever\",\n"
//                    + "  \"namespace\" : \"com.acme\",\n"
//                    + "  \"size\" : 42,\n"
//                    + "  \"doc\" : \"yadda yadda evil quote\\\"\"\n"
//                    + "}";
//    private final static String FIXED_TYPE_NO_NAMESPACE_SCHEMA_JSON =
//            "{\n"
//                    + "  \"type\" : \"fixed\",\n"
//                    + "  \"name\" : \"Whatever\",\n"
//                    + "  \"size\" : 42,\n"
//                    + "  \"doc\" : \"w00t\"\n"
//                    + "}";
//    private final static String ENUM_CLASS_JSON =
//            "{\n"
//                    + "  \"type\":\"enum\",\n"
//                    + "  \"name\":\"BobSmith\",\n"
//                    + "  \"namespace\":\"com.dot\",\n"
//                    + "  \"symbols\":[\"Bread\",\"Butter\",\"Jam\"],\n"
//                    + "  \"doc\" : \"Bob Smith Store\"\n"
//                    + "  }";
//    private final static String ENUM_CLASS_NO_NAMESPACE_JSON =
//            "{\n"
//                    + "  \"type\":\"enum\",\n"
//                    + "  \"name\":\"BobSmith\",\n"
//                    + "  \"symbols\":[\"Bread\",\"Butter\",\"Jam\"],\n"
//                    + "  \"doc\" : \"Bob Smith Store\"\n"
//                    + "  }";

    private Avro14Adapter adapter;

    @Test
    public void testSchemaCanonicalization() throws Exception {
        Schema withDocs = Schema.parse(TestUtil.load("HasSymbolDocs.avsc"));
        Schema withoutDocs = Schema.parse(TestUtil.load("HasNoSymbolDocs.avsc"));
        Assert.assertNotEquals(withDocs.toString(true), withoutDocs.toString(true));
        String c1 = adapter.toParsingForm(withDocs);
        String c2 = adapter.toParsingForm(withoutDocs);
        Assert.assertEquals(c1, c2);
    }
}
