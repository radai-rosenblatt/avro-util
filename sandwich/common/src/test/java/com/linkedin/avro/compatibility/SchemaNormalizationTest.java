package com.linkedin.avro.compatibility;

import com.linkedin.avro.test.TestUtil;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SchemaNormalizationTest {

    @Test
    public void testSchemaCanonicalization() throws Exception {
        Schema withDocs = Schema.parse(TestUtil.load("HasSymbolDocs.avsc"));
        Schema withoutDocs = Schema.parse(TestUtil.load("HasNoSymbolDocs.avsc"));
        Assert.assertNotEquals(withDocs.toString(true), withoutDocs.toString(true));
        String c1 = SchemaNormalization.toParsingForm(withDocs);
        String c2 = SchemaNormalization.toParsingForm(withoutDocs);
        Assert.assertEquals(c1, c2);
    }
}
