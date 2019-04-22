package com.linkedin.avro.compatibility;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

public class GeneratedCodeOpsTest {

    @Test
    public void testSafeSplit() throws Exception {
        Assert.assertEquals(
                Arrays.asList("1234567890", "abcdefghij"),
                GeneratedCodeOps.safeSplit("1234567890abcdefghij", 10));
        Assert.assertEquals(
                Arrays.asList("1234567890", "abcdefghij", "AB"),
                GeneratedCodeOps.safeSplit("1234567890abcdefghijAB", 10));
        Assert.assertEquals(Collections.singletonList("1234567890"),
                GeneratedCodeOps.safeSplit("1234567890", 10));
        //dont chop at '
        Assert.assertEquals(
                Arrays.asList("12345678", "9'abcdefgh", "ij"),
                GeneratedCodeOps.safeSplit("123456789'abcdefghij", 10));
        //unicode escapes not on the boundary
        Assert.assertEquals(
                Arrays.asList("xx\\u1234xx", "xxxxxxxxxx"),
                GeneratedCodeOps.safeSplit("xx\\u1234xxxxxxxxxxxx", 10));
        Assert.assertEquals(
                Arrays.asList("xxxx\\u1234", "xxxxxxxxxx"),
                GeneratedCodeOps.safeSplit("xxxx\\u1234xxxxxxxxxx", 10));
        //unicode escapes cross the boundary
        Assert.assertEquals(
                Arrays.asList("xxxx","x\\u1234xxx", "xxxxxx"),
                GeneratedCodeOps.safeSplit("xxxxx\\u1234xxxxxxxxx", 10));
        Assert.assertEquals(
                Arrays.asList("xxxxx","x\\u1234xxx", "xxxxx"),
                GeneratedCodeOps.safeSplit("xxxxxx\\u1234xxxxxxxx", 10));
        Assert.assertEquals(
                Arrays.asList("xxxxxx","x\\u1234xxx", "xxxx"),
                GeneratedCodeOps.safeSplit("xxxxxxx\\u1234xxxxxxx", 10));
        Assert.assertEquals(
                Arrays.asList("xxxxxxx","x\\u1234xxx", "xxx"),
                GeneratedCodeOps.safeSplit("xxxxxxxx\\u1234xxxxxx", 10));
        Assert.assertEquals(
                Arrays.asList("xxxxxxxx","x\\u1234xxx", "xx"),
                GeneratedCodeOps.safeSplit("xxxxxxxxx\\u1234xxxxx", 10));
        Assert.assertEquals(
                Arrays.asList("xxxxxxxxx","x\\u1234xxx", "x"),
                GeneratedCodeOps.safeSplit("xxxxxxxxxx\\u1234xxxx", 10));
        Assert.assertEquals(
                Arrays.asList("xxxxxxxxx","x\\u1234xxx", "x"),
                GeneratedCodeOps.safeSplit("xxxxxxxxxx\\u1234xxxx", 10));
        Assert.assertEquals(
                Arrays.asList("xxxxxxxxxx","x\\u1234xxx"),
                GeneratedCodeOps.safeSplit("xxxxxxxxxxx\\u1234xxx", 10));
    }
}
