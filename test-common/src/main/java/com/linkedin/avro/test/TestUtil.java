/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.test;

import org.apache.commons.io.IOUtils;

import java.io.IOException;


public class TestUtil {
  private TestUtil() {
    //util
  }

  public static String load(String path) throws IOException {
    return IOUtils.toString(Thread.currentThread().getContextClassLoader().getResourceAsStream(path), "utf-8");
  }
}
