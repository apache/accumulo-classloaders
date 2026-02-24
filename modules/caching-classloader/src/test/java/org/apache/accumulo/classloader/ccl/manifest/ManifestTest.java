/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.classloader.ccl.manifest;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

class ManifestTest {

  private static final SecureRandom RAND = new SecureRandom();

  private static String mockJson(boolean withComment, boolean withMonitorInterval,
      int withResourceCount) {
    final String COMMA = ",";

    StringBuilder json = new StringBuilder().append("{");
    if (withComment) {
      json.append("'comment': 'an optional comment'");
      if (withMonitorInterval || withResourceCount >= 0) {
        json.append(COMMA);
      }
    }
    if (withMonitorInterval) {
      json.append("'monitorIntervalSeconds': 5");
      if (withResourceCount >= 0) {
        json.append(COMMA);
      }
    }
    if (withResourceCount >= 0) {
      json.append("'resources': [");
      for (int i = 0; i < withResourceCount; i++) {
        if (i > 0) {
          json.append(COMMA);
        }
        var n = i;
        if (withResourceCount == 10) {
          // special value to make all resources the same to test deduplication
          n = 0;
        }
        json.append("{'location': 'file:/home/user/ClassLoaderTestA/" + n + ".jar'").append(COMMA);
        json.append("'algorithm': 'MOCK',").append("'checksum': '" + n + "'}");
      }
      json.append("]");
    }
    return json.append("}").toString().replace("'", "\"");
  }

  @Test
  void testCreate() throws Exception {
    var manifest = Manifest.create(null, 27, "SHA-512", Path.of("pom.xml").toUri().toURL());
    assertEquals(null, manifest.getComment());
    assertEquals(27, manifest.getMonitorIntervalSeconds());
    assertEquals("SHA-512", manifest.getChecksumAlgorithm());
    assertNotNull(manifest.getChecksum());
    assertEquals(1, manifest.getResources().size());

    var json = manifest.toJson();
    try (var in = new ByteArrayInputStream(json.getBytes(UTF_8))) {
      var manifest2 = Manifest.fromStream(in);
      assertNotSame(manifest, manifest2);
      assertEquals(manifest, manifest2);
    }
  }

  @Test
  void testDeserializing() throws Exception {
    int resourceCount = RAND.nextInt(100) + 15;
    var json = mockJson(true, true, resourceCount);
    try (var in = new ByteArrayInputStream(json.getBytes(UTF_8))) {
      var manifest = Manifest.fromStream(in);
      assertEquals("an optional comment", manifest.getComment());
      assertEquals(5, manifest.getMonitorIntervalSeconds());
      assertEquals(resourceCount, manifest.getResources().size());
      var resources = manifest.getResources();
      var iter = resources.iterator();
      for (int i = 0; i < resourceCount; i++) {
        var r = iter.next();
        assertEquals(i + ".jar", r.getFileName());
        assertEquals("MOCK", r.getAlgorithm());
        assertEquals(String.valueOf(i), r.getChecksum());
      }
      assertFalse(iter.hasNext());
    }

    // no optional comment
    json = mockJson(false, true, 3);
    try (var in = new ByteArrayInputStream(json.getBytes(UTF_8))) {
      var manifest = Manifest.fromStream(in);
      assertNull(manifest.getComment());
      assertEquals(5, manifest.getMonitorIntervalSeconds());
      assertEquals(3, manifest.getResources().size());
    }
    // no monitor interval
    json = mockJson(false, false, 3);
    try (var in = new ByteArrayInputStream(json.getBytes(UTF_8))) {
      assertNull(Manifest.fromStream(in));
    }
    // empty resources
    json = mockJson(false, true, 0);
    try (var in = new ByteArrayInputStream(json.getBytes(UTF_8))) {
      var manifest = Manifest.fromStream(in);
      assertNull(manifest.getComment());
      assertEquals(5, manifest.getMonitorIntervalSeconds());
      assertEquals(0, manifest.getResources().size());
    }
    // missing resources
    json = mockJson(false, false, -1);
    try (var in = new ByteArrayInputStream(json.getBytes(UTF_8))) {
      assertNull(Manifest.fromStream(in));
    }
  }

  @Test
  void testDeserializationWithDeduplication() throws Exception {
    Pattern numExpectedJar = Pattern.compile("0[.]jar");
    var json = mockJson(true, true, 1);
    var jsonWithDuplicates = mockJson(true, true, 10);
    assertNotEquals(json, jsonWithDuplicates);
    assertEquals(1, numExpectedJar.matcher(json).results().count());
    assertEquals(10, numExpectedJar.matcher(jsonWithDuplicates).results().count());
    final Manifest manifest;
    try (var in = new ByteArrayInputStream(json.getBytes(UTF_8))) {
      manifest = Manifest.fromStream(in);
    }
    final Manifest manifestFromDuplicates;
    try (var in = new ByteArrayInputStream(jsonWithDuplicates.getBytes(UTF_8))) {
      manifestFromDuplicates = Manifest.fromStream(in);
    }
    assertEquals(manifest, manifestFromDuplicates);
    assertEquals(manifest.toJson(), manifestFromDuplicates.toJson());
    assertEquals("an optional comment", manifest.getComment());
    assertEquals(5, manifest.getMonitorIntervalSeconds());
    assertEquals(1, manifest.getResources().size());
    var resources = manifest.getResources();
    var iter = resources.iterator();
    var r = iter.next();
    assertEquals("0.jar", r.getFileName());
    assertEquals("MOCK", r.getAlgorithm());
    assertEquals(String.valueOf(0), r.getChecksum());
    assertFalse(iter.hasNext());
  }
}
