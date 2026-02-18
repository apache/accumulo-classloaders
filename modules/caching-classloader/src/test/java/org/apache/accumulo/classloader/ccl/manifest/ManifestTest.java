package org.apache.accumulo.classloader.ccl.manifest;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

class ManifestTest {

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
        json.append("{'location': 'file:/home/user/ClassLoaderTestA/" + i + ".jar'").append(COMMA);
        json.append("'algorithm': 'MOCK',").append("'checksum': '" + i + "'}");
      }
      json.append("]");
    }
    return json.append("}").toString().replace("'", "\"");
  }

  @Test
  void testCreate() throws Exception {
    var manifest = Manifest.create(null, 27, "SHA-512", Paths.get("pom.xml").toUri().toURL());
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
    var json = mockJson(true, true, 3);
    try (var in = new ByteArrayInputStream(json.getBytes(UTF_8))) {
      var manifest = Manifest.fromStream(in);
      assertEquals("an optional comment", manifest.getComment());
      assertEquals(5, manifest.getMonitorIntervalSeconds());
      assertEquals(3, manifest.getResources().size());
      var resources = manifest.getResources();
      var iter = resources.iterator();
      for (int i = 0; i < 3; i++) {
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
}
