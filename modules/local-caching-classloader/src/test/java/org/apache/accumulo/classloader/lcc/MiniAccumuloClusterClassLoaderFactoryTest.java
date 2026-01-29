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
package org.apache.accumulo.classloader.lcc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.apache.accumulo.classloader.lcc.LocalCachingContextClassLoaderFactory.ALLOWED_URLS_PATTERN;
import static org.apache.accumulo.classloader.lcc.LocalCachingContextClassLoaderFactory.CACHE_DIR_PROPERTY;
import static org.apache.accumulo.classloader.lcc.LocalCachingContextClassLoaderFactory.UPDATE_FAILURE_GRACE_PERIOD_MINS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.management.JMX;
import javax.management.MalformedObjectNameException;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.accumulo.classloader.lcc.jmx.ContextClassLoadersMXBean;
import org.apache.accumulo.classloader.lcc.util.LocalStore;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.AccumuloServerException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestIngest.IngestParams;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;

public class MiniAccumuloClusterClassLoaderFactoryTest extends SharedMiniClusterBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(MiniAccumuloClusterClassLoaderFactoryTest.class);

  private static class TestMACConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg,
        org.apache.hadoop.conf.Configuration coreSite) {
      cfg.getJvmOptions().add("-XX:-PerfDisableSharedMem");
      cfg.setNumTservers(3);
      cfg.setProperty(Property.TSERV_NATIVEMAP_ENABLED.getKey(), "false");
      cfg.setProperty(Property.GENERAL_CONTEXT_CLASSLOADER_FACTORY.getKey(),
          LocalCachingContextClassLoaderFactory.class.getName());
      cfg.setProperty(CACHE_DIR_PROPERTY, tempDir.resolve("base").toUri().toString());
      cfg.setProperty(ALLOWED_URLS_PATTERN, ".*");
      cfg.setProperty(UPDATE_FAILURE_GRACE_PERIOD_MINS, "1");
    }
  }

  @TempDir
  private static Path tempDir;

  private static final Set<PosixFilePermission> CACHE_DIR_PERMS =
      EnumSet.of(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE);
  private static final FileAttribute<Set<PosixFilePermission>> PERMISSIONS =
      PosixFilePermissions.asFileAttribute(CACHE_DIR_PERMS);
  private static final String ITER_CLASS_NAME =
      "org.apache.accumulo.classloader.vfs.examples.ExampleIterator";
  private static final int MONITOR_INTERVAL_SECS =
      LocalCachingContextClassLoaderFactoryTest.MONITOR_INTERVAL_SECS;

  private static URL jarAOrigLocation;
  private static URL jarBOrigLocation;

  @BeforeAll
  public static void beforeAll() throws Exception {

    // Find the Test jar files
    jarAOrigLocation = MiniAccumuloClusterClassLoaderFactoryTest.class
        .getResource("/ExampleIteratorsA/example-iterators-a.jar");
    assertNotNull(jarAOrigLocation);
    jarBOrigLocation = MiniAccumuloClusterClassLoaderFactoryTest.class
        .getResource("/ExampleIteratorsB/example-iterators-b.jar");
    assertNotNull(jarBOrigLocation);

    startMiniClusterWithConfig(new TestMACConfiguration());
  }

  @AfterAll
  public static void afterAll() throws Exception {
    stopMiniCluster();
  }

  @Test
  public void testClassLoader() throws Exception {
    final var baseDirPath = tempDir.resolve("base");
    final var resourcesDirPath = baseDirPath.resolve("resources");
    final var jsonDirPath = tempDir.resolve("simulatedRemoteContextFiles");
    Files.createDirectory(jsonDirPath, PERMISSIONS);

    // Create a context definition that only references jar A
    final var testContextDef =
        ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-256", jarAOrigLocation);
    final String testContextDefJson = testContextDef.toJson();
    final File testContextDefFile = jsonDirPath.resolve("testContextDefinition.json").toFile();
    Files.writeString(testContextDefFile.toPath(), testContextDefJson, StandardOpenOption.CREATE);
    assertTrue(Files.exists(testContextDefFile.toPath()));

    Resource jarAResource = testContextDef.getResources().iterator().next();
    String jarALocalFileName = LocalStore.localResourceName(jarAResource);

    final String[] names = this.getUniqueNames(1);
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      List<String> tservers = client.instanceOperations().getTabletServers();
      Collections.sort(tservers);
      assertEquals(3, tservers.size());

      final String tableName = names[0];

      final IngestParams params = new IngestParams(client.properties(), tableName, 100);
      params.cols = 10;
      params.dataSize = 10;
      params.startRow = 0;
      params.columnFamily = "test";
      params.createTable = true;
      params.numsplits = 3;
      params.flushAfterRows = 0;

      TestIngest.createTable(client, params);

      // Confirm 4 tablets, spread across 3 tablet servers
      client.instanceOperations().waitForBalance();

      final List<TabletMetadata> tm = getLocations(((ClientContext) client).getAmple(),
          client.tableOperations().tableIdMap().get(tableName));
      assertEquals(4, tm.size()); // 3 tablets

      final Set<String> tabletLocations = new TreeSet<>();
      tm.forEach(t -> tabletLocations.add(t.getLocation().getHostPort()));
      assertEquals(3, tabletLocations.size()); // 3 locations

      // both collections are sorted
      assertIterableEquals(tservers, tabletLocations);

      TestIngest.ingest(client, params);

      final VerifyParams vp = new VerifyParams(client.properties(), tableName, params.rows);
      vp.cols = params.cols;
      vp.rows = params.rows;
      vp.dataSize = params.dataSize;
      vp.startRow = params.startRow;
      vp.columnFamily = params.columnFamily;
      vp.cols = params.cols;
      VerifyIngest.verifyIngest(client, vp);

      // Set the table classloader context. The context is the URL to the context definition file
      final String contextURL = testContextDefFile.toURI().toURL().toString();
      client.tableOperations().setProperty(tableName, Property.TABLE_CLASSLOADER_CONTEXT.getKey(),
          contextURL);

      // check that the table is returning unique values
      // before applying the iterator
      final byte[] jarAValueBytes = "foo".getBytes(UTF_8);
      assertEquals(0, countExpectedValues(client, tableName, jarAValueBytes));
      Set<String> refFiles = getReferencedFiles();
      assertEquals(1, refFiles.size());
      assertTrue(refFiles
          .contains(resourcesDirPath.resolve(jarALocalFileName).toUri().toURL().toString()));

      // Attach a scan iterator to the table
      IteratorSetting is = new IteratorSetting(101, "example", ITER_CLASS_NAME);
      client.tableOperations().attachIterator(tableName, is, EnumSet.of(IteratorScope.scan));

      // confirm that all values get transformed to "foo"
      // by the iterator
      int count = 0;
      while (count != 1000) {
        count = countExpectedValues(client, tableName, jarAValueBytes);
      }
      refFiles = getReferencedFiles();
      assertEquals(1, refFiles.size());
      assertTrue(refFiles
          .contains(resourcesDirPath.resolve(jarALocalFileName).toUri().toURL().toString()));

      // Update the context definition to point to jar B
      final ContextDefinition testContextDefUpdate =
          ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarBOrigLocation);
      final String testContextDefUpdateJson = testContextDefUpdate.toJson();
      Files.writeString(testContextDefFile.toPath(), testContextDefUpdateJson,
          StandardOpenOption.TRUNCATE_EXISTING);
      assertTrue(Files.exists(testContextDefFile.toPath()));

      Resource jarBResource = testContextDefUpdate.getResources().iterator().next();
      String jarBLocalFileName = LocalStore.localResourceName(jarBResource);

      // Wait 2x the monitor interval
      Thread.sleep(2 * MONITOR_INTERVAL_SECS * 1000);

      // Rescan with same iterator class name
      // confirm that all values get transformed to "bar"
      // by the iterator
      final byte[] jarBValueBytes = "bar".getBytes(UTF_8);
      assertEquals(1000, countExpectedValues(client, tableName, jarBValueBytes));
      refFiles = getReferencedFiles();
      assertEquals(2, refFiles.size());
      assertTrue(refFiles
          .contains(resourcesDirPath.resolve(jarALocalFileName).toUri().toURL().toString()));
      assertTrue(refFiles
          .contains(resourcesDirPath.resolve(jarBLocalFileName).toUri().toURL().toString()));

      // Copy jar A, create a context definition using the copy, then
      // remove the copy so that it's not found when the context classloader
      // updates.
      var jarAPath = Path.of(jarAOrigLocation.toURI());
      var jarAPathParent = jarAPath.getParent();
      assertNotNull(jarAPathParent);
      var jarACopy = jarAPathParent.resolve("jarACopy.jar");
      assertTrue(!Files.exists(jarACopy));
      Files.copy(jarAPath, jarACopy, StandardCopyOption.REPLACE_EXISTING);
      assertTrue(Files.exists(jarACopy));

      final ContextDefinition testContextDefUpdate2 =
          ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarACopy.toUri().toURL());
      Files.delete(jarACopy);
      assertTrue(!Files.exists(jarACopy));

      final String testContextDefUpdateJson2 = testContextDefUpdate2.toJson();
      Files.writeString(testContextDefFile.toPath(), testContextDefUpdateJson2,
          StandardOpenOption.TRUNCATE_EXISTING);
      assertTrue(Files.exists(testContextDefFile.toPath()));

      // Wait 2x the monitor interval
      Thread.sleep(2 * MONITOR_INTERVAL_SECS * 1000);

      // Rescan and confirm that all values get transformed to "bar"
      // by the iterator. The previous classloader is still being used after
      // the monitor interval because the jar referenced does not exist.
      assertEquals(1000, countExpectedValues(client, tableName, jarBValueBytes));
      refFiles = getReferencedFiles();
      assertEquals(2, refFiles.size());
      assertTrue(refFiles
          .contains(resourcesDirPath.resolve(jarALocalFileName).toUri().toURL().toString()));
      assertTrue(refFiles
          .contains(resourcesDirPath.resolve(jarBLocalFileName).toUri().toURL().toString()));

      // Wait 2 minutes, 2 times the UPDATE_FAILURE_GRACE_PERIOD_MINS
      Thread.sleep(120_000);

      // Scan of table with iterator setting should now fail.
      final Scanner scanner2 = client.createScanner(tableName);
      RuntimeException re =
          assertThrows(RuntimeException.class, () -> scanner2.iterator().hasNext());
      Throwable cause = re.getCause();
      assertTrue(cause instanceof AccumuloServerException);
    }
  }

  private Set<String> getReferencedFiles() {
    final Map<String,List<String>> referencedFiles = new HashMap<>();
    for (VirtualMachineDescriptor vmd : VirtualMachine.list()) {
      if (vmd.displayName().contains("org.apache.accumulo.start.Main")
          && !vmd.displayName().contains("zookeeper")) {
        LOG.info("Attempting to connect to {}", vmd.displayName());
        try {
          var vm = VirtualMachine.attach(vmd);
          String connectorAddress = vm.getAgentProperties()
              .getProperty("com.sun.management.jmxremote.localConnectorAddress");
          if (connectorAddress == null) {
            connectorAddress = vm.startLocalManagementAgent();
          }
          var url = new JMXServiceURL(connectorAddress);
          try (var connector = JMXConnectorFactory.connect(url)) {
            var mbsc = connector.getMBeanServerConnection();
            var proxy = JMX.newMXBeanProxy(mbsc, ContextClassLoadersMXBean.getObjectName(),
                ContextClassLoadersMXBean.class);
            referencedFiles.putAll(proxy.getReferencedFiles());
          }
        } catch (MalformedObjectNameException | AttachNotSupportedException | IOException e) {
          LOG.error("Error getting referenced files from {}", vmd.displayName(), e);
        }
      }
    }
    Set<String> justTheFiles = new HashSet<>();
    referencedFiles.values().forEach(justTheFiles::addAll);
    LOG.info("Referenced files with contexts: {}", referencedFiles);
    LOG.info("Referenced files: {}", justTheFiles);
    return justTheFiles;
  }

  private int countExpectedValues(AccumuloClient client, String table, byte[] expectedValue)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    Scanner scanner = client.createScanner(table);
    int count = 0;
    for (Entry<Key,Value> e : scanner) {
      if (Arrays.equals(e.getValue().get(), expectedValue)) {
        count++;
      }
    }
    return count;
  }

  private static List<TabletMetadata> getLocations(Ample ample, String tableId) {
    try (TabletsMetadata tabletsMetadata = ample.readTablets().forTable(TableId.of(tableId))
        .fetch(TabletMetadata.ColumnType.LOCATION).build()) {
      return tabletsMetadata.stream().collect(Collectors.toList());
    }
  }
}
