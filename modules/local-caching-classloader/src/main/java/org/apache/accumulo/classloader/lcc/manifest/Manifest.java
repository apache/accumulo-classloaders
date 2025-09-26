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
package org.apache.accumulo.classloader.lcc.manifest;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import org.apache.accumulo.classloader.lcc.Constants;

public class Manifest {
  private final int monitorIntervalSeconds;
  private final Map<String,ContextDefinition> contexts;
  private volatile transient byte[] checksum = null;

  public Manifest(int monitorIntervalSeconds, Map<String,ContextDefinition> contexts) {
    this.monitorIntervalSeconds = monitorIntervalSeconds;
    this.contexts = contexts;
  }

  public int getMonitorIntervalSeconds() {
    return monitorIntervalSeconds;
  }

  public Map<String,ContextDefinition> getContexts() {
    return contexts;
  }

  public synchronized byte[] getChecksum() throws NoSuchAlgorithmException {
    if (checksum == null) {
      checksum =
          MessageDigest.getInstance("MD5").digest(Constants.GSON.toJson(this).getBytes(UTF_8));
    }
    return checksum;
  }
}
