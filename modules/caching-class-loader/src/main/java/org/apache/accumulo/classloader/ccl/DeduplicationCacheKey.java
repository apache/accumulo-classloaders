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
package org.apache.accumulo.classloader.ccl;

import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.apache.accumulo.classloader.ccl.manifest.Manifest;

class DeduplicationCacheKey {
  private final String location;
  private final Manifest manifest;

  public DeduplicationCacheKey(String location, Manifest manifest) {
    this.location = requireNonNull(location);
    this.manifest = requireNonNull(manifest);
  }

  public String getLocation() {
    return location;
  }

  public Manifest getManifest() {
    return manifest;
  }

  @Override
  public int hashCode() {
    return hash(location, manifest);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof DeduplicationCacheKey) {
      var o = (DeduplicationCacheKey) obj;
      return Objects.equals(location, o.location) && Objects.equals(manifest, o.manifest);
    }
    return false;
  }

  @Override
  public String toString() {
    return manifest.getChecksumAlgorithm() + " (" + location + ") = " + manifest.getChecksum();
  }

}
