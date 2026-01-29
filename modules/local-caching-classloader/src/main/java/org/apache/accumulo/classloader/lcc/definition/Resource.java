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
package org.apache.accumulo.classloader.lcc.definition;

import java.net.URL;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

public class Resource {

  private URL location;
  private String algorithm;
  private String checksum;

  public Resource() {}

  public Resource(URL location, String algorithm, String checksum) {
    this.location = location;
    this.algorithm = normalizeAlgorithm(algorithm);
    this.checksum = checksum;
  }

  protected static String normalizeAlgorithm(String algorithm) {
    try {
      // try to normalize the algorithm name by finding the provider, then getting the MessageDigest
      // service for that algorithm, and asking that service for the canonical algorithm name
      return MessageDigest.getInstance(algorithm).getProvider()
          .getService("MessageDigest", algorithm).getAlgorithm();
    } catch (NoSuchAlgorithmException e) {
      // just keep the provided name if we can't find a provider for that algorithm
      return algorithm;
    }
  }

  public URL getLocation() {
    return location;
  }

  public String getAlgorithm() {
    return algorithm;
  }

  public String getChecksum() {
    return checksum;
  }

  public String getFileName() {
    var nameAsPath = Path.of(location.getPath()).getFileName();
    if (nameAsPath == null) {
      return "unknown";
    }
    String name = nameAsPath.toString();
    if (name.isBlank()) {
      return "unknown";
    }
    return name;
  }

  @Override
  public int hashCode() {
    return Objects.hash(location, checksum);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Resource other = (Resource) obj;
    return Objects.equals(checksum, other.checksum) && Objects.equals(location, other.location);
  }

}
