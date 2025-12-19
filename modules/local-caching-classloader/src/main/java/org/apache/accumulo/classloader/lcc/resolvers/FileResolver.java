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
package org.apache.accumulo.classloader.lcc.resolvers;

import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Objects;

public abstract class FileResolver {

  public static FileResolver resolve(URL url) throws IOException {
    requireNonNull(url, "URL must be supplied");
    switch (url.getProtocol()) {
      case "http":
      case "https":
        return new HttpFileResolver(url);
      case "file":
        return new LocalFileResolver(url);
      case "hdfs":
        return new HdfsFileResolver(url);
      default:
        throw new IOException("Unhandled protocol: " + url.getProtocol());
    }
  }

  private final URL url;

  protected FileResolver(URL url) {
    this.url = url;
  }

  protected URL getURL() {
    return this.url;
  }

  public abstract String getFileName();

  public abstract InputStream getInputStream() throws IOException;

  @Override
  public int hashCode() {
    return hash(url);
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
    FileResolver other = (FileResolver) obj;
    return Objects.equals(url, other.url);
  }

}
