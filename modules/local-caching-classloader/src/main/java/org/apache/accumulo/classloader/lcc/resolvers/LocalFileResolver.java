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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import com.google.common.base.Preconditions;

public final class LocalFileResolver extends FileResolver {

  private final File file;

  public LocalFileResolver(URI uri) throws IOException {
    super(uri);

    // Converting to a URL will perform more strict checks, also ensure the protocol is correct.
    var url = uri.toURL();
    Preconditions.checkArgument(url.getProtocol().equals("file"), "Not a file url : " + url);

    if (uri.getHost() != null && !uri.getHost().isBlank()) {
      throw new IOException(
          "Unsupported file uri, only local files are supported. host = " + uri.getHost());
    }

    final Path path = Path.of(uri);
    if (Files.notExists(path)) {
      throw new IOException("File: " + uri + " does not exist.");
    }
    file = path.toFile();
  }

  @Override
  public String getFileName() {
    return file.getName();
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return new BufferedInputStream(Files.newInputStream(file.toPath()));
  }
}
