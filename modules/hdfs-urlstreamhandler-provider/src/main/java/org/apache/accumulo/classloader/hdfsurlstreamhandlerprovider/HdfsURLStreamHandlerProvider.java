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
package org.apache.accumulo.classloader.hdfsurlstreamhandlerprovider;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.spi.URLStreamHandlerProvider;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;

@AutoService(URLStreamHandlerProvider.class)
public class HdfsURLStreamHandlerProvider extends URLStreamHandlerProvider {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsURLStreamHandlerProvider.class);

  private static class HdfsURLConnection extends URLConnection {

    private final Supplier<Configuration> conf = Suppliers.memoize(Configuration::new);

    private InputStream is;

    public HdfsURLConnection(URL url) {
      super(requireNonNull(url, "null url argument"));
      Preconditions.checkArgument("hdfs".equals(url.getProtocol()),
          "HdfsUrlConnection only supports hdfs: URLs");
    }

    @Override
    public void connect() throws IOException {
      Preconditions.checkState(is == null, "Already connected");
      LOG.debug("Connecting to {}", url);
      final URI uri;
      try {
        uri = url.toURI();
      } catch (URISyntaxException e) {
        // this came from a URL that should be RFC2396-compliant, so it shouldn't happen
        throw new IllegalArgumentException(e);
      }
      var fs = FileSystem.get(uri, conf.get());
      is = fs.open(new Path(uri));
    }

    @Override
    public InputStream getInputStream() throws IOException {
      if (is == null) {
        connect();
      }
      return is;
    }
  }

  private static class HdfsURLStreamHandler extends URLStreamHandler {
    @Override
    protected HdfsURLConnection openConnection(URL url) throws IOException {
      return new HdfsURLConnection(url);
    }
  }

  private final Supplier<URLStreamHandler> handler = Suppliers.memoize(HdfsURLStreamHandler::new);

  @Override
  public URLStreamHandler createURLStreamHandler(String protocol) {
    return protocol.equals("hdfs") ? handler.get() : null;
  }

}
