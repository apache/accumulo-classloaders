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
package org.apache.accumulo.classloader.lcc.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class HdfsUrlStreamHandlerFactory implements URLStreamHandlerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsUrlStreamHandlerFactory.class);

  private class HdfsUrlConnection extends URLConnection {

    private final Configuration conf;

    private InputStream is;

    public HdfsUrlConnection(Configuration conf, URL url) {
      super(url);
      Objects.requireNonNull(conf, "null conf argument");
      Objects.requireNonNull(url, "null url argument");
      this.conf = conf;
    }

    @Override
    public void connect() throws IOException {
      Preconditions.checkState(is == null, "Already connected");
      try {
        LOG.debug("Connecting to {}", url);
        URI uri = url.toURI();
        FileSystem fs = FileSystem.get(uri, conf);
        // URI#getPath returns null value if path contains relative path
        // i.e file:root/dir1/file1
        // So path can not be constructed from URI.
        // We can only use schema specific part in URI.
        // Uri#isOpaque return true if path is relative.
        if (uri.isOpaque() && uri.getScheme().equals("file")) {
          is = fs.open(new Path(uri.getSchemeSpecificPart()));
        } else {
          is = fs.open(new Path(uri));
        }
      } catch (URISyntaxException e) {
        throw new IOException(e.toString());
      }
    }

    @Override
    public InputStream getInputStream() throws IOException {
      if (is == null) {
        connect();
      }
      return is;
    }
  }

  private class HdfsUrlStreamHandler extends URLStreamHandler {

    private Configuration conf;

    HdfsUrlStreamHandler(Configuration conf) {
      this.conf = conf;
    }

    @Override
    protected HdfsUrlConnection openConnection(URL url) throws IOException {
      return new HdfsUrlConnection(conf, url);
    }
  }

  private final HdfsUrlStreamHandler handler;

  public HdfsUrlStreamHandlerFactory(Configuration conf) {
    this.handler = new HdfsUrlStreamHandler(conf);
  }

  @Override
  public URLStreamHandler createURLStreamHandler(String protocol) {
    if (protocol.equals("hdfs")) {
      return handler;
    }
    return null;
  }

}
