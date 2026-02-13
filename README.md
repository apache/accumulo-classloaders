<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Apache Accumulo Classloader Extras

This repository contains a variety of Java classloader-related utilities, or
libraries to support those utilities, for use with Apache Accumulo.

### CachingClassLoaderFactory

An implementation of Accumulo's [ContextClassLoaderFactory][spi] that
downloads, verifies, and locally caches classloader resources using a manifest
file at a specified URL, which itself contained a listing of resource URLs to
download, and corresponding checksums to verify their contents.

See [modules/caching-class-loader/README.md](modules/caching-class-loader/README.md).

### HdfsURLStreamHandlerProvider

An implementation of [URLStreamHandlerProvider][urlprovider] that supports the
`hdfs://` URL scheme, written to support using [HDFS][hadoop] for remote
resource locations in the
[CachingClassLoaderFactory](#CachingClassLoaderFactory), because that project
does not currently implement such a provider.

See [modules/hdfs-urlstreamhandler-provider/README.md](modules/hdfs-urlstreamhandler-provider/README.md).

### VFS ClassLoaderFactory (Legacy)

An experimental implementation of Accumulo's [ContextClassLoaderFactory][spi]
was developed in `modules/vfs-class-loader` in commit
2c3eb3b18d30d9fa18d744584c4595e4b4ffca9f, but was subsequently removed. It
supported using classpaths containing remote URLs, and used Apache commons-vfs2
to interpret remote locations as filesystems containing resource files. Loading
classes from VFS this way can be prone to substantial errors, and it is not
recommended.


[spi]: https://accumulo.apache.org/docs/2.x/apidocs/org/apache/accumulo/core/spi/common/ContextClassLoaderFactory.html
[urlprovider]: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/net/spi/URLStreamHandlerProvider.html
[hadoop]: https://hadoop.apache.org
