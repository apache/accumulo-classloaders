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
# HdfsURLStreamHandlerProvider

This library contains a single class that implements
[URLStreamHandlerProvider][1], to provide a stream handler for `hdfs:` URL
types that uses a default Hadoop `Configuration` from your class path. This can
be placed on your classpath to automatically be registered using the Java
`ServiceLoader` when handling `hdfs:` URLs. This libary is unnecessary if
another library on your classpath provides an equivalent handler.


[1]: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/net/spi/URLStreamHandlerProvider.html
