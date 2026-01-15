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
# Local Caching ClassLoader

`LocalCachingContextClassLoaderFactory` implements Accumulo's
`ContextClassLoaderFactory` SPI. Given a context parameter, supplied as a
String containing a URL to a remote context definition file, it will produce
and return `ClassLoader` instances to load classes and resources for a Java
application, based on the list of resource URLs contained in the remote context
definition. It will also monitor the URL to the context definition file for any
changes, at the monitoring interval specified in the context definition file.

## Introduction

This factory creates `ClassLoader` instances that point to locally cached
copies of remote resource files. In this way, this factory allows placing
common resources in a remote location for use across many hosts, but without
many of the problems that can occur when loading resources from a remote
location.

This factory uses a storage cache in the local filesystem for any files it
downloads from a remote URL.

To use this factory, one must store resource files in a location that can be
specified by a supported URL, and then must create a JSON-formatted context
definition file that contains a monitoring interval (in seconds, greater than
0), and a list of resource URLs along with a checksum for each resource file.
This context definition file must then be stored somewhere where this factory
can download it, and use the URL to that context definition file as the
`context` parameter for this factory's `getClassLoader(String context)` method.

This factory can handle context and resource URLs that use the `file`, `hdfs`,
`http`, or `https` URL scheme.

Here is an example context definition file:

```json
{
  "monitorIntervalSeconds": 5,
  "resources": [
    {
      "location": "file:/home/user/ClassLoaderTestA/TestA.jar",
      "checksum": "a10883244d70d971ec25cbfa69b6f08f"
    },
    {
      "location": "hdfs://localhost:8020/contextB/TestB.jar",
      "checksum": "a02a3b7026528156fb782dcdecaaa097"
    },
    {
      "location": "http://localhost:80/TestC.jar",
      "checksum": "f464e66f6d07a41c656e8f4679509215"
    }
  ]
}
```

## How it Works

When this factory receives a request for a `ClassLoader` for a given URL, it
downloads a copy of the context definition file and parses it. If it has
recently acquired that context definition file, based on the monitoring
interval from a previous retrieval, it can skip this step and use the
definition from the earlier retrieval, which is kept up-to-date by the
background monitoring process that started when it was previously retrieved.
Once it has the context definition, it then returns a `ClassLoader` instance
containing the resources defined in that context definition file, first
downloading any missing resources and verifying them using the checksums in the
context definition file.

`ClassLoader` instances are stored in a de-deduplicating cache in memory with a
minimum lifetime of 24 hours. So, no two instances will ever exist in a process
for the same context definition.

If this context definition had not previously been downloaded, a background
monitoring task is set up to ensure the URL is watched for any changes to the
context definition. This monitoring continues for as long as there exists
`ClassLoader` instances in the system that were constructed from the definition
file at that URL (at least 24 hours, since that is the minimum time they will
exist in the de-duplicating cache).

## Local Storage Cache

The local storage cache location is configured by the user by setting the
Accumulo property named `general.custom.classloader.lcc.cache.dir` to a
directory on the local filesystem. This location may be specified as an
absolute path or as a URL representing an absolute path with the `file` scheme.

The selected location should be a persistent location with plenty of space to
store downloaded resources (usually jar files), and should be writable by all
the processes which use this factory to share the same resources.

Resources downloaded to this cache may be used by multiple contexts, threads,
and processes, so be very careful when removing old contents to ensure that
they are no longer needed. If a resource file is deleted from the local storage
cache while a `ClassLoader` exists that references it, that `ClassLoader` may,
and probably will, stop working correctly. Similarly, files that have been
downloaded should not be modified, because checksums are only verified on first
download, and any modification will likely cause unexpected behavior.

* Do **NOT** use a temporary directory for the local storage cache location.
* The local storage cache location **MUST** use a filesystem that supports
  atomic moves.

## Creating a ContextDefinition file

Users may take advantage of the `ContextDefinition.create(int,URL[])` method to
construct a `ContextDefinition` object. This will calculate the checksums of
the classpath elements. `ContextDefinition.toJson()` can be used to serialize
the `ContextDefinition` to a `String` to store in a file.

Alternatively, if the local-caching-classloader jar is built and placed into the $ACCUMULO_HOME/lib directory, then
the `$ACCUMULO_HOME/bin/accumulo create-context-definition` command can be used to create the
ContextDefinition json. The command takes 3 arguments: the name of the context, the monitor interval,
and a list of file URLs. The resulting json is printed to stdout and can be redirected to a file. Users
may take advantage of the `ContextDefinition.create` and `ContextDefinition.toJson` methods to
construct a ContextDefinition object if they wish to do this programmatically.

## Updating a ContextDefinition file

This factory uses a background thread to fetch the context definition file at
its initial URL using the interval specified in the definition file the last
time it was retrieved. The definition file at a watched URL can be replaced at
any time with any changes, including to changes to the monitor interval and
resources. When the context definition is next retrieved, the new context
definition file will be used as though it were an entirely new context at that
URL. The next retrieval will occur after the monitor interval read from the
most recent retrieval elapses. Changes to the context resources in any way will
result in those new resources being downloaded, verified, and a new
`ClassLoader` instance created and ready to be returned the next time
`getClassLoader(String context)` is called with that context URL.

Note: if the contents of a context definition file change in only
inconsequential ways, such as JSON formatting changes, then those changes will
not trigger any new downloads or `ClassLoader` staging. The is because the
context definition JSON files are normalized prior to computing their checksum
to determine if any changes have occurred.

## Error Handling

If there is an exception in creating the initial `ClassLoader`, then a
`ContextClassLoaderException` is thrown. If there is an exception when updating
the classloader, then the exception is logged and the classloader is not
updated. Calls to `getClassLoader(String context)` will return the most recent
classloader with valid contents. If the checksum of a downloaded resource does
not match the checksum in the context definition file, then the downloaded
version of the file is deleted from the context cache directory so that it can
be retried at the next interval.

The property `general.custom.classloader.lcc.update.grace.minutes` determines
how long the update process continues to return the most recent valid
classloader when an exception occurs in the background update thread. A zero
value (default) will cause the most recent valid classloader to be returned.
Otherwise, if a non-zero number is configured, then monitoring will stop after
the update has failed for that number of minutes. Once monitoring has stopped,
any subsequent calls to `getClassLoader(String context)` will behave as it
would during an initial request, throwing a `ContextClassLoaderException` if
the context definition cannot be retrieved or a `ClassLoader` cannot be
constructed at that time.

## Cleanup


Because the cache directory is shared among multiple processes, and one process
can't know what the other processes are doing, this class cannot clean up the
shared cache directory of unused resources. It is left to the user to remove
unused files from the cache. While the context definition JSON files are always
safe to delete, it is not recommended to do so for any that are still in use,
because they can be useful for troubleshooting.

To aid in this task a JMX MXBean has been created to expose the files that are still referenced
 by the classloaders that are created. For an example of how to use this MXBean, please see the test method `MiniAccumuloClusterClassLoaderFactoryTest.getReferencedFiles`. This method attaches to the
local Accumulo JVM processes to get the set of referenced files. It should be safe to delete files that are located
in the base cache directory (set by property `general.custom.classloader.lcc.cache.dir`) that are NOT in the set
of referenced files and existed before references were gathered.

**IMPORTANT**: as mentioned earlier, it is not safe to delete resource files
that are still referenced by any `ClassLoader` instances. Each `ClassLoader`
instance assumes that the locally cached resources exist and can be read. They
will not attempt to download any files.  Downloading and verifying files only
occurs when `ClassLoader` instances are initially created for a context
definition.


## Accumulo Configuration

To use this with Accumulo:

1. Set the following Accumulo properties:
   * `general.context.class.loader.factory=org.apache.accumulo.classloader.lcc.LocalCachingContextClassLoaderFactory`
   * `general.custom.classloader.lcc.cache.dir=file://path/to/some/directory`
2. Set the following table property:
   * `table.class.loader.context=(file|hdfs|http|https)://path/to/context/definition.json`

