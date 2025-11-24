<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Local Caching ClassLoader

The LocalCachingContextClassLoaderFactory is an Accumulo ContextClassLoaderFactory implementation that creates and maintains a
LocalCachingContext. The `LocalCachingContextClassLoaderFactory.getClassLoader(String)` method expects the method
argument to be a valid `file`, `hdfs`, `http` or `https` URL to a context definition file.

The context definition file is a JSON formatted file that contains the name of the context, the interval (in seconds) at which
the context definition file should be monitored, and a list of classpath resources. The LocalCachingContextClassLoaderFactory
creates the LocalCachingContext based on the initial contents of the context definition file, and updates the classloader
as changes are noticed based on the monitoring interval. An example of the context definition file is below.

```
{
    "contextName": "myContext",
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

## Creating a ContextDefinition file

Users may take advantage of the `ContextDefinition.create` method to construct a ContextDefinition object. This
will calculate the checksums of the classpath elements. `ContextDefinition.toJson` can be used to serialize the
ContextDefinition to a file.

## Updating a ContextDefinition file

The LocalCachingContextClassLoaderFactory uses a background thread to fetch the context definition file at the
specified interval. Users can change the context name, monitor interval, and list of resources. Changes to the
context name are ignored however as the context cache directory is created using the context name upon initial
creation. The LocalCachingContextClassLoaderFactory will schedule the next download the of the context
definition file based on the updated monitor interval, and if the list of resources have changed, then they will
be downloaded, verified against their checksums, and used to construct a new ClassLoader for the context.

## Local Caching

The property `general.custom.classloader.lcc.cache.dir` is required to be set to a local directory on the host. The
LocalCachingContext creates a directory at this location for each named context. Each context cache directory
contains a lock file and a copy of each fetched resource that is named in the context definition file using the format:
`fileName_checksum`. The lock file is used with Java's `FileChannel.tryLock` to enable exclusive access (on supported
platforms) to the directory from different processes on the same host.

## Error Handling

If there is an exception in creating the initial classloader, then a ContextClassLoaderException is thrown. If there is
an exception when updating the classloader, then the exception is logged and the classloader is not updated. Calls
to `LocalCachingContextClassLoaderFactory.getClassLoader(String)` will return the most recent classloader
with valid contents. If the checksum of a downloaded resource does not match the checksum in the context definition
file, then the downloaded version of the file is deleted from the context cache directory so that it can be retried
at the next interval.

The property `general.custom.classloader.lcc.update.grace.minutes` determines how long the update process
continues to return the most recent valid classloader when an exception occurs in the background update thread.
A zero value (default) will cause the most recent valid classloader to be returned. Otherwise, the update thread
will fail for N minutes, then clear the reference to the classloader internally. This will cause a subsequent
call to `LocalCachingContextClassLoaderFactory.getClassLoader(String)` to act like the initial call to
create the classloader and return the exception to the calling code.

## Cleanup

Because the cache directory is shared among multiple processes, and one process can't know what the other processes are doing,
this class cannot clean up the shared cache directory. It is left to the user to remove unused context cache directories
and unused old files within a context cache directory.

## Accumulo Configuration

To use this with Accumulo:

  1. Set the following Accumulo site properties: `general.context.class.loader.factory=org.apache.accumulo.classloader.lcc.LocalCachingContextClassLoaderFactory`
`general.custom.lcc.classloader.cache.dir=file://path/to/some/directory`
  
  2. Set the following table property: `table.class.loader.context=(file|hdfs|http|https)://path/to/context/definition.json`


