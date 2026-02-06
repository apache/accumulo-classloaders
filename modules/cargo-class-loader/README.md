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
# Cargo ClassLoader Factory

`CargoClassLoaderFactory` implements Accumulo's `ContextClassLoaderFactory`
SPI. For this implementation, "cargo" refers to a set of remote resources
(typically, `.jar` files) listed in a JSON-formatted cargo manifest file.

The URL to the cargo manifest file, as a String, is used for the context
parameter.  In return, this factory transports the cargo from the remote
locations specified in the manifest to a local storage cache, where they will
be used to produce a corresponding `ClassLoader` instance. The specified URL
will then be monitored for any changes to the cargo manifest at that location,
at the monitoring interval specified in the manifest.

## Introduction

This factory creates `ClassLoader` instances that point to locally cached
copies of remote resource files. In this way, this factory allows placing
common resources in a remote location for use across many hosts, but without
many of the problems that can occur in alternate solutions.

This factory uses a storage cache in the local filesystem for any files it
downloads from a remote URL.

To use this factory, one must store resource files in a location that can be
specified by a supported URL, and then must create a JSON-formatted cargo
manifest file that contains a monitoring interval (in seconds, greater than 0),
and a list of resource URLs along with a checksum for each resource file. This
cargo manifest file must then be stored somewhere where this factory can
download it, and use the URL to that file as the `context` parameter for this
factory's `getClassLoader(String context)` method.

This factory can handle cargo manifest and cargo resource URLs of any type that
are supported by your application via a registered
[URLStreamHandlerProvider][1], such as the built-in `file:` and `http:` types.
A provider that handles `hdfs:` URL types must be provided by the user. This
may be provided by the Apache Hadoop project, or by another library. A
reference implementation is available [elsewhere in this project][2].

Here is an example cargo manifest file:

```json
{
  "monitorIntervalSeconds": 5,
  "resources": [
    {
      "location": "file:/home/user/ClassLoaderTestA/TestA.jar",
      "algorithm": "MD5",
      "checksum": "ae5e8248a9243751d60dbcaaedeb93ba"
    },
    {
      "location": "hdfs://localhost:8020/contextB/TestB.jar",
      "algorithm": "SHA-256",
      "checksum": "ed95fe130090fd64c2caddc3e37555ada8ba49a91bfd8ec1fd5c989d340ad0e0"
    },
    {
      "location": "http://localhost:80/TestC.jar",
      "algorithm": "SHA3-224",
      "checksum": "958f12ddc5acf87c2fe0ceed645327bb0c92e268acf915c4a374c14b"
    },
    {
      "location": "https://localhost:80/TestD.jar",
      "algorithm": "SHA-512/224",
      "checksum": "f7f982521ceb8ca97662973ada9b92b86de6bbaf233f14fd47efd792"
    }
  ]
}
```

## How it Works

When this factory receives a request for a `ClassLoader` for a given URL, it
downloads a copy of the cargo manifest file and parses it. If it has recently
acquired that cargo manifest file, based on the monitoring interval from a
previous retrieval, it can skip this step and use the manifest from the earlier
retrieval, which is kept up-to-date by the background monitoring process that
started when it was previously retrieved. Once it has the cargo manifest, it
then returns a `ClassLoader` instance containing the resources defined in that
manifest, first downloading any missing resources and verifying them using the
checksums in manifest.

`ClassLoader` instances are stored in a de-deduplicating cache in memory with a
minimum lifetime of 24 hours. So, no two instances will ever exist in a process
for the same cargo manifest.

If this cargo manifest had not previously been downloaded, a background
monitoring task is set up to ensure the URL is watched for any changes to the
cargo manifest. This monitoring continues for as long as there exists
`ClassLoader` instances in the system that were constructed from the manifest
file at that URL (at least 24 hours, since that is the minimum time they will
exist in the de-duplicating cache).

## Local Storage Cache

The local storage cache location is configured by the user by setting the
required Accumulo property named `general.custom.classloader.cargo.cache.dir`
to a directory on the local filesystem. This location may be specified as an
absolute path or as a URL representing an absolute path with the `file` scheme.
The location, and its directory structure will be created on first use, if it
does not already exist. This will cause an error if the application does not
have permission to create the directories.

The selected location should be a persistent location with plenty of space to
store downloaded resources (usually jar files), and should be writable by all
the processes which use this factory to share the same resources. You may wish
to pre-create the base directory specified by the property, and the three
sub-directories, `manifests`, `resources`, and `working`, to set the appropriate
permissions and ACLs.

Resources downloaded to this cache may be used by multiple cargo manifests,
threads, and processes, so be very careful when removing old contents to ensure
that they are no longer needed. If a resource file is deleted from the local
storage cache while a `ClassLoader` exists that references it, that
`ClassLoader` may, and probably will, stop working correctly. Similarly, files
that have been downloaded should not be modified, because any modification will
likely cause unexpected behavior to classloaders still using the file.

* Do **NOT** use a temporary directory for the local storage cache location.
* The local storage cache location **MUST** use a filesystem that supports
  atomic moves and hard links.

## Security

The Accumulo property `general.custom.classloader.cargo.allowed.urls.pattern`
is another required parameter. It is used to limit the allowed URLs that can be
fetched when downloading cargo manifests or cargo resources. Since the process
using this factory will be using its own permissions to fetch resources, and
placing a copy of those resources in a local directory where others may access
them, that presents presents a potential file disclosure security risk. This
property allows a system administrator to mitigate that risk by restricting
access to only approved URLs. (e.g. to exclude non-approved locations like
`file:/path/to/accumulo.properties` or
`hdfs://host/path/to/accumulo/rfile.rf`).

An example value for this property might look like:
`https://example.com/path/to/manifests/.*` or
`(file:/etc|hdfs://example[.]com:9000)/path/to/manifests/.*`

Note: this property affects all URLs fetched by this factory, including cargo
manifest URLs and any resource URLs defined inside any fetched cargo
manifest. It should be updated by a system maintainer if any new cargo
manifests have need to use new locations. It may be updated on a running
system, and will take effect after approximately a minute.

## Creating a CargoManifest file

Users may take advantage of the `CargoManifest.create(int,String,URL[])`
method to construct a `CargoManifest` object, programmatically. This will
calculate the checksums of the classpath elements. `CargoManifest.toJson()`
can be used to serialize the `CargoManifest` to a `String` to store in a
file.

Alternatively, if this library's jar is built and placed onto Accumulo's
`CLASSPATH`, then one can run `bin/accumulo create-cargo-manifest` to
create the CargoManifest json file using the command-line. The resulting
json is printed to stdout and can be redirected to a file. The command takes
two arguments:

1. the monitor interval, in seconds (e.g. `-i 300`),
2. an optional checksum algorithm to use (e.g. `-a 'SHA3-512'`), and
3. a list of file URLs (e.g. `hdfs://host:port/path/to/one.jar file://host/path/to/two.jar`)

## Updating a CargoManifest file

This factory uses a background thread to fetch the cargo manifest file at its
initial URL using the interval specified in the manifest file the last time it
was retrieved. The manifest file at a watched URL can be replaced at any time
with any changes, including to changes to the monitor interval and resources.
When the cargo manifest is next retrieved, the new cargo manifest file will be
used as though it were an entirely new cargo manifest at that URL. The next
retrieval will occur after the monitor interval read from the most recent
retrieval elapses. Changes to the cargo resources in any way will result in
those new resources being downloaded, verified, and a new `ClassLoader`
instance created and ready to be returned the next time `getClassLoader(String
context)` is called with that URL.

Note: if the contents of a cargo manifest file change in only inconsequential
ways, such as JSON formatting changes, then those changes will not trigger any
new downloads or `ClassLoader` staging. The is because the cargo manifest JSON
files are normalized prior to computing their checksum to determine if any
changes have occurred.

## Error Handling

If there is an exception in creating the initial `ClassLoader`, such as being
unable to retrieve the cargo manifest file at the specified URL, or if a cargo
resource does not match its checksum in the manifest, then a
`ContextClassLoaderException` is thrown. If there is an exception when updating
the classloader, then the exception is logged and the classloader is not
updated. Calls to `getClassLoader(String context)` will return the most recent
classloader with valid contents.

The property `general.custom.classloader.cargo.update.grace.minutes` determines
how long the update process continues to return the most recent valid
classloader when an exception occurs in the background update thread. A zero
value (default) will cause the most recent valid classloader to be returned.
Otherwise, if a non-zero number is configured, then monitoring will stop after
the update has failed for that number of minutes. Once monitoring has stopped,
any subsequent calls to `getClassLoader(String context)` will behave as it
would during an initial request, throwing a `ContextClassLoaderException` if
the cargo manifest cannot be retrieved or a `ClassLoader` cannot be constructed
at that time.

## Cleanup

Because the cache directory is shared among multiple processes, and one process
can't know what the other processes are doing, this class cannot always clean
up the shared cache directory of unused resources automatically. It is left to
the user to remove unused files from the cache. The local storage is organized
into several directories, which are explained here to aid in understanding when
unused files can be safely removed.

### Manifests

The `manifests` directory contents are always safe to delete. These contain
only copies of the JSON files from which a ClassLoader was constructed. It is
never used by this factory, and copies are placed here solely to provide more
information to a user. Because these files are small text files and contain
useful information, it is generally not recommended to delete these, because it
may impair troubleshooting.

### Resources

The `resources` directory contains a shared pool of remote resource files that
have been fetched for all cargo manifests (typically, `.jar` files). The files
in this directory are generally safe to delete any time. However, some
considerations should be made:

1. Deleting resources that are still needed will cause them to be downloaded
   again the next time they are needed, which may cause an increase in network
   activity.
2. If any of the removed files had hard-linked "copies" in the `working`
   directory, the newly downloaded copy will increase the total amount of
   storage (whereas the original would have shared storage space with the
   hard-linked "copies").

### Working

The `working` directory contains temporary files for files currently being
downloaded, and temporary directories containing hard-linked "copies" of files
from the `resources` directory. These files and directories contain the process
ID (PID) for the process that created them. Normally, these files are
automatically cleaned up, but if a process is killed before that can happen,
they may be left behind. The files with the PID in them can safely be removed,
so long as the process that created them has been terminated.

This directory also contains files that do not contain a PID. These files end
with the `.downloading` suffix and exist to signal that a resource file is
currently being downloaded by a process. These files are very small, containing
only the PID of the most recent process to attempt downloading the file. They
are removed when a download completes, or whenever the next time the
corresponding resource file is used, if it has already been successfully
downloaded by a previously failed process. Removing them won't break the
application in any way, but doing so may result in a redundant download, which
can result in increased network activity or storage space (see the previous
section for considerations regarding the `resources` directory).

## Accumulo Configuration

To use this with Accumulo:

1. Set the following Accumulo properties:
   * `general.context.class.loader.factory=org.apache.accumulo.classloader.cargo.CargoClassLoaderFactory`
   * `general.custom.classloader.cargo.cache.dir=file://path/to/some/directory`
   * `general.custom.classloader.cargo.allowed.urls.pattern=someRegexPatternForAllowedUrls`
   * (optional) `general.custom.classloader.cargo.update.grace.minutes=30`
2. Set the following table property to link to a cargo manifest file. For example:
   * `table.class.loader.context=(file|hdfs|http|https)://path/to/cargo/manifest.json`


[1]: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/net/spi/URLStreamHandlerProvider.html
[2]: https://github.com/apache/accumulo-classloaders/tree/main/modules/hdfs-urlstreamhandler-provider
