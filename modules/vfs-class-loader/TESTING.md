 # <!--
 # Licensed to the Apache Software Foundation (ASF) under one
 # or more contributor license agreements.  See the NOTICE file
 # distributed with this work for additional information
 # regarding copyright ownership.  The ASF licenses this file
 # to you under the Apache License, Version 2.0 (the
 # "License"); you may not use this file except in compliance
 # with the License.  You may obtain a copy of the License at
 # 
 #   http://www.apache.org/licenses/LICENSE-2.0
 # 
 # Unless required by applicable law or agreed to in writing,
 # software distributed under the License is distributed on an
 # "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 # KIND, either express or implied.  See the License for the
 # specific language governing permissions and limitations
 # under the License.
 # -->

# Setup

After running `mvn clean package` add the built jars to HDFS:

```
hadoop fs -mkdir -p /iterators/example-a
hadoop fs -mkdir -p /iterators/example-b
hadoop fs -put -f modules/example-iterators-a/target/example-iterators-a-1.0.0-SNAPSHOT.jar /iterators/example-a/examples.jar
hadoop fs -put -f modules/example-iterators-b/target/example-iterators-b-1.0.0-SNAPSHOT.jar /iterators/example-b/examples.jar
hadoop fs -cp -f /iterators/example-a/examples.jar /iterators/examples.jar
hadoop fs -cp -f /iterators/example-b/examples.jar /iterators/context-examples.jar
```

Copy the new class loader jar to /tmp:

```
cp ./modules/vfs-class-loader/target/vfs-reloading-classloader-1.0.0-SNAPSHOT.jar /tmp/.
```

## Running Accumulo with new VFS ClassLoader as SystemClassLoader

### Configure Accumulo to use new classloader

Stop Accumulo if it's running and add the following to the accumulo-env.sh:

```	
a. Add vfs-reloading-classloader-1.0.0-SNAPSHOT.jar to CLASSPATH
b. Add "-Djava.system.class.loader=org.apache.accumulo.classloader.vfs.AccumuloVFSClassLoader" to JAVA_OPTS
c. Add "-Dvfs.class.loader.classpath=hdfs://localhost:9000/iterators/examples.jar" to JAVA_OPTS
d. Add "-Dvfs.classpath.monitor.seconds=10" to JAVA_OPTS
e. (optional) Add "-Dvfs.class.loader.debug=true" to JAVA_OPTS
```
	
### Test setting iterator retrieved from jar in HDFS with System ClassLoader

The goal of this test is to create a table, insert some data, and change the value of the data using an iterator that is loaded from HDFS via the AccumuloVFSClassLoader set up as the System ClassLoader. After setting the iterator the value should be `foo` in subsequent scans.

```
createtable test
insert a b c this_is_a_test
scan
setiter -class org.apache.accumulo.classloader.vfs.examples.ExampleIterator -scan -t test -name example -p 100
scan
```
      
## Setting scan context on table (Legacy)

### Define a Table Context and load the iterator class with the same name, but different behavior

In this test we will define a context name with an associated classpath. Then we will set that context on the table. Note that
we did not change the iterator class name, the context classloader will load a new class with the same name. Scans performed after the context is set on the table should return the value `bar`.
    
a. Set Accumulo Classpath Context property:

```
config -s general.vfs.context.classpath.cx1=hdfs://localhost:9000/iterators/context-examples.jar
config -s general.vfs.context.classpath.cx1.delegation=post
```

b. Set Accumulo Table Context property:

```
config -t test -s table.classpath.context=cx1
```

c. Test context classpath iterator setting:

```
scan
```
	
### Testing Reloading
	
This test will continue from the previous test and we will copy a jar over the jar referenced in the cx1 context classpath. The legacy AccumuloReloadingVFSClassLoader has a hard-coded filesystem monitor time of 5 minutes, so we will need to wait some number of minutes after overwriting the jar before the scans will return the new value of `foo`.

a. Copy the example-a.jar over the context-examples.jar to force a reload. The value in the scan result should change from 'bar' back to 'foo'.

```
hadoop fs -cp -f /iterators/example-a/examples.jar /iterators/context-examples.jar
```

b. Wait 10 minutes for a reload
    
c. Test that class loader has been updated and is returning a new class

```
scan
```
	
### Change the context on the table

In this test we will unset the properties that we set in the previous tests. Instead of testing reloading we are testing that changing the context will have the same effect on the iterator class.

a. Unset prior properties and define two contexts:

```
deleteiter -n example -t test -scan
config -t test -d table.classpath.context
config -d general.vfs.context.classpath.cx1
config -d general.vfs.context.classpath.cx1.delegation
config -s general.vfs.context.classpath.cx1=hdfs://localhost:9000/iterators/example-a/examples.jar
config -s general.vfs.context.classpath.cx1.delegation=post
config -s general.vfs.context.classpath.cx2=hdfs://localhost:9000/iterators/example-b/examples.jar
config -s general.vfs.context.classpath.cx2.delegation=post
```

b. Set Accumulo Table Context property:

```
config -t test -s table.classpath.context=cx1
```

c. Test context classpath iterator setting:

The initial scan command should return the value `this_is_a_test`. After setting the iterator, the scan should return `foo`.

```
scan
setiter -class org.apache.accumulo.classloader.vfs.examples.ExampleIterator -scan -t test -name example -p 100
scan
```

d. Change the context Table Context property:

```
config -t test -s table.classpath.context=cx2
```

e. Test Context change

After the context change, the scan should return `bar`.

```	
scan
```

# Setting scan context on table (New)

For this test we will use the new ReloadingVFSContextClassLoaderFactory for the table context classloaders. 

a. First, let's clean up from the prior tests

```
droptable -f test
config -d general.vfs.context.classpath.cx1
config -d general.vfs.context.classpath.cx1.delegation
config -d general.vfs.context.classpath.cx2
config -d general.vfs.context.classpath.cx2.delegation

```

b. Then, create a file on the local filesystem for the context configuration.

```
{
  "contexts": [
    {
      "name": "cxA",
      "config": {
        "classPath": "hdfs://localhost:9000/iterators/example-a/examples.jar",
        "postDelegate": true,
        "monitorIntervalMs": 10000
      }
    },
    {
      "name": "cxB",
      "config": {
        "classPath": "hdfs://localhost:9000/iterators/example-b/examples.jar",
        "postDelegate": true,
        "monitorIntervalMs": 10000
      }
    }
  ]
}
```

c. Next, shutdown Accumulo and make the following changes in the accumulo configuration. Then re-start Accumulo.

```
a. Add "general.context.class.loader.factory=org.apache.accumulo.classloader.vfs.context.ReloadingVFSContextClassLoaderFactory" to accumulo.properties
b. Add "-Dvfs.context.class.loader.config=file:///path/to/config/file.json" to JAVA_OPTS
```

d. Create the table as we did in the last test. Create a table, insert some data, and change the value of the data using an iterator that is loaded from HDFS via the VFSClassLoader set up as the System ClassLoader. After setting the iterator the value should be `foo` in subsequent scans.


```
createtable test
insert a b c this_is_a_test
scan
setiter -class org.apache.accumulo.classloader.vfs.examples.ExampleIterator -scan -t test -name example -p 100
scan
```

e. Set the table context to cxA. The scan on the table should return the value `foo`.

```
config -t test -s table.classpath.context=cxA
scan
```

f. Set the table context to cxB. The scan on the table should return the value `bar`.

```
config -t test -s table.classpath.context=cxB
scan
```

g. Now, to test the reloading copy /iterators/example-a/examples.jar to /iterators/example-b/examples.jar and rescan. The value from the scan should be `foo`.

```
hadoop fs -cp -f /iterators/example-a/examples.jar /iterators/example-b/examples.jar
scan
```
