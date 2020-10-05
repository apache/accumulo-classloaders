<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Setup

  After running `mvn clean package` add the built jars to HDFS:
  
  ```
  hadoop fs -mkdir -p /iterators/example-a
  hadoop fs -put modules/example-iterators-a/target/example-iterators-a-1.0.0-SNAPSHOT.jar /iterators/example-a/examples.jar
  
  hadoop fs -mkdir -p /iterators/example-b
  hadoop fs -put modules/example-iterators-b/target/example-iterators-b-1.0.0-SNAPSHOT.jar /iterators/example-b/examples.jar
  
  hadoop fs -cp /iterators/example-a/examples.jar /iterators/examples.jar
  ```
  
  Copy the new class loader jar to `/tmp`: 
  ```
  cp ./modules/vfs-class-loader/target/vfs-reloading-classloader-1.0.0-SNAPSHOT.jar /tmp/.
  ```

# Running Accumulo with new VFS ClassLoader as SystemClassLoader

	## Configure Accumulo to use new classloader
	
	a. Add vfs-reloading-classloader-1.0.0-SNAPSHOT.jar to CLASSPATH
	b. Add "-Djava.system.class.loader=org.apache.accumulo.classloader.vfs.ReloadingVFSClassLoader" to JAVA_OPTS
	c. Add "-Dvfs.class.loader.classpath=hdfs://localhost:9000/iterators/examples.jar" to JAVA_OPTS
	d. Add "-Dvfs.classpath.monitor.seconds=10" to JAVA_OPTS
	e. (optional) Add "-Dvfs.class.loader.debug=true" to JAVA_OPTS
	
	## Test setting iterator
	```
	Create table, insert some test data
	createtable test
	insert a b c this_is_a_test
	scan  // The value should be "this_is_a_test"
	setiter -class org.apache.accumulo.classloader.vfs.examples.ExampleIterator -scan -t test -name example -p 100
	scan  // The value should be the word "foo"
	deleteiter -n example -t test -scan
	scan  // The value should be "this_is_a_test"
	```
	
	## Test reloading function (NOT WORKING)
	
	a. Copy the example-b jar to /iterators/examples.jar:
	```
	hadoop fs -cp -f /iterators/example-b/examples.jar /iterators/examples.jar
	```
	b. Wait 10+ seconds, then
	```
	scan  // The value should be "this_is_a_test"
	setiter -class org.apache.accumulo.classloader.vfs.examples.ExampleIterator -scan -t test -name example -p 100
	scan  // The value should be the word "bar"
	deleteiter -n example -t test -scan
	scan  // The value should be "this_is_a_test"
	```
		
	## Reset the state of the jar
    ```
    hadoop fs -cp /iterators/example-a/examples.jar /iterators/examples.jar
    ```
      
# Setting scan context on table (Legacy)

	a. Set Accumulo Classpath Context property:
	```
	config -s general.vfs.context.classpath.cx1=hdfs://localhost:9000/iterators/example-a/examples.jar
	```
	b. Set Accumulo Table Context property:
	```
	config -t test -s table.classpath.context=cx1
	```
	c. Test context classpath iterator setting:
	```
	scan  // The value should be "this_is_a_test"
	setiter -class org.apache.accumulo.classloader.vfs.examples.ExampleIterator -scan -t test -name example -p 100
	scan  // The value should be the word "foo"
	deleteiter -n example -t test -scan
	scan  // The value should be "this_is_a_test"
	```

# Setting scan context on table (New)

	a. Add "-Dorg.apache.accumulo.classloader.vfs.context.ReloadingVFSContextClassLoaderFactory" to JAVA_OPTS
	b. Add "-Dvfs.context.class.loader.config=<path to config file>" to JAVA_OPTS
	
	

