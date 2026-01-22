#! /usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#
# This script is an example of how cleanup could be done
# using the JMX MXBean contained in this module and Jolokia.
# Jolokia exposes the JVM MXBeans via a HTTP API enabling the
# user to interact with the MXBeans using a wider range of
# tools than the Java JMX client.
#
# This script is intended to be used when starting Accumulo
# processes with the Jolokia JVM Agent[0]. To use the Jolokia
# agent, download the agent jar file and then add the following
# to the JAVA_OPTS variable in the accumulo-env.sh file: 
#
#  '-javaagent:/path/to/jolokia-agent-jvm-x.y.z-javaagent.jar=port=0,host=localhost'
#
# When the Accumulo server process starts the Jolokia JVM Agent
# will bind to an unused port. As of version 2.4.2 the URL endpoint
# of the Jolokia agent is set as the system property 'jolokia.agent'.
#
# This script does the following:
#
# 1. Obtains the pids of the Accumulo processes running on the host
#    using the `accumulo-service list` command.
# 2. For each pid, find the `jolokia.agent` system property using
#    the jinfo utility to find the host and port the Jolokia agent
#    is listening on for each Accumulo process.
# 3. Pause classloader creation in each Accumulo process by calling
#    the appropriate Jolokia endpoint.
# 4. Gets the list of files in the local cache directory.
# 5. Get the list of referenced files by calling the appropriate
#    Jolokia endpoint in each Accumulo process.
# 6. Determine which files in the local cache directory are not
#    in the list of referenced files.
# 7. Check that the elapsed time to this point is not greater than
#    the pause time sent to the Accumulo processes.
# 8. Delete the unreferenced files if any argument was passed to this
#    script. If no arguments were passed, then this is treated as
#    a dry run.
#
#  Throughout these steps the script checks the list of pids gathered
#  in step 1 against the list of pids of currently running Accumulo
#  processes and aborts if there are any differences.
#
# [0] https://jolokia.org/reference/html/manual/agents.html#agents-jvm
#

DO_IT="$1"
ACCUMULO_HOME=""
BASE_CACHE_DIR=""
EXPECTED_SERVICES="tserver sserver compactor"
PAUSE_TIME=2

# Constants
OBJECT_NAME="org.apache.accumulo.classloader:type=ContextClassLoaders"

#Dependencies
if ! JINFO=$(which jinfo); then
  echo "This script requires the 'jinfo' utility to be installed. Install and try again"
  exit 1
fi

if ! CURL=$(which curl); then
  echo "This script requires the 'curl' utility to be installed. Install and try again"
  exit 1
fi

if ! JQ=$(which jq); then
  echo "This script requires the 'jq' utility to be installed. Install and try again"
  exit 1
fi

function getPids {
  declare allpids
  for service in $EXPECTED_SERVICES; do
    pids=$("$ACCUMULO_HOME"/bin/accumulo-service "$service" list | tail -n +2 | awk '{ print $2 }')
    for pid in $pids; do
      allpids+=$pid
    done
  done
  output=$(sortStringOfNumbers "${allpids[*]}")
  echo "$output"
}

function getAgentUrl {
  pid=$1
  if ! system_property=$($JINFO "$pid" | grep "jolokia.agent="); then
    echo ""
  fi
  url=$(echo "$system_property" | tr -d '\\' | sed 's/jolokia.agent=//')
  echo "$url"
}

function setPauseTime {
  # See https://jolokia.org/reference/html/manual/protocol/exec.html
  jolokiaUrl="$1/exec/$OBJECT_NAME/pauseClassLoaderCreation/$PAUSE_TIME"
  response=$($CURL -s "$jolokiaUrl")
  ec=$?
  if [ "$ec" == "0" ]; then
	http_status_code=$(echo "$response" | $JQ '.status')
	if [ "$http_status_code" == "200" ]; then
	  echo "Successfully set pause time to $PAUSE_TIME for agent at $1"
	else
	  err_msg=$(echo "$response" | $JQ '.error')
	  echo "Jolokia request to $jolokiaUrl returned HTTP status: $http_status_code and msg: $err_msg"
	  exit 1
	fi
  else
	echo "curl request to $jolokiaUrl returned $ec"
	exit 1
  fi
}

function getReferencedFiles {
  # See https://jolokia.org/reference/html/manual/jolokia_protocol.html#read
  jolokiaUrl="$1/read/$OBJECT_NAME/ReferencedFiles"
  response=$($CURL -s "$jolokiaUrl")
  ec=$?
  if [ "$ec" == "0" ]; then
	http_status_code=$(echo "$response" | $JQ '.status')
	if [ "$http_status_code" == "200" ]; then
	  files=$(echo "$response" | $JQ '.value | to_entries | .[].value[]' --raw-output)
	  echo "$files"
	else
	  err_msg=$(echo "$response" | $JQ '.error')
	  echo "Jolokia request to $jolokiaUrl returned HTTP status: $http_status_code and msg: $err_msg"
	  exit 1
	fi
  else
	echo "curl request to $jolokiaUrl returned $ec"
	exit 1
  fi
}

function failIfDifferentPids {
  starting_pids=$1
  current_pids=$(getPids)
  if [ "$starting_pids" != "$current_pids" ]; then
    echo "Difference in Accumulo processes since script start, aborting..."
    echo "starting pids: $starting_pids"
    echo "current pids: $current_pids"
    exit 1
  fi
}

function sortStringOfNumbers {
  input=$1
  output=$(echo "$input" | tr " " "\\n" | sort -n | tr "\\n" " " | sed 's/ $//')
  echo "$output"
}

function sortString {
  input=$1
  output=$(echo "$input" | tr " " "\\n" | sort | tr "\\n" " " | sed 's/ $//')
  echo "$output"
}

#main

filesInResourcesDirectory=$(find "$BASE_CACHE_DIR/resources" -type f | sort)
if [ -z "$filesInResourcesDirectory" ]; then
  echo "No files in $BASE_CACHE_DIR/resources directory, no cleanup needed."
  exit 0
fi

declare orig_pids_array
declare agent_urls_array

#
# Get the pids and agent URLs for the running Accumulo processes
#
for service in $EXPECTED_SERVICES; do
  pids=$("$ACCUMULO_HOME"/bin/accumulo-service "$service" list | tail -n +2 | awk '{ print $2 }')
  echo "$service pids: $pids"
  for pid in $pids; do
    agent_url=$(getAgentUrl "$pid")
    if [ -z "$agent_url" ]; then
      echo "Did not find expected 'jolokia.agent' system property in $service with pid $pid"
      exit 1
    else
      echo "$service with pid $pid listening at $agent_url"
      orig_pids_array+=$pid
      agent_urls_array+=$agent_url
    fi
  done
done

orig_pids=$(sortStringOfNumbers "${orig_pids_array[*]}")
echo "Starting pids: $orig_pids"
agent_urls=$(sortString "${agent_urls_array[*]}")
echo "Agent urls: $agent_urls"

pause_start_time=$(date +"%s")

echo "Setting pause time on all known servers"
for url in $agent_urls; do
  setPauseTime "$url"
done

failIfDifferentPids "$orig_pids"

filesInResourcesDirectory=$(find "$BASE_CACHE_DIR/resources" -type f | sort)
echo "Found the following files in the $BASE_CACHE_DIR/resources directory:"
echo "$filesInResourcesDirectory"

failIfDifferentPids "$orig_pids"

echo "Getting referenced files"
for url in $agent_urls; do
  referenced_files=$(getReferencedFiles "$url")
done
referenced_files=$(echo "$referenced_files" | sort -u)
if [ -z "$referenced_files" ]; then
  echo "Accumulo servers are not referencing any files in $BASE_CACHE_DIR/resources"
else
  echo "Accumulo servers are referencing the following files:"
  echo "$referenced_files"
fi

failIfDifferentPids "$orig_pids"

declare files_to_delete
for file in $filesInResourcesDirectory; do
  if [ -z "$referenced_files" ]; then
    files_to_delete+="$file"
  else
    if ! grep "$file" "$referenced_files"; then
      files_to_delete+="$file"
    fi
  fi
done

if [ ${#files_to_delete[*]} -eq 0 ]; then
  echo "All files in $BASE_CACHE_DIR/resources are referenced by the Accumulo processes, no files to delete"
  exit 0
else
  echo "The following files were found in $BASE_CACHE_DIR/resources, but not referenced by any classloader"
  echo "${files_to_delete[*]}"
fi

failIfDifferentPids "$orig_pids"

time_now=$(date +"%s")
time_difference_minutes=$(( (pause_start_time - time_now) / 60))

if [[ $time_difference_minutes -gt $PAUSE_TIME ]]; then
  echo "Pause time has elapsed before completing cleanup process. Not removing files. Increase pause time."
  exit 1
fi

if [ -z "$DO_IT" ]; then
  echo "This is a dry run, pass an argument to this script delete the following files:"
  echo "$files_to_delete"
else
  echo "Deleting the following files:"
  echo "$files_to_delete"
  for f in $files_to_delete; do
    /bin/rm "$f"
  done
fi
