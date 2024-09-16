#!/bin/bash
#Launch mvn clean install for a list of databases (to test each)

# exit if an error occurs
set -e

#./smokeTestJbangScripts.sh

for databaseName in h2 postgres sqlserver mysql oracle; do
  echo -e "\n\n\n Running tests for ${databaseName}\n"
  ACTIVE_DB=${databaseName} mvn clean install || (echo -e "\n\n\n Last test run with ${databaseName}\n"; exit 1) 
done

