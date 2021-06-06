#!/bin/bash
#Launch mvn clean install for a list of databases (to test each)

# exit if an error occurs
set -e

./smokeTestJbangScripts.sh

for databaseName in h2 postgres oracle sqlserver mysql; do
  echo -e "\n\n\n Running tests for ${databaseName}\n"
  ACTIVE_DB=${databaseName} mvn clean install
done

