#!/bin/bash

# expects a local postgresql db with some table definitions and a jbang setup
#  refer to README.md

# todo: somehow cygwin bash does not like jbang without the .cmd extension?
# CAVEAT: windows .cmd needs these ^^ escapings!

jbang JsonExport.java -p 2 -t blogpost  -db postgres  -u "jdbc:postgresql://localhost/demo" -l postgres -pw admin -fks 'user_table(id)-preferences(user_id)' --diagram "test.png" --sqlScript "./src/test/resources/db/migration/V1.0.2__blog.sql" > blogpost2.json

jbang JsonImport.java -j blogpost2.json -t blogpost -db postgres -u "jdbc:postgresql://localhost/demo" -l postgres -pw admin --log=CHANGE

