What it is:
===========
* Works on graphs of database rows, called "linked db rows": rows in database tables that are linked via foreign keys. 
* Allows Selecting, Exporting and Importing again such "linked db rows" from/ into relational databases.

Usage (2 minute version):
--------------------------
* Export a row and all the rows that are linked to it as JSON, starting from the row of the `book` 
table with the primary key of 1.
 ```Java
String jsonString = new DbExporter().contentAsTree(dbConnection, "book", "1").asJson();
```

Example export:
```JSON
{
	"id": 1,
	"author_id": 2,
	"author_id*author*": [
		{
			"id": 2,
			"last_name": "Huxley"
		}
	],
	"title": "Brave new world"
}
```

* Import an exported JSON string again into a database (maybe another one):
```Java
  DbImporter dbImporter = new DbImporter();
  dbImporter.insertRecords(dbConnection, dbImporter.jsonToRecord(dbConnection, "book", jsonString));
```

Limitations:
------------
* Only tested on Postgresql for now
* Test coverage needs improving
* It solves a problem I have - quite hacky in many ways
* Proprietary JSON structure
* Arrays (as e.g. Postgresql supports them) and other advanced constructs are currently not supported

License:
---------
* Apache version 2.0

Additional features:
---------------------
* By default, when inserting *remaps* the primary keys of all the inserted rows in order to not clash with existing ones.
* Determine the order in which tables can be inserted (taking care of their dependencies)
* Various other helpers for JDBC, refer to JdbHelpers for more details.
* Some options on how to export/ re-import linked db rows.

Usage (longer version):
-----------------------
#### Configure how to export and import
There are accessors on DbImporter and DbExporter that allow setting various options:
1. Exporter
    * stopTablesExcluded: tables that we do NOT want in the exported tree - the export stops before those.
    * stopTablesIncluded: tables that we want in the exported tree, but from which no more FK following shall occur.
2. Importer
    * defaultPkGenerator:  how to generate primary keys for new rows (default NextValuePkGenerator)
    * overriddenPkGenerators: pk generator overrides for special tables
    * fieldMappers: if you want to treat inserting certain fields in a special way (matching by field name for now). This allows e.g. to NOT treat certain fields.
    * forceInsert: in case an update would be possible: create a new row and remap other entries.    
#### Remapping entries to add them somewhere else
One can add a tree of linked db rows in *another* part of the graph of rows. E.g. one can take a blog entry (with its comments) and 
duplicate it on another user. Refer to the org.oser.tools.jdbc.DbExporterBasicTests#blog test: it takes a blog entry 
(with the blogpost, its comments and with the link to its user) and adds it to *another* user.   
#### Add artificial (=virtual) foreign keys
One can configure foreign keys that do not exist in the db, just for the exporting or importing. Refer to the examples
in the  org.oser.tools.jdbc.DbExporterBasicTests#blog_artificialFk test. We added a new table `preferences` that holds the
user preferences. There is no FK between the `user_table` and the `preferences` table. The test demonstrates how to add a virtual FK externally.
CAVEAT: (1) one needs to define the FK on *both* tables, on the second one it is inverted (inverted = true). (2) one needs to get the existing FKs and can then add the new FK. 
#### Sakila database example
The Sakila demo database https://github.com/jOOQ/jOOQ/tree/main/jOOQ-examples/Sakila is used in tests (the arrays fields are disabled for inserts)
#### Export script
*jbang QuickExport.java -t tableName -p PK -u jdbc:postgresql://localhost/demo
*Requires installing https://www.jbang.dev/
*Help about options:  jbang QuickExport.java -h

How to run the tests:
---------------------
It expects a local postgresql database with the name "demo" that is initialized with the *.sql files.
It also expects a "sakila" database that contains the Salika database tables and content: https://github.com/jOOQ/jOOQ/tree/main/jOOQ-examples/Sakila

Incomplete test support for other databases is available via the ACTIVE_DB environment variable (default postgres). 

Further Ideas:
--------------
* Clean ups
    - Combine the metadata code lines in readOneRecord and similar methods
    - Reduce the limitations
    - Fix hints marked as todo
    - Support for different schemas
* Fix bugs:
    - Upper/ lower case names in JSON
    - Escaping of table and field names
