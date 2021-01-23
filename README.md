What it is:
===========
* Relational databases work with _rows of data_. These rows can be linked to other rows via _foreign keys_. All linked rows form a graph. 
* _Linked db rows_ works on graphs of such database rows: It allows exporting such graphs to JSON and re-importing them again into databases.

Usage (2 minute version):
--------------------------
* Export a row and all the rows that are linked to it as JSON, starting from the row of the `book` 
table with the primary key of 1.  
 ```Java
JsonNode json = new DbExporter().contentAsTree(dbConnection, "book", "1").asJsonNode();
```
* The representation is a tree (starting from the chosen row), but all the relationships are preserved in the export.


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

* Import an exported JSON string again into a database schema (maybe another one):
```Java
  DbImporter dbImporter = new DbImporter();
  dbImporter.insertRecords(dbConnection, dbImporter.jsonToRecord(dbConnection, "book", json));
```

Rationale for the tool
--------------
* Initialize the database
* Testing (with canonicalization of primary keys)
* For general data import/ export
* To replay a prod database setup in development 
* To compare 2 database situations
* Maybe as simpler high-level database access abstraction?


Additional features:
---------------------
* By default, when inserting it can *remap* the primary keys of inserted rows in order to not clash with existing primary keys. 
(So if in the JSON there is a book with PK 7 (book/7) and in the db also, it looks for another PK to insert the entry, and then it remaps all other links to the book/7.)
* Determine the order in which tables can be inserted (taking care of their dependencies).
* Various other helpers for JDBC, refer to JdbHelpers for more details.
* Optional canonicalization of primary keys in exported data (to more easily compare data)
* Some options on how to export/ re-import linked db rows (see below).


Limitations:
------------
* Most tested on postgres for now, starts to work with h2, sqlserver and oracle (mysql with limitations)
* Test coverage can be improved
* It solves a problem I have - quite hacky in many ways
* Cycles in FKs of the database schema (DDL) are not treated for insertion (refer to Sakila and ignoreFkCycles)
* Arrays (as e.g. Postgresql supports them) and other advanced constructs are currently not supported

License:
---------
* Apache version 2.0

Usage (longer version):
-----------------------
#### Options to export and import
There are accessors on DbImporter and DbExporter that allow setting various options:
1. DbExporter
    * stopTablesExcluded: tables that we do NOT want in the exported tree - the export stops before those.
    * stopTablesIncluded: tables that we want in the exported tree, but from which no more FK following shall occur.
    * fieldExporter: add custom handling to load certain db fields (e.g. ignore them)
2. DbImporter
    * defaultPkGenerator:  how to generate primary keys for new rows (default: NextValuePkGenerator)
    * overriddenPkGenerators: pk generator overrides for special tables
    * fieldMappers: if you want to treat inserting certain fields in a special way (matching by field name for now). This allows e.g. to NOT treat certain fields.
    * forceInsert: in case an update would be possible: create a new row and remap other entries. Default: true 
      If forceInsert is false we update the existing entries (if entries exist for the given primary key).  
    * ignoreFkCycles: by default if in your DDL there are cycles between your table relationships, it refuses to re-import them. Setting this flag to true, ignores cycles (and imports non-cycles anyways).
          
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

#### Scripts to export/ import via command line
 * Exports a db row and all linked rows as JSON (you can chose a supported db via a short name, it downloads the needed jdbc driver)
 * Requires installing of https://www.jbang.dev/
 * Examples:
 *  `jbang JsonExport.java -t tableName -p PK -u jdbc:postgresql://localhost/demo`
 *  `jbang JsonExport.java -p 3 --stopTablesExcluded="user_table"`
 *  `jbang JsonExport.java -p 3 --stopTablesExcluded="user_table" --url "jdbc:h2:mem:demo" -db h2 -l sa -pw " "`   
 *  `jbang.cmd ./JsonExport.java -p 3 --stopTablesExcluded="user_table"  -db postgres > out.json`
 *  `jbang.cmd ./JsonImport.java -j out.json -db postgres`
    
 * Help about options:  `jbang JsonExport.java -h`
 *                      `jbang JsonImport.java -h`

How to run the tests:
---------------------
It expects a local postgresql database with the name "demo" that is initialized with the *.sql files.
It also expects a "sakila" database that contains the Sakila database tables and content: https://github.com/jOOQ/jOOQ/tree/main/jOOQ-examples/Sakila
Test support for alternative databases is available via the `ACTIVE_DB` environment variable (default: postgres). 

The script `./launchTests.sh` launches tests for all the db systems where the tests run through (db systems other than Postgresql
are launched automatically).  

Further Ideas:
--------------
* Clean ups
    - Reduce the limitations
    - Fix hints marked as todo
    - Support for different schemas
    - Handle uppercase letters of table names in mysql queries correctly for importing
* Fix bugs:
    - Escaping of table and field names
* Extension ideas
    - Do more unification of Datatype handling. E.g. oracle treats DATE different from Postgres (so at the moment
    we need to adapt it in the JSON/ Record). Refer e.g. to DbExporterBasicTests#datatypesTest(). 
