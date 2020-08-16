What it is:
===========
* Collect, export and import "linked db rows" from/ into relational databases.
* Linked db rows: rows in tables that are linked via foreign keys.

Usage (2 minute version):
--------------------------
* Export a linked db row to JSON, starting from the row of the `book` table with the primary key of 1 (=`RowLink` book/1)
 ```
  String jsonString = new DbExporter().contentAsTree(dbConnection, "book", "1").asJson();
```

* Import an exported JSON string again into a database:
```
  DbImporter dbImporter = new DbImporter();
  dbImporter.insertRecords(dbConnection, dbImporter.jsonToRecord(dbConnection, "book", jsonString));
```

Limitations:
------------
* Only tested on postgresql for now
* Test coverage needs improving
* It solves a problem I have - quite hacky in many ways
* Proprietary JSON structure
* Only one-element foreign keys per table supported for now
* No arrays are currently supported

License:
---------
* Apache version 2.0

Additional features:
---------------------
* Determine the order in which tables can be inserted (taking care of their dependencies)
* Various other helpers for JDBC, refer to JdbHelpers for more detail.
* Some options on how to export/ re-import a JSON structure

Usage (longer version):
-----------------------
There are accessors on DbImporter and DbExporter that allow setting various options:
1. Exporter
    * stopTablesExcluded: tables that we do NOT want in the exported tree - the export is stopped before those
    * stopTablesIncluded: tables that we want in the exported tree, but from which no more FK following shall occur.
2. Importer
    * forceInsert: in case an update would be possible: create a new row and remap other entries
    * defaultPkGenerator:  how to generate primary keys for new entries (default NextValuePkGenerator)
    * overriddenPkGenerators: pk generator overrides for special tables
    * fieldMappers: if you want to treat inserting certain fields in a special way (matching by field name for now)


Ideas:
-------
* Use the Sakila https://github.com/jOOQ/jOOQ/tree/main/jOOQ-examples/Sakila demo database more (started, but uses arrays)
* Performance tuning (one step already done)
* Allow excluding fields
* Clean ups
    - combine the metadata code lines in readOneRecord and similar methods
    - reduce the limitations
    - Fix hints marked as todo
* Fix bugs:
    - Upper/ lower case names in JSON

