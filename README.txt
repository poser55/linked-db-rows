What it is:
*Collect, export and import "linked db rows" from/ into relational databases.
*Linked db rows: table rows that are linked via foreign keys.

Usage (2 minute version):
*Export a linked db row to json, starting from the row of the book table with the primary key of 1 (=RowLink book/1)
  String jsonString = new DbExporter().contentAsTree(dbConnection, "book", "1").asJson();

*Import an exported json again into a database:
  DbImporter dbImporter = new DbImporter();
  dbImporter.insertRecords(dbConnection, dbImporter.jsonToRecord(dbConnection, "book", jsonString));

Limitations:
* Only tested on postgresql for now
* Test coverage needs improving
* It solves a problem I have - quite hacky in many ways
* Proprietary json structure
* Only simple primary keys supported for now

License:
* Apache version 2.0

Additional features:
* Determine order in which tables can be inserted (taking care of their dependencies)
* Various other helpers for JDBC, refer to JdbHelpers for more detail.
* Some options on how to export/ re-import a Json structure

Usage (longer version)
* There are accessors on DbImporter and DbExporter that allow to set various options. Refer to section
"Configuration options" for details


Configuration options
* Exporter
** stopTablesExcluded: tables that we do NOT want in the exported tree - the export is stopped at those
* Importer
** forceInsert: in case an update would be possible: create a new row and remap other entries
** defaultPkGenerator:  how to generate primary keys for new entries (default NextValuePkGenerator)
** overriddenPkGenerators: pk generator overrides for special tables




Ideas:
* Use the Sakila https://github.com/jOOQ/jOOQ/tree/main/jOOQ-examples/Sakila demo database more?
* Performance tuning