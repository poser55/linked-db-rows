What it is:
*Export and import "linked db rows" from/ into relational databases.
*Linked db rows: table rows that are linked via foreign keys.

Usage (2 minute version):
*Export a linked db row to json, starting from the row of the book table with the primary key of 1 (=RowLink)
  String jsonString = DbExporter.contentAsGraph(dbConnection, "book", "1").asJson();

*Import an exported json again into a database:
  DbImporter.insertRecords(dbConnection, DbImporter.jsonToRecord(dbConnection, "book" jsonString), new InserterOptions());

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
* Various other helpers for JDBC
* Some options on how to export/ re-import a Json structure

Usage (longer version)
* <TODO>

Configuration options
* <TODO>



Ideas:
 * use the Sakila https://github.com/jOOQ/jOOQ/tree/main/jOOQ-examples/Sakila demo database?