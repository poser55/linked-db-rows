What it is
===========
* Relational databases work with _rows of data_. These rows can be linked to other rows via _foreign keys_. All linked rows form a graph. 
* _Linked db rows_ works on graphs of such database rows: It allows exporting such graphs to JSON and re-importing them again into databases.

Usage (2 minute version)
--------------------------
* Export a row and all the rows that are linked to it as JSON, starting from the row of the `book` 
table with the primary key of 1.  
 ```Java
JsonNode json = new DbExporter().contentAsTree(dbConnection, "book", "1").asJsonNode();
```
* The representation is a tree (starting from the chosen row), but all the relationships are preserved in the export.

(Command line: `jbang db-export-json@poser55  -t book -p 1 -db postgres  -u "jdbc:postgresql://localhost/demo" -l postgres -pw admin > blogpost3.json`)


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
(Command line:   `jbang db-import-json@poser55 -j blogpost3.json -t book -db postgres -u "jdbc:postgresql://localhost/demo" -l postgres -pw admin` )

Maven dependency:
```XML
<dependency>
  <groupId>org.oser.tools.jdbc</groupId>
  <artifactId>linked-db-rows</artifactId>
  <version>0.8</version>
</dependency>
```

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.oser.tools.jdbc/linked-db-rows/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.oser.tools.jdbc/linked-db-rows)
![CI](https://github.com/poser55/linked-db-rows/actions/workflows/maven.yml/badge.svg)


What purpose does this have? 
--------------
* Initialize the database
* Testing (with canonicalization of primary keys)
* For general data import/ export
* To use a prod database setup in development 
* To compare 2 database situations
* Maybe as a simpler high-level database access abstraction?


Additional features
---------------------
* By default, when inserting it can *remap* the primary keys of inserted rows in order to not clash with existing primary keys. 
(So if in the JSON there is a book with primary key 7 (book/7) and in the db also, it looks for another PK to insert the entry, and then it remaps all other links to the book/7 in the JSON.)
* Determine the order in which tables can be inserted (taking care of their dependencies).
* Various other helpers for JDBC, refer to JdbHelpers for more details.
* Optional canonicalization of primary keys in exported data (to more easily compare data).
* Some options on how to export/ re-import linked db rows (see below).


Limitations
------------
* Most tested on postgres for now, starts to work with h2, sqlserver and oracle (mysql with limitations)
* Test coverage can be improved
* It solves a problem I have - quite hacky in many ways
* Treatment of cycles in FKs of the database schema (DDL) is a new feature (refer to Sakila and `ignoreFkCycles`)
* Arrays (as e.g. Postgresql supports them) and other advanced constructs are currently not supported

License
---------
* Apache version 2.0

Usage (longer version)
-----------------------
#### Options to export and import
There are accessors on `DbImporter` and `DbExporter` that allow setting various options:
1. DbExporter
    * stopTablesExcluded: tables that we do NOT want in the exported tree - the export stops before those.
    * stopTablesIncluded: tables that we want in the exported tree, but from which no more FK following shall occur.
    * fieldExporter and typeFieldExporters: add custom handling to load certain fields from the db (e.g. to ignore them).
      You can match by field name and (optionally) table name (refer to `DbExporter#registerFieldExporter()`) or by 
      JDBC type (refer to `DbExporter#getTypeFieldExporters()`).
2. DbImporter
    * defaultPkGenerator:  how to generate primary keys for new rows (default: NextValuePkGenerator)
    * overriddenPkGenerators: pk generator overrides for special tables
    * fieldImporter and typeFieldImporters: if you want to insert certain fields or types in a special way. You can
      match by field and (optional) table name (refer to `DbImporter#registerFieldImporter()` ) or by 
      JDBC type name (refer to `DbImporter#getTypeFieldImporters()`). 
      This allows also e.g. to NOT treat certain fields or types (FieldImporter.NOP_FIELDIMPORTER). Take care to 
      return true to stop the default treatment after the plugin is called! 
    * forceInsert: in case an update would be possible: create a new row and remap other entries. Default: true 
      If forceInsert is false we update the existing entries (if entries exist for the given primary key).  
    * ignoreFkCycles: by default if in your DDL there are cycles between your table relationships, it refuses to re-import them. 
      Setting this flag to true, ignores cycles (and imports non-cycles anyways).
    
#### Add artificial (=virtual) foreign keys
One can configure foreign keys that do not exist in the db, just for the exporting or importing (=virtual foreign keys). 

You can add a FK via a string notation such as `user_table(id)-preferences(user_id)`. The foreign key is then added from 
the table `user_table` and the column `id` to the table `preferences` with the column `user_id`. Multiple virtual foreign keys can be separated by `;`.
Alternatively you can also configure it via the 4 parameters (first and second table, first and second list of columns), 
refer to `Fk#addVirtualForeignKeyAsString()` or `Fk#addVirtualForeignKey()`

Refer to the examples in the  org.oser.tools.jdbc.DbExporterBasicTests#blog_artificialFk test. We added a new table `preferences` that holds the
user preferences. There is no FK between the `user_table` and the `preferences` table in the db DDL.
Another example illustrates virtual foreign keys between tables in different database schemas: MultiSchemaExperimentTests.

#### Canonicalization of primary keys
Two graphs may be equivalent given their contained data but just have different primary keys (if we assume that the primary keys
do not hold business meaning, beyond mapping rows). This feature allows to convert a record to a canonical form (that is assumed 
to be the same even if the primary keys vary).
Id orders are determined based on the original order in the database (so assuming integer primary keys this
should be stable for equality). We do not use any data in the records to determine the order. 
Refer to `RecordCanonicalizer.canonicalizeIds()` for more details.

#### JBang scripts to export/ import via command line
* Exports a db row and all linked rows as JSON (you can choose a supported db via a short name, it downloads the needed jdbc driver if needed)
  It requires installing https://www.jbang.dev/
* Examples:
    * `jbang JsonExport.java  -t blogpost -p 3 --stopTablesExcluded="user_table"  -db postgres  \
      -u "jdbc:postgresql://localhost/demo" -l postgres -pw admin -fks 'user_table(id)-preferences(user_id)' > blogpost3.json`
        * This exports the data of the table blogpost with primary key 3 to the file blogpost3.json
        * It defines also a virtual foreign key
        * You can replace `jbang JsonExport.java` with `jbang db-export-json@poser55`  (the latter does not need code locally)
    * `jbang JsonImport.java -j blogpost3.json -t blogpost -db postgres -u "jdbc:postgresql://localhost/demo" -l oracle -pw admin --log=CHANGE`
        * This imports the JSON file blogpost3.json into the local postgres "demo" db
        * You can replace `jbang JsonImport.java` with `jbang db-import-json@poser55`

* Help about options:  `jbang db-import-json@poser55 -h` or `jbang db-export-json@poser55 -h`


#### Logging SQL statements
We use SLF4j/ Logback. There is a convenience method to enable some loggers, example use:
`Loggers.enableLoggers(EnumSet.of(Loggers.CHANGE, Loggers.SELECT));`
Alternatively use the loggers:
org.oser.tools.jdbc.Loggers.SELECT
org.oser.tools.jdbc.Loggers.CHANGE
org.oser.tools.jdbc.Loggers.DELETE
org.oser.tools.jdbc.Loggers.WARNING
org.oser.tools.jdbc.Loggers.INFO

#### Deleting a graph
Refer to `DbExporter.getDeleteStatements()`. It does a db export first (using all the parameters of DbExporter). 
You should check that the export to JSON is correct before proceeding!  
CAVEAT: `DbExporter.deleteRecursively()` really DELETES data!

#### Remapping entries to add them somewhere else
One can add a tree of linked db rows in *another* part of the graph of rows. E.g. one can take a blog entry (with its comments) and
duplicate it on another user. Refer to the org.oser.tools.jdbc.DbExporterBasicTests#blog test: it takes a blog entry
(with the blogpost, its comments and with the link to its user) and adds it to *another* user.

#### JSON format
  * Numbers, Booleans, Strings are directly usable. 
  * Date-Types are mapped to Strings. We use [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) dates by default for timestamps,
     example: `2019-01-01T12:19:11`, the `T` character can be replaced by a blank, as the normal toString() of
    java.sql.Timestamp. Dates (without a time) are in the form of 2019-01-01.  
    (Some DBs need a config to default to this format, refer to tests.)
  * Blobs are serialized as BASE64 encoded Strings. 
  * Subtables are added after the field that links to them (via the foreign key). Subtables are always in sub-arrays.
    They are behind a JSON entry of the name  `NAME_OF_FK_COLUMN*NAME_OF_SUBTABLE*`, example: `author_id*author*`.

#### Show an exported graph of records as Graphviz graph (experimental)
  * Example output is here:
    ![Alt text](resources/exampleGraph.png?raw=true "Example Graphviz graph")
  * Prerequisite: Requires the optional (maven) dependency to https://github.com/nidi3/graphviz-java <br/>
  * Sample code:
```Java
   DbExporter exporter = new DbExporter();
   Record records = exporter.contentAsTree(demo, "Nodes", 1);

   RecordAsGraph asGraph = new RecordAsGraph();
   MutableGraph graph = asGraph.recordAsGraph(demo, records);
   asGraph.renderGraph(graph, 900, new File( "graph.png"));
```
  * You can optionally choose what attributes to display for each table (use the optional 3rd argument of `RecordAsGraph#recordAsGraph()`) 

#### Transactions
The library participates in the current transaction setting: it supports both auto-commit or manual 
transaction handling. When running e.g. in an existing Spring transaction context, you could do the following:
```Java
@Autowired
EntityManager em;

// flushing before
em.flush();
Session session = (Session) em.getDelegate();

Object result = session.doReturningWork(new ReturningWork<Object>() {

   public Object execute(Connection connection) throws SQLException {
      // use linked-db-rows code here, using the same connection as Spring
        
      return result;
   }
};

// end transaction as normal

```

How to run the tests
---------------------
The basic tests run (without configuration) for h2 (they run directly via `mvn clean install`).
For the complete test set, it expects a local postgresql database with the name "demo" that is initialized with the *.sql files.
It also expects a "sakila" database that contains the Sakila database tables and content: https://github.com/jOOQ/jOOQ/tree/main/jOOQ-examples/Sakila
Test support for alternative databases is available via the `ACTIVE_DB` environment variable (default: postgres). These other dbs are run with testcontainer (so they need a local docker installation).

The script `./launchTests.sh` launches tests for all the db systems where the tests run (db systems other than Postgresql and h2
are launched automatically via testcontainer).  

#### Sakila database example
The Sakila demo database https://github.com/jOOQ/jOOQ/tree/main/jOOQ-examples/Sakila is used in tests (the arrays fields are disabled for inserts)


Deploying
--------------
 * Description: https://andresalmiray.com/publishing-to-maven-central-using-apache-maven/ and
   https://proandroiddev.com/publishing-a-maven-artifact-3-3-step-by-step-instructions-to-mavencentral-publishing-bd661081645d
 * Test run: `mvn -Ppublication,local-deploy -Dlocal.repository.path=c:/tmp/repository deploy`
 * To release, add [release] as first part of the git commit message
 

Further Ideas
--------------
* Clean ups
    - Reduce the limitations
    - Fix hints marked as todo
    - Test support for different schemas more
    - Handle uppercase letters of table names in mysql queries correctly for importing
* Fix bugs:
    - Escaping of table and field names
* Extension ideas
    - Do more unification of Datatype handling. E.g. oracle treats DATE different from Postgres (so at the moment
    we need to adapt it in the JSON/ Record). Refer e.g. to DbExporterBasicTests#datatypesTest(). 
