/**
 * Export database content that is connected with foreign keys as JSON and reimport it again into another database.
 * <p>
 *
 * In others words: Relational databases work with rows of data that can be linked to other rows via foreign keys.
 * All linked rows form a graph. Linked db rows works on graphs of such database rows:
 * It allows exporting such graphs to JSON and re-importing them again into other databases.
 * <p>
 * <p>
 * Refer to <a href="https://github.com/poser55/linked-db-rows">linked-db-rows</a>.
 * <p>
 * <p>
 * Example:<p>
 * <code>
 *   JsonNode json = new DbExporter().contentAsTree(dbConnection, "book", "1").asJsonNode();
 * </code>
 * <p>
 * <p>
 * <p>
 * The representation is a tree (starting from the chosen row), but all the relationships are preserved in the export.
 * <p>
 * Example export:<p>
 * <code><pre>
 * {
 * 	"id": 1,
 * 	"author_id": 2,
 * 	"author_id*author*": [
 *                {
 * 			"id": 2,
 * 			"last_name": "Huxley"
 *        }
 * 	],
 * 	"title": "Brave new world"
 * }
 * <p>
 * </pre></code>
 *
 * Import an exported JSON string again into a database schema (maybe another one): <p>
 * <code><pre>
 *   DbImporter dbImporter = new DbImporter();
 *   dbImporter.insertRecords(dbConnection, dbImporter.jsonToRecord(dbConnection, "book", json));
 * </pre></code>
 *
 * */
package org.oser.tools.jdbc;