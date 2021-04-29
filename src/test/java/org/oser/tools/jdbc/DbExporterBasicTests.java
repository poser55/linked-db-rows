package org.oser.tools.jdbc;

import ch.qos.logback.classic.Level;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DbExporterBasicTests {

    @BeforeAll
    public static void init() {
        TestHelpers.initLogback();
    }

    @Test
    void datatypesTest() throws Exception {
        TestHelpers.DbConfig databaseConfig = TestHelpers.getDbConfig();

        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                        dbExporter -> {},
                        dbImporter -> {},
                        record -> {
                            if (databaseConfig.getShortname().equals("oracle")) {
                                // for oracle a "DATE" type has also a time, we remove it here again
                                String dateWithTime = (String) record.findElementWithName("date_type").value;
                                int tPosition = dateWithTime.indexOf("T");
                                record.findElementWithName("date_type").value = dateWithTime.substring(0, tPosition);
                            }
                        },
                        new HashMap<>(),
                        "datatypes", 1, 1, true);

        assertEquals(6, basicChecksResult.getAsRecordAgain().content.size());
        assertEquals(6, basicChecksResult.getAsRecord().content.size());
    }

    @Test
    void blog() throws Exception {
        Connection demo = TestHelpers.getConnection("demo");
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(demo,
                dbExporter -> {
                    Fk.initFkCacheForMysql_LogException(demo, dbExporter.getFkCache());
                },
                dbImporter -> {
                    Fk.initFkCacheForMysql_LogException(demo, dbImporter.getFkCache());
                },
                "blogpost", 2, 3);

        // now duplicate this blog post entry to user_table/1 (it is now linked to user_table/2)

        DbImporter importer = new DbImporter();
        Map<RowLink, Object> remapping = new HashMap<>();
        remapping.put(new RowLink("user_table/2"), 1);
        // to make it interesting, adapt the entry
        basicChecksResult.getAsRecordAgain().findElementWithName("title").value = "new title";
        importer.insertRecords(demo, basicChecksResult.getAsRecordAgain(), remapping);
    }

    @Test
    void blog_artificialFk() throws Exception {
        Connection demo = TestHelpers.getConnection("demo");
        AtomicReference<DbImporter> importer = new AtomicReference<>(new DbImporter());
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(demo,
                dbExporter -> {
                    try {
                        Fk.initFkCacheForMysql_LogException(demo, dbExporter.getFkCache());

                        Fk.addVirtualForeignKey(demo, dbExporter,
                                "user_table", "id", "preferences", "user_id");

                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }, dbImporter -> {
                    try {
                        Fk.initFkCacheForMysql_LogException(demo, dbImporter.getFkCache());

                        Fk.addVirtualForeignKey(demo, dbImporter,
                                "user_table", "id", "preferences", "user_id");

                        importer.set(dbImporter);
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }, "blogpost", 2, 4 // now we have 1 node more than in #blog(), the preferences
        );
    }


    @Test
    void testBookTable() throws Exception {
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                "book", 1, 2);
    }

    @Test
    // update instead of insert
    void testBookTable_update() throws Exception {
        AtomicReference<Long> pages = new AtomicReference<>(0L);
        AtomicReference<DbExporter> exporter = new AtomicReference<>();
        Map<RowLink, Object> remapping = new HashMap<>();
        Connection demoConnection = TestHelpers.getConnection("demo");
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(
                demoConnection,
                dbExporter -> {
                    exporter.set(dbExporter);
                },
                dbImporter -> {
                    dbImporter.setForceInsert(false);
                },
                record -> {
                    Object number_pages = record.findElementWithName("number_pages").value;
                    number_pages =  (number_pages != null) ? (1 + ((Long)number_pages)) : 1;
                    pages.set((Long)number_pages);
                    record.findElementWithName("number_pages").value = number_pages;
                },
                remapping,
                "book", 1, 2, false);

        Record book = exporter.get().contentAsTree(demoConnection, "book", 1);
        System.out.println(book);
        assertEquals(pages.get() +"", ""+book.findElementWithName("number_pages").value);
    }

    @Test // Bug https://github.com/poser55/linked-db-rows/issues/1
    void testLocalDbHasMoreFields() throws Exception {
        // this insert is missing the "page_number" field:
        String toInsert = "{ \t\"id\": 1, \t\"author_id\": 2, \t\"author_id*author*\": [ \t\t{ \t\t\t\"id\": 2, \t\t\t\"last_name\": \"Huxley22\" \t\t} \t], \"title\": \"Brave new world2\", \"newfield\": \"xxx\"  }";

        Connection demo = TestHelpers.getConnection("demo");

        DbImporter dbImporter = new DbImporter();
        Record asRecordAgain = dbImporter.jsonToRecord(demo, "book", toInsert);

        Map<RowLink, Object> rowLinkObjectMap = dbImporter.insertRecords(demo, asRecordAgain);
    }

    @Test
    @DisabledIfSystemProperty(named = "mixedCaseTableNames", matches = "false")
    void testGraph() throws Exception {
        TestHelpers.setLoggerLevel(EnumSet.of(DbImporter.Loggers.I_UPDATES), Level.DEBUG);
        Connection demo = TestHelpers.getConnection("demo");
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(demo,
                dbExporter -> {
                    Fk.initFkCacheForMysql_LogException(demo, dbExporter.getFkCache());
                },
                dbImporter -> {
                    Fk.initFkCacheForMysql_LogException(demo, dbImporter.getFkCache());
                },
                "Nodes", 1, 10);
        TestHelpers.setLoggerLevel(EnumSet.of(DbImporter.Loggers.I_UPDATES), Level.INFO);

        // test simple deletion
        Object newPk = basicChecksResult.getRowLinkObjectMap().entrySet().stream().filter(r -> r.getKey().getTableName().equals("nodes")).findFirst().get().getValue();

        DbExporter dbExporter = new DbExporter();
        List<String> nodes = dbExporter.getDeleteStatements(demo, dbExporter.contentAsTree(demo, "nodes", newPk));
        System.out.println(nodes);
        dbExporter.deleteRecursively(demo, "Nodes", newPk);

        assertThrows(IllegalArgumentException.class, () -> dbExporter.deleteRecursively(demo, "Nodes", newPk));
    }


    @Test
    void testJsonToRecord() throws Exception {
        Connection demoConnection = TestHelpers.getConnection("demo"); // getConnectionTestContainer("demo");
        DbExporter db2Graphdemo = new DbExporter();
        Record book = db2Graphdemo.contentAsTree(demoConnection, "book", "1");

        System.out.println("book:" + book.asJson());

        DbImporter dbImporter = new DbImporter();
        Record book2 = dbImporter.jsonToRecord(demoConnection, "book", book.asJson());

        System.out.println("book2:" + book2.asJson());

        ObjectMapper mapper = Record.getObjectMapper();

        // todo: known issue .jsonToRecord converts json keys to upper case
        assertEquals(mapper.readTree(book.asJson().toLowerCase()), mapper.readTree(book2.asJson().toLowerCase()));
        assertEquals(book.getAllNodes(), book2.getAllNodes());

        Record author1 = db2Graphdemo.contentAsTree(demoConnection, "author", "1");
        Record author2 = dbImporter.jsonToRecord(demoConnection, "author", author1.asJson());

        assertEquals(mapper.readTree(author1.asJson().toLowerCase()), mapper.readTree(author2.asJson().toLowerCase()));

        // as inserts

        Map<RowLink, Object> rowLinkObjectMap = dbImporter.insertRecords(demoConnection, book2);
        System.out.println("\ninserts: " + rowLinkObjectMap.size());
    }

    @Test
    void testRemapping() throws Exception {
        Connection demoConnection = TestHelpers.getConnection("demo"); // getConnectionTestContainer("demo");
        DbExporter db2Graphdemo = new DbExporter();
        Record book = db2Graphdemo.contentAsTree(demoConnection, "book", "1");

        System.out.println("book:" + book.asJson());

        DbImporter dbImporter = new DbImporter();
        Record book2 = dbImporter.jsonToRecord(demoConnection, "book", book.asJson());

        Map<RowLink, Object> pkAndTableObjectMap = dbImporter.insertRecords(demoConnection, book2);
        System.out.println("remapped: " + pkAndTableObjectMap.size() + " new book Pk" + pkAndTableObjectMap.keySet().stream()
                .filter(p -> p.tableName.equals("book")).map(pkAndTableObjectMap::get).collect(toList()));

        assertEquals(2, pkAndTableObjectMap.size());
    }


    @Test
    void testInsert() throws Exception {
        Connection demoConnection = TestHelpers.getConnection("demo");

        String json = "{ \"id\":7,\"author_id\":1, \"author_id*author*\":[{\"id\":1,\"last_name\":\"Orwell\"}],\"title\":\"1984_summer\" }";

        DbImporter dbImporter = new DbImporter();
        Record book = dbImporter.jsonToRecord(demoConnection, "book", json);
        assertEquals(2, book.getAllNodes().size());

        System.out.println(dbImporter.insertRecords(demoConnection, book));
    }


    @Test
    void testWorkingOnNonExistingTable() {
        assertThrows(IllegalArgumentException.class, ()-> TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                "notExisting", 1, 0));
    }

    @Test
    void testWorkingOnNonExistingPrimaryKey() {
        assertThrows(IllegalArgumentException.class, ()->TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                "nodes", 99999999L, 0));
    }

    @Test
    // https://github.com/poser55/linked-db-rows/issues/2
    void testNullHandlingVarcharVsText() throws Exception {
        TestHelpers.DbConfig databaseConfig = TestHelpers.getDbConfig();

        Connection demoConnection = TestHelpers.getConnection("demo");
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(demoConnection,
                dbExporter -> {},
                dbImporter -> {},
                record -> {
                    if (databaseConfig.getShortname().equals("oracle")) {
                        // for oracle a "DATE" type has also a time, we remove it here again
                        String dateWithTime = (String) record.findElementWithName("date_type").value;
                        int tPosition = dateWithTime.indexOf("T");
                        record.findElementWithName("date_type").value = dateWithTime.substring(0, tPosition);
                    }
                },
                new HashMap<>(),
                "datatypes", 100, 1, true);

        Long o = (Long) basicChecksResult.getRowLinkObjectMap().values().stream().findFirst().get();

        DbExporter dbExporter = new DbExporter();
        Record asRecord = dbExporter.contentAsTree(demoConnection, "datatypes", o);

        assertNull(asRecord.findElementWithName("text_type").value);
    }

}
