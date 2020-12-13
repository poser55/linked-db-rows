package org.oser.tools.jdbc;

import ch.qos.logback.classic.Level;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DbExporterBasicTests {

    @BeforeAll
    public static void init() {
        TestHelpers.initLogback();
    }

    @Test
    void datatypesTest() throws Exception {
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"), "datatypes", 1, 1);
        assertEquals(6, basicChecksResult.getAsRecordAgain().content.size());
        assertEquals(6, basicChecksResult.getAsRecord().content.size());
    }

    @Test
    void blog() throws Exception {
        Connection demo = TestHelpers.getConnection("demo");
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(demo,
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
                        List<Fk> fks = Fk.getFksOfTable(demo, "user_table", dbExporter.getFkCache());
                        // add artificial FK
                        fks.add(new Fk("user_table", "id", "preferences", "user_id", false));
                        dbExporter.getFkCache().put("user_table", fks);
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }, dbImporter -> {
                    try {
                        List<Fk> fksUserTable = Fk.getFksOfTable(demo, "user_table", dbImporter.getFkCache());
                        // add artificial FK
                        fksUserTable.add(new Fk("user_table", "id", "preferences", "user_id", false));
                        dbImporter.getFkCache().put("user_table", fksUserTable);

                        List<Fk> fksPreferences = Fk.getFksOfTable(demo, "preferences", dbImporter.getFkCache());
                        // add artificial FK (reverted)
                        fksPreferences.add(new Fk("user_table", "id", "preferences", "user_id", true));
                        dbImporter.getFkCache().put("preferences", fksPreferences);

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
    void testGraph() throws Exception {
        TestHelpers.setLoggerLevel(EnumSet.of(DbImporter.Loggers.I_UPDATES), Level.DEBUG);
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                "nodes", 1, 10);
        TestHelpers.setLoggerLevel(EnumSet.of(DbImporter.Loggers.I_UPDATES), Level.INFO);
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
        Assertions.assertThrows(IllegalArgumentException.class, ()-> TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                "notExisting", 1, 0));
    }

    @Test
    void testWorkingOnNonExistingPrimaryKey() {
        Assertions.assertThrows(IllegalArgumentException.class, ()->TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                "nodes", 99999999L, 0));
    }

    @Test
    // https://github.com/poser55/linked-db-rows/issues/2
    void testNullHandlingVarcharVsText() throws Exception {
        Connection demoConnection = TestHelpers.getConnection("demo");
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(demoConnection, "datatypes", 100, 1);

        Long o = (Long) basicChecksResult.getRowLinkObjectMap().values().stream().findFirst().get();

        DbExporter dbExporter = new DbExporter();
        Record asRecord = dbExporter.contentAsTree(demoConnection, "datatypes", o);

        assertEquals(asRecord.findElementWithName("text_type").value, null);
    }

}
