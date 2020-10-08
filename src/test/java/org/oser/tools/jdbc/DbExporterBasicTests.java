package org.oser.tools.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DbExporterBasicTests {

    @BeforeAll
    public static void init() {
        TestHelpers.initLogback();
    }

    @Test
    void datatypesTest() throws Exception {
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"), 1, "datatypes", 1);
        assertEquals(6, basicChecksResult.getAsRecordAgain().content.size());
        assertEquals(6, basicChecksResult.getAsRecord().content.size());
    }

    @Test
    void blog() throws Exception {
        Connection demo = TestHelpers.getConnection("demo");
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(demo,
                3, "blogpost", 2);

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
                4, // now we have 1 node more than in #blog(), the preferences
                dbExporter -> {
                    try {
                        List<Fk> fks = Fk.getFksOfTable(demo, "user_table", dbExporter.getFkCache());
                        // add artificial FK
                        fks.add(new Fk("user_table", "id", "preferences", "user_id", false));
                        dbExporter.getFkCache().put("user_table", fks);
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                },
                dbImporter -> {
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
                },
                "blogpost", 2);
    }


    @Test
    void testBookTable() throws Exception {
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                2, "book", 1);
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
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                10, "nodes", 1);
    }

    @Test
    void testStopTableIncluded() throws Exception {
        final FieldMapper nopFieldMapper = (metadata, statement, insertIndex, value) -> statement.setArray(insertIndex, null);

        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("sakila"),
                12448, dbExporter -> dbExporter.getStopTablesIncluded().add("inventory"),
                dbImporter -> {
                    dbImporter.getFieldMappers().put("SPECIAL_FEATURES", nopFieldMapper);

                }, "actor", 199);

        System.out.println("classified:"+Record.classifyNodes(basicChecksResult.getAsRecord().getAllNodes()));
    }

    @Test // is a bit slow
    void sakila() throws SQLException, ClassNotFoundException, IOException {
        Connection sakilaConnection = TestHelpers.getConnection("sakila");

        List<String> actorInsertList = JdbcHelpers.determineOrder(sakilaConnection, "actor");
        System.out.println("list:"+actorInsertList +"\n");

        DbExporter dbExporter = new DbExporter();
        dbExporter.getStopTablesExcluded().add("inventory");

        Record actor199 = dbExporter.contentAsTree(sakilaConnection, "actor", 199);
        String asString = actor199.asJson();

        Set<RowLink> allNodes = actor199.getAllNodes();
        System.out.println(asString +" \nnumberNodes:"+ allNodes.size());

        System.out.println("classified:"+Record.classifyNodes(allNodes));

        DbImporter dbImporter = new DbImporter();
        Record asRecord = dbImporter.jsonToRecord(sakilaConnection, "actor", asString);

        // todo: still many issues with importing due to missing array support
        //dbImporter.insertRecords(sakilaConnection, asRecord);

        //Map<RowLink, Object> actor = dbImporter.insertRecords(sakilaConnection, asRecord);
        // System.out.println(actor + " "+actor.size());
    }

    @Test
    void sakilaJustOneTable() throws Exception {
        Connection connection = TestHelpers.getConnection("sakila");

        List<String> actorInsertList = JdbcHelpers.determineOrder(connection, "actor");
        System.out.println("list:"+actorInsertList +"\n");

        final FieldMapper nopFieldMapper = (metadata, statement, insertIndex, value) -> statement.setArray(insertIndex, null);

        HashMap<RowLink, Object> remapping = new HashMap<>();
        // why is this needed? Somehow he does not detect that language (originally =1 for these films) should be remapped
        // and the other test with all entries fails (as he adds the entries inserted here to what he exports in the other test)
        // it is correct: the stop-table avoids that we add (and remap!) the language table
        remapping.put(new RowLink("language/1"), 7);

        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(connection,
                31,
                dbExporter -> {  dbExporter.getStopTablesIncluded().add("film");
                                                    dbExporter.getStopTablesExcluded().add("inventory");
                                        },
                dbImporter -> {
                    dbImporter.getFieldMappers().put("SPECIAL_FEATURES", nopFieldMapper);
                },
                null,
                remapping,
                "actor", 199);

        System.out.println("classified:"+Record.classifyNodes(basicChecksResult.getAsRecord().getAllNodes()));
    }

    @Test
    void testWorkingOnNonExistingTable() {
        Assertions.assertThrows(IllegalArgumentException.class, ()-> TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                0, "notExisting", 1));
    }

    @Test
    void testWorkingOnNonExistingPrimaryKey() {
        Assertions.assertThrows(IllegalArgumentException.class, ()->TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                0, "nodes", 9999999999999L));
    }

    @Test
    // https://github.com/poser55/linked-db-rows/issues/2
    void testNullHandlingVarcharVsText() throws Exception {
        Connection demoConnection = TestHelpers.getConnection("demo");
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(demoConnection, 1, "datatypes", 100);

        Long o = (Long) basicChecksResult.getRowLinkObjectMap().values().stream().findFirst().get();

        DbExporter dbExporter = new DbExporter();
        Record asRecord = dbExporter.contentAsTree(demoConnection, "datatypes", o);

        assertEquals(asRecord.findElementWithName("text_type").value, null);
    }

}
