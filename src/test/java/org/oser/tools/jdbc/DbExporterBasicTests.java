package org.oser.tools.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import guru.nidi.graphviz.model.MutableGraph;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.oser.tools.jdbc.graphviz.RecordAsGraph;

import java.io.File;
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
                                String dateWithTime = (String) record.findElementWithName("date_type").getValue();
                                int tPosition = dateWithTime.indexOf("T");
                                record.findElementWithName("date_type").setValue(dateWithTime.substring(0, tPosition));
                            }
                        },
                        new HashMap<>(),
                        "datatypes", 1, 1, true);

        assertEquals(6, basicChecksResult.getAsRecordAgain().getContent().size());
        assertEquals(6, basicChecksResult.getAsRecord().getContent().size());
    }

    @Test
    void blog() throws Exception {
        Loggers.enableLoggers(EnumSet.of(Loggers.CHANGE, Loggers.SELECT));

        Connection demo = TestHelpers.getConnection("demo");
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(demo,
                dbExporter -> {
                    Fk.initFkCacheForMysql_LogException(demo, dbExporter.getFkCache());
                },
                dbImporter -> {
                    Fk.initFkCacheForMysql_LogException(demo, dbImporter.getFkCache());
                },
                "blogpost", 2, 3);
        Loggers.disableDefaultLogs();

        // now duplicate this blog post entry to user_table/1 (it is now linked to user_table/2)

        DbImporter importer = new DbImporter();
        Map<RowLink, DbImporter.Remap> remapping = new HashMap<>();
        remapping.put(new RowLink("user_table/2"), new DbImporter.Remap(1, 0));
        // to make it interesting, adapt the entry
        basicChecksResult.getAsRecordAgain().findElementWithName("title").setValue("new title");
        importer.insertRecords(demo, basicChecksResult.getAsRecordAgain(), remapping);
    }

    @Test
    @DisabledIfSystemProperty(named = "mixedCaseTableNames", matches = "false") // mysql returns null as schema name
    void blog_withSchemaPrefix() throws Exception {
        Connection demo = TestHelpers.getConnection("demo");
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(demo,
                dbExporter -> {
                    Fk.initFkCacheForMysql_LogException(demo, dbExporter.getFkCache());
                },
                dbImporter -> {
                    Fk.initFkCacheForMysql_LogException(demo, dbImporter.getFkCache());
                },
                demo.getSchema()+".blogpost", 2, 3);
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

                        Fk.addVirtualForeignKeyAsString(demo, dbImporter, "user_table(id)-preferences(user_id)");

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
        Map<RowLink, DbImporter.Remap> remapping = new HashMap<>();
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
                    Object number_pages = record.findElementWithName("number_pages").getValue();
                    number_pages =  (number_pages != null) ? (1 + ((Long)number_pages)) : 1;
                    pages.set((Long)number_pages);
                    record.findElementWithName("number_pages").setValue(number_pages);
                },
                remapping,
                "book", 1, 2, false);

        Record book = exporter.get().contentAsTree(demoConnection, "book", 1);
        System.out.println(book);
        assertEquals(pages.get() +"", ""+book.findElementWithName("number_pages").getValue());
    }

    @Test // Bug https://github.com/poser55/linked-db-rows/issues/1
    void testLocalDbHasMoreFields() throws Exception {
        // this insert is missing the "page_number" field:
        String toInsert = "{ \t\"id\": 1, \t\"author_id\": 2, \t\"author_id*author*\": [ \t\t{ \t\t\t\"id\": 2, \t\t\t\"last_name\": \"Huxley22\" \t\t} \t], \"title\": \"Brave new world2\", \"newfield\": \"xxx\"  }";

        Connection demo = TestHelpers.getConnection("demo");

        DbImporter dbImporter = new DbImporter();
        Record asRecordAgain = dbImporter.jsonToRecord(demo, "book", toInsert);

        Map<RowLink, DbImporter.Remap> rowLinkObjectMap = dbImporter.insertRecords(demo, asRecordAgain);
    }

    @Test
    @DisabledIfSystemProperty(named = "mixedCaseTableNames", matches = "false")
    void testGraph() throws Exception {
        Loggers.enableLoggers(EnumSet.of(Loggers.CHANGE, Loggers.SELECT));
        Connection demo = TestHelpers.getConnection("demo");
        long start = System.currentTimeMillis();
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(demo,
                dbExporter -> {
                    Fk.initFkCacheForMysql_LogException(demo, dbExporter.getFkCache());
                },
                dbImporter -> {
                    Fk.initFkCacheForMysql_LogException(demo, dbImporter.getFkCache());
                },
                "Nodes", 1, 10);
        System.out.println("timing: " + (System.currentTimeMillis() - start));

        Loggers.disableDefaultLogs();

        // test simple deletion
        Object newPk = basicChecksResult.getRowLinkObjectMap().entrySet().stream().filter(r -> r.getKey().getTableName().equals("nodes")).findFirst().get().getValue().getPkField();

        DbExporter dbExporter = new DbExporter();
        List<String> nodes = dbExporter.getDeleteStatements(demo, dbExporter.contentAsTree(demo, "nodes", newPk));
        System.out.println(nodes);
        dbExporter.deleteRecursively(demo, "Nodes", newPk);

        assertThrows(IllegalArgumentException.class, () -> dbExporter.deleteRecursively(demo, "Nodes", newPk));
    }

    @Test
    @DisabledIfSystemProperty(named = "mixedCaseTableNames", matches = "false")
    void testGraph_getStopTableIncluded() throws Exception {
        Connection demo = TestHelpers.getConnection("demo");

        DbExporter dbExporter = new DbExporter();
        dbExporter.getStopTablesIncluded().add("nodes");

        Record nodes1 = dbExporter.contentAsTree(demo, "Nodes", 1);

        assertEquals(4, nodes1.getAllNodes().size());

        System.out.printf("nodes: " + nodes1.getAllNodes());

        RecordAsGraph asGraph = new RecordAsGraph();
        MutableGraph graph = asGraph.recordAsGraph(demo, nodes1);
        asGraph.renderGraph(graph, 900, new File( "graph_exported.png"));
    }


    @Test
    void testJsonToRecord() throws Exception {
        Timer t = new Timer();
        Connection demoConnection = TestHelpers.getConnection("demo");
        t.printCurrent("-2");

        DbExporter dbExporter = new DbExporter();

        t.printCurrent("-1");

        Record book = dbExporter.contentAsTree(demoConnection, "book", "1");

        t.printCurrent("-0.5");

        System.out.println("book:" + book.asJsonNode().toString());

        t.printCurrent("0");

        DbImporter dbImporter = new DbImporter();
        Record book2 = dbImporter.jsonToRecord(demoConnection, "book", book.asJsonNode().toString());

        System.out.println("book2:" + book2.asJsonNode().toString());

        t.printCurrent("1");

        ObjectMapper mapper = Record.getObjectMapper();

        // todo: known issue .jsonToRecord converts json keys to upper case
        assertEquals(mapper.readTree(book.asJsonNode().toString().toLowerCase()), mapper.readTree(book2.asJsonNode().toString().toLowerCase()));
        assertEquals(book.getAllNodes(), book2.getAllNodes());

        Record author1 = dbExporter.contentAsTree(demoConnection, "author", "1");
        Record author2 = dbImporter.jsonToRecord(demoConnection, "author", author1.asJsonNode().toString());

        t.printCurrent("2");

        assertEquals(mapper.readTree(author1.asJsonNode().toString().toLowerCase()), mapper.readTree(author2.asJsonNode().toString().toLowerCase()));

        // as inserts

        Map<RowLink, DbImporter.Remap> rowLinkObjectMap = dbImporter.insertRecords(demoConnection, book2);
        System.out.println("\ninserts: " + rowLinkObjectMap.size());

        dbExporter.contentAsTree(demoConnection, "book", "1");

        t.printCurrent("end");
    }

    public static class Timer {
        long started;
        long lastDelta;

        public Timer() {
            this.started = System.currentTimeMillis();
            lastDelta = started;
        }
        public void printCurrent(String x){
            long now = System.currentTimeMillis();
            long fullDelta = now - started;
            System.out.println(x+". delta: "+ fullDelta +" "+(now - lastDelta));
            lastDelta = now;
        }
    }

    @Test
    void testRemapping() throws Exception {
        Connection demoConnection = TestHelpers.getConnection("demo"); // getConnectionTestContainer("demo");
        DbExporter db2Graphdemo = new DbExporter();
        Record book = db2Graphdemo.contentAsTree(demoConnection, "book", "1");

        System.out.println("book:" + book.asJsonNode().toString());

        DbImporter dbImporter = new DbImporter();
        Record book2 = dbImporter.jsonToRecord(demoConnection, "book", book.asJsonNode().toString());

        Map<RowLink, DbImporter.Remap> pkAndTableObjectMap = dbImporter.insertRecords(demoConnection, book2);
        System.out.println("remapped: " + pkAndTableObjectMap.size() + " new book Pk" + pkAndTableObjectMap.keySet().stream()
                .filter(p -> p.getTableName().equals("book")).map(pkAndTableObjectMap::get).collect(toList()));

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
                        String dateWithTime = (String) record.findElementWithName("date_type").getValue();
                        int tPosition = dateWithTime.indexOf("T");
                        record.findElementWithName("date_type").setValue(dateWithTime.substring(0, tPosition));
                    }
                },
                new HashMap<>(),
                "datatypes", 100, 1, true);

        Long o = (Long) basicChecksResult.getRowLinkObjectMap().values().stream().findFirst().map(DbImporter.Remap::getPkField).get();

        DbExporter dbExporter = new DbExporter();
        Record asRecord = dbExporter.contentAsTree(demoConnection, "datatypes", o);

        assertNull(asRecord.findElementWithName("text_type").getValue());
    }

    @Test
    void testFieldExporter() {
        DbExporter dbExporter = new DbExporter();

        FieldExporter tfExporter = (tableName, fieldName, metadata, rs) -> {  return null;  };
        FieldExporter nullFfExporter = (tableName, fieldName, metadata, rs) -> {  return new Record.FieldAndValue("name", null, null);  };
        dbExporter.registerFieldExporter("t", "f", tfExporter);
        dbExporter.registerFieldExporter(null, "ff", nullFfExporter);
        assertEquals(tfExporter, dbExporter.getFieldExporter("t", "f"));
        assertEquals(nullFfExporter, dbExporter.getFieldExporter("anyTable", "ff"));
        assertNull(dbExporter.getFieldExporter("tt", "f"));
    }
}
