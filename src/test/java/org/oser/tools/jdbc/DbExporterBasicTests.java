package org.oser.tools.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DbExporterBasicTests {

    @Test
    void datatypesTest() throws SQLException, ClassNotFoundException, IOException {
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"), 1, "datatypes", 1);
        assertEquals(6, basicChecksResult.getAsRecordAgain().content.size());
        assertEquals(6, basicChecksResult.getAsRecord().content.size());
    }


    @Test
    void testBookTable() throws SQLException, IOException, ClassNotFoundException {
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                2, "book", 1);
    }

    @Test
    void testGraph() throws SQLException, IOException, ClassNotFoundException {
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                10, "nodes", 1);
    }

    @Test
    @Disabled // todo: not yet working
    void testStopTableIncluded() throws SQLException, ClassNotFoundException, IOException {
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("sakila"),
                17645, dbExporter -> dbExporter.getStopTablesIncluded().add("inventory"), null, "actor", 199);

        System.out.println("classified:"+Record.classifyNodes(basicChecksResult.getAsRecord().getAllNodes()));
    }


    @Test
    // todo: replace with new version once string diff is ok
    void testStopTableIncluded_old() throws SQLException, ClassNotFoundException, IOException {
        Connection sakilaConnection = TestHelpers.getConnection("sakila");

        DbExporter dbExporter = new DbExporter();
        dbExporter.getStopTablesIncluded().add("inventory");

        Record actor199 = dbExporter.contentAsTree(sakilaConnection, "actor", 199);
        System.out.println(Record.classifyNodes(actor199.getAllNodes()));
        String asString = actor199.asJson();

        Set<RowLink> allNodes = actor199.getAllNodes();
        System.out.println("numberNodes:" + allNodes.size());

        System.out.println("classified:"+Record.classifyNodes(allNodes));
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
    void sakilaJustOneTable() throws SQLException, ClassNotFoundException, IOException {
        Connection connection = TestHelpers.getConnection("sakila");

        List<String> actorInsertList = JdbcHelpers.determineOrder(connection, "actor");
        System.out.println("list:"+actorInsertList +"\n");

        final FieldMapper nopFieldMapper = new FieldMapper() {
            @Override
            public void mapField(JdbcHelpers.ColumnMetadata metadata, PreparedStatement statement, int insertIndex, String value) throws SQLException {
                statement.setArray(insertIndex, null);
            }
        };

        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(connection,
                31, dbExporter -> {  dbExporter.getStopTablesIncluded().add("film");
                                                    dbExporter.getStopTablesExcluded().add("inventory");
                                        },
                dbImporter -> {
                    dbImporter.getFieldMappers().put("SPECIAL_FEATURES", nopFieldMapper);
                }, "actor", 199);

        System.out.println("classified:"+Record.classifyNodes(basicChecksResult.getAsRecord().getAllNodes()));
    }

    @Test
    void testWorkingOnNonExistingTable() throws SQLException, ClassNotFoundException, IOException {
        Assertions.assertThrows(IllegalArgumentException.class, ()-> TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                0, "notExisting", 1));
    }

    @Test
    void testWorkingOnNonExistingPrimaryKey() throws SQLException, ClassNotFoundException, IOException {
        Assertions.assertThrows(IllegalArgumentException.class, ()->TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                0, "nodes", 9999999999999L));
    }
}
