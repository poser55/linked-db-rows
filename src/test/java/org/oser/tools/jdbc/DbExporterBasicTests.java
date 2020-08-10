package org.oser.tools.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
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
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"), 2, "book", 1);
    }

    @Test
    void testGraph() throws SQLException, IOException, ClassNotFoundException {
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                10, "nodes", 1);
    }


    @Test
    void testStopTableIncluded() throws SQLException, ClassNotFoundException, IOException {
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
        Connection sakilaConnection = TestHelpers.getConnection("sakila");

        List<String> actorInsertList = JdbcHelpers.determineOrder(sakilaConnection, "actor");
        System.out.println("list:"+actorInsertList +"\n");

        DbExporter dbExporter = new DbExporter();

        dbExporter.getStopTablesIncluded().add("film");
        dbExporter.getStopTablesExcluded().add("inventory");

        Record actor199 = dbExporter.contentAsTree(sakilaConnection, "actor", 199);
        String asString = actor199.asJson();

        Set<RowLink> allNodes = actor199.getAllNodes();
        System.out.println(asString +" \nnumberNodes:"+ allNodes.size());

        System.out.println("classified:"+Record.classifyNodes(allNodes));

        DbImporter dbImporter = new DbImporter();
        Record asRecord = dbImporter.jsonToRecord(sakilaConnection, "actor", asString);
    }

    @Test
    void testWorkingOnNonExistingTable() throws SQLException, ClassNotFoundException, IOException {
        Assertions.assertThrows(IllegalArgumentException.class, ()-> TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                0, "notExisting", 1));
    }

    @Test
    void testWorkingOnNonExistingPrimaryKey() throws SQLException, ClassNotFoundException, IOException {
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                1, "nodes", 77);

    }
}
