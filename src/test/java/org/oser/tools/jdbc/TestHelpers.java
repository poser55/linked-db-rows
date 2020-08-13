package org.oser.tools.jdbc;

import lombok.Getter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHelpers {
    public static Connection getConnection(String dbName) throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");

        Connection con = DriverManager.getConnection(
                "jdbc:postgresql://localhost/" + dbName, "postgres", "admin");

        con.setAutoCommit(true);
        return con;
    }

    /** Makes a full test with a RowLink */
    public static BasicChecksResult testExportImportBasicChecks(Connection demoConnection, int numberNodes,
                                                                String tableName, Object primaryKeyValue) throws SQLException, ClassNotFoundException, IOException {
        return testExportImportBasicChecks(demoConnection, numberNodes, null, null, tableName, primaryKeyValue);
    }

    /** Makes a full test with a RowLink <br/>
     *   Allows to configure the DbImporter/ DbExporter */
    public static BasicChecksResult testExportImportBasicChecks(Connection demoConnection, int numberNodes,
                                                                Consumer<DbExporter> optionalExporterConfigurer,
                                                                Consumer<DbImporter> optionalImporterConfigurer,
                                                                String tableName, Object primaryKeyValue) throws SQLException, ClassNotFoundException, IOException {
        DbExporter dbExporter = new DbExporter();
        if (optionalExporterConfigurer != null) {
            optionalExporterConfigurer.accept(dbExporter);
        }
        Record asRecord = dbExporter.contentAsTree(demoConnection, tableName, primaryKeyValue);
        String asString = asRecord.asJson();
        System.out.println("export as json:"+asString);

        DbImporter dbImporter = new DbImporter();
        if (optionalImporterConfigurer != null) {
            optionalImporterConfigurer.accept(dbImporter);
        }
        Record asRecordAgain = dbImporter.jsonToRecord(demoConnection, tableName, asString);
        String asStringAgain = asRecordAgain.asJson();

        assertEquals(numberNodes, asRecord.getAllNodes().size());
        assertEquals(numberNodes, asRecordAgain.getAllNodes().size());
        assertEquals(asString.toUpperCase(), asStringAgain.toUpperCase());

        // todo: bug: asRecord is missing columnMetadata/ other values
        Map<RowLink, Object> rowLinkObjectMap = dbImporter.insertRecords(demoConnection, asRecordAgain);

        return  new BasicChecksResult(asRecord, asString, asRecordAgain, asStringAgain, rowLinkObjectMap);
    }

    @Getter
    public static class BasicChecksResult {
        private Record asRecord;
        private String asString;
        private Record asRecordAgain;
        private String asStringAgain;
        private Map<RowLink, Object> rowLinkObjectMap;

        public BasicChecksResult(Record asRecord, String asString, Record asRecordAgain, String asStringAgain, Map<RowLink, Object> rowLinkObjectMap) {
            this.asRecord = asRecord;
            this.asString = asString;
            this.asRecordAgain = asRecordAgain;
            this.asStringAgain = asStringAgain;
            this.rowLinkObjectMap = rowLinkObjectMap;
        }
    }
}
