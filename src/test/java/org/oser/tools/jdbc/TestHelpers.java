package org.oser.tools.jdbc;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
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
                                                                String tableName, Object primaryKeyValue) throws Exception {
        return testExportImportBasicChecks(demoConnection, numberNodes, null, null, tableName, primaryKeyValue);
    }

    public static BasicChecksResult testExportImportBasicChecks(Connection demoConnection, int numberNodes,
                                                                Consumer<DbExporter> optionalExporterConfigurer,
                                                                Consumer<DbImporter> optionalImporterConfigurer,
                                                                String tableName, Object primaryKeyValue) throws Exception {
        return testExportImportBasicChecks(demoConnection, numberNodes, optionalExporterConfigurer, optionalImporterConfigurer, null,
                new HashMap<>(), tableName, primaryKeyValue);
    }

        /** Makes a full test with a RowLink <br/>
         *   Allows to configure the DbImporter/ DbExporter */
    public static BasicChecksResult testExportImportBasicChecks(Connection demoConnection, int numberNodes,
                                                                Consumer<DbExporter> optionalExporterConfigurer,
                                                                Consumer<DbImporter> optionalImporterConfigurer,
                                                                Consumer<Record> optionalRecordChanger,
                                                                Map<RowLink, Object> remapping,
                                                                String tableName, Object primaryKeyValue) throws Exception {
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
        assertEquals(canonicalize(asString), canonicalize(asStringAgain));

        if (optionalRecordChanger != null) {
            optionalRecordChanger.accept(asRecordAgain);
        }

        // todo: bug: asRecord is missing columnMetadata/ other values
        Map<RowLink, Object> rowLinkObjectMap = dbImporter.insertRecords(demoConnection, asRecordAgain, remapping);

        return new BasicChecksResult(asRecord, asString, asRecordAgain, asStringAgain, rowLinkObjectMap);
    }

    @NotNull
    private static String canonicalize(String asString) {
        return asString.toUpperCase()
                // remove precision differences
                .replace(".000,", ",")
                .replace(".0,", ",");
    }

    @Getter
    public static class BasicChecksResult {
        private final Record asRecord;
        private final String asString;
        private final Record asRecordAgain;
        private final String asStringAgain;
        private final Map<RowLink, Object> rowLinkObjectMap;

        public BasicChecksResult(Record asRecord, String asString, Record asRecordAgain, String asStringAgain, Map<RowLink, Object> rowLinkObjectMap) {
            this.asRecord = asRecord;
            this.asString = asString;
            this.asRecordAgain = asRecordAgain;
            this.asStringAgain = asStringAgain;
            this.rowLinkObjectMap = rowLinkObjectMap;
        }
    }

    public static void initLogback() {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger rootLogger = lc.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.INFO);
    }
}
