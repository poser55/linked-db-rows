package org.oser.tools.jdbc;

import lombok.Getter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHelpers {
    public static Connection getConnection(String dbName) throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");

        Connection con = DriverManager.getConnection(
                "jdbc:postgresql://localhost/" + dbName, "postgres", "admin");

        con.setAutoCommit(true);
        return con;
    }

    public static BasicChecksResult testExportImportBasicChecks(Connection demoConnection, int numberNodes, String tableName, Object primaryKeyValue) throws SQLException, ClassNotFoundException, IOException {
        Record asRecord = new DbExporter().contentAsTree(demoConnection, tableName, primaryKeyValue);
        String asString = asRecord.asJson();
        System.out.println("export as json:"+asString);

        Record asRecordAgain = new DbImporter().jsonToRecord(demoConnection, tableName, asString);
        String asStringAgain = asRecordAgain.asJson();

        assertEquals(asString.toUpperCase(), asStringAgain.toUpperCase());
        assertEquals(numberNodes, asRecordAgain.getAllNodes().size());
        assertEquals(numberNodes, asRecord.getAllNodes().size());

        return  new BasicChecksResult(asRecord, asString, asRecordAgain, asStringAgain);
    }

    @Getter
    public static class BasicChecksResult {
        private Record asRecord;
        private String asString;
        private Record asRecordAgain;
        private String asStringAgain;

        public BasicChecksResult(Record asRecord, String asString, Record asRecordAgain, String asStringAgain) {
            this.asRecord = asRecord;
            this.asString = asString;
            this.asRecordAgain = asRecordAgain;
            this.asStringAgain = asStringAgain;
        }
    }
}
