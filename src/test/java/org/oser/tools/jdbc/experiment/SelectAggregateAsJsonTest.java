package org.oser.tools.jdbc.experiment;

import com.github.benmanes.caffeine.cache.Cache;
import org.junit.jupiter.api.Test;
import org.oser.tools.jdbc.DbExporter;
import org.oser.tools.jdbc.DbRecord;
import org.oser.tools.jdbc.Fk;
import org.oser.tools.jdbc.TestHelpers;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

class SelectAggregateAsJsonTest {

    @Test
    void basicRequest() throws SQLException, IOException, ClassNotFoundException {
        Connection connection = TestHelpers.getConnection("demo");

        DbExporter dbExporter = new DbExporter();
        DbRecord book = dbExporter.contentAsTree(connection, "book", 3);

        Cache<String, List<Fk>> filteredFkCache = dbExporter.getFilteredFkCache();
        String sqlStatement = SelectAggregateAsJson.selectStatementForAggregateSelection("book", filteredFkCache, "oracle");
   //     String sqlStatement = SelectAggregateAsJson.selectStatementForAggregateSelection("book", filteredFkCache, "postgres");

        System.out.println(sqlStatement);

        invokeDbAndPrintJson(connection, sqlStatement);
    }

    private static void invokeDbAndPrintJson(Connection connection, String sqlStatement) throws SQLException {
        if (SelectAggregateAsJson.supportedDatabases.contains(TestHelpers.getDbConfig().getShortname())) {
            try (PreparedStatement pkSelectionStatement = connection.prepareStatement(sqlStatement + " where id = 3")) {
                try (ResultSet rs = pkSelectionStatement.executeQuery()) {
                    if (rs.next()) {
                        String json = rs.getString(1);
                        System.out.println("json:" + json);
                    }
                }
            }
        }
    }

    @Test
    // needs local env variable with ORACLE JDBC URL
    void basicRequestOracle() throws SQLException, IOException, ClassNotFoundException {
        Class.forName("oracle.jdbc.driver.OracleDriver");

        String localOracleDbUrl = System.getenv("LOCAL_ORACLE_JDBC_URL");
        if (localOracleDbUrl == null) {
            return;
        }

        Connection connection= DriverManager.getConnection(
                localOracleDbUrl,"system","admin");
        
        DbExporter dbExporter = new DbExporter();
        DbRecord book = dbExporter.contentAsTree(connection, "book", 3);

        Cache<String, List<Fk>> filteredFkCache = dbExporter.getFilteredFkCache();
        String sqlStatement = SelectAggregateAsJson.selectStatementForAggregateSelection("book", filteredFkCache, "oracle");
        //     String sqlStatement = SelectAggregateAsJson.selectStatementForAggregateSelection("book", filteredFkCache, "postgres");

        System.out.println(sqlStatement);

        invokeDbAndPrintJson(connection, sqlStatement);
    }

}