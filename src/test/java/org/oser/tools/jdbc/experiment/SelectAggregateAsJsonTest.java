package org.oser.tools.jdbc.experiment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.github.benmanes.caffeine.cache.Cache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.oser.tools.jdbc.DbExporter;
import org.oser.tools.jdbc.DbRecord;
import org.oser.tools.jdbc.Fk;
import org.oser.tools.jdbc.TestHelpers;
import org.oser.tools.jdbc.experiment.testbed.Book;

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
    //    String sqlStatement = SelectAggregateAsJson.selectStatementForAggregateSelection("book", filteredFkCache, "oracle");
        String sqlStatement = SelectAggregateAsJson.selectStatementForAggregateSelection("book", filteredFkCache, "postgres");

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

    // can we map hierarchies easily? Y
    // does it work with arrays in json and single values in Java? Y
    // can we easily convert underscore_separated to camelCase names? Y
    @Test
    void deserializeJsonHierarchyToJava() throws JsonProcessingException {
        String s = "{\"id\": 3, \"title\": \"Slaughter House Five\", \"dms_id\": 2, \"author_id\": 3, \"number_pages\": 350, \"images\": [{\"id\":33, \"name\": \"authorPhoto\" }], \"author\": [{\"id\": 3, \"last_name\": \"Vonnegut\"}]}";

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        mapper.configure(DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS, true);
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        Book bookFromJSON = mapper.readValue(s, Book.class);

        Assertions.assertNotNull(bookFromJSON);
        System.out.println(bookFromJSON);
    }
}