package org.oser.tools.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.internal.jdbc.DriverDataSource;
import org.h2.tools.Server;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;


import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestContainerTest {

    @Test
    void testJsonToRecord() throws Exception {
        Connection demoConnection = TestHelpers.getConnection("demo"); // getConnectionTestContainer("demo");
        DbExporter db2Graphdemo = new DbExporter();
        Record book = db2Graphdemo.contentAsTree(demoConnection, "book", "1");

        System.out.println("book:" + book.asJson());

        DbImporter dbImporter = new DbImporter();
        Record book2 = dbImporter.jsonToRecord(demoConnection, "book", book.asJson());

        System.out.println("book2:" + book2.asJson());

        ObjectMapper mapper = JdbcHelpers.getObjectMapper();

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


    public static Connection getConnectionTestContainer(String dbName) throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");

        ds = getDb(dbName);

        Connection con = ds.getConnection();
        con.setAutoCommit(true);
        return con;
    }

    static DriverDataSource ds;


    // todo handle multiple db names properly
    @NotNull
    private static DriverDataSource getDb(String dbName) throws SQLException {
        if (ds != null) {
            return ds;
        }

        DriverDataSource ds = new DriverDataSource(TestContainerTest.class.getClassLoader(),
                "", "jdbc:tc:postgresql:12.3:///" + dbName + "?TC_DAEMON=true", "postgres", "admin");

        // db console at  http://localhost:8082/
        Server webServer = Server.createWebServer("-webAllowOthers", "-webPort", "8082", "-webAdminPassword", "admin").start();

        Flyway flyway = Flyway.configure().dataSource(ds).load();
        flyway.migrate();
        return ds;
    }
}
