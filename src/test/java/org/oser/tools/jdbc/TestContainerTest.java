package org.oser.tools.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.internal.jdbc.DriverDataSource;
import org.h2.tools.Server;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Map;


import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestContainerTest {

    @Test
    void testJsonToRecord() throws SQLException, IOException, ClassNotFoundException {
        Connection mortgageConnection = Db2GraphSmallTest.getConnection("mortgage"); // getConnectionTestContainer("demo");
        Db2Graph db2GraphMortgage = new Db2Graph();
        Record book = db2GraphMortgage.contentAsGraph(mortgageConnection, "book", "1");

        System.out.println("book:"+book.asJson());

        Record book2 = JsonImporter.jsonToRecord(mortgageConnection, "book", book.asJson());

        System.out.println("book2:"+book2.asJson());

        ObjectMapper mapper = new ObjectMapper();

        // todo: known issue .jsonToRecord converts json keys to upper case
        assertEquals(mapper.readTree(book.asJson().toLowerCase()), mapper.readTree(book2.asJson().toLowerCase()));


        Record author1 = db2GraphMortgage.contentAsGraph(mortgageConnection, "author", "1");
        Record author2 = JsonImporter.jsonToRecord(mortgageConnection, "author", author1.asJson());

        assertEquals(mapper.readTree(author1.asJson().toLowerCase()), mapper.readTree(author2.asJson().toLowerCase()));

        // as inserts

//        String s = JsonImporter.asInserts(mortgageConnection, book2, EnumSet.noneOf(TreatmentOptions.class));
        String s = JsonImporter.recordAsInserts(mortgageConnection, book2, EnumSet.of(TreatmentOptions.ForceInsert));
        System.out.println("\ninserts: "+s);

    }

    @Test
    void testRemapping() throws SQLException, IOException, ClassNotFoundException {
        Connection mortgageConnection = Db2GraphSmallTest.getConnection("mortgage"); // getConnectionTestContainer("demo");
        Db2Graph db2GraphMortgage = new Db2Graph();
        Record book = db2GraphMortgage.contentAsGraph(mortgageConnection, "book", "1");

        System.out.println("book:" + book.asJson());

        Record book2 = JsonImporter.jsonToRecord(mortgageConnection, "book", book.asJson());

        Map<Db2Graph.PkAndTable, Object> pkAndTableObjectMap = JsonImporter.insertRecords(mortgageConnection, book2, new InserterOptions());
        System.out.println("remapped: " + pkAndTableObjectMap.size() + " lenderPk" + pkAndTableObjectMap.keySet().stream()
                .filter(p -> p.tableName.equals("book")).map(pkAndTableObjectMap::get).collect(toList()));


    }


        @Test
    void testInsert() throws SQLException, ClassNotFoundException, IOException {
        Connection mortgageConnection = Db2GraphSmallTest.getConnection("mortgage");

        String json = "{ \"id\":7,\"author_id\":1, \"author\":[{\"id\":1,\"last_name\":\"Orwell\"}],\"title\":\"1984_summer\" }";

        Record book = JsonImporter.jsonToRecord(mortgageConnection, "book", json);
        assertEquals(2, book.getAllNodes().size());

        System.out.println(JsonImporter.recordAsInserts(mortgageConnection, book, EnumSet.noneOf(TreatmentOptions.class)));
    }

    @Test
    void tableNotExistingTest() throws SQLException, IOException, ClassNotFoundException {
        Connection mortgage = Db2GraphSmallTest.getConnection("mortgage");
        Assertions.assertThrows(IllegalArgumentException.class, () -> Db2Graph.assertTableExists(mortgage, "xxx"));
        Db2Graph.assertTableExists(mortgage, "book");
//        Assertions.assertThrows(IllegalArgumentException.class, () -> Db2Graph.assertTableExists(getConnectionTestContainer("demo"), "xxx"));
//        Db2Graph.assertTableExists(getConnectionTestContainer("demo"), "book");
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
                "", "jdbc:tc:postgresql:12.3:///" + dbName +"?TC_DAEMON=true", "postgres", "admin" );

        // db console at  http://localhost:8082/
        Server webServer = Server.createWebServer("-webAllowOthers", "-webPort", "8082", "-webAdminPassword", "admin").start();

        Flyway flyway = Flyway.configure().dataSource(ds).load();
        flyway.migrate();
        return ds;
    }

}
