package org.oser.tools.jdbc;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.internal.jdbc.DriverDataSource;
import org.h2.tools.Server;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class TestContainerTest {


    @Test
    void testJsonToRecord() throws SQLException, IOException, ClassNotFoundException {
        Connection mortgageConnection = getConnection("demo");
        Db2Graph db2GraphMortgage = new Db2Graph();
        Record book = db2GraphMortgage.contentAsGraph(mortgageConnection, "book", "1");

        System.out.println("book:"+book.asJson());

    }


    public static Connection getConnection(String dbName) throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");

        //DriverDataSource ds = initDb(dbName);

        Connection con = ds.getConnection();
        con.setAutoCommit(true);
        return con;
    }

    static DriverDataSource ds;

    static {
        try {
            ds = initDb("demo");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @NotNull
    private static DriverDataSource initDb(String dbName) throws SQLException {
        DriverDataSource ds = new DriverDataSource(TestContainerTest.class.getClassLoader(),
                "", "jdbc:tc:postgresql:12.3:///" + dbName +"?TC_DAEMON=true", "postgres", "admin" );

        // db at  http://localhost:8082/
        Server webServer = Server.createWebServer("-webAllowOthers", "-webPort", "8082", "-webAdminPassword", "admin").start();

        Flyway flyway = Flyway.configure().dataSource(ds).load();
        flyway.migrate();
        return ds;
    }

}
