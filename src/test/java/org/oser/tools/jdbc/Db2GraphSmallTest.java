package org.oser.tools.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Db2GraphSmallTest {

    @Test
    void pkTable() {
        Db2Graph.PkTable t1 = new Db2Graph.PkTable("lender/1");
        assertEquals("lender", t1.tableName);
        assertEquals(1L, t1.pk);

        Assertions.assertThrows(IllegalArgumentException.class, () -> {new Db2Graph.PkTable("l");});
    }

    @Test
    void testJsonToRecord() throws SQLException, IOException, ClassNotFoundException {
        Connection mortgageConnection = getConnection("mortgage");
        Db2Graph db2GraphMortgage = new Db2Graph();
        Record book = db2GraphMortgage.contentAsGraph(mortgageConnection, "book", "1");

        System.out.println("book:"+book.asJson());

        Record book2 = JsonImporter.jsonToRecord("book", book.asJson(), mortgageConnection);

        System.out.println("book2:"+book2.asJson());
    }

    public static Connection getConnection(String dbName) throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");

        Connection con = DriverManager.getConnection(
                "jdbc:postgresql://localhost/" + dbName, "postgres", "admin");

        con.setAutoCommit(true);
        return con;
    }
}
