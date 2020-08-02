package org.oser.tools.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DbExporterSmallTest {

    @Test
    void pkTable() {
        DbExporter.RowLink t1 = new DbExporter.RowLink("lender/1");
        assertEquals("lender", t1.tableName);
        assertEquals(1L, t1.pk);

        Assertions.assertThrows(IllegalArgumentException.class, () -> {new DbExporter.RowLink("l");});


        assertEquals(new DbExporter.RowLink("1", (byte)1), new DbExporter.RowLink("1", (long)1));
    }

    @Test
    void testJsonToRecord() throws SQLException, IOException, ClassNotFoundException {
        Connection mortgageConnection = getConnection("mortgage");
        DbExporter db2GraphMortgage = new DbExporter();
        Record book = db2GraphMortgage.contentAsGraph(mortgageConnection, "book", "1");

        System.out.println("book:"+book.asJson());

        Record book2 = DbImporter.jsonToRecord(mortgageConnection, "book", book.asJson());

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
