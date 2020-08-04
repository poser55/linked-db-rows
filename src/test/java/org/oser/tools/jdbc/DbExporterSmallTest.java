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
        RowLink t1 = new RowLink("lender/1");
        assertEquals("lender", t1.tableName);
        assertEquals(1L, t1.pk);

        Assertions.assertThrows(IllegalArgumentException.class, () -> {new RowLink("l");});

        assertEquals(new RowLink("1", (byte)1), new RowLink("1", (long)1));
    }

    @Test
    void testJsonToRecord() throws SQLException, IOException, ClassNotFoundException {
        Connection demoConnection = getConnection("demo");
        DbExporter dbExporter = new DbExporter();
        Record book = dbExporter.contentAsTree(demoConnection, "book", "1");

        System.out.println("book:"+book.asJson());

        Record book2 = new DbImporter().jsonToRecord(demoConnection, "book", book.asJson());

        System.out.println("book2:"+book2.asJson());
    }

    @Test
    void datatypesTest() throws SQLException, ClassNotFoundException, IOException {
        Connection demoConnection = getConnection("demo");

        Record datatypes = new DbExporter().contentAsTree(demoConnection, "datatypes", 1);
        String asString = datatypes.asJson();
        System.out.println("datatypes:"+asString);

        Record asRecord = new DbImporter().jsonToRecord(demoConnection, "datatypes", asString);
        String asStringAgain = asRecord.asJson();

        assertEquals(asString.toUpperCase(), asStringAgain.toUpperCase());
    }

    @Test // is a bit slow
    void sakila() throws SQLException, ClassNotFoundException, IOException {
        Connection sakilaConnection = getConnection("sakila");

        DbExporter dbExporter = new DbExporter();
        dbExporter.getStopTablesExcluded().add("inventory");

        Record actor199 = dbExporter.contentAsTree(sakilaConnection, "actor", 199);
        String asString = actor199.asJson();

        System.out.println(asString);

        DbImporter dbImporter = new DbImporter();
        Record asRecord = dbImporter.jsonToRecord(sakilaConnection, "actor", asString);

        // todo error: has infinite loop in determineOrder
        // Map<RowLink, Object> actor = dbImporter.insertRecords(sakilaConnection, asRecord);
        // System.out.println(actor + " "+actor.size());
    }



    public static Connection getConnection(String dbName) throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");

        Connection con = DriverManager.getConnection(
                "jdbc:postgresql://localhost/" + dbName, "postgres", "admin");

        con.setAutoCommit(true);
        return con;
    }


}
