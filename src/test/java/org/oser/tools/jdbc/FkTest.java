package org.oser.tools.jdbc;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FkTest {

    @Test
    void testQuoteRemoval() {
        assertEquals("abc", Fk.removeOptionalQuotes("abc"));
        assertEquals("abc", Fk.removeOptionalQuotes("\"abc\""));
        assertEquals("\"abc", Fk.removeOptionalQuotes("\"abc"));
    }


    @Test
    void testFks() throws SQLException, IOException, ClassNotFoundException {
        Connection demo = TestHelpers.getConnection("demo");
        List<String> allDemoTables = JdbcHelpers.getAllTableNames(demo);
        assertTrue(allDemoTables.size() > 9);
        System.out.println("all tables:" + allDemoTables);
        for (String table : allDemoTables) {
            try {
                System.out.print("table:"+table);
                System.out.println(" "+Fk.getFksOfTable(demo, table));
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }
}