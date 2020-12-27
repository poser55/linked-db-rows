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
        Connection demo1 = TestHelpers.getConnection("demo");
        List<String> demo = JdbcHelpers.getAllTableNames(demo1);
        assertTrue(demo.size() > 9);
        System.out.println("all tables:" + demo);
        demo.forEach(table -> {
            try {
                System.out.println("table:"+table+" "+Fk.getFksOfTable(demo1, table));
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        });
    }
}