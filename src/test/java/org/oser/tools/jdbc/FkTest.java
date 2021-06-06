package org.oser.tools.jdbc;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

    @Test
    void matchFkStrings() {
        Fk.FkMatchedFields parsed = new Fk.FkMatchedFields("a(b)-c(d)").parse();
        assertEquals("a", parsed.getTable1());
        assertEquals("c", parsed.getTable2());
        assertEquals("b", parsed.getFields1AsString());
        assertEquals("d", parsed.getFields2AsString());
    }

    @Test
    void parseMultiFks() throws SQLException, IOException, ClassNotFoundException {
        Connection demo = TestHelpers.getConnection("demo");
        DbExporter importerOrExporter = new DbExporter();
        Fk.addVirtualForeignKeyAsString(demo, importerOrExporter, "a(b)-c(d);aa(bb)-cc(dd)");
        List<Fk> a = importerOrExporter.getFkCache().getIfPresent("a");
        List<Fk> aa = importerOrExporter.getFkCache().getIfPresent("aa");
        assertNotNull(a);
        assertNotNull(aa);
    }
}