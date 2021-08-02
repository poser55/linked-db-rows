package org.oser.tools.jdbc;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

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

    @Test
    void link2self() throws SQLException, IOException, ClassNotFoundException {
        Connection demo = TestHelpers.getConnection("demo");
        List<Fk> fks = Fk.getFksOfTable(demo, "link2self");
        System.out.println(fks);
        assertEquals(3, fks.size());
        List<Fk> link2self = fks.stream().filter(e -> e.getFkName().toLowerCase().equals("link2self")).collect(Collectors.toList());
        assertEquals(2, link2self.size());
        assertEquals(link2self.get(0).getFktable(), link2self.get(0).getPktable());
        assertEquals(link2self.get(1).getFktable(), link2self.get(1).getPktable());
        assertEquals(1, link2self.get(0).getFkcolumn().length);
    }
}