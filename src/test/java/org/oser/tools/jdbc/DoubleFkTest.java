package org.oser.tools.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


class DoubleFkTest {

    @Test
    void basicDoubleFk() throws Exception {
        Connection demo = TestHelpers.getConnection("demo");
        List<Fk> linked = Fk.getFksOfTable(demo, "link");
        List<Fk> combined = Fk.getFksOfTable(demo, "combined");
        System.out.println(linked);
        System.out.println(combined);

        assertEquals(2, linked.get(0).getFkcolumn().length);
        assertEquals(2, linked.get(0).getPkcolumn().length);

        System.out.println(Fk.getFksOfTable(demo, "Edge"));

        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                "link", 1, 2);

        // wrong result before latest refactoring
        System.out.println(new DbExporter().contentAsTree(demo, "combined", 1, 2));
    }
}
