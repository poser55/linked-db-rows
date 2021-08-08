package org.oser.tools.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


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

        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(demo,
                dbExporter -> {
                    Fk.initFkCacheForMysql_LogException(demo, dbExporter.getFkCache());
                },
                dbImporter -> {
                    Fk.initFkCacheForMysql_LogException(demo, dbImporter.getFkCache());
                },
                "link", 1, 5);

        // wrong result before latest refactoring
        System.out.println(new DbExporter().contentAsTree(demo, "combined", 1, 2));

        // delete entries again (for other tests)
        Map<RowLink, Object> rowLinkObjectMap = basicChecksResult.getRowLinkObjectMap();
        Optional<Object> insertedPks = rowLinkObjectMap.keySet().stream().filter(r -> r.getTableName().equals("link")).map(rowlink -> rowLinkObjectMap.get(rowlink)).findFirst();
        assertTrue(insertedPks.isPresent());
        DbExporter dbExporter = new DbExporter();
        Fk.initFkCacheForMysql_LogException(demo, dbExporter.getFkCache());
        Record link = dbExporter.deleteRecursively(demo, "link", insertedPks.get());
    }
}
