package org.oser.tools.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DeletionTests {

    @Test
    void basicDeleteTest_blog() throws Exception {
        Connection demo = TestHelpers.getConnection("demo");
        DbExporter dbExporter = new DbExporter();

        Fk.initFkCacheForMysql_LogException(demo, dbExporter.getFkCache());

        Record blogpost = dbExporter.contentAsTree(demo, "blogpost", 2);
        List<String> deletionStatements = dbExporter.getDeleteStatements(demo, blogpost);
        assertNotNull(deletionStatements);
        assertEquals(3, deletionStatements.size());
    }

    @Test
    @DisabledIfSystemProperty(named = "mixedCaseTableNames", matches = "false")  // mysql has issues with such tables
    void basicDeleteTest2_graph() throws Exception {
        Connection demo = TestHelpers.getConnection("demo");
        DbExporter dbExporter = new DbExporter();
        List<String> deletionStatements = dbExporter.getDeleteStatements(demo, dbExporter.contentAsTree(demo, "Nodes", 1));
        assertNotNull(deletionStatements);
        System.out.println(deletionStatements);
        assertEquals(10, deletionStatements.size());
    }
}
