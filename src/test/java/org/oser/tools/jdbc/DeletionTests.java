package org.oser.tools.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DeletionTests {

    @Test
    void basicDeleteTest_blog() throws Exception {
        Connection demo = TestHelpers.getConnection("demo");
        DbExporter dbExporter = new DbExporter();
        List<String> deletionStatements = dbExporter.getDeleteStatements(demo, dbExporter.contentAsTree(demo, "blogpost", 2));
        assertNotNull(deletionStatements);
        assertEquals(3, deletionStatements.size());
    }

    @Test
    void basicDeleteTest2_graph() throws Exception {
        Connection demo = TestHelpers.getConnection("demo");
        DbExporter dbExporter = new DbExporter();
        List<String> deletionStatements = dbExporter.getDeleteStatements(demo, dbExporter.contentAsTree(demo, "Nodes", 1));
        assertNotNull(deletionStatements);
        System.out.println(deletionStatements);
        assertEquals(10, deletionStatements.size());
    }
}
