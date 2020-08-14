package org.oser.tools.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JdbcHelpersTest {

    @Test
    void primaryKeyIndex() {
        List<String> primaryKeys = Arrays.asList("A", "B", "C", "d");
        Map<String, Integer> stringIntegerMap = JdbcHelpers.getStringIntegerMap(primaryKeys);

        assertEquals(3, stringIntegerMap.get("D"));
        assertEquals(0, stringIntegerMap.get("A"));
        assertEquals(4, stringIntegerMap.keySet().size());
    }

    @Test
    void pkTable() {
        RowLink t1 = new RowLink("lender/1");
        assertEquals("lender", t1.tableName);
        assertEquals(1L, t1.pks[0]);

        Assertions.assertThrows(IllegalArgumentException.class, () -> {new RowLink("l");});

        assertEquals(new RowLink("1", (byte)1), new RowLink("1", (long)1));

        RowLink t2 = new RowLink("lender/1/a/a/a");

        System.out.println(t2);
    }

    @Test
    void tableNotExistingTest() throws SQLException, IOException, ClassNotFoundException {
        Connection demo = TestHelpers.getConnection("demo");
        Assertions.assertThrows(IllegalArgumentException.class, () -> JdbcHelpers.assertTableExists(demo, "xxx"));
        JdbcHelpers.assertTableExists(demo, "book");
    }

    @Test
    void doesPkTableExist() throws SQLException, IOException, ClassNotFoundException {
        Connection demo = TestHelpers.getConnection("demo");
        Map<String, JdbcHelpers.ColumnMetadata> columnMetadata = JdbcHelpers.getColumnMetadata(demo.getMetaData(), "edge");
        assertTrue(JdbcHelpers.doesPkTableExist(demo, "edge", Arrays.asList("begin_id", "end_id"), Arrays.asList(1, 2), columnMetadata));
        assertFalse(JdbcHelpers.doesPkTableExist(demo, "edge", Arrays.asList("begin_id", "end_id"), Arrays.asList(1, 9), columnMetadata));

        Map<String, JdbcHelpers.ColumnMetadata> columnMetadata2 = JdbcHelpers.getColumnMetadata(demo.getMetaData(), "nodes");
        assertTrue(JdbcHelpers.doesPkTableExist(demo, "nodes", Arrays.asList("node_id"), Arrays.asList(5), columnMetadata2));
        assertFalse(JdbcHelpers.doesPkTableExist(demo, "nodes", Arrays.asList("node_id"), Arrays.asList(99999999999L), columnMetadata2));
    }
}