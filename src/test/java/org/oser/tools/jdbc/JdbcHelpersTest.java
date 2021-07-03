package org.oser.tools.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
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

        assertEquals(3, stringIntegerMap.get("d"));
        assertEquals(0, stringIntegerMap.get("a"));
        assertEquals(4, stringIntegerMap.keySet().size());
    }

    @Test
    void pkTable() {
        RowLink t1 = new RowLink("lender/1");
        assertEquals("lender", t1.getTableName());
        assertEquals(1L, t1.getPks()[0]);

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
        Map<String, JdbcHelpers.ColumnMetadata> columnMetadata = JdbcHelpers.getColumnMetadata(demo.getMetaData(), "Edge");
        assertTrue(JdbcHelpers.doesRowWithPrimaryKeysExist(demo, "Edge", Arrays.asList("begin_id", "end_id"), Arrays.asList(1, 2), columnMetadata));
        assertFalse(JdbcHelpers.doesRowWithPrimaryKeysExist(demo, "Edge", Arrays.asList("begin_id", "end_id"), Arrays.asList(1, 9), columnMetadata));

        Map<String, JdbcHelpers.ColumnMetadata> columnMetadata2 = JdbcHelpers.getColumnMetadata(demo.getMetaData(), "Nodes");
        assertTrue(JdbcHelpers.doesRowWithPrimaryKeysExist(demo, "Nodes", Arrays.asList("node_id"), Arrays.asList(5), columnMetadata2));
        assertFalse(JdbcHelpers.doesRowWithPrimaryKeysExist(demo, "Nodes", Arrays.asList("node_id"), Arrays.asList(99999999L), columnMetadata2));
    }

    @Test
    void getNumberElementsInEachTable() throws SQLException, ClassNotFoundException, IOException {
        Connection demo = TestHelpers.getConnection("demo");
        List<String> allTableNames = JdbcHelpers.getAllTableNames(demo);
        Map<String, Integer> counts = JdbcHelpers.getNumberElementsInEachTable(demo);
        assertNotNull(allTableNames);
        assertNotNull(counts);

        String dbname = TestHelpers.getDbConfig().getShortname();
        if (!(dbname.equals("oracle") || dbname.equals("sqlserver")  || dbname.equals("mysql"))) {
            Map<String, Integer> counts2 = JdbcHelpers.getNumberElementsInEachTable(demo, "doc");
            assertEquals(1, counts2.size());
        }

        System.out.println(counts + " "+allTableNames);
        assertTrue( counts.keySet().size() >= 10);
        assertTrue(allTableNames.size() >= 10);
    }

    @Test
    void getPrimaryKeys_notExistingTable() throws SQLException, IOException, ClassNotFoundException {
        Connection demo1 = TestHelpers.getConnection("demo");
        DatabaseMetaData metaData = demo1.getMetaData();
        List<String> xxx = JdbcHelpers.getPrimaryKeys(metaData, "xxx");
        assertNotNull(xxx);
    }

    @Test
    void testTable() throws SQLException, IOException, ClassNotFoundException {
        Connection demo1 = TestHelpers.getConnection("demo");
        JdbcHelpers.Table table = new JdbcHelpers.Table(demo1, "aaa.bbb");
        assertEquals("aaa", table.getSchema().toLowerCase());
        assertEquals("bbb", table.getTableName().toLowerCase());

        JdbcHelpers.Table table2 = new JdbcHelpers.Table(demo1, "xxx");
        assertEquals("xxx", table2.getTableName().toLowerCase() );
        String s = table2.getSchema().toLowerCase();
        assertTrue("public".equals(s) || "system".equals(s) || "dbo".equals(s) || "".equals(""));
    }

    @Test
    void multischemaPrimaryKeys() throws SQLException, IOException, ClassNotFoundException {
        Connection demo = TestHelpers.getConnection("demo");
        DatabaseMetaData metaData = demo.getMetaData();
        List<String> book = JdbcHelpers.getPrimaryKeys(metaData, "book");
        assertEquals(1, book.size());

        // does not work for h2
//        book = JdbcHelpers.getPrimaryKeys(metaData, "public.book");
//        assertEquals(1, book.size());

        String dbname = TestHelpers.getDbConfig().getShortname();
        if (!(dbname.equals("oracle") || dbname.equals("sqlserver") || dbname.equals("mysql"))) {
            List<String> document = JdbcHelpers.getPrimaryKeys(metaData, "doc.document");
            assertNotNull(document);
            assertEquals(1, document.size());
        }
    }

    @Test
    void numberOfElementsInTables() throws SQLException, IOException, ClassNotFoundException {
        Connection demo1 = TestHelpers.getConnection("demo");
        Map<String, Integer> numberElementsInEachTable = JdbcHelpers.getNumberElementsInEachTable(demo1, Arrays.asList(demo1.getSchema(), "doc"));
        System.out.println(numberElementsInEachTable);
        assertTrue(numberElementsInEachTable.size() > 0);
    }


}