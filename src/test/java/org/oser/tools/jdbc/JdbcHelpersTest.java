package org.oser.tools.jdbc;

import org.junit.jupiter.api.Test;

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
}