package org.oser.tools.jdbc.spi.pkgenerator;

import org.junit.jupiter.api.Test;
import org.oser.tools.jdbc.TestHelpers;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SequencePkGeneratorTest {

    @Test
    void basicTest() throws SQLException, ClassNotFoundException, IOException {
        Connection connection = TestHelpers.getConnection("demo");
        Object nextValue = new SequencePkGenerator().generatePk(connection, "datatypes" , "", "");
        assertTrue(nextValue instanceof Number);
        assertThrows(IllegalArgumentException.class, () -> new SequencePkGenerator().generatePk(connection, "**-", "",""));
        Object nextValue2 = new SequencePkGenerator("_seq", Collections.emptyMap()).generatePk(connection, "datatypes_id", "", "");
        assertTrue(nextValue2 instanceof Number);
        assertTrue((Long)nextValue < (Long)nextValue2);
        Object nextValue3 = new SequencePkGenerator( Map.of("nodes", "strange_id_seq")).generatePk(connection, "nodes", "", "");
        assertTrue(nextValue3 instanceof Number);
    }
}