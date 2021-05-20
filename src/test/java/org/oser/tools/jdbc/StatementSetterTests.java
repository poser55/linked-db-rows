package org.oser.tools.jdbc;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.oser.tools.jdbc.spi.statements.JdbcStatementSetter;

import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;

class StatementSetterTests {

    @BeforeAll
    public static void init() {
        TestHelpers.initLogback();
    }

    @Test
    // remark: in the mean time we have added the UUID type in JdbcHelpers (this is no longer needed but this still works and tests the plugin mechanism)
    void datatypesTest() throws Exception {
        TestHelpers.DbConfig databaseConfig = TestHelpers.getDbConfig();

        JdbcStatementSetter uuidSetter = (preparedStatement, statementIndex, columnMetadata, valueToInsert) -> {
            try {
                if (valueToInsert == null) {
                    preparedStatement.setObject(statementIndex, valueToInsert, Types.OTHER);
                } else {
                    preparedStatement.setNull(statementIndex, Types.OTHER);
                }

            } catch (SQLException throwables) {
                // do nothing
            }

            return true; // bypass normal treatment
        };

        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                        dbExporter -> {},
                        dbImporter -> {
                             dbImporter.getJdbcStatementSetterPlugins().put("UUID", uuidSetter);
                        },
                        record -> {}
                        ,
                        new HashMap<>(),
                        "special_datatypes", 1, 1, true);

        TestHelpers.BasicChecksResult basicChecksResult2 = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                        dbExporter -> {},
                        dbImporter -> {
                            dbImporter.getJdbcStatementSetterPlugins().put("UUID", uuidSetter);
                        },
                        record -> {}
                        ,
                        new HashMap<>(),
                        "special_datatypes", 2, 1, true);

    }

}
