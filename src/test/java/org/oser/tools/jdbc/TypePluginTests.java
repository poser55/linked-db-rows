package org.oser.tools.jdbc;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TypePluginTests {

    @BeforeAll
    public static void init() {
        TestHelpers.initLogback();
    }

    @Test
    // remark: in the mean time we have added the UUID & CLOB types in JdbcHelpers (this is no longer needed but this still works and tests the plugin mechanism)
    void datatypesTest() throws Exception {
        TestHelpers.DbConfig databaseConfig = TestHelpers.getDbConfig();
        Connection connection = TestHelpers.getConnection("demo");

        FieldImporter uuidSetter = (tableToIgnore, columnMetadata, preparedStatement, statementIndex, valueToInsert) -> {
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

        FieldExporter clobExporter = (tableName, fieldName, metadata, resultSet) -> {
            Clob clob = resultSet.getClob(fieldName);
            return new Record.FieldAndValue(fieldName, metadata, clob.getSubString(1, (int) clob.length()));
        };

        FieldImporter clobImporter = (tableName, metadata, statement, insertIndex, value ) -> {
            Clob clob = statement.getConnection().createClob();
            clob.setString(1, (String) value);
            statement.setClob(insertIndex, clob);
            return true;
        };



        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(connection,
                        dbExporter -> {
               //             dbExporter.getTypeFieldExporters().put("CLOB", clobExporter);
                        },
                        dbImporter -> {
                            dbImporter.getTypeFieldImporters().put("UUID", uuidSetter);
                 //           dbImporter.getTypeFieldImporters().put("CLOB", clobImporter);
                        },
                        record -> {}
                        ,
                        new HashMap<>(),
                        "special_datatypes", 1, 1, true);
        assertTrue(basicChecksResult.getAsRecord().asJsonNode().toString().contains("bla"));

        TestHelpers.BasicChecksResult basicChecksResult2 = TestHelpers.testExportImportBasicChecks(connection,
                        dbExporter -> {
                            dbExporter.getTypeFieldExporters().put("CLOB", clobExporter);
                        },
                        dbImporter -> {
                            dbImporter.getTypeFieldImporters().put("UUID", uuidSetter);
                            dbImporter.getTypeFieldImporters().put("CLOB", clobImporter);
                        },
                        record -> {}
                        ,
                        new HashMap<>(),
                        "special_datatypes", 2, 1, true);

    }


    @Test
    /** test BYTEA type plugin (only for postgresql), again is now a std plugin */
    void postgresTest() throws Exception {
        Connection connection = TestHelpers.getConnection("demo");

        if (!connection.getMetaData().getDatabaseProductName().contains("PostgreSQL")) {
            return;
        }

        FieldImporter postgresImporter = (tableName, metadata, statement, insertIndex, value ) -> {
            System.out.println("===");
            if (value != null) {
                InputStream inputStream = new ByteArrayInputStream(value.getBytes());
                statement.setBinaryStream(insertIndex, inputStream);
            } else {
                statement.setArray(insertIndex, null);
            }
            return true;
        };

        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(connection,
                dbExporter -> {
                },
                dbImporter -> {
//                    List.of("file", "thumbnail").
//                            forEach(f -> dbImporter.registerFieldImporter(null, f, /*FieldImporter.NOP_FIELDIMPORTER*/ postgresImporter));
                },
                record -> {}
                ,
                new HashMap<>(),
                "postgres_test", 13, 1, true);

    }



}
