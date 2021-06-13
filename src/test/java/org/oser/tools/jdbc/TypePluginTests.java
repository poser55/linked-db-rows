package org.oser.tools.jdbc;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;

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

        byte[] apachePicture = fileToByteArray("testpictures/icon-apache.png");

        try (PreparedStatement preparedStatement = connection.prepareStatement(
                "UPDATE special_datatypes SET a_blob = ? WHERE id = ?")) {

            preparedStatement.setBlob(1,
                    new ByteArrayInputStream(apachePicture));
            preparedStatement.setInt(2, 3);
            int i = preparedStatement.executeUpdate();
            System.out.println("inserted picture in blob "+i);
        }

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

        TestHelpers.BasicChecksResult basicChecksResult3 = TestHelpers.testExportImportBasicChecks(connection,
                "special_datatypes", 3, 1);
        System.out.println("id 3 as json"+ basicChecksResult3.getAsRecordAgain());
        System.out.println("");

        byte[] asByte = (byte[]) basicChecksResult3.getAsRecord().findElementWithName("a_blob").value;
        Assert.assertArrayEquals( apachePicture, asByte );

        Object remappedPkId3 = basicChecksResult3.getRowLinkObjectMap().values().stream().findFirst().get();
        TestHelpers.BasicChecksResult basicChecksResult4 = TestHelpers.testExportImportBasicChecks(connection,
                "special_datatypes", remappedPkId3, 1);

        System.out.println(basicChecksResult4.getAsRecordAgain());

        byte[] asByte2 = (byte[]) basicChecksResult4.getAsRecord().findElementWithName("a_blob").value;
        Assert.assertArrayEquals( apachePicture, asByte2 );

    }

    public static byte[] fileToByteArray(String filename) throws IOException {
        final ClassPathResource classPathResource = new ClassPathResource(filename);
        return IOUtils.toByteArray(classPathResource.getInputStream());
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
