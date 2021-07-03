package org.oser.tools.jdbc;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;

/**
 * Hint: a virtual foreign key should usually be a primary key one one side (otherwise inserting it multiple times
 *  leads to duplicated entries).
 *
 * */
public class MultiSchemaExperimentTests {

    public static final String FK_DEFINITION = "book(dms_id)-doc.document(id)";

    @BeforeAll
    public static void init() {
        TestHelpers.initLogback();
    }

    @Test
    void testBasicMultiSchema() throws Exception {
        TestHelpers.DbConfig databaseConfig = TestHelpers.getDbConfig();
        if (databaseConfig.getShortname().equals("oracle") ||
                databaseConfig.getShortname().equals("sqlserver") ||
                databaseConfig.getShortname().equals("mysql")) {
            return;
        }

        Connection demo = TestHelpers.getConnection("demo");
        Statement statement = demo.createStatement();

        // remove duplication of entries with  dms_id that disturb the count of inserts in the test
        int i = statement.executeUpdate(" delete from book where dms_id = 2 and id <> 3");

        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(demo,
                        dbExporter -> {
                            try {
                                Fk.addVirtualForeignKeyAsString(demo, dbExporter, FK_DEFINITION);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        },
                        dbImporter -> {
                            try {
                                Fk.addVirtualForeignKeyAsString(demo, dbImporter, FK_DEFINITION);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        },
                        record -> {
                        },
                        new HashMap<>(),
                        "book", 3,3, true);

        System.out.println("end");
    }

}
