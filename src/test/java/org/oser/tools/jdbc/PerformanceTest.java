package org.oser.tools.jdbc;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** run with mission control profiler of intellij, e.g. with postgres; check Call Tree  */
@Disabled
public class PerformanceTest {

    @Test
    void perf1_export() throws SQLException, IOException, ClassNotFoundException {
        Connection demoConnection = TestHelpers.getConnection("demo");
        DbExporter dbExporter = new DbExporter();

        List<Record> r = new ArrayList<>(10000);
        DbExporterBasicTests.Timer t = new DbExporterBasicTests.Timer();
        for (int i = 0; i < 10000; i++) {
            Record record = export1(demoConnection, dbExporter);
            r.add(record);
        }
        t.printCurrent("end");
    }

    private Record export1(Connection demoConnection, DbExporter dbExporter) throws SQLException {
        return dbExporter.contentAsTree(demoConnection, "Nodes", "1");
    }

    @Test
    void perf1_import() throws Exception {
        Connection demoConnection = TestHelpers.getConnection("demo");
        DbExporter dbExporter = new DbExporter();

        Record record1 = export1(demoConnection, dbExporter);

        DbImporter dbImporter = new DbImporter();

        List<Record> r = new ArrayList<>(10000);
        DbExporterBasicTests.Timer t = new DbExporterBasicTests.Timer();
        String string = record1.asJsonNode().toString();
        for (int i = 0; i < 1000; i++) {
            Record record = import1(demoConnection, dbImporter, string);
            r.add(record);
        }
        t.printCurrent("end");
    }

    private Record import1(Connection demoConnection, DbImporter dbImporter, String string) throws Exception {
        Record book2 = dbImporter.jsonToRecord(demoConnection, "Nodes", string);

        Map<RowLink, Object> pkAndTableObjectMap = dbImporter.insertRecords(demoConnection, book2);
        return book2;
    }

}
