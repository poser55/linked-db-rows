package org.oser.tools.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

@EnabledIfSystemProperty(named = "sakila", matches = "false")
public class SakilaTests {

    @Test
        // is a bit slow
    void sakila() throws SQLException, ClassNotFoundException, IOException {
        Connection sakilaConnection = TestHelpers.getConnection("sakila");

        List<String> actorInsertList = JdbcHelpers.determineOrder(sakilaConnection, "actor");
        System.out.println("list:"+actorInsertList +"\n");

        DbExporter dbExporter = new DbExporter();
        dbExporter.getStopTablesExcluded().add("inventory");

        Record actor199 = dbExporter.contentAsTree(sakilaConnection, "actor", 199);
        String asString = actor199.asJson();

        Set<RowLink> allNodes = actor199.getAllNodes();
        System.out.println(asString +" \nnumberNodes:"+ allNodes.size());

        System.out.println("classified:"+Record.classifyNodes(allNodes));

        DbImporter dbImporter = new DbImporter();
        Record asRecord = dbImporter.jsonToRecord(sakilaConnection, "actor", asString);

        // todo: still many issues with importing due to missing array support
        //dbImporter.insertRecords(sakilaConnection, asRecord);

        //Map<RowLink, Object> actor = dbImporter.insertRecords(sakilaConnection, asRecord);
        // System.out.println(actor + " "+actor.size());
    }

    @Test
    void sakilaJustOneTable() throws Exception {
        Connection connection = TestHelpers.getConnection("sakila");

        List<String> actorInsertList = JdbcHelpers.determineOrder(connection, "actor");
        System.out.println("list:"+actorInsertList +"\n");

        HashMap<RowLink, Object> remapping = new HashMap<>();
        // why is this needed? Somehow he does not detect that language (originally =1 for these films) should be remapped
        // and the other test with all entries fails (as he adds the entries inserted here to what he exports in the other test)
        // it is correct: the stop-table avoids that we add (and remap!) the language table
        remapping.put(new RowLink("language/1"), 7);

        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(connection,
                31,
                dbExporter -> {
                    dbExporter.getStopTablesIncluded().add("film");
                    dbExporter.getStopTablesExcluded().add("inventory");

                    // not exporting release_year (custom types work on in basic way, the mapping to json is suboptimal)
                    dbExporter.getFieldExporters().put("release_year", FieldExporter.NOP_FIELDEXPORTER);
                },
                dbImporter -> {
                    dbImporter.getFieldMappers().put("special_features", FieldMapper.NOP_FIELDMAPPER);
                },
                null,
                remapping,
                "actor", 199);

        System.out.println("classified:"+Record.classifyNodes(basicChecksResult.getAsRecord().getAllNodes()));
    }

    @Test
    void testStopTableIncluded() throws Exception {
        final FieldMapper nopFieldMapper = (metadata, statement, insertIndex, value) -> statement.setArray(insertIndex, null);

        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("sakila"),
                12448,
                dbExporter -> {
                    dbExporter.getStopTablesIncluded().add("inventory");

                    // not exporting release_year (custom types work on in basic way, the mapping to json is suboptimal)
                    dbExporter.getFieldExporters().put("release_year", FieldExporter.NOP_FIELDEXPORTER);
                },
                dbImporter -> {
                    dbImporter.getFieldMappers().put("special_features", nopFieldMapper);

                }, "actor", 199);

        System.out.println("classified:"+Record.classifyNodes(basicChecksResult.getAsRecord().getAllNodes()));
    }



}
