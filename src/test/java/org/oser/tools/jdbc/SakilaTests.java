package org.oser.tools.jdbc;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisabledIfSystemProperty(named = "sakila", matches = "false")
class SakilaTests {

    @BeforeAll
    public static void init() {
        TestHelpers.initLogback();
    }

    @Test
        // is a bit slow
    void sakila() throws Exception {
        Connection sakilaConnection = TestHelpers.getConnection("sakila");

        List<String> actorInsertList = JdbcHelpers.determineOrder(sakilaConnection, "actor", false);
        System.out.println("list:"+actorInsertList +"\n");

        DbExporter dbExporter = new DbExporter();
        dbExporter.getStopTablesExcluded().add("inventory");

        Record actor199 = dbExporter.contentAsTree(sakilaConnection, "actor", 199);
        String asString = actor199.asJsonNode().toString();

        Set<RowLink> allNodes = actor199.getAllNodes();
        System.out.println(asString +" \nnumberNodes:"+ allNodes.size());

        System.out.println("classified:"+ Record.classifyNodes(allNodes));

        DbImporter dbImporter = new DbImporter();
        dbImporter.setIgnoreFkCycles(true);
        dbImporter.registerFieldImporter(null, "special_features", FieldImporter.NOP_FIELDIMPORTER);
        Record asRecord = dbImporter.jsonToRecord(sakilaConnection, "actor", asString);

        dbImporter.insertRecords(sakilaConnection, asRecord);

        //Map<RowLink, DbImporter.Remap> actor = dbImporter.insertRecords(sakilaConnection, asRecord);
        // System.out.println(actor + " "+actor.size());
    }

    @Test
    void sakilaJustOneTable() throws Exception {
        Connection connection = TestHelpers.getConnection("sakila");

        List<String> actorInsertList = JdbcHelpers.determineOrder(connection, "actor", false);
        System.out.println("list:"+actorInsertList +"\n");

        HashMap<RowLink, DbImporter.Remap> remapping = new HashMap<>();
        // why is this needed? Somehow he does not detect that language (originally =1 for these films) should be remapped
        // and the other test with all entries fails (as he adds the entries inserted here to what he exports in the other test)
        // it is correct: the stop-table avoids that we add (and remap!) the language table
        remapping.put(new RowLink("language/1"), new DbImporter.Remap(7, 0));

        // todo: enable checkUpdates again (when we can limit the number of ? in the update statement in f() of the fieldMapper)
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(connection,
                dbExporter -> {
                    dbExporter.getStopTablesIncluded().add("film");
                    dbExporter.getStopTablesExcluded().add("inventory");

                    // not exporting release_year (custom types work on in basic way, the mapping to json is suboptimal)
                    dbExporter.registerFieldExporter(null, "release_year", FieldExporter.NOP_FIELDEXPORTER);
                    dbExporter.registerFieldExporter(null, "rating", FieldExporter.NOP_FIELDEXPORTER);
                }, dbImporter -> {
                    dbImporter.registerFieldImporter("film","special_features", FieldImporter.NOP_FIELDIMPORTER);
                    dbImporter.setIgnoreFkCycles(true);
                }, null, remapping, "actor", 199, 31, false
        );

        System.out.println("classified:"+ Record.classifyNodes(basicChecksResult.getAsRecord().getAllNodes()));

        System.out.println("\n canonicalized:"+basicChecksResult.getAsRecord().asJsonNode());
    }

    @Test
    @Disabled // would need to handle the inventory table correctly (which is part of the Fk cycle) to work correctly
    void testStopTableIncluded() throws Exception {
        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("sakila"),
                dbExporter -> {
                    dbExporter.getStopTablesIncluded().add("inventory");

                    // not exporting release_year (custom types work on in basic way, the mapping to json is suboptimal)
                    dbExporter.registerFieldExporter(null, "release_year", FieldExporter.NOP_FIELDEXPORTER);
                }, dbImporter -> {
                    dbImporter.registerFieldImporter(null, "special_features", FieldImporter.NOP_FIELDIMPORTER);
                    dbImporter.setIgnoreFkCycles(true);
                }, "actor", 199, 12671
        );

        System.out.println("classified:"+ Record.classifyNodes(basicChecksResult.getAsRecord().getAllNodes()));
    }

    @Test
    void testGetInsertionOrder() throws SQLException, ClassNotFoundException, IOException {
        List<String> insertionOrder = JdbcHelpers.determineOrder(TestHelpers.getConnection("sakila"), "actor", false);
        assertNotNull(insertionOrder);
        System.out.println(insertionOrder);

        assertThrows(IllegalStateException.class, () ->
                JdbcHelpers.determineOrder(TestHelpers.getConnection("sakila"), "actor", true));
    }
}
