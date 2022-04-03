package org.oser.tools.jdbc.graphviz;

import guru.nidi.graphviz.model.MutableGraph;
import org.junit.jupiter.api.Test;
import org.oser.tools.jdbc.DbExporter;
import org.oser.tools.jdbc.JdbcHelpers;
import org.oser.tools.jdbc.Record;
import org.oser.tools.jdbc.TestHelpers;


import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RecordAsGraphTest {

    @Test
    void simple() throws SQLException, IOException, ClassNotFoundException {
        Connection demo = TestHelpers.getConnection("demo");

        DbExporter exporter = new DbExporter();
        Record records = exporter.contentAsTree(demo, "Nodes", 1);

        RecordAsGraph asGraph = new RecordAsGraph();
        MutableGraph graph = asGraph.recordAsGraph(demo, records, t -> List.of("name", "node_id"));

        asGraph.renderGraph(graph, 900, new File( "graph.png"));
    }


    @Test
    void testFieldMapping() {
        Record r = new Record("abc", new Object[] {"1", "2", "3"});
        r.getContent().add(new Record.FieldAndValue("name", new JdbcHelpers.ColumnMetadata("name", "1", 1, 1, 1, "COLDEF", 1), 1));
        r.getContent().add(new Record.FieldAndValue("title", new JdbcHelpers.ColumnMetadata("title", "1", 1, 1, 1, "COLDEF", 2), 2));
        assertEquals("<br/>1 <br/>2", RecordAsGraph.getOptionalFieldNameValues(r, TableToFieldMapper.DEFAULT_TABLE_TO_FIELD_MAPPER));
    }
}