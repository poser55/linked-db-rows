package org.oser.tools.jdbc.graphviz;

import guru.nidi.graphviz.model.Graph;
import guru.nidi.graphviz.model.MutableGraph;
import org.junit.jupiter.api.Test;
import org.oser.tools.jdbc.DbExporter;
import org.oser.tools.jdbc.Record;
import org.oser.tools.jdbc.TestHelpers;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

public class RecordAsGraphTest {

    @Test
    void simple() throws SQLException, IOException, ClassNotFoundException {
        Connection demo = TestHelpers.getConnection("demo");

        DbExporter exporter = new DbExporter();
        Record records = exporter.contentAsTree(demo, "Nodes", 1);

        RecordAsGraph asGraph = new RecordAsGraph();
        MutableGraph graph = asGraph.recordAsGraph(demo, records);

        asGraph.renderGraph(graph, 900, new File( "graph.png"));

    }
}