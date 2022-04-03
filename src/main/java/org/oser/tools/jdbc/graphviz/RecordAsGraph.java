package org.oser.tools.jdbc.graphviz;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import guru.nidi.graphviz.attribute.Label;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.MutableNode;
import org.oser.tools.jdbc.Fk;
import org.oser.tools.jdbc.FkCacheAccessor;
import org.oser.tools.jdbc.Record;
import org.oser.tools.jdbc.RowLink;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static guru.nidi.graphviz.model.Factory.mutGraph;
import static guru.nidi.graphviz.model.Factory.mutNode;

/**
 * Experimental code to display a Record as a graphviz graph.
 *   Requires the optional (maven) dependency to https://github.com/nidi3/graphviz-java <br/>
 *
 *   Format: <br/>
 *     * the name of a node is the string representation of its {@link RowLink }  <br/>
 *     * we add some details to nodes (such as the name attribute in case it exists)
 */
public class RecordAsGraph implements FkCacheAccessor {

    // todo add helper methods to control the displaying of the graph
    //  allow to add other attributes to the node

    private final Cache<String, List<Fk>> fkCache = Caffeine.newBuilder().maximumSize(10_000).build();

    public MutableGraph recordAsGraph(Connection connection, Record r) throws SQLException {
        Map<Record, MutableNode> nodes = new HashMap();
        r.getAllRecords().forEach(record -> nodes.put(record, recordAsNode(record)));
        MutableNode[] nodesAsArray = nodes.values().toArray(new MutableNode[0]);

        Map<Record, Set<Record>> fkLinks = Record.determineRowDependencies(connection, new ArrayList<>(r.getAllRecords()), fkCache);
        addFkLinksToNodes(fkLinks, nodes);

        MutableGraph g = mutGraph().setDirected(true).add(nodesAsArray);

        return g;
    }

    private void addFkLinksToNodes(Map<Record, Set<Record>> fkLinks, Map<Record, MutableNode> nodes) {
        for (Map.Entry<Record, Set<Record>> entries : fkLinks.entrySet()) {
            for (Record targetRecord : entries.getValue()){
                nodes.get(targetRecord).addLink(nodes.get(entries.getKey()));
            }
        }
    }

    private MutableNode recordAsNode(Record record) {
        String rowlinkAsString = record.getRowLink().toString();
        return mutNode(rowlinkAsString).add(Label.html("<b>"+rowlinkAsString+"</b>"+getDisplayContentForRecord(record)));
    }

    private String getDisplayContentForRecord(Record record) {
        Record.FieldAndValue optionalName = record.findElementWithName("name");
        return optionalName != null ? ("<br/>"+ Objects.toString(optionalName.getValue())) : "";
    }

    /** Render file as png file */
    public void renderGraph(MutableGraph g, int width, File f) throws IOException {
        // to debug text output:
        //System.out.println("file:"+ Graphviz.fromGraph(g).width(width).render(Format.PLAIN).toString());

        Graphviz.fromGraph(g).width(width).render(Format.PNG).toFile(f);
    }

    @Override
    public Cache<String, List<Fk>> getFkCache() {
        return fkCache;
    }
}
