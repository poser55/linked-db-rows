package org.oser.tools.jdbc.graphviz;

import java.util.List;

/** What fields should we show for each table in the graphviz graph?
 *  Ignores field names that are not found (so you can often make it simple and add all you want to see globally).*/
@FunctionalInterface
public interface TableToFieldMapper {

    List<String> fieldsToShowPerTable(String tableName);

    /** Default fields to show in a graphviz graph for each table */
    TableToFieldMapper DEFAULT_TABLE_TO_FIELD_MAPPER = (tableName) -> {
        return List.of("name", "code", "title", "last_name", "username");
    };
}
