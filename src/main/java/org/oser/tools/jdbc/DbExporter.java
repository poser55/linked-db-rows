package org.oser.tools.jdbc;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;

import static org.oser.tools.jdbc.Fk.getFksOfTable;

/**
 *  Export db data to json.
 *
 * License: Apache 2.0 */
public class DbExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbExporter.class);

    // configuration settings:
    private final Set<String> stopTablesExcluded = new HashSet<>();
    private final Set<String> stopTablesIncluded = new HashSet<>();

    private final Cache<String, List<Fk>> fkCache = Caffeine.newBuilder()
            .maximumSize(1000).build();

    private final Cache<String, List<String>> pkCache = Caffeine.newBuilder()
            .maximumSize(1000).build();

    private final Cache<String, SortedMap<String, JdbcHelpers.ColumnMetadata>> metadataCache = Caffeine.newBuilder()
            .maximumSize(1000).build();


    protected DbExporter() {}

    /**
     * Main method: recursively read a tree of linked db rows and return it
     */
    public Record contentAsTree(Connection connection, String tableName, Object... pkValue) throws SQLException {
        ExportContext context = new ExportContext();

        JdbcHelpers.assertTableExists(connection, tableName);

        Record data = readOneRecord(connection, tableName, pkValue, context);
        addSubRowDataFromFks(connection, tableName, data, context);

        data.optionalMetadata.put(RecordMetadata.EXPORT_CONTEXT, context);

        return data;
    }

    /**
     * stores context about the export (to avoid infinite loops)
     */
    public static class ExportContext {
        Map<RowLink, Record> visitedNodes = new HashMap<>();
        Set<Fk> treatedFks = new HashSet<>();

        @Override
        public String toString() {
            return "ExportContext{" +
                    "visitedNodes=" + visitedNodes +
                    ", treatedFks=" + treatedFks +
                    '}';
        }

        public boolean containsNode(String tableName, Object[] pk){
            return visitedNodes.containsKey(new RowLink(tableName, pk));
        }

    }

    Record readOneRecord(Connection connection, String tableName, Object[] pkValues, ExportContext context) throws SQLException {
        Record data = new Record(tableName, pkValues);

        DatabaseMetaData metaData = connection.getMetaData();
        Map<String, JdbcHelpers.ColumnMetadata> columns = JdbcHelpers.getColumnMetadata(metaData, tableName, metadataCache);
        List<String> primaryKeys = JdbcHelpers.getPrimaryKeys(metaData, tableName, pkCache);

        data.setColumnMetadata(columns);

        String pkName = primaryKeys.get(0);
        String selectPk = selectStatementByPks(tableName, pkName, primaryKeys);

        try (PreparedStatement pkSelectionStatement = connection.prepareStatement(selectPk)) { // NOSONAR: now unchecked values all via prepared statement
            JdbcHelpers.ColumnMetadata columnMetadata = columns.get(pkName.toUpperCase());
            setPksStatementFields(pkSelectionStatement, primaryKeys, columns, pkValues, pkName);

            try (ResultSet rs = pkSelectionStatement.executeQuery()) {
                ResultSetMetaData rsMetaData = rs.getMetaData();
                int columnCount = rsMetaData.getColumnCount();
                if (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = rsMetaData.getColumnName(i);
                        Record.FieldAndValue d = new Record.FieldAndValue(columnName, columns.get(columnName.toUpperCase()), rs.getObject(i) );
                        data.content.add(d);
                    }
                }
            }
        }
        context.visitedNodes.put(new RowLink(tableName, pkValues), data);

        return data;
    }


    // todo: for now we just support such select statement with 1 fkName

    private String selectStatementByPks(String tableName, String fkName, List<String> primaryKeys) {
        return  "SELECT * from " + tableName + " where  " + fkName + " = ?";
//        } else {
//            LOGGER.error("!!! multiple fks not yet supported! {} {}", tableName, primaryKeys);
//            String whereClause = primaryKeys.stream().collect(Collectors.joining(" = ?,", "", " = ?"));
//            return "SELECT * from " + tableName + " where  " + whereClause;
//        }
    }

    private void setPksStatementFields(PreparedStatement pkSelectionStatement, List<String> primaryKeys, Map<String, JdbcHelpers.ColumnMetadata> columnMetadata, Object[] values, String fkName) throws SQLException {
        JdbcHelpers.ColumnMetadata fieldMetadata = columnMetadata.get(fkName.toUpperCase());
        JdbcHelpers.innerSetStatementField(pkSelectionStatement, fieldMetadata.getType(), 1, Objects.toString(values[0]), fieldMetadata);

//            int i = 0;
//            for (String pkName : primaryKeys) {
//                JdbcHelpers.ColumnMetadata fieldMetadata = columnMetadata.get(pkName.toUpperCase());
//                JdbcHelpers.innerSetStatementField(pkSelectionStatement, fieldMetadata.getType(), i + 1, Objects.toString(values[i]), fieldMetadata);
//                i++;
//            }
//        }
    }


    List<Record> readLinkedRecords(Connection connection, String tableName, String fkName, Object[] fkValues, boolean nesting, ExportContext context) throws SQLException {
        List<Record> listOfRows = new ArrayList<>();

        if (stopTablesExcluded.contains(tableName)) {
            return listOfRows;
        }

        DatabaseMetaData metaData = connection.getMetaData();
        Map<String, JdbcHelpers.ColumnMetadata> columns = JdbcHelpers.getColumnMetadata(metaData, tableName, metadataCache);
        List<String> primaryKeys = JdbcHelpers.getPrimaryKeys(metaData, tableName, pkCache);

        if (primaryKeys.isEmpty()) {
            return listOfRows; // for tables without a pk
        }

        String pkName = primaryKeys.get(0);
        String selectPk = selectStatementByPks(tableName, fkName, primaryKeys);

        try (PreparedStatement pkSelectionStatement = connection.prepareStatement(selectPk)) { // NOSONAR: now unchecked values all via prepared statement
            JdbcHelpers.ColumnMetadata columnMetadata = columns.get(pkName.toUpperCase());
            setPksStatementFields(pkSelectionStatement, primaryKeys, columns, fkValues, fkName);

            try (ResultSet rs = pkSelectionStatement.executeQuery()) {
                ResultSetMetaData rsMetaData = rs.getMetaData();
                int columnCount = rsMetaData.getColumnCount();
                while (rs.next()) { // treat 1 fk-link
                    Record row = innerReadRecord(tableName, columns, pkName, rs, rsMetaData, columnCount, primaryKeys);
                    if (context.containsNode(tableName, row.rowLink.pks)) {
                        continue; // we have already read this node
                    }

                    boolean doNotNestThisRecord = false;

                    // todo: clean up condition (first part cannot occur)
                    if (context.containsNode(tableName, row.rowLink.pks) || stopTablesIncluded.contains(tableName)) {
                        // termination condition
                        doNotNestThisRecord = true;
                    }
                    context.visitedNodes.put(new RowLink(tableName, row.rowLink.pks), row);

                    if (nesting && !doNotNestThisRecord) {
                        addSubRowDataFromFks(connection, tableName, row, context);
                    }

                    listOfRows.add(row);
                }
            }
        }

        return listOfRows;
    }

    /**
     * complement the record "data" by starting from "tableName" and recursively adding data that is connected via FKs
     */
    void addSubRowDataFromFks(Connection connection, String tableName, Record data, ExportContext context) throws SQLException {
        List<Fk> fks = getFksOfTable(connection, tableName, fkCache);

        for (Fk fk : fks) {
            context.treatedFks.add(fk);

            Record.FieldAndValue elementWithName = data.findElementWithName(fk.inverted ? fk.fkcolumn : fk.pkcolumn);
            if ((elementWithName != null) && (elementWithName.value != null)) {
                String subTableName = fk.inverted ? fk.pktable : fk.fktable;
                String subFkName = fk.inverted ? fk.pkcolumn : fk.fkcolumn;

                List<Record> subRow = this.readLinkedRecords(connection, subTableName,
                        subFkName, new Object[] {elementWithName.value }, true, context); // todo: fix can there be multiple fields in a fk?
                if (!subRow.isEmpty()) {
                    elementWithName.subRow.put(subTableName, subRow);
                }
            }
        }
    }



    private static Record innerReadRecord(String tableName, Map<String, JdbcHelpers.ColumnMetadata> columns, String pkName, ResultSet rs, ResultSetMetaData rsMetaData, int columnCount, List<String> primaryKeys) throws SQLException {
        Record row = new Record(tableName, null);
        row.setColumnMetadata(columns);

        Map<String, Integer> primaryKeyArrayPosition = JdbcHelpers.getStringIntegerMap(primaryKeys);
        Object[] primaryKeyValues = new Object[primaryKeys.size()];

        for (int i = 1; i <= columnCount; i++) {
            String columnName = rsMetaData.getColumnName(i);
            Record.FieldAndValue d = new Record.FieldAndValue(columnName, columns.get(columnName.toUpperCase()), rs.getObject(i));
            row.content.add(d);

            if (primaryKeyArrayPosition.containsKey(d.name.toUpperCase())){
                primaryKeyValues[primaryKeyArrayPosition.get(d.name.toUpperCase())] = d.value;
            }
        }
        row.setPkValue(primaryKeyValues);
        return row;
    }



    public Set<String> getStopTablesExcluded() {
        return stopTablesExcluded;
    }

    public Set<String> getStopTablesIncluded() {
        return stopTablesIncluded;
    }
}
