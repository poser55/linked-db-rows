package org.oser.tools.jdbc;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import static org.oser.tools.jdbc.Fk.getFksOfTable;

/**
 *  Export db data to json.
 *
 * License: Apache 2.0 */
public class DbExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbExporter.class);

    private Set<String> stopTablesExcluded = new HashSet<>();
    private Set<String> stopTablesIncluded = new HashSet<>();

    private Cache<String, List<Fk>> fkCache = Caffeine.newBuilder()
            .maximumSize(1000).build();

    private Cache<String, List<String>> pkCache = Caffeine.newBuilder()
            .maximumSize(1000).build();

    private Cache<String, SortedMap<String, JdbcHelpers.ColumnMetadata>> metadataCache = Caffeine.newBuilder()
            .maximumSize(1000).build();


    protected DbExporter() {}

    /**
     * Main method: recursively scan a tree linked db rows and return it
     */
    public Record contentAsTree(Connection connection, String tableName, Object pkValue) throws SQLException {
        ExportContext context = new ExportContext();

        JdbcHelpers.assertTableExists(connection, tableName);

        Record data = readOneRecord(connection, tableName, pkValue, context);
        addSubRowDataFromFks(connection, tableName, pkValue, data, context);

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

        public boolean containsNode(String tableName, Object pk){
            return visitedNodes.containsKey(new RowLink(tableName, pk));
        }

    }

    Record readOneRecord(Connection connection, String tableName, Object pkValue, ExportContext context) throws SQLException {
        Record data = new Record(tableName, pkValue);

        DatabaseMetaData metaData = connection.getMetaData();
        Map<String, JdbcHelpers.ColumnMetadata> columns = JdbcHelpers.getColumnMetadata(metaData, tableName, metadataCache);
        List<String> primaryKeys = JdbcHelpers.getPrimaryKeys(metaData, tableName, pkCache);

        String pkName = primaryKeys.get(0);
        String selectPk = "SELECT * from " + tableName + " where  " + pkName + " = ?";


        try (PreparedStatement pkSelectionStatement = connection.prepareStatement(selectPk)) { // NOSONAR: now unchecked values all via prepared statement
            innerSetStatementField(columns.get(pkName.toUpperCase()).getType(), pkSelectionStatement, 1, pkValue.toString());

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
        context.visitedNodes.put(new RowLink(tableName, pkValue), data);

        return data;
    }

    List<Record> readLinkedRecords(Connection connection, String tableName, String fkName, Object fkValue, boolean nesting, ExportContext context) throws SQLException {
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
        String selectPk = "SELECT * from " + tableName + " where  " + fkName + " = ?";

        try (PreparedStatement pkSelectionStatement = connection.prepareStatement(selectPk)) { // NOSONAR: now unchecked values all via prepared statement
            innerSetStatementField(columns.get(pkName.toUpperCase()).getType(), pkSelectionStatement, 1, fkValue.toString());

            try (ResultSet rs = pkSelectionStatement.executeQuery()) {
                ResultSetMetaData rsMetaData = rs.getMetaData();
                int columnCount = rsMetaData.getColumnCount();
                while (rs.next()) { // treat 1 fk-link
                    Record row = innerReadRecord(tableName, columns, pkName, rs, rsMetaData, columnCount);
                    if (context.containsNode(tableName, row.rowLink.pk)) {
                        continue; // we have already read this node
                    }

                    boolean doNotNestThisRecord = false;

                    // todo: clean up condition (first part cannot occur)
                    if (context.containsNode(tableName, row.rowLink.pk) || stopTablesIncluded.contains(tableName)) {
                        // termination condition
                        doNotNestThisRecord = true;
                    }
                    context.visitedNodes.put(new RowLink(tableName, row.rowLink.pk), row);

                    if (nesting && !doNotNestThisRecord) {
                        addSubRowDataFromFks(connection, tableName, row.rowLink.pk, row, context);
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
    void addSubRowDataFromFks(Connection connection, String tableName, Object pkValue, Record data, ExportContext context) throws SQLException {
        List<Fk> fks = getFksOfTable(connection, tableName, fkCache);

        for (Fk fk : fks) {
            context.treatedFks.add(fk);

            Record.FieldAndValue elementWithName = data.findElementWithName(fk.inverted ? fk.fkcolumn : fk.pkcolumn);
            if ((elementWithName != null) && (elementWithName.value != null)) {
                String subTableName = fk.inverted ? fk.pktable : fk.fktable;
                String subFkName = fk.inverted ? fk.pkcolumn : fk.fkcolumn;

             //   if (!context.containsNode(subTableName, elementWithName.value)) {
                    List<Record> subRow = this.readLinkedRecords(connection, subTableName,
                            subFkName, elementWithName.value, true, context);
                    if (!subRow.isEmpty()){
                        elementWithName.subRow.put(subTableName, subRow);
                    }

               // }
            }

        }
    }



    private static Record innerReadRecord(String tableName, Map<String, JdbcHelpers.ColumnMetadata> columns, String pkName, ResultSet rs, ResultSetMetaData rsMetaData, int columnCount) throws SQLException {
        Record row = new Record(tableName, null);

        for (int i = 1; i <= columnCount; i++) {
            String columnName = rsMetaData.getColumnName(i);
            Record.FieldAndValue d = new Record.FieldAndValue(columnName, columns.get(columnName.toUpperCase()), rs.getObject(i));
            row.content.add(d);

            if (d.name.toUpperCase().equals(pkName.toUpperCase())) {
                row.setPkValue(d.value);
            }

        }
        return row;
    }



    private static void innerSetStatementField(String typeAsString, PreparedStatement preparedStatement, int statementIndex, String valueToInsert) throws SQLException {
        switch (typeAsString) {
            case "BOOLEAN":
            case "bool":
                preparedStatement.setBoolean(statementIndex, Boolean.parseBoolean(valueToInsert.trim()));
                break;
            case "int2":
            case "int4":
            case "int8":
            case "serial":
                preparedStatement.setLong(statementIndex, Long.parseLong(valueToInsert.trim()));
                break;
            case "numeric":
            case "DECIMAL":
                if (valueToInsert.trim().isEmpty()) {
                    preparedStatement.setNull(statementIndex, Types.NUMERIC);
                } else {
                    preparedStatement.setDouble(statementIndex, Double.parseDouble(valueToInsert.trim()));
                }
                break;
            case "timestamp":
                preparedStatement.setTimestamp(statementIndex, Timestamp.valueOf(LocalDateTime.parse(valueToInsert)));
                break;
            default:
                preparedStatement.setObject(statementIndex, valueToInsert);
        }
    }

    public Set<String> getStopTablesExcluded() {
        return stopTablesExcluded;
    }

    public Set<String> getStopTablesIncluded() {
        return stopTablesIncluded;
    }
}
