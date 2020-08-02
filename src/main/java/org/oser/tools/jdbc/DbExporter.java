package org.oser.tools.jdbc;

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
import java.util.Objects;
import java.util.Set;

/**
 *  Export db data to json.
 *
 * License: Apache 2.0 */
public class DbExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbExporter.class);

    protected DbExporter() {}

    /**
     * A table & its pk  (uniquely identifies a db row)
     */
    public static class RowLink {
        public RowLink(String tableName, Object pk) {
            this.tableName = tableName;
            this.pk = normalizePk(pk);
        }

        public static Object normalizePk(Object pk) {
            return pk instanceof Number ? ((Number) pk).longValue() : pk;
        }

        public RowLink(String shortExpression) {
            if (shortExpression == null) {
                throw new IllegalArgumentException("Must not be null");
            }
            int i = shortExpression.indexOf("/");
            if ( i == -1) {
                throw new IllegalArgumentException("Wrong format, missing /:"+shortExpression);
            }
            tableName = shortExpression.substring(0, i);
            String rest = shortExpression.substring(i+1);

            long optionalLongValue = 0;
            try {
                optionalLongValue = Long.parseLong(rest);
            } catch (NumberFormatException e) {
                pk = normalizePk(rest);
            }
            pk = normalizePk(optionalLongValue);
        }

        public String tableName;
        public Object pk;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            String asString = o.toString();
            return asString.equals(this.toString());
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.toString());
        }

        @Override
        public String toString() {
            return tableName + "/" + pk ;
        }
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


    /**
     * Main method: recursively scan a tree linked db rows and return it
     */
    public static Record contentAsTree(Connection connection, String tableName, Object pkValue) throws SQLException {
        ExportContext context = new ExportContext();

        JdbcHelpers.assertTableExists(connection, tableName);

        Record data = readOneRecord(connection, tableName, pkValue, context);
        JdbcHelpers.addSubRowDataFromFks(connection, tableName, pkValue, data, context);

        data.optionalMetadata.put(RecordMetadata.EXPORT_CONTEXT, context);

        return data;
    }


    static Record readOneRecord(Connection connection, String tableName, Object pkValue, ExportContext context) throws SQLException {
        Record data = new Record(tableName, pkValue);

        DatabaseMetaData metaData = connection.getMetaData();
        Map<String, JdbcHelpers.ColumnMetadata> columns = JdbcHelpers.getColumnMetadata(metaData, tableName);
        List<String> primaryKeys = getPrimaryKeys(metaData, tableName);

        String pkName = primaryKeys.get(0);
        String selectPk = "SELECT * from " + tableName + " where  " + pkName + " = ?";


        try (PreparedStatement pkSelectionStatement = connection.prepareStatement(selectPk)) { // NOSONAR: now unchecked values all via prepared statement
            innerSetStatementField(columns.get(pkName.toUpperCase()).getType(), pkSelectionStatement, 1, pkValue.toString());

            try (ResultSet rs = pkSelectionStatement.executeQuery()) {
                ResultSetMetaData rsMetaData = rs.getMetaData();
                int columnCount = rsMetaData.getColumnCount();
                if (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        Record.FieldAndValue d = new Record.FieldAndValue();
                        d.name = rsMetaData.getColumnName(i);
                        d.value = rs.getObject(i);
                        d.metadata = columns.get(d.name.toUpperCase());
                        data.content.add(d);
                    }
                }
            }
        }
        context.visitedNodes.put(new RowLink(tableName, pkValue), data);

        return data;
    }

    static List<Record> readLinkedRecords(Connection connection, String tableName, String fkName, Object fkValue, boolean nesting, ExportContext context) throws SQLException {
        List<Record> listOfRows = new ArrayList<>();

        DatabaseMetaData metaData = connection.getMetaData();
        Map<String, JdbcHelpers.ColumnMetadata> columns = JdbcHelpers.getColumnMetadata(metaData, tableName);
        List<String> primaryKeys = getPrimaryKeys(metaData, tableName);

        String pkName = primaryKeys.get(0);
        String selectPk = "SELECT * from " + tableName + " where  " + fkName + " = ?";

        try (PreparedStatement pkSelectionStatement = connection.prepareStatement(selectPk)) { // NOSONAR: now unchecked values all via prepared statement
            innerSetStatementField(columns.get(pkName.toUpperCase()).getType(), pkSelectionStatement, 1, fkValue.toString());

            try (ResultSet rs = pkSelectionStatement.executeQuery()) {
                ResultSetMetaData rsMetaData = rs.getMetaData();
                int columnCount = rsMetaData.getColumnCount();
                while (rs.next()) { // treat 1 fk-link
                    Record row = innerReadRecord(tableName, columns, pkName, rs, rsMetaData, columnCount);

                    boolean doNotNestThisRecord = false;
                    if (context.containsNode(tableName, row.rowLink.pk)) {
                        // termination condition
                        doNotNestThisRecord = true;
                    }
                    context.visitedNodes.put(new RowLink(tableName, row.rowLink.pk), row);

                    if (nesting && !doNotNestThisRecord) {
                        JdbcHelpers.addSubRowDataFromFks(connection, tableName, row.rowLink.pk, row, context);
                    }

                    listOfRows.add(row);
                }
            }
        }

        return listOfRows;
    }

    private static Record innerReadRecord(String tableName, Map<String, JdbcHelpers.ColumnMetadata> columns, String pkName, ResultSet rs, ResultSetMetaData rsMetaData, int columnCount) throws SQLException {
        Record row = new Record(tableName, null);

        for (int i = 1; i <= columnCount; i++) {
            Record.FieldAndValue d = new Record.FieldAndValue();
            d.name = rsMetaData.getColumnName(i);
            d.value = rs.getObject(i);
            d.metadata = columns.get(d.name.toUpperCase());
            row.content.add(d);

            if (d.name.toUpperCase().equals(pkName.toUpperCase())) {
                row.setPkValue(d.value);
            }

        }
        return row;
    }



    ////////////////////


    static List<String> getPrimaryKeys(DatabaseMetaData metadata, String tableName) throws SQLException {
        List<String> result = new ArrayList<>();

        ResultSet rs = metadata.getPrimaryKeys(null, null, JdbcHelpers.adaptCaseForDb(tableName, metadata.getDatabaseProductName()));

        while (rs.next()) {
            result.add(rs.getString("COLUMN_NAME"));
        }

        return result;
    }

    private static void innerSetStatementField(String typeAsString, PreparedStatement preparedStatement, int statementIndex, String valueToInsert) throws SQLException {
        switch (typeAsString) {
            case "BOOLEAN":
            case "bool":
                preparedStatement.setBoolean(statementIndex, Boolean.parseBoolean(valueToInsert.trim()));
                break;
            case "int4":
            case "int8":
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

}
