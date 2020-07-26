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
import java.util.SortedMap;
import java.util.TreeMap;

/**
 *  Export db data to json.
 *
 * License: Apache 2.0 */
public class Db2Graph {
    private static final Logger LOGGER = LoggerFactory.getLogger(Db2Graph.class);

    /*
        Issues:
          *table2Graph: lacks recursion, lacks recursion termination
             done
          *data structure Data: its subRow should hold different fk links (e.g. the product AND the volumeAddon)
                better: subRow-> subRowS
                   Map<String, List<List<Data>>>
                       rel'name   rows underneath
               done
          *method rootToJson / to insert statements whatever
                done
    ---
      what if "wrong" data is linked?
        e.g. a case has a person with chosen_asset_amounts, on this chosen_asset_amounts there is ANOTHER case linked

         => is actually correct

     ---
      importing:
         *check order
         *check whether entries (table/PK) already exist
         *warn about schema deltas
         *allow to re-map to other  PKs

     */

    public Db2Graph() {}

    public static class Fk {
        public String targetTable;
        public String columnName;
        public String type;
        public String originTable;
        public String targetColumn;
        public boolean inverted; // excluded in equals!

        @Override
        public String toString() {
            return "Fk{" +
                    "targetTable='" + targetTable + '\'' +
                    ", columnName='" + columnName + '\'' +
                    ", type='" + type + '\'' +
                    ", originTable='" + originTable + '\'' +
                    ", targetColumn='" + targetColumn + '\'' +
                    ", inverted=" + inverted +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Fk fk = (Fk) o;

            if (targetTable != null ? !targetTable.equals(fk.targetTable) : fk.targetTable != null) return false;
            if (columnName != null ? !columnName.equals(fk.columnName) : fk.columnName != null) return false;
            if (type != null ? !type.equals(fk.type) : fk.type != null) return false;
            if (originTable != null ? !originTable.equals(fk.originTable) : fk.originTable != null) return false;
            return targetColumn != null ? targetColumn.equals(fk.targetColumn) : fk.targetColumn == null;
        }

        @Override
        public int hashCode() {
            int result = targetTable != null ? targetTable.hashCode() : 0;
            result = 31 * result + (columnName != null ? columnName.hashCode() : 0);
            result = 31 * result + (type != null ? type.hashCode() : 0);
            result = 31 * result + (originTable != null ? originTable.hashCode() : 0);
            result = 31 * result + (targetColumn != null ? targetColumn.hashCode() : 0);
            return result;
        }
    }

    /**
     * a table & its pk  (uniquely identifies a db row)
     */
    static class PkTable {
        public PkTable(String tableName, Object pk) {
            this.tableName = tableName;
            this.pk = normalizePk(pk);
        }

        public static Object normalizePk(Object pk) {
            return pk instanceof Number ? ((Number) pk).longValue() : pk;
        }

        public PkTable(String shortExpression) {
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
        Map<PkTable, Record> visitedNodes = new HashMap<>();
        Set<Fk> treatedFks = new HashSet<>();

        @Override
        public String toString() {
            return "ExportContext{" +
                    "visitedNodes=" + visitedNodes +
                    ", treatedFks=" + treatedFks +
                    '}';
        }

        public boolean containsNode(String tableName, Object pk){
            return visitedNodes.containsKey(new PkTable(tableName, pk));
        }

    }


    /**
     * Main method: recursively scan a graph of db data and return it
     */
    public static Record contentAsGraph(Connection connection, String tableName, Object pkValue) throws SQLException {
        ExportContext context = new ExportContext();

        assertTableExists(connection, tableName);

        Record data = readOneRecord(connection, tableName, pkValue, context);
        addSubRowDataFromFks(connection, tableName, pkValue, data, context);

        data.optionalMetadata.put(RecordMetadata.EXPORT_CONTEXT, context);

        return data;
    }


    /**
     * complement the "data" by starting from "tableName" and recursively adding data that is connected via FKs
     */
    private static void addSubRowDataFromFks(Connection connection, String tableName, Object pkValue, Record data, ExportContext context) throws SQLException {
        List<Fk> fks = table2Fk(connection, tableName);

        for (Fk fk : fks) {
            context.treatedFks.add(fk);

            Record.Data elementWithName = findElementWithName(data, fk.inverted ? fk.targetColumn : fk.columnName);
            if ((elementWithName != null) && (elementWithName.value != null)) {
                String subTableName = fk.inverted ? fk.originTable : fk.targetTable;
                String subFkName = fk.inverted ? fk.columnName : fk.targetColumn;

                if (!context.containsNode(subTableName, elementWithName.value)) {
                    List<Record> subRow = readLinkedRecords(connection, subTableName,
                            subFkName, elementWithName.value, true, context);
                    elementWithName.subRow.put(subTableName, subRow);
                }
            }

        }
    }

    static Record.Data findElementWithName(Record data, String columnName) {
        for (Record.Data d : data.content) {
            if (d.name.equals(columnName)) {
                return d;
            }
        }
        return null;
    }


    /**
     * get FK metadata of one table (both direction of metadata, exported and imported FKs)
     */
    public static List<Fk> table2Fk(Connection connection, String table) throws SQLException {
        List<Fk> fks = new ArrayList<>();
        DatabaseMetaData dm = connection.getMetaData();

        ResultSet rs = dm.getExportedKeys(null, null, table);
        addFks(fks, rs, false);

        rs = dm.getImportedKeys(null, null, table);
        addFks(fks, rs, true);

        return fks;
    }

    private static void addFks(List<Fk> fks, ResultSet rs, boolean inverted) throws SQLException {
        while (rs.next()) {
            Fk fk = new Fk();

            fk.originTable = rs.getString("pktable_name");
            fk.columnName = rs.getString("pkcolumn_name");
            fk.targetTable = rs.getString("fktable_name");
            fk.targetColumn = rs.getString("fkcolumn_name");
            fk.inverted = inverted;

            fks.add(fk);
        }
    }


    static Record readOneRecord(Connection connection, String tableName, Object pkValue, ExportContext context) throws SQLException {
        Record data = new Record(tableName, pkValue);

        DatabaseMetaData metaData = connection.getMetaData();
        Map<String, ColumnMetadata> columns = getColumnNamesAndTypes(metaData, tableName);
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
                        Record.Data d = new Record.Data();
                        d.name = rsMetaData.getColumnName(i);
                        d.value = rs.getObject(i);
                        d.metadata = columns.get(d.name.toUpperCase());
                        data.content.add(d);
                    }
                }
            }
        }
        context.visitedNodes.put(new PkTable(tableName, pkValue), data);

        return data;
    }

    static List<Record> readLinkedRecords(Connection connection, String tableName, String fkName, Object fkValue, boolean nesting, ExportContext context) throws SQLException {
        List<Record> listOfRows = new ArrayList<>();

        DatabaseMetaData metaData = connection.getMetaData();
        Map<String, ColumnMetadata> columns = getColumnNamesAndTypes(metaData, tableName);
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
                    if (context.containsNode(tableName, row.pkTable.pk)) {
                        // termination condition
                        doNotNestThisRecord = true;
                    }
                    context.visitedNodes.put(new PkTable(tableName, row.pkTable.pk), row);

                    if (nesting && !doNotNestThisRecord) {
                        addSubRowDataFromFks(connection, tableName, row.pkTable.pk, row, context);
                    }

                    listOfRows.add(row);
                }
            }
        }

        return listOfRows;
    }

    private static Record innerReadRecord(String tableName, Map<String, ColumnMetadata> columns, String pkName, ResultSet rs, ResultSetMetaData rsMetaData, int columnCount) throws SQLException {
        Record row = new Record(tableName, null);

        for (int i = 1; i <= columnCount; i++) {
            Record.Data d = new Record.Data();
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

    public static void assertTableExists(Connection connection, String tableName) throws SQLException {
        DatabaseMetaData dbm = connection.getMetaData();

        ResultSet tables = dbm.getTables(null, null, tableName, null);
        if (tables.next()) {
            return; // Table exists
        }
        else {
            throw new IllegalArgumentException("Table " + tableName + " does not exist");
        }
    }


    static List<String> getPrimaryKeys(DatabaseMetaData metadata, String tableName) throws SQLException {
        List<String> result = new ArrayList<>();

        ResultSet rs = metadata.getPrimaryKeys(null, null, adaptCaseForDb(tableName, metadata.getDatabaseProductName()));

        while (rs.next()) {
            result.add(rs.getString("COLUMN_NAME"));
        }

        return result;
    }

    private static String adaptCaseForDb(String originalName, String dbProductName) {
        if (dbProductName.equals("PostgreSQL")) {
            return originalName;
        } else if (dbProductName.equals("H2")) {
            return originalName.toUpperCase();
        }
        return originalName.toUpperCase();
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

    /**
     * @return Map à la fieldName1 -> ColumnMetadata
     */
    static SortedMap<String, ColumnMetadata> getColumnNamesAndTypes(DatabaseMetaData metadata, String tableName) throws SQLException {
        SortedMap<String, ColumnMetadata> result = new TreeMap<>();

        ResultSet rs = metadata.getColumns(null, null, adaptCaseForDb(tableName, metadata.getDatabaseProductName()), null);

        while (rs.next()) {
            result.put(rs.getString("COLUMN_NAME").toUpperCase(),
                    new ColumnMetadata(rs.getString("COLUMN_NAME"),
                            rs.getString("TYPE_NAME"),
                            rs.getString("COLUMN_SIZE"),
                            rs.getInt("ORDINAL_POSITION")));
        }

        return result;
    }

    /**
     * represents simplified JDBC metadata
     */
    public static class ColumnMetadata {
        String name;
        String type;
        String size; // adapt later?
        // starts at 1
        private int ordinalPos;

        public ColumnMetadata(String name, String type, String size, int ordinalPos) {
            this.name = name;
            this.type = type;
            this.size = size;
            this.ordinalPos = ordinalPos;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public String getSize() {
            return size;
        }

        public int getOrdinalPos() {
            return ordinalPos;
        }
    }
}
