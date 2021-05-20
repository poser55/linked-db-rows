package org.oser.tools.jdbc;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;

import static org.oser.tools.jdbc.Fk.getFksOfTable;

/**
 *  Export db data to JSON.
 */
public class DbExporter implements FkCacheAccessor {
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

    /** experimental feature to order results by first pk when exporting */
    private boolean orderResults = true;

    public DbExporter() {}

    /**
     * Main method: recursively read a tree of linked db rows and return it
     */
    public Record contentAsTree(Connection connection, String tableName, Object... pkValue) throws SQLException {
        ExportContext context = new ExportContext(connection);

        JdbcHelpers.assertTableExists(connection, tableName);

        Record data = readOneRecord(connection, tableName, pkValue, context);
        addSubRowDataFromFks(connection, tableName, data, context);

        data.optionalMetadata.put(RecordMetadata.EXPORT_CONTEXT, context);

        return data;
    }

    /**
     * Stores context about the export (to avoid infinite loops)
     *  (NB: Will likely be changed)
     */
    @Getter
    public static class ExportContext {
        Map<RowLink, Record> visitedNodes = new HashMap<>();
        Set<Fk> treatedFks = new HashSet<>();

        DatabaseMetaData metaData;
        String dbProductName;

        public ExportContext(Connection connection) throws SQLException {
            metaData = connection.getMetaData();
            dbProductName = metaData.getDatabaseProductName();
        }

        @Override
        public String toString() {
            return "ExportContext{" +
                    "visitedNodes=" + visitedNodes +
                    ", treatedFks=" + treatedFks +
                    '}';
        }

        public boolean containsNode(String tableName, Object[] pk){
            return visitedNodes.containsKey(new RowLink(tableName.toLowerCase(), pk));
        }

    }

    Record readOneRecord(Connection connection, String tableName, Object[] pkValues, ExportContext context) throws SQLException {
        Record data = new Record(tableName, pkValues);

        DatabaseMetaData metaData = connection.getMetaData();
        Map<String, JdbcHelpers.ColumnMetadata> columns = JdbcHelpers.getColumnMetadata(metaData, tableName, metadataCache);
        List<String> primaryKeys = JdbcHelpers.getPrimaryKeys(metaData, tableName, pkCache);

        data.setColumnMetadata(columns);

        String pkName = primaryKeys.get(0);
        String selectPk = selectStatementByPks(tableName, pkName, primaryKeys, false);

        try (PreparedStatement pkSelectionStatement = connection.prepareStatement(selectPk)) { // NOSONAR: now unchecked values all via prepared statement
            setPksStatementFields(pkSelectionStatement, primaryKeys, columns, pkValues, pkName);

            try (ResultSet rs = pkSelectionStatement.executeQuery()) {
                ResultSetMetaData rsMetaData = rs.getMetaData();
                int columnCount = rsMetaData.getColumnCount();
                if (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = rsMetaData.getColumnName(i).toLowerCase();

                        Record.FieldAndValue d = retrieveFieldAndValue(tableName, columns, rs, i, columnName, context);

                        if (d != null) {
                            data.content.add(d);
                        }
                    }
                } else {
                    throw new IllegalArgumentException("Entry not found "+tableName+" "+ Arrays.toString(pkValues) + " "+pkSelectionStatement);
                }
            }
        }
        context.visitedNodes.put(new RowLink(tableName, pkValues), data);

        return data;
    }

    private static final Set<Integer> STRING_TYPES = Set.of(12, 2004, 2005);

    private Record.FieldAndValue retrieveFieldAndValue(String tableName, Map<String, JdbcHelpers.ColumnMetadata> columns, ResultSet rs, int i, String columnName, ExportContext context) throws SQLException {
        FieldExporter localFieldExporter = getFieldExporter(tableName, columnName);

        // this is a bit hacky for now (as we do not yet have full type support and h2 behaves strangely)
        boolean useGetString = context.getDbProductName().equals("H2") &&
                STRING_TYPES.contains(columns.get(columnName.toLowerCase()).getDataType());
        Object valueAsObject = useGetString ? rs.getString(i) : rs.getObject(i);

        Record.FieldAndValue d;
        if (localFieldExporter != null) {
            d = localFieldExporter.exportField(tableName, columnName, columns.get(columnName.toLowerCase()), valueAsObject, rs);
        } else {
            d = new Record.FieldAndValue(columnName, columns.get(columnName.toLowerCase()), valueAsObject);
        }
        return d;
    }


    // todo: for now we just support such select statement with 1 fkName
    private String selectStatementByPks(String tableName, String fkName, List<String> primaryKeys, boolean orderResult) {
        return  "SELECT * from " + tableName + " where  " + fkName + " = ?" +
                (orderResult ? (" order by "+primaryKeys.get(0)+" asc " ) : "");
//        } else {
//            LOGGER.error("!!! multiple fks not yet supported! {} {}", tableName, primaryKeys);
//            String whereClause = primaryKeys.stream().collect(Collectors.joining(" = ?,", "", " = ?"));
//            return "SELECT * from " + tableName + " where  " + whereClause;
//        }
    }

    private void setPksStatementFields(PreparedStatement pkSelectionStatement, List<String> primaryKeys, Map<String, JdbcHelpers.ColumnMetadata> columnMetadata, Object[] values, String fkName) throws SQLException {
        JdbcHelpers.ColumnMetadata fieldMetadata = columnMetadata.get(fkName.toLowerCase());
        JdbcHelpers.innerSetStatementField(pkSelectionStatement, 1, fieldMetadata, Objects.toString(values[0]));

//            int i = 0;
//            for (String pkName : primaryKeys) {
//                JdbcHelpers.ColumnMetadata fieldMetadata = columnMetadata.get(pkName.toUpperCase());
//                JdbcHelpers.innerSetStatementField(pkSelectionStatement, fieldMetadata.getType(), i + 1, Objects.toString(values[i]), fieldMetadata);
//                i++;
//            }
//        }
    }


    List<Record> readLinkedRecords(Connection connection, String tableName, String fkName, Object[] fkValues, ExportContext context) throws SQLException {
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

        String selectPk = selectStatementByPks(tableName, fkName, primaryKeys, orderResults);

        try (PreparedStatement pkSelectionStatement = connection.prepareStatement(selectPk)) { // NOSONAR: now unchecked values all via prepared statement
            setPksStatementFields(pkSelectionStatement, primaryKeys, columns, fkValues, fkName);

            try (ResultSet rs = pkSelectionStatement.executeQuery()) {
                ResultSetMetaData rsMetaData = rs.getMetaData();
                int columnCount = rsMetaData.getColumnCount();
                while (rs.next()) { // treat 1 fk-link
                    Record row = innerReadRecord(tableName, columns, rs, rsMetaData, columnCount, primaryKeys, context);
                    if (context.containsNode(tableName, row.rowLink.pks)) {
                        continue; // we have already read this node
                    }

                    context.visitedNodes.put(new RowLink(tableName, row.rowLink.pks), row);
                    listOfRows.add(row);
                }
            }
        }

        // now treat subtables
        for (Record row : listOfRows) {
            if (!stopTablesIncluded.contains(tableName)) {
                addSubRowDataFromFks(connection, tableName, row, context);
            }
        }

        return listOfRows;
    }

    /**
     * complement the record "data" by starting from "tableName" and recursively adding data that is connected via FKs
     */
    void addSubRowDataFromFks(Connection connection, String tableName, Record data, ExportContext context) throws SQLException {
        List<Fk> fks = getFksOfTable(connection, tableName, fkCache);

        data.optionalFks = fks;

        for (Fk fk : fks) {
            context.treatedFks.add(fk);

            Record.FieldAndValue elementWithName = data.findElementWithName(fk.inverted ? fk.fkcolumn : fk.pkcolumn);
            if ((elementWithName != null) && (elementWithName.value != null)) {
                String databaseProductName = context.metaData.getDatabaseProductName();

                String subTableName;
                if (databaseProductName.equals("MySQL")) {
                    subTableName = fk.inverted ? fk.pktable : fk.fktable;
                } else {
                    subTableName = (fk.inverted ? fk.pktable : fk.fktable).toLowerCase();
                }

                String subFkName =    (fk.inverted ? fk.pkcolumn : fk.fkcolumn).toLowerCase();

                List<Record> subRow = this.readLinkedRecords(connection, subTableName,
                        subFkName, new Object[] {elementWithName.value }, context); // todo: fix can there be multiple fields in a fk?
                if (!subRow.isEmpty()) {
                    if (!elementWithName.subRow.containsKey(subTableName)) {
                        elementWithName.subRow.put(subTableName, subRow);
                    } else {
                        elementWithName.subRow.get(subTableName).addAll(subRow);
                    }
                }
            }
        }
    }


    private Record innerReadRecord(String tableName, Map<String, JdbcHelpers.ColumnMetadata> columns, ResultSet rs, ResultSetMetaData rsMetaData, int columnCount, List<String> primaryKeys, ExportContext context) throws SQLException {
        Record row = new Record(tableName, null);
        row.setColumnMetadata(columns);

        Map<String, Integer> primaryKeyArrayPosition = JdbcHelpers.getStringIntegerMap(primaryKeys);
        Object[] primaryKeyValues = new Object[primaryKeys.size()];

        for (int i = 1; i <= columnCount; i++) {
            String columnName = rsMetaData.getColumnName(i);
            Record.FieldAndValue d = retrieveFieldAndValue(tableName, columns, rs, i, columnName, context);
            if (d != null) {
                row.content.add(d);

                if (primaryKeyArrayPosition.containsKey(d.name.toLowerCase())){
                    primaryKeyValues[primaryKeyArrayPosition.get(d.name.toLowerCase())] = d.value;
                }

            }
        }
        row.setPkValue(primaryKeyValues);
        return row;
    }

    //region delete

    /** Get SQL statements to delete all the content of a record (needs to be created before).
     * They are in an order they can be executed. */
    public List<String> getDeleteStatements(Connection connection, Record exportedRecord) throws Exception {
        List<String> result = new ArrayList<>();

        CheckedFunction<Record, Void> collectDeletionStatement = (Record r) -> {
            result.add(DbExporter.recordEntryToDeleteStatement(connection.getMetaData(), r, pkCache));
            return null;
        };

        exportedRecord.visitRecordsInInsertionOrder(connection, collectDeletionStatement, false, fkCache);
        Collections.reverse(result);
        return result;
    }

    private static String recordEntryToDeleteStatement(DatabaseMetaData m, Record r, Cache<String, List<String>> pkCache) throws SQLException {
        // todo ensure this is correct in record (so does not need to be gotten again)
        List<String> primaryKeys = JdbcHelpers.getPrimaryKeys(m, r.getRowLink().getTableName(), pkCache);


        String whereClause = "";
        for (int i = 0; i < primaryKeys.size(); i++) {
            JdbcHelpers.ColumnMetadata columnMetadata = r.getColumnMetadata().get(primaryKeys.get(i));
            String optionalQuote = columnMetadata.needsQuoting() ? "'" : "";
            String pk = optionalQuote + r.getRowLink().getPks()[i].toString() + optionalQuote;
            whereClause += primaryKeys.get(i) + "=" + pk + " and ";
        }
        // remove last "and"
        whereClause = whereClause.substring(0, whereClause.length() - 4);

        try (Formatter formatter = new Formatter()){
            return formatter.format("delete from %s where %s", r.getRowLink().getTableName(), whereClause).toString();
        }
    }

    /** Delete the record with all linked rows
     *  CAVEAT: really deletes data, check the data first!
     *   @throws SQLException or an IllegalStateException if there is a problem during deletion
     * @return the record that was deleted */
    public Record deleteRecursively(Connection connection, String tableName, Object... pkValue) throws Exception {
        Record record = contentAsTree(connection, tableName, pkValue);

        List<String> deleteStatements = getDeleteStatements(connection, record);

        doDeletionWithException(connection, deleteStatements);

        return record;
    }

    public void doDeletionWithException(Connection connection, List<String> deleteStatements) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            for (String sqlString : deleteStatements) {
                int count = stmt.executeUpdate(sqlString);
                if (count != 1) {
                    throw new IllegalStateException("Deletion not successful "+sqlString+" result: "+count);
                }
            }
        }
    }

    //endregion delete

    public Set<String> getStopTablesExcluded() {
        return stopTablesExcluded;
    }

    public Set<String> getStopTablesIncluded() {
        return stopTablesIncluded;
    }

    public Cache<String, List<Fk>> getFkCache() {
        return fkCache;
    }

    public Cache<String, List<String>> getPkCache() {
        return pkCache;
    }

    /**  fieldName : String -> <optionalTableName : String, FieldExporter> */
    private final Map<String, Map<String, FieldExporter>> fieldExporters = new HashMap<>();

    /** @param tableName the table for which the FieldExporter should be used, may be null, means all tables
     *  @param fieldName the field for which the FieldExporter should be used */
    public void registerFieldExporter(String tableName, String fieldName, FieldExporter newFieldExporter){
        Objects.requireNonNull(fieldName, "Field must not be null");
        Objects.requireNonNull(newFieldExporter, "FieldExporter must not be null");

        fieldName = fieldName.toLowerCase();
        tableName = (tableName != null) ? tableName.toLowerCase() : null;

        if (!fieldExporters.containsKey(fieldName)){
            fieldExporters.put(fieldName, new HashMap<>());
        }
        fieldExporters.get(fieldName).put(tableName, newFieldExporter);
    }

    /** Access to internal fieldExporters */
    public Map<String, Map<String, FieldExporter>> getFieldExporters() {
        return fieldExporters;
    }


    //@VisibleForTesting
    /** Internal retrieval, @return null if none is found */
    FieldExporter getFieldExporter(String tableName, String fieldName) {
        fieldName = fieldName.toLowerCase();
        tableName = tableName.toLowerCase();

        // find best match
        Map<String, FieldExporter> fieldMatch = fieldExporters.get(fieldName);
        FieldExporter match = null;
        if (fieldMatch != null) {
            match = fieldMatch.get(tableName);

            if (match != null) {
                return match;
            } else {
                match = fieldMatch.get(null);
                return match;
            }
        }
        return null;
    }

    /** experimental feature to order results by first pk when exporting (default: true) */
    public void setOrderResults(boolean orderResults) {
        this.orderResults = orderResults;
    }
}
