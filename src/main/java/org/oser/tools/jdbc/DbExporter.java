package org.oser.tools.jdbc;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Getter;

import java.sql.Blob;
import java.sql.Clob;
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
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.oser.tools.jdbc.Fk.getFksOfTable;

/**
 *  Export db data to JSON.
 */
public class DbExporter implements FkCacheAccessor {
    // configuration settings:
    private final Set<String> stopTablesExcluded = new HashSet<>();
    private final Set<String> stopTablesIncluded = new HashSet<>();
    private final Set<String> stopTablesIncludeOne = new HashSet<>();

    private final Cache<String, List<Fk>> fkCache = Caffeine.newBuilder()
            .maximumSize(1000).build();

    private final Cache<String, List<String>> pkCache = Caffeine.newBuilder()
            .maximumSize(1000).build();

    private final Cache<String, SortedMap<String, JdbcHelpers.ColumnMetadata>> metadataCache = Caffeine.newBuilder()
            .maximumSize(1000).build();

    /** to overwrite how special JDBC types are retrieved on ResultSets. The keys must be the uppercased JDBC type name.
     */
    private final Map<String, FieldExporter> typeFieldExporters = new HashMap<>();

    { // init field exporters
        FieldExporter clobExporter = (tableName, fieldName, metadata, resultSet) -> {
            Clob clob = resultSet.getClob(fieldName);
            return new DbRecord.FieldAndValue(fieldName, metadata, clob == null ? null : clob.getSubString(1, (int) clob.length()));
        };
        typeFieldExporters.put("CLOB", clobExporter);

        FieldExporter blobExporter = (tableName, fieldName, metadata, resultSet) -> {
            Blob blob = resultSet.getBlob(fieldName);

            if (blob != null) {
                long length = blob.length();
                if (length > (int) length) {
                    throw new IllegalStateException("Blob too long (does not fit in int)!" + fieldName + " " + tableName + " " + metadata);
                }
                return new DbRecord.FieldAndValue(fieldName, metadata, blob.getBytes(1, (int) length));
            } else {
                return new DbRecord.FieldAndValue(fieldName, metadata, null);
            }
        };
        typeFieldExporters.put("BLOB", blobExporter);
    }

    /** experimental feature to order results by first pk when exporting */
    private boolean orderResults = true;

    /**
     * Main method: recursively read a tree of linked db rows and return it
     */
    public DbRecord contentAsTree(Connection connection, String tableName, Object... pkValue) throws SQLException {
        ExportContext context = new ExportContext(connection);

        if (pkCache.getIfPresent(tableName) == null) {
            JdbcHelpers.assertTableExists(connection, tableName);
        }

        DbRecord data = readOneRecord(connection, tableName, pkValue, context);
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
        Map<RowLink, DbRecord> visitedNodes = new HashMap<>();
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

        public boolean containsTable(String tableName){
            return visitedNodes.keySet().stream().map(RowLink::getTableName).anyMatch(table -> tableName.equals(table));
        }

    }

    DbRecord readOneRecord(Connection connection, String tableName, Object[] pkValues, ExportContext context) throws SQLException {
        DbRecord data = new DbRecord(tableName, pkValues);

        DatabaseMetaData metaData = connection.getMetaData();
        Map<String, JdbcHelpers.ColumnMetadata> columns = JdbcHelpers.getColumnMetadata(metaData, tableName, metadataCache);
        List<String> primaryKeys = JdbcHelpers.getPrimaryKeys(metaData, tableName, pkCache);
        data.setPkNames(primaryKeys);

        if (primaryKeys.size() == 0) {
            throw new IllegalStateException("Primary keys of " + tableName + " not found.");
        }

        data.setColumnMetadata(columns);

        String selectPk = selectStatementByPks(tableName, primaryKeys, false);

        try (PreparedStatement pkSelectionStatement = connection.prepareStatement(selectPk)) { // NOSONAR: now unchecked values all via prepared statement
            for (int i = 0; i < primaryKeys.size(); i++) {
                JdbcHelpers.innerSetStatementField(pkSelectionStatement, i+1, columns.get(primaryKeys.get(i).toLowerCase()),
                        Objects.toString(pkValues[i]), null);
            }

            Loggers.logSelectStatement(pkSelectionStatement, selectPk, Arrays.asList(pkValues));

            try (ResultSet rs = pkSelectionStatement.executeQuery()) {
                ResultSetMetaData rsMetaData = rs.getMetaData();
                int columnCount = rsMetaData.getColumnCount();
                if (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = rsMetaData.getColumnName(i).toLowerCase();

                        DbRecord.FieldAndValue d = retrieveFieldAndValue(tableName, columns, rs, i, columnName, context);

                        if (d != null) {
                            data.getContent().add(d);
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

    private DbRecord.FieldAndValue retrieveFieldAndValue(String tableName,
                                                         Map<String, JdbcHelpers.ColumnMetadata> columns,
                                                         ResultSet rs, int i,
                                                         String columnName,
                                                         ExportContext context) throws SQLException {
        FieldExporter localFieldExporter = getFieldExporter(tableName, columnName);

        if (localFieldExporter == null) {
            localFieldExporter = typeFieldExporters.get(columns.get(columnName).getType().toUpperCase());
        }

        DbRecord.FieldAndValue d;
        if (localFieldExporter != null) {
            d = localFieldExporter.exportField(tableName, columnName, columns.get(columnName.toLowerCase()), rs);
        } else {
            // this is a bit hacky as h2 behaves strangely if we do not get string types via ResultSet#getString
            boolean useGetString = context.getDbProductName().equals("H2") &&
                    STRING_TYPES.contains(columns.get(columnName.toLowerCase()).getDataType());
            Object valueAsObject = useGetString ? rs.getString(i) : rs.getObject(i);

            d = new DbRecord.FieldAndValue(columnName, columns.get(columnName.toLowerCase()), valueAsObject);
        }
        return d;
    }


    private String selectStatementByPks(String tableName, List<String> fkNames, boolean orderResult) {
        String whereClause = fkNames.stream().collect(Collectors.joining(" = ? AND ", "", " = ?"));
        return  "SELECT * FROM " + tableName + " WHERE  " + whereClause +
                (orderResult ? (" ORDER BY "+fkNames.get(0)+" asc " ) : "");
    }

    List<DbRecord> readLinkedRecords(Connection connection, String tableName, String[] fkNames, Object[] fkValues, ExportContext context) throws SQLException {
        List<DbRecord> listOfRows = new ArrayList<>();

        if (stopTablesExcluded.contains(tableName) || stopAfterFirstInstance(tableName, context)  ||
                (stopTablesIncluded.contains(tableName) && context.containsTable(tableName) ))  {
            return listOfRows;
        }

        DatabaseMetaData metaData = connection.getMetaData();
        Map<String, JdbcHelpers.ColumnMetadata> columns = JdbcHelpers.getColumnMetadata(metaData, tableName, metadataCache);
        List<String> primaryKeys = JdbcHelpers.getPrimaryKeys(metaData, tableName, pkCache);

        if (primaryKeys.isEmpty()) {
            return listOfRows; // for tables without a pk
        }

        String selectPk = selectStatementByPks(tableName, Arrays.asList(fkNames), orderResults);

        try (PreparedStatement pkSelectionStatement = connection.prepareStatement(selectPk)) { // NOSONAR: now unchecked values all via prepared statement
            for (int i = 0; i < fkValues.length; i++) {
                JdbcHelpers.innerSetStatementField(pkSelectionStatement, i+1, columns.get(fkNames[i].toLowerCase()),
                        Objects.toString(fkValues[i]), null);
            }

            Loggers.logSelectStatement(pkSelectionStatement, selectPk, Arrays.asList(fkValues));
            try (ResultSet rs = pkSelectionStatement.executeQuery()) {
                ResultSetMetaData rsMetaData = rs.getMetaData();
                int columnCount = rsMetaData.getColumnCount();
                while (rs.next()) { // treat 1 fk-link
                    DbRecord row = innerReadRecord(tableName, columns, rs, rsMetaData, columnCount, primaryKeys, context);
                    if (context.containsNode(tableName, row.getRowLink().getPks())) {
                        continue; // we have already read this node
                    }

                    context.visitedNodes.put(new RowLink(tableName, row.getRowLink().getPks()), row);
                    listOfRows.add(row);
                }
            }
        }

        // now treat subtables
        for (DbRecord row : listOfRows) {
            if (!stopTablesIncluded.contains(tableName)) {
                addSubRowDataFromFks(connection, tableName, row, context);
            }
        }

        return listOfRows;
    }

    /**
     * complement the record "data" by starting from "tableName" and recursively adding data that is connected via FKs
     */
    void addSubRowDataFromFks(Connection connection, String tableName, DbRecord data, ExportContext context) throws SQLException {
        List<Fk> fks = getFksOfTable(connection, tableName, fkCache);

        data.setOptionalFks(fks);

        for (Fk fk : fks) {
            context.treatedFks.add(fk);

            String[] elementPkName = fk.isInverted() ? fk.getFkcolumn() : fk.getPkcolumn();
            List<DbRecord.FieldAndValue> elementsWithName = Stream.of(elementPkName).map(s -> data.findElementWithName(s)).map(Optional::ofNullable)
                    .flatMap(Optional::stream).collect(toList());
            boolean anyElementNull = elementsWithName.stream().map(DbRecord.FieldAndValue::getValue).anyMatch(x -> x == null);

            if (!anyElementNull) {
                String subTableName = Fk.getSubtableName(fk, context.metaData.getDatabaseProductName());

                String[] subFkNames =    (fk.isInverted() ? fk.getPkcolumn() : fk.getFkcolumn());
                for (int i = 0; i < subFkNames.length; i++) {
                    subFkNames[i] = subFkNames[i].toLowerCase();
                }

                List<DbRecord> subRow = this.readLinkedRecords(connection,
                        subTableName,
                        subFkNames,
                        elementsWithName.stream().map(DbRecord.FieldAndValue::getValue).toArray(),
                        context);
                if (!subRow.isEmpty()) {
                    if (!elementsWithName.get(0).getSubRow().containsKey(subTableName)) {
                        elementsWithName.get(0).getSubRow().put(subTableName, subRow);
                    } else {
                        elementsWithName.get(0).getSubRow().get(subTableName).addAll(subRow);
                    }
                }
            }
        }
    }

    private DbRecord innerReadRecord(String tableName, Map<String, JdbcHelpers.ColumnMetadata> columns, ResultSet rs, ResultSetMetaData rsMetaData, int columnCount, List<String> primaryKeys, ExportContext context) throws SQLException {
        DbRecord row = new DbRecord(tableName, null);
        row.setColumnMetadata(columns);

        Map<String, Integer> primaryKeyArrayPosition = JdbcHelpers.getStringIntegerMap(primaryKeys);
        Object[] primaryKeyValues = new Object[primaryKeys.size()];

        for (int i = 1; i <= columnCount; i++) {
            String columnName = rsMetaData.getColumnName(i);
            DbRecord.FieldAndValue d = retrieveFieldAndValue(tableName, columns, rs, i, columnName, context);
            if (d != null) {
                row.getContent().add(d);

                if (primaryKeyArrayPosition.containsKey(d.getName().toLowerCase())){
                    primaryKeyValues[primaryKeyArrayPosition.get(d.getName().toLowerCase())] = d.getValue();
                }

            }
        }
        row.setPkValue(primaryKeyValues);

        row.setPkNames(primaryKeys);

        return row;
    }

    //region delete

    /** Get SQL statements to delete all the content of a record (needs to be created before).
     * They are in an order they can be executed. */
    public List<String> getDeleteStatements(Connection connection, DbRecord exportedDbRecord) throws Exception {
        List<String> result = new ArrayList<>();

        CheckedFunction<DbRecord, Void> collectDeletionStatement = (DbRecord r) -> {
            result.add(DbExporter.recordEntryToDeleteStatement(connection.getMetaData(), r, pkCache));
            return null;
        };

        exportedDbRecord.visitRecordsInInsertionOrder(connection, collectDeletionStatement, false, fkCache);
        Collections.reverse(result);
        return result;
    }

    private static String recordEntryToDeleteStatement(DatabaseMetaData m, DbRecord r, Cache<String, List<String>> pkCache) throws SQLException {
        List<String> primaryKeys = r.getPkNames();

        String whereClause = "";
        for (int i = 0; i < primaryKeys.size(); i++) {
            JdbcHelpers.ColumnMetadata columnMetadata = r.getColumnMetadata().get(primaryKeys.get(i));
            String optionalQuote = columnMetadata.needsQuoting() ? "'" : "";
            String pk = optionalQuote + r.getRowLink().getPks()[i].toString() + optionalQuote;
            whereClause += primaryKeys.get(i) + "=" + pk + " AND ";
        }
        // remove last "and"
        whereClause = whereClause.isEmpty() ? whereClause : whereClause.substring(0, whereClause.length() - 4);

        try (Formatter formatter = new Formatter()){
            return formatter.format("DELETE FROM %s WHERE %s", r.getRowLink().getTableName(), whereClause).toString();
        }
    }

    /** Delete the record with all linked rows
     *  CAVEAT: really deletes data, check the data first!
     *   @throws SQLException or an IllegalStateException if there is a problem during deletion
     * @return the record that was deleted */
    public DbRecord deleteRecursively(Connection connection, String tableName, Object... pkValue) throws Exception {
        DbRecord dbRecord = contentAsTree(connection, tableName, pkValue);

        List<String> deleteStatements = getDeleteStatements(connection, dbRecord);

        doDeletionWithException(connection, deleteStatements);

        return dbRecord;
    }

    public void doDeletionWithException(Connection connection, List<String> deleteStatements) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            for (String sqlString : deleteStatements) {

                Loggers.LOGGER_DELETE.info("{}", sqlString);
                int count = stmt.executeUpdate(sqlString);
                if (count != 1) {
                    throw new IllegalStateException("Deletion not successful "+sqlString+" result: "+count);
                }
            }
        }
    }

    //endregion delete

    boolean stopAfterFirstInstance(String tableName, ExportContext context) {
        return (stopTablesIncludeOne.contains(tableName) &&
                context.visitedNodes.keySet().stream().map(RowLink::getTableName).collect(Collectors.toSet()).contains(tableName));
    }

    /** If one these tables occurs in collecting the graph, we stop before collecting them. */
    public Set<String> getStopTablesExcluded() {
        return stopTablesExcluded;
    }

    /** If one these tables occurs in collecting the graph, we stop right <em>after</em> collecting them. */
    public Set<String> getStopTablesIncluded() {
        return stopTablesIncluded;
    }

    /** If one these tables occurs in collecting the graph (with depth first search), we stop before we collect the 2nd instance.
     *  The goal is to follow the FKs of these stop tables as well (but not collect subsequent instances).
     *  This is experimental */
    public Set<String> getStopTablesIncludeOne() {
        return stopTablesIncludeOne;
    }

    @Override
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

        fieldExporters.computeIfAbsent(fieldName, k -> new HashMap());
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
        FieldExporter match;
        if (fieldMatch != null) {
            match = fieldMatch.get(tableName);

            if (match == null) {
                match = fieldMatch.get(null);
            }
            return match;
        }
        return null;
    }

    /** Allows overriding how we get fields from a ResultSet. Use uppercase JDBC type names.
     *  Refer to DbExporter#retrieveFieldAndValue()
     *  Overridden field handling takes precedence.
     *  */
    public Map<String, FieldExporter> getTypeFieldExporters() {
        return typeFieldExporters;
    }


    /** experimental feature to order results by first pk when exporting (default: true) */
    public void setOrderResults(boolean orderResults) {
        this.orderResults = orderResults;
    }


    /**
     * Get only the cache entries that are excluded by the stopTablesExcluded
     */
    public Cache<String, List<Fk>> getFilteredFkCache() {
        ConcurrentMap<String, List<Fk>> map = fkCache.asMap();
        List<Map.Entry<String, List<Fk>>> removedKeys = map.entrySet().stream().filter(e -> !stopTablesExcluded.contains(e.getKey())).toList();

        Cache<String, List<Fk>> fkCacheSubset1 = Caffeine.newBuilder()
                .maximumSize(1000).build();
        removedKeys.stream().map(e -> filterEntry(e, stopTablesExcluded)).forEach(e -> fkCacheSubset1.put(e.key(), e.fks()));
        Cache<String, List<Fk>> fkCacheSubset = fkCacheSubset1;
        return fkCacheSubset;
    }

    private static KeyAndFkList filterEntry(Map.Entry<String, List<Fk>> e, Set<String> stopTablesExcluded) {
        List<Fk> list = e.getValue().stream().filter(fk -> !(stopTablesExcluded.contains(fk.getPktable()) || stopTablesExcluded.contains(fk.getFktable()))).toList();
        return new KeyAndFkList(e.getKey(), list);
    }

    record KeyAndFkList(String key, List<Fk> fks) {
    }

}
