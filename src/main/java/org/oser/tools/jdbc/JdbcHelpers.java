package org.oser.tools.jdbc;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.oser.tools.jdbc.Fk.getFksOfTable;

/** Helper methods for core JDBC and JDBC metadata */
public final class JdbcHelpers {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcHelpers.class);

    private JdbcHelpers() {}

    /** @see JdbcHelpers#determineOrder(Connection, String, boolean, Cache) */
    public static List<String> determineOrder(Connection connection, String rootTable, boolean exceptionWithCycles) throws SQLException {
        return determineOrder(connection, rootTable, exceptionWithCycles, Caffeine.newBuilder()
                .maximumSize(10_000).build());
    }

    /** If one would like to import the tree starting at rootTable, in what order should one insert the tables?
     *  @return a List<String> with the table names in the order in which to insert them, the table names are converted to
     *   lower case
     *  CAVEAT: may return a partial list (in case there are cycles/ there is no layering in the table dependencies)
     *
     * @throws IllegalStateException if there is a cycle and exceptionWithCycles is true
     * @throws SQLException if there is an issue with SQL metadata queries <p>
     *
     *  */
    public static List<String> determineOrder(Connection connection, String rootTable, boolean exceptionWithCycles, Cache<String, List<Fk>> cache) throws SQLException {
        return determineOrderWithDetails(connection, rootTable,exceptionWithCycles, cache).getLeft();
    }

    /** Similar to {@link #determineOrder(Connection, String, boolean, Cache)} but return also all tables as
     * set (in case there was a cycle) */
    public static Pair<List<String>, Set<String>> determineOrderWithDetails(Connection connection, String rootTable, boolean exceptionWithCycles, Cache<String, List<Fk>> cache) throws SQLException {
        Set<String> treated = new HashSet<>();

        Map<String, Set<String>> dependencyGraph = initDependencyGraph(rootTable, treated, connection, cache);
        List<String> strings = topologicalSort(dependencyGraph, treated, exceptionWithCycles);
        return new Pair<>(strings, treated);
    }

    @Getter
    @AllArgsConstructor
    public static class Pair<A,B> {
        private final A left;
        private final B right;
    }

    /**
     * @param dependencyGraph the dependencies to sort. Put them like a requires b ( a->{b}) so that b is ordered before a.
     * @param treated init treated to Set with all entries
     * @param exceptionWithCycles whether we should throw an exception if there are cycles
     * @return the sorted list of entries
     * */
    public static <T> List<T> topologicalSort(Map<T, Set<T>> dependencyGraph, Set<T> treated, boolean exceptionWithCycles) {
        List<T> orderedTables = new ArrayList<>();

        Set<T> stillToTreat = new HashSet<>(treated);
        while (!stillToTreat.isEmpty()) {
            // remove all for which we have a constraint
            Set<T> treatedThisTime = new HashSet<>(stillToTreat);
            treatedThisTime.removeAll(dependencyGraph.keySet());

            orderedTables.addAll(treatedThisTime);
            stillToTreat.removeAll(treatedThisTime);

            if (treatedThisTime.isEmpty()) {
                String dependencyGraphAsString = dependencyGraph.toString();
                // limiting the length of the message
                String shortDependencyGraph = dependencyGraphAsString.substring(0, Math.min(dependencyGraphAsString.length(), 10000));
                LOGGER.warn("Not a layered organization of dependencies - excluding entries with cycles: {}", shortDependencyGraph);
                if (exceptionWithCycles) {
                    // this is just for debugging
                    if (treated.iterator().next().getClass().equals(Record.class)){
                        List<AbstractMap.SimpleEntry<RowLink, Set<RowLink>>> collect = getDbRecordDependencyGraph(dependencyGraph);
                        LOGGER.warn("rowlink deps: {}", collect);
                    }

                    throw new IllegalStateException("Cyclic sql dependencies - aborting " + shortDependencyGraph);
                }
                break; // returning a partial ordered list
            }

            // remove the constraints that get eliminated by treating those
            dependencyGraph.keySet().forEach(key -> dependencyGraph.get(key).removeAll(treatedThisTime));
            dependencyGraph.entrySet().removeIf(e -> e.getValue().isEmpty());
        }
        return orderedTables;
    }

    public static <T> List<AbstractMap.SimpleEntry<RowLink, Set<RowLink>>> getDbRecordDependencyGraph(Map<T, Set<T>> dependencyGraph) {
        return  dependencyGraph.entrySet().stream().map(e ->
                new AbstractMap.SimpleEntry<>(((Record) e.getKey()).getRowLink(),
                dbRecordSetToRowLinkSet((Set<Record>)e.getValue()))).collect(Collectors.toList());
    }

    static Set<RowLink> dbRecordSetToRowLinkSet(Set<Record> dbRecords) {
        return dbRecords.stream().map(Record::getRowLink).collect(Collectors.toSet());
    }


    /**
     *  Determine all "Y requires X" relationships between the tables of the db, starting from rootTable
     *  @param rootTable is the root table we start from <p>
     *  @param treated all tables followed<p>
     *  @param cache is the FK cache <p>
     *
     * @return constraints in the form Map<X, Y> :  X is used in all Y (X is the key, Y are the values (Y is a set of all values),
     * all tables are lower case
     */
    public static Map<String, Set<String>> initDependencyGraph(String rootTable, Set<String> treated, Connection connection, Cache<String, List<Fk>> cache) throws SQLException {
        rootTable = rootTable.toLowerCase();
        Set<String> tablesToTreat = new HashSet<>();
        tablesToTreat.add(rootTable);

        Map<String, Set<String>> dependencyGraph = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        while (!tablesToTreat.isEmpty()) {
            String next = tablesToTreat.iterator().next();
            tablesToTreat.remove(next);

            List<Fk> fks = getFksOfTable(connection, next, cache);
            for (Fk fk : fks) {
                String tableToAdd = fk.getPktable().toLowerCase();
                String otherTable = fk.getFktable().toLowerCase();

                addToTreat(tablesToTreat, treated, tableToAdd);
                addToTreat(tablesToTreat, treated, otherTable);

                addDependency(dependencyGraph, tableToAdd, otherTable);
            }

            treated.add(next);
        }
        return dependencyGraph;
    }

    private static void addToTreat(Set<String> tablesToTreat, Set<String> treated, String tableToAdd) {
        if (!treated.contains(tableToAdd)) {
            tablesToTreat.add(tableToAdd);
        }
    }

    private static void addDependency(Map<String, Set<String>> dependencyGraph, String lastTable, String tableToAdd) {
        if (!lastTable.equals(tableToAdd)) {
            dependencyGraph.putIfAbsent(tableToAdd, new HashSet<>());
            dependencyGraph.get(tableToAdd).add(lastTable);
        }
    }

    /**
     * generate insert or update statement to insert columnNames into tableName
     * @return a pair (sqlstatement, list of all occurring fields)
     */
    public static SqlChangeStatement getSqlInsertOrUpdateStatement(String tableName, List<String> columnNames, List<String> pkNames, boolean isInsert, Map<String, ColumnMetadata> columnMetadata) {
        List<String> individualFields = columnNames.stream().filter(fieldName -> (isInsert || isNoPrimaryKeyName(fieldName, pkNames))).collect(Collectors.toList());
        String concatenatedFields = String.join(isInsert ? ", " : " = ?, ", individualFields);

        String statement;
        if (isInsert) {
            Map<String, ColumnMetadata> metadataInCurrentTableAndInsert = columnMetadata.entrySet().stream().filter(e -> columnNames.contains(e.getKey().toLowerCase())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            String questionsMarks = metadataInCurrentTableAndInsert.values().stream().sorted(Comparator.comparing(ColumnMetadata::getOrdinalPos))
                    .map(JdbcHelpers::questionMarkOrTypeCasting).collect(Collectors.joining(", "));
            statement = "INSERT INTO " + tableName + " (" + concatenatedFields + ") VALUES (" + questionsMarks + ")";
        } else {
            concatenatedFields += " = ? ";

            statement = "UPDATE " + tableName + " SET " + concatenatedFields + " WHERE ";
            for (int i = 0; i < pkNames.size(); i++)  {
                individualFields.add(pkNames.get(i).toLowerCase());
                statement +=  pkNames.get(i) + " = ?" + ((i != (pkNames.size() - 1))? " AND ":"");
            }
        }

        // sanity check (the columnDef settings are a bit magic)
        long questionMarkCount = statement.chars().filter(ch -> ch == '?').count();
        if (questionMarkCount != individualFields.size()){
            throw new IllegalStateException(" Not enough ? in insert statement! "+statement+" "+individualFields.size());
        }

        return new SqlChangeStatement(statement, individualFields);
    }

    private static boolean isNoPrimaryKeyName(String fieldName, List<String> pkNames) {
        return !pkNames.stream().anyMatch(fieldName::equalsIgnoreCase);
    }

    @Getter
    @AllArgsConstructor
    @ToString
    public static class SqlChangeStatement {
        private final String statement;
        private final List<String> fields;

    }

    private static String questionMarkOrTypeCasting(ColumnMetadata e) {
        if (e != null && e.columnDef != null && e.columnDef.endsWith(e.type) &&
                // todo: make this more robust (also pluggable?)

                // mysql puts CURRENT_TIMESTAMP as the columnDef of Timestamp, this leads to an automatically set fields
                // postgres text has a ::text here
                !(e.columnDef.equals("CURRENT_TIMESTAMP") || e.type.equals("text"))){
            // to handle inserts e.g. for enums correctly
            return e.columnDef.replace("'G'", "?");
        }

        return "?";
    }

    public static void assertTableExists(Connection connection, String tableName) throws SQLException {
        DatabaseMetaData dbm = connection.getMetaData();

        Table table = new Table(connection, tableName);

        try (ResultSet tables = dbm.getTables(connection.getCatalog(), table.getSchema(), table.getTableName(), null)) {
            if (!tables.next()) {
                throw new IllegalArgumentException("Table " + tableName + " does not exist");
            }
        }
    }

    static String adaptCaseForDb(String originalName, String dbProductName) {
        if (originalName == null) {
            return null;
        }
        switch (dbProductName) {
            case "PostgreSQL":
                return originalName.toLowerCase();
            case "H2":
                return originalName.toUpperCase();
            case "MySQL":
                return originalName;
            case "HSQL Database Engine":
                return originalName.toUpperCase();
            case "Microsoft SQL Server":
                return originalName.toLowerCase();
            default:
                return originalName.toUpperCase();
        }
    }

    public static SortedMap<String, ColumnMetadata> getColumnMetadata(DatabaseMetaData metadata, String tableName, Cache<String, SortedMap<String, ColumnMetadata>> cache) throws SQLException {
        SortedMap<String, ColumnMetadata> result = cache.getIfPresent(tableName);
        if (result == null){
            result = getColumnMetadata(metadata, tableName);
        }
        cache.put(tableName, result);
        return result;
    }

    /**
     * @return Map à la fieldName1 -> ColumnMetadata (simplified JDBC metadata)
     */
    public static SortedMap<String, ColumnMetadata> getColumnMetadata(DatabaseMetaData metadata, String tableName) throws SQLException {
        SortedMap<String, ColumnMetadata> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        Table table = new Table(metadata.getConnection(), tableName);

        try (ResultSet rs = metadata.getColumns(null, table.getSchema(), table.getTableName(), null)) {

            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME").toLowerCase();
                result.put(columnName,
                        new ColumnMetadata(columnName,
                                rs.getString("TYPE_NAME"),
                                rs.getInt("DATA_TYPE"),
                                rs.getInt("SOURCE_DATA_TYPE"),
                                rs.getInt("COLUMN_SIZE"),
                                rs.getString("COLUMN_DEF"),
                                rs.getInt("ORDINAL_POSITION")));

                // todo rm again
//            ResultSetMetaData rsMetaData = rs.getMetaData();
//            for (int i = 1; i<= rsMetaData.getColumnCount() ; i++){
//                System.out.println(rsMetaData.getColumnName(i)+" "+rs.getObject(i));
//                if (rsMetaData.getColumnName(i).equals("COLUMN_DEF") && rs.getObject(i) != null){
//                    System.out.println(rs.getObject(i).getClass());
//                }
//            }
//            System.out.println();
            }


            return result;
        }
    }

    /** @see #getPrimaryKeys(DatabaseMetaData, String) with optional caching */
    public static List<String> getPrimaryKeys(DatabaseMetaData metadata, String tableName, Cache<String, List<String>> cache) throws SQLException {
        List<String> result = cache.getIfPresent(tableName);
        if (result == null){
            result = getPrimaryKeys(metadata, tableName);
        }
        cache.put(tableName, result);
        return result;
    }

    /** Get the list of primary keys of a table */
    public static List<String> getPrimaryKeys(DatabaseMetaData metadata, String tableName) throws SQLException {
        List<String> result = new ArrayList<>();

        Table table = new Table(metadata.getConnection(), tableName);

        try (ResultSet rs = metadata.getPrimaryKeys(null, table.getSchema(), table.getTableName())) {
            while (rs.next()) {
                result.add(rs.getString("COLUMN_NAME"));
            }
            return result;
        }
    }

    @Getter
    public static class Table {
        private String tableName;
        private String schema;

        /** indicates whether this Table has a schema prefix */
        private boolean hasSchemaPrefix;

        public Table(Connection connection, String expression) throws SQLException {
            hasSchemaPrefix = expression.contains(".");
            if (hasSchemaPrefix){
               int i = expression.indexOf(".");
               schema = expression.substring(0, i);
               tableName = expression.substring(i + 1);
           } else {
               tableName = expression;
               String rawSchema = connection.getSchema(); // mysql returns null for default schema
               this.schema = rawSchema == null ? "": rawSchema; // default schema
           }
            String databaseProductName = connection.getMetaData().getDatabaseProductName();
            tableName = adaptCaseForDb(tableName, databaseProductName);
            schema = adaptCaseForDb(schema, databaseProductName);
        }
    }


    /**
     * Set a value on a jdbc Statement
     *
     *  Allows overriding the handling of certain SQL types by registering a plugin in "setterPlugins". The name is supposed to be
     *  the string type name of JDBC, all uppercase. setterPlugins can be null.
     */
    public static void innerSetStatementField(PreparedStatement preparedStatement,
                                              int statementIndex,
                                              ColumnMetadata columnMetadata,
                                              Object valueToInsert,
                                              Map<String, FieldImporter> setterPlugins) throws SQLException {
        if ((setterPlugins != null) && (setterPlugins.containsKey(columnMetadata.getType().toUpperCase()))) {
            // in some cases metadata of preparedStatements may be null (e.g. for h2 and some insert statements)
            ResultSetMetaData metaData = preparedStatement.getMetaData();
            String tableName = metaData != null ? metaData.getTableName(statementIndex) : "";
            boolean bypassNormalTreatment =
                    setterPlugins.get(columnMetadata.getType().toUpperCase()).importField(tableName, columnMetadata,  preparedStatement, statementIndex, valueToInsert);
            if (bypassNormalTreatment) {
                return;
            }
        }

        boolean isEmpty = valueToInsert == null;
        switch (columnMetadata.type.toUpperCase()) {
            case "BOOLEAN":
            case "BIT":
            case "BOOL":
                if (isEmpty) {
                    preparedStatement.setNull(statementIndex, Types.BOOLEAN);
                } else {
                    preparedStatement.setBoolean(statementIndex,
                            valueToInsert instanceof String ? Boolean.parseBoolean(((String)valueToInsert).trim()): (Boolean)valueToInsert);
                }
                break;
            case "SERIAL":
            case "INT":
            case "INT2":
            case "INT4":
            case "INTEGER":
            case "NUMBER":
            case "INT8":
            case "FLOAT4":
            case "FLOAT8":
            case "NUMERIC":
            case "DECIMAL":
                if (isEmpty) {
                    preparedStatement.setNull(statementIndex, Types.NUMERIC);
                } else {
                    if (valueToInsert instanceof String) {
                        preparedStatement.setDouble(statementIndex, Double.parseDouble(((String)valueToInsert).trim()));
                    } else  if (valueToInsert instanceof Number) {
                        if ((valueToInsert instanceof Double) || (valueToInsert instanceof Float)) {
                            preparedStatement.setDouble(statementIndex, ((Number)valueToInsert).doubleValue());
                        } else if (valueToInsert instanceof BigDecimal) {
                            preparedStatement.setBigDecimal(statementIndex, (BigDecimal)valueToInsert);
                        } else {
                            preparedStatement.setLong(statementIndex, ((Number)valueToInsert).longValue());
                        }
                    } else {
                        throw new IllegalStateException("Issue with type mapping: " + preparedStatement + " " + statementIndex + " " + valueToInsert);
                    }
                }
                break;
            case "UUID":
                if (valueToInsert == null) {
                    preparedStatement.setObject(statementIndex, valueToInsert, Types.OTHER);
                } else {
                    preparedStatement.setNull(statementIndex, Types.OTHER);
                }
                break;
            case "CLOB":
                if (valueToInsert == null) {
                    preparedStatement.setObject(statementIndex, valueToInsert, Types.OTHER);
                } else {
                    Clob clob = preparedStatement.getConnection().createClob();
                    clob.setString(1, (String)valueToInsert);
                    preparedStatement.setClob(statementIndex, clob);
                }
                break;

            case "DATE":
            case "TIMESTAMP":
                if (isEmpty) {
                    preparedStatement.setNull(statementIndex, Types.TIMESTAMP);
                } else {

                    // todo: we convert date types to string to do the conversion here
                    //  we should find a better approach
                    String valueAsString = Objects.toString(valueToInsert);

                    if (columnMetadata.type.equalsIgnoreCase("TIMESTAMP")) {
                        LocalDateTime localDateTime = LocalDateTime.parse(valueAsString.replace(" ", "T"));
                        preparedStatement.setTimestamp(statementIndex, Timestamp.valueOf(localDateTime));
                    } else {
                        LocalDate localDate = LocalDate.parse(valueAsString.replace(" ", "T"));
                        preparedStatement.setDate(statementIndex, Date.valueOf(String.valueOf(localDate)));
                    }
                }
                break;
            default:
                if (columnMetadata.getDataType() != Types.ARRAY) {
                    if (valueToInsert == null){
                        preparedStatement.setNull(statementIndex, columnMetadata.getDataType());
                    } else {
                        preparedStatement.setObject(statementIndex, valueToInsert, columnMetadata.dataType);
                    }
                } else {
                    // todo: do we need more null handling? (but we lack the int dataType)
                    preparedStatement.setObject(statementIndex, valueToInsert);
                }
        }
    }

    /**
     * Represents simplified JDBC metadata about a column
     */
    @Getter
    @ToString
    // for jackson serialization
    @NoArgsConstructor
    public static class ColumnMetadata {
        private static final Set<Integer> QUOTING_DATATYPES = Set.of(Types.DATE, Types.TIMESTAMP, Types.TIME, Types.TIME_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE,
                Types.ARRAY, Types.BLOB, Types.CHAR, Types.CLOB, Types.DATALINK, Types.LONGNVARCHAR, Types.VARCHAR, Types.SQLXML, Types.NCHAR);
        String name;
        String type;
        /** {@link java.sql.Types} */
        private int dataType; //
        /** source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types
         * (<code>null</code> if DATA_TYPE  isn't DISTINCT or user-generated REF) */
        private int sourceDataType;

        private int size;
        private String columnDef;
        // starts at 1
        private int ordinalPos;

        public ColumnMetadata(String name, String type, int dataType, int sourceDataType, int size, String columnDef, int ordinalPos) {
            this.name = name;
            this.type = type;
            this.dataType = dataType;
            this.sourceDataType = sourceDataType;
            this.size = size;
            this.columnDef = columnDef;
            this.ordinalPos = ordinalPos;
        }

        public boolean needsQuoting() {
            return QUOTING_DATATYPES.contains(dataType);
        }
    }

    /** get Map with keys = the field names and value = index of the key (0 started) */
    public static Map<String, Integer> getStringIntegerMap(List<String> primaryKeys) {
        final int[] j = {0};
        return primaryKeys.stream().collect(Collectors.toMap(e -> e.toLowerCase(), e -> j[0]++));
    }

    /** Does the row of the table tableName and primary key pkNames and the pkValues exist? */
    public static boolean doesRowWithPrimaryKeysExist(Connection connection,
                                                      String tableName,
                                                      List<String> pkNames,
                                                      List<Object> pkValues,
                                                      Map<String,
                                                              JdbcHelpers.ColumnMetadata> columnMetadata) throws SQLException {
        String selectStatement = selectStatementByPks(tableName, pkNames, columnMetadata);

        boolean exists = false;
        try (PreparedStatement pkSelectionStatement = connection.prepareStatement(selectStatement)) {
            for (int i = 0; i < pkValues.size(); i++) {
                JdbcHelpers.innerSetStatementField(pkSelectionStatement, i+1, columnMetadata.get(pkNames.get(i).toLowerCase()),
                        Objects.toString(pkValues.get(i)), null);
            }

            Loggers.logSelectStatement(pkSelectionStatement, selectStatement, pkValues);
            try (ResultSet rs = pkSelectionStatement.executeQuery()) {
                exists = rs.next();
            }
        }
        return exists;
    }

    private static String selectStatementByPks(String tableName, List<String> primaryKeys, Map<String, JdbcHelpers.ColumnMetadata> columnMetadata) {
        String whereClause = primaryKeys.stream().map(e -> e + " = " + questionMarkOrTypeCasting(columnMetadata.get(e.toLowerCase())))
                .collect(Collectors.joining(" AND "));
        return "SELECT * FROM " + tableName + " WHERE  " + whereClause;
    }

    /** not yet very optimized <br/>
     *
     *   takes the current default schema */
    public static Map<String, Integer> getNumberElementsInEachTable(Connection connection) throws SQLException {
        return getNumberElementsInEachTable(connection, connection.getSchema());
    }

    public static Map<String, Integer> getNumberElementsInEachTable(Connection connection, List<String> schemas) throws SQLException {
        if (schemas == null) {
            throw new IllegalArgumentException("Schemas must not be null");
        }
        Map<String, Integer> combined = new HashMap<>();

        for (String schema : schemas) {
            Map<String, Integer> elements = getNumberElementsInEachTable(connection, schema);

            String schemaPrefix = getSchemaPrefix(connection, schema);

            elements.forEach((key, value) -> combined.put(schemaPrefix + key, value));
        }

        return combined;
    }

    /** not yet very optimized */
    public static Map<String, Integer> getNumberElementsInEachTable(Connection connection, String schema) throws SQLException {
        Map<String, Integer> result = new HashMap<>();
        schema = adaptCaseForDb(schema, connection.getMetaData().getDatabaseProductName());

        for (String tableName : getAllTableNames(connection, schema)) {
            try (Statement statement = connection.createStatement()) {
                String schemaPrefix = getSchemaPrefix(connection, schema);

                // mysql wants a quote around mixedcase table names
                String optionalQuote = connection.getMetaData().getDatabaseProductName().equals("MySQL") ? "\"" : "";
                try (ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM " + optionalQuote + schemaPrefix + tableName + optionalQuote)) {

                    while (resultSet.next()) {
                        result.put(tableName, resultSet.getInt(1));
                    }
                }
            }
        }

        return result;
    }

    /** system is default schema in oracle,
     dbo is the default schema in sqlserver,
     Java-null is the default schema in mysql,
     public is the default schema in other dbs */
    public static String getSchemaPrefix(Connection connection, String schema) throws SQLException {
        String connectionSchema = connection.getSchema();
        return ((connectionSchema == null && schema == null) ||
                (connectionSchema != null && connectionSchema.equalsIgnoreCase(schema))) ? "" : schema + ".";
    }

    /** of current default schema */
    public static List<String> getAllTableNames(Connection connection) throws SQLException {
        return getAllTableNames(connection, connection.getSchema());
    }


    public static List<String> getAllTableNames(Connection connection, String schema) throws SQLException {
        List<String> tableNames = new ArrayList<>();

        DatabaseMetaData md = connection.getMetaData();
        try (ResultSet rs = md.getTables(connection.getCatalog(), adaptCaseForDb(schema, connection.getMetaData().getDatabaseProductName()), "%", new String[]{"TABLE"})) {
            while (rs.next()) {
                if (Objects.equals(schema, rs.getString("TABLE_SCHEM"))) {
                    // mysql returns tables also in other schemas
                    tableNames.add(rs.getString("TABLE_NAME"));
                }
            }
        }
        return tableNames;
    }

    /** convert a value to byte[]
     *  strings are assumed to be byte64 encoded */
    public static byte[] valueToByteArray(Object value) {
        if (value == null || value instanceof byte[]){
            return (byte[])value;
        } else if (value instanceof String){
            return Base64.getDecoder().decode((String) value);
        }
        throw new IllegalStateException("Unkown how to convert value "+value+" to byte[].");
    }

}
