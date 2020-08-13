package org.oser.tools.jdbc;

import com.github.benmanes.caffeine.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
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

public final class JdbcHelpers {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcHelpers.class);

    private JdbcHelpers() {}

    /** if one would like to import the tree starting at rootTable, what order should one insert the tables?
     *  @return a List<String> with the table names in the order in which to insert them
     *  CAVEAT: may return a partial list (in case there are cycles/ there is no layering in the table dependencies)
     *
     *  todo: could we all separate non cyclic parts of the graph? Would that help?
     *  */
    public static List<String> determineOrder(Connection connection, String rootTable) throws SQLException {
        Set<String> treated = new HashSet<>();

        Map<String, Set<String>> dependencyGraph = calculateDependencyGraph(rootTable, treated, connection);
        List<String> orderedTables = new ArrayList<>();

        Set<String> stillToTreat = new HashSet<>(treated);
        while (!stillToTreat.isEmpty()) {
            // remove all for which we have a constraint
            Set<String> treatedThisTime = new HashSet<>(stillToTreat);
            treatedThisTime.removeAll(dependencyGraph.keySet());

            orderedTables.addAll(treatedThisTime);
            stillToTreat.removeAll(treatedThisTime);

            if (treatedThisTime.isEmpty()) {
                LOGGER.warn("Not a layered organization of table dependencies - excluding connected tables: {}", dependencyGraph);
                break; // returning a partial list
            }

            // remove the constraints that get eliminated by treating those
            dependencyGraph.keySet().forEach(key -> dependencyGraph.get(key).removeAll(treatedThisTime));
            dependencyGraph.entrySet().removeIf(e -> e.getValue().isEmpty());
        }

        return orderedTables;
    }

    private static Map<String, Set<String>> calculateDependencyGraph(String rootTable, Set<String> treated, Connection connection) throws SQLException {
        Set<String> tablesToTreat = new HashSet<>();
        tablesToTreat.add(rootTable);

        Map<String, Set<String>> dependencyGraph = new HashMap<>();

        while (!tablesToTreat.isEmpty()) {
            String next = tablesToTreat.stream().findFirst().get();
            tablesToTreat.remove(next);

            List<Fk> fks = getFksOfTable(connection, next);
            for (Fk fk : fks) {
                String tableToAdd = fk.pktable;
                String otherTable = fk.fktable;

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
     */
    public static String getSqlInsertOrUpdateStatement(String tableName, List<String> columnNames, String pkName, boolean isInsert, Map<String, ColumnMetadata> columnMetadata) {
        String result;
        String fieldList = columnNames.stream().filter(name -> (isInsert || !name.equals(pkName.toUpperCase()))).collect(Collectors.joining(isInsert ? ", " : " = ?, "));

        if (isInsert) {
            String questionsMarks = columnMetadata.values().stream().sorted(Comparator.comparing(ColumnMetadata::getOrdinalPos))
                    .map(JdbcHelpers::questionMarkOrTypeCasting).collect(Collectors.joining(", "));
            result = "insert into " + tableName + " (" + fieldList + ") values (" + questionsMarks + ");";
        } else {
            fieldList += " = ? ";

            result = "update " + tableName + " set " + fieldList + " where " + pkName + " = ?";
        }

        return result;
    }

    private static String questionMarkOrTypeCasting(ColumnMetadata e) {
        if (e != null && e.columnDef != null && e.columnDef.endsWith(e.type)){
            // to handle inserts e.g. for enums correctly
            return e.columnDef.replace("'G'", "?");
        }

        return "?";
    }

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

    static String adaptCaseForDb(String originalName, String dbProductName) {
        if (dbProductName.equals("PostgreSQL")) {
            return originalName;
        } else if (dbProductName.equals("H2")) {
            return originalName.toUpperCase();
        }
        return originalName.toUpperCase();
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
        SortedMap<String, ColumnMetadata> result = new TreeMap<>();

        ResultSet rs = metadata.getColumns(null, null, adaptCaseForDb(tableName, metadata.getDatabaseProductName()), null);

        while (rs.next()) {
            String column_name = rs.getString("COLUMN_NAME");
            result.put(column_name.toUpperCase(),
                    new ColumnMetadata(column_name,
                            rs.getString("TYPE_NAME"),
                            rs.getInt("DATA_TYPE"),
                            rs.getInt("SOURCE_DATA_TYPE"),
                            rs.getString("COLUMN_SIZE"),
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

        ResultSet rs = metadata.getPrimaryKeys(null, null, adaptCaseForDb(tableName, metadata.getDatabaseProductName()));
        while (rs.next()) {
            result.add(rs.getString("COLUMN_NAME"));
        }
        return result;
    }


    /**
     * Set a value on a jdbc Statement
     *
     *   for cases where we have less info, columnMetadata can be null
     */
    // todo: one could use the int type info form the metadata
    // todo: clean up arguments (redundant)
    public static void innerSetStatementField(PreparedStatement preparedStatement, String typeAsString, int statementIndex, String valueToInsert, ColumnMetadata columnMetadata) throws SQLException {
        boolean isEmpty = valueToInsert == null || (valueToInsert.trim().isEmpty() || valueToInsert.equals("null"));
        switch (typeAsString.toUpperCase()) {
            case "BOOLEAN":
            case "BOOL":
                preparedStatement.setBoolean(statementIndex, Boolean.parseBoolean(valueToInsert.trim()));
                break;
            case "SERIAL":
            case "INT2":
            case "INT4":
            case "INT8":
                if (isEmpty) {
                    preparedStatement.setNull(statementIndex, Types.NUMERIC);
                } else {
                    preparedStatement.setLong(statementIndex, Long.parseLong(valueToInsert.trim()));
                }
                break;
            case "NUMERIC":
            case "DECIMAL":
                if (isEmpty) {
                    preparedStatement.setNull(statementIndex, Types.NUMERIC);
                } else {
                    preparedStatement.setDouble(statementIndex, Double.parseDouble(valueToInsert.trim()));
                }
                break;
            case "DATE":
            case "TIMESTAMP":
                if (isEmpty) {
                    preparedStatement.setNull(statementIndex, Types.TIMESTAMP);
                } else {
                    if (typeAsString.toUpperCase().equals("TIMESTAMP")) {
                        LocalDateTime localDateTime = LocalDateTime.parse(valueToInsert.replace(" ", "T"));
                        preparedStatement.setTimestamp(statementIndex, Timestamp.valueOf(localDateTime));
                    } else {
                        LocalDate localDate = LocalDate.parse(valueToInsert.replace(" ", "T"));
                        preparedStatement.setDate(statementIndex, Date.valueOf(String.valueOf(localDate)));
                    }
                }
                break;
            default:
                if (columnMetadata != null && columnMetadata.getDataType() != Types.ARRAY ) {
                    preparedStatement.setObject(statementIndex, valueToInsert, columnMetadata.dataType);
                } else {
                    preparedStatement.setObject(statementIndex, valueToInsert);
                }
        }
    }

    /**
     * represents simplified JDBC metadata
     */
    public static class ColumnMetadata {
        String name;
        String type;
        /** {@link java.sql.Types} */
        private final int dataType; //
        /** source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (<code>null</code> if DATA_TYPE     isn't DISTINCT or user-generated REF) */
        private final int sourceDataType;

        String size; // adapt later?
        private final String columnDef;
        // starts at 1
        private final int ordinalPos;

        public ColumnMetadata(String name, String type, int dataType, int sourceDataType, String size, String columnDef, int ordinalPos) {
            this.name = name;
            this.type = type;
            this.dataType = dataType;
            this.sourceDataType = sourceDataType;
            this.size = size;
            this.columnDef = columnDef;
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

        public int getDataType() {
            return dataType;
        }
    }

    /** get Map with keys = the field names and value = index of the key (0 started) */
    public static Map<String, Integer> getStringIntegerMap(List<String> primaryKeys) {
        final int[] j = {0};
        return primaryKeys.stream().collect(Collectors.toMap(e -> e.toUpperCase(), e -> j[0]++));
    }

    /** Does the row of the table tableName and primary key pkNames and the pkValues exist? */
    public static boolean doesPkTableExist(Connection connection, String tableName, List<String> pkNames,
                                           List<Object> pkValues, Map<String, JdbcHelpers.ColumnMetadata> columnMetadata) throws SQLException {
        String selectStatement = selectStatementByPks(tableName, pkNames, columnMetadata);

        boolean exists = false;
        try (PreparedStatement pkSelectionStatement = connection.prepareStatement(selectStatement)) {
            setPksStatementFields(pkSelectionStatement, pkNames, columnMetadata, pkValues);
            try (ResultSet rs = pkSelectionStatement.executeQuery()) {
                exists = rs.next();
            }
        }
        return exists;
    }

    private static String selectStatementByPks(String tableName, List<String> primaryKeys, Map<String, JdbcHelpers.ColumnMetadata> columnMetadata) {
        String whereClause = primaryKeys.stream().map(e -> e + " = " + questionMarkOrTypeCasting(columnMetadata.get(e.toUpperCase())))
                .collect(Collectors.joining(" and "));
        return "SELECT * from " + tableName + " where  " + whereClause;
    }

    private static void setPksStatementFields(PreparedStatement pkSelectionStatement, List<String> primaryKeys, Map<String, JdbcHelpers.ColumnMetadata> columnMetadata, List<Object> values) throws SQLException {
        int i = 0;
        for (String pkName : primaryKeys) {
            JdbcHelpers.ColumnMetadata fieldMetadata = columnMetadata.get(pkName.toUpperCase());
            if (fieldMetadata == null) {
                throw new IllegalArgumentException("Issue with metadata " + columnMetadata);
            }
            JdbcHelpers.innerSetStatementField(pkSelectionStatement, fieldMetadata.getType(), i + 1, Objects.toString(values.get(i)), fieldMetadata);
            i++;
        }
    }
}
