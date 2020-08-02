package org.oser.tools.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.oser.tools.jdbc.Fk.getFksOfTable;

public final class JdbcHelpers {

    private JdbcHelpers() {}

    /** if one would like to import the tree starting at rootTable, what order should one import the tables?
     *  @return a List<String> with the table names in the order in which to import them*/
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

            // remove the constraints that get eliminated by treating those
            for (String key : dependencyGraph.keySet()) {
                dependencyGraph.get(key).removeAll(treatedThisTime);
            }
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
    public static String getSqlInsertOrUpdateStatement(String tableName, List<String> columnNames, String pkName, boolean isInsert) {
        String result;
        String fieldList = columnNames.stream().filter(name -> (isInsert || !name.equals(pkName.toUpperCase()))).collect(Collectors.joining(isInsert ? ", " : " = ?, "));

        if (isInsert) {
            String questionsMarks = columnNames.stream().map(name -> "").collect(Collectors.joining("?, ")) + "?";

            result = "insert into " + tableName + " (" + fieldList + ") values (" + questionsMarks + ");";
        } else {
            fieldList += " = ? ";

            result = "update " + tableName + " set " + fieldList + " where " + pkName + " = ?";
        }

        return result;
    }

    /**
     * complement the "data" by starting from "tableName" and recursively adding data that is connected via FKs
     */
    static void addSubRowDataFromFks(Connection connection, String tableName, Object pkValue, Record data, DbExporter.ExportContext context) throws SQLException {
        List<Fk> fks = getFksOfTable(connection, tableName);

        for (Fk fk : fks) {
            context.treatedFks.add(fk);

            Record.FieldAndValue elementWithName = data.findElementWithName(fk.inverted ? fk.fkcolumn : fk.pkcolumn);
            if ((elementWithName != null) && (elementWithName.value != null)) {
                String subTableName = fk.inverted ? fk.pktable : fk.fktable;
                String subFkName = fk.inverted ? fk.pkcolumn : fk.fkcolumn;

                if (!context.containsNode(subTableName, elementWithName.value)) {
                    List<Record> subRow = DbExporter.readLinkedRecords(connection, subTableName,
                            subFkName, elementWithName.value, true, context);
                    elementWithName.subRow.put(subTableName, subRow);
                }
            }

        }
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

    /**
     * @return Map à la fieldName1 -> ColumnMetadata (simplified JDBC metadata)
     */
    public static SortedMap<String, ColumnMetadata> getColumnMetadata(DatabaseMetaData metadata, String tableName) throws SQLException {
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
