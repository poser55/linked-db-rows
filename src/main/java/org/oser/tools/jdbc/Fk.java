package org.oser.tools.jdbc;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Getter;
import lombok.Setter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.oser.tools.jdbc.JdbcHelpers.adaptCaseForDb;

/**
 * Represents one foreign key constraint in JDBC. <a>
 * <p>
 * In JDBC <em>one</em> fk constraint between table 1 and table 2 has <em>two</em> representations: the link from
 * table 1 to table 2 and vice versa. One of the 2 constraints is <em>inverted</em>, refer to the field <code>inverted</code>.
 *  <a>
 *  Supports virtual foreign keys (that do not exist in the db).
 */
@Getter
public class Fk {
    private String pktable;
    @Setter
    private String[] pkcolumn;

    private String fktable;
    @Setter
    private String[] fkcolumn;

    private final String keySeq;
    private final String fkName;

    /**
     * The same as JDBC calls exported or imported.  <br/>
     * inverted = false: the foreign key columns that reference the given table's primary key columns (the foreign keys exported by a table)   <br/>
     * inverted = true: the primary key columns that are referenced by the given table's foreign key columns (the primary keys imported by a table). <br/>
     * <p>
     * Is excluded in equals.
     */
    private boolean inverted;

    public Fk() { // todo rm again
        keySeq = "";
        fkName = "";
    }


    /**
     * Constructor taking single values only
     */
    public Fk(String pktable, String pkcolumn, String fktable, String fkcolumn, String keySeq, String fkName, boolean inverted) {
        this.pktable = pktable;
        this.pkcolumn = new String[]{pkcolumn};
        this.fktable = fktable;
        this.fkcolumn = new String[]{fkcolumn};
        this.keySeq = keySeq;
        this.fkName = fkName;
        this.inverted = inverted;
    }

    public Fk(String pktable, String[] pkcolumnArray, String fktable, String[] fkcolumnArray, String keySeq, String fkName, boolean inverted) {
        this.pktable = pktable;
        this.pkcolumn = pkcolumnArray;
        this.fktable = fktable;
        this.fkcolumn = fkcolumnArray;
        this.keySeq = keySeq;
        this.fkName = fkName;
        this.inverted = inverted;
    }


    /**
     * Refer to {@link #getFksOfTable(Connection, String)}
     * Uses a cache.
     */
    public static List<Fk> getFksOfTable(Connection connection, String table, Cache<String, List<Fk>> cache) throws SQLException {
        List<Fk> result = cache.getIfPresent(table);
        if (result == null) {
            result = getFksOfTable(connection, table);
        }
        cache.put(table, result);
        return result;
    }

    public static boolean hasSelfLink(List<Fk> fks) {
        return fks.stream().anyMatch(e -> hasSelfLink(e));
    }

    /**
     * Get FK metadata of one table (both direction of metadata, exported and imported FKs)
     * <p>
     * If the tableName has a schema prefix (e.g. mySchema.) it adds it to the table names
     * of the returned FKs.
     */
    public static List<Fk> getFksOfTable(Connection connection, String tableName) throws SQLException {
        List<Fk> fks = new CopyOnWriteArrayList<>();
        DatabaseMetaData dm = connection.getMetaData();

        String adaptedTableName = adaptCaseForDb(tableName, dm.getDatabaseProductName());

        JdbcHelpers.Table table = new JdbcHelpers.Table(connection, adaptedTableName);

        try (ResultSet rs = dm.getExportedKeys(null, table.getSchema(), table.getTableName())) {
            addFks(fks, rs, false, table);
        }

        try (ResultSet rs = dm.getImportedKeys(null, table.getSchema(), table.getTableName())) {
            addFks(fks, rs, true, table);
        }

        fks.sort(Comparator.comparing(Fk::getFktable).thenComparing(fk -> fk.getFkcolumn()[0]));

        return unifyFks(fks);
    }

    /**
     * One Fk can contain multiple columns, we need to merge those that form the same Fk
     */
    public static List<Fk> unifyFks(List<Fk> input) {
        Map<String, List<Fk>> fkNameToFk = new HashMap<>();
        input.forEach(f -> fkNameToFk.computeIfAbsent(f.getFkName(), l -> new ArrayList<>()).add(f));

        List<Map.Entry<String, List<Fk>>> toMerge =
                fkNameToFk.entrySet().stream().filter(e -> e.getValue().size() > 1 && !hasSelfLink(e.getValue().get(0))).collect(toList());
        for (Map.Entry<String, List<Fk>> e : toMerge) {
            e.getValue().sort(Comparator.comparing(Fk::getKeySeq));

            e.getValue().get(0).setFkcolumn(e.getValue().stream().map(fk -> fk.getFkcolumn()[0]).collect(toList()).toArray(new String[]{}));
            e.getValue().get(0).setPkcolumn(e.getValue().stream().map(fk -> fk.getPkcolumn()[0]).collect(toList()).toArray(new String[]{}));

            e.getValue().stream().skip(1).forEach(fk -> input.remove(fk));
        }

        return input;
    }

    /**
     * check for link to self (e.g. table N having a FK to itself
     */
    public static boolean hasSelfLink(Fk e) {
        return e.getFktable().equals(e.getPktable());
    }

    private static void addFks(List<Fk> fks, ResultSet rs, boolean inverted, JdbcHelpers.Table table) throws SQLException {
        String optionalPrefix = table.isHasSchemaPrefix() ? table.getSchema() + "." : "";
        while (rs.next()) {
            Fk fk = new Fk(optionalPrefix + getStringFromResultSet(rs, "pktable_name"),
                    getStringFromResultSet(rs, "pkcolumn_name"),
                    optionalPrefix + getStringFromResultSet(rs, "fktable_name"),
                    getStringFromResultSet(rs, "fkcolumn_name"),
                    getStringFromResultSet(rs, "KEY_SEQ"),
                    getStringFromResultSet(rs, "fk_name"), inverted);

//            ResultSetMetaData rsMetaData = rs.getMetaData();
//            for (int i = 1; i<= rsMetaData.getColumnCount() ; i++){
//                System.out.println(rsMetaData.getColumnName(i)+" "+rs.getObject(i));
//            }
//            System.out.println();

            fks.add(fk);
        }
    }

    private static String getStringFromResultSet(ResultSet rs, String columnName) throws SQLException {
        return removeOptionalQuotes(rs.getString(columnName));
    }

    // to remove " that mysql puts
    static String removeOptionalQuotes(String string) {
        if (string != null && string.startsWith("\"") && string.endsWith("\"")) {
            string = string.substring(1, string.length() - 1);
        }
        return string;
    }

    @Override
    public String toString() {
        return "Fk{" +
                "fktable='" + fktable + '\'' +
                ", fkcolumn='" + Arrays.asList(fkcolumn) + '\'' +
                ", pktable='" + pktable + '\'' +
                ", pkcolumn='" + Arrays.asList(pkcolumn) + '\'' +
                ", keyseq=" + keySeq +
                ", fkName=" + fkName +
                ", inverted=" + inverted +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Fk fk = (Fk) o;

        if (!Objects.equals(fktable, fk.fktable)) return false;
        if (!Arrays.equals(pkcolumn, fk.pkcolumn)) return false;
        if (!Objects.equals(pktable, fk.pktable)) return false;
        return Arrays.equals(fkcolumn, fk.fkcolumn);
    }

    @Override
    public int hashCode() {
        int result = fktable != null ? fktable.hashCode() : 0;
        result = 31 * result + (pkcolumn != null ? Arrays.hashCode(pkcolumn) : 0);
        result = 31 * result + (pktable != null ? pktable.hashCode() : 0);
        result = 31 * result + (fkcolumn != null ? Arrays.hashCode(fkcolumn) : 0);
        return result;
    }

    /**
     * mysql does not return the indirekt FKs with {@link DatabaseMetaData#getExportedKeys(String, String, String)}
     * (it only shows the direct links via {@link DatabaseMetaData#getImportedKeys(String, String, String)})
     * - so we fake it here and add these indirect keys to the cache
     * (goes through all database tables for this)
     * <p>
     * does only take the current schema into account
     */
    public static void initFkCacheForMysql(Cache<String, List<Fk>> fkCache, Connection connection) throws SQLException {
        if (!connection.getMetaData().getDatabaseProductName().equals("MySQL")) {
            return;
        }

        List<String> allTableNames = JdbcHelpers.getAllTableNames(connection);
        for (String tableName : allTableNames) {
            List<Fk> fksOfTable = Fk.getFksOfTable(connection, tableName, fkCache);
            for (Fk fk : fksOfTable) {
                List<Fk> current = fkCache.getIfPresent(fk.pktable);
                current = (current == null) ? new CopyOnWriteArrayList<>() : current;
                // todo: is keyseq ok?
                current.add(new Fk(fk.pktable, fk.pkcolumn, fk.fktable, fk.fkcolumn, fk.keySeq, fk.fkName + "inv", false));
                fkCache.put(fk.pktable, current);
            }
        }
    }

    /**
     * Like  {@link #initFkCacheForMysql(Cache, Connection)} but logs exceptions to stdout
     */
    public static void initFkCacheForMysql_LogException(Connection demo, Cache<String, List<Fk>> fkCache) {
        try {
            Fk.initFkCacheForMysql(fkCache, demo);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    /**
     * Add a virtualForeignKey to the foreign key cache contained in "importerOrExporter".
     * (needs to be done once for the DbImporter AND the DbExporter).
     * This requires 2 internal foreign keys, one of which is inverted
     */
    public static void addVirtualForeignKey(Connection dbConnection,
                                            FkCacheAccessor importerOrExporter,
                                            String tableOne,
                                            String[] tableOneColumn,
                                            String tableTwo,
                                            String[] tableTwoColumn) throws SQLException {
        List<Fk> fks = Fk.getFksOfTable(dbConnection, tableOne, importerOrExporter.getFkCache());
        // add artificial FK
        // todo check keyseq
        fks.add(new Fk(tableOne, tableOneColumn, tableTwo, tableTwoColumn, "1", tableOne + tableOneColumn[0], false));
        importerOrExporter.getFkCache().put(tableOne, fks);

        List<Fk> fks2 = Fk.getFksOfTable(dbConnection, tableTwo, importerOrExporter.getFkCache());
        // add artificial FK
        fks2.add(new Fk(tableOne, tableOneColumn, tableTwo, tableTwoColumn, "1", tableOne + tableOneColumn[0], true));
        importerOrExporter.getFkCache().put(tableTwo, fks2);
    }

    /**
     * Add a virtualForeignKey to the foreign key cache "importerOrExporter".
     * (needs to be done once for the DbImporter AND the DbExporter).
     * This requires 2 internal foreign keys, one of which is reverted
     */
    public static void addVirtualForeignKey(Connection dbConnection,
                                            FkCacheAccessor importerOrExporter,
                                            String tableOne,
                                            String tableOneColumn,
                                            String tableTwo,
                                            String tableTwoColumn) throws SQLException {
        addVirtualForeignKey(dbConnection, importerOrExporter, tableOne, new String[]{tableOneColumn},
                tableTwo, new String[]{tableTwoColumn});
    }

    /// Helper

    public static Map<String, List<Fk>> fksByColumnName(List<Fk> fksOfTable) {
        Map<String, List<Fk>> fksByColumnName = new HashMap<>();

        for (Fk fk : fksOfTable) {
            String[] names = fk.inverted ? fk.getFkcolumn() : fk.getPkcolumn();
            Stream.of(names).forEach(name -> fksByColumnName.computeIfAbsent(name.toLowerCase(), l -> new ArrayList<>()).add(fk));
        }
        return fksByColumnName;
    }

    /**
     * register virtualFK via String
     * experimental string config of a virtual foreing key
     * table1(field1,field2)-table2(field3,field4)
     */
    public static void addOneVirtualForeignKeyAsString(Connection dbConnection, FkCacheAccessor importerOrExporter, String asString) throws SQLException {
        FkMatchedFields fkMatchedFields = new FkMatchedFields(asString).parse();
        String table1 = fkMatchedFields.getTable1();
        String fields1AsString = fkMatchedFields.getFields1AsString();
        String table2 = fkMatchedFields.getTable2();
        String fields2AsString = fkMatchedFields.getFields2AsString();

        addVirtualForeignKey(dbConnection, importerOrExporter, table1, fields1AsString.split(","), table2, fields2AsString.split(","));
    }

    /**
     * Same as {@link #addOneVirtualForeignKeyAsString(Connection, FkCacheAccessor, String) } but one can add multiples, separated by ; }
     */
    public static void addVirtualForeignKeyAsString(Connection dbConnection, FkCacheAccessor importerOrExporter, String asString) throws SQLException {
        if (asString.contains(";")) {
            String[] split = asString.split(";");
            for (String one : split) {
                addOneVirtualForeignKeyAsString(dbConnection, importerOrExporter, one);
            }
        } else {
            addOneVirtualForeignKeyAsString(dbConnection, importerOrExporter, asString);
        }
    }

    /** To parse String FKs */
    @Getter
    static class FkMatchedFields {
        private String asString;
        private String table1;
        private String fields1AsString;
        private String table2;
        private String fields2AsString;

        public FkMatchedFields(String asString) {
            this.asString = asString;
        }

        public FkMatchedFields parse() {
            String regex = "([A-Za-z0-9_.]*)\\(([A-Za-z0-9,_]*)\\)-([A-Za-z0-9_.]*)\\(([A-Za-z0-9,_]*)\\)";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(asString);
            if (!matcher.find()) {
                throw new IllegalArgumentException("Wrong pattern '" + asString + "'. Not matched.");
            }

            table1 = matcher.group(1);
            fields1AsString = matcher.group(2);
            table2 = matcher.group(3);
            fields2AsString = matcher.group(4);
            return this;
        }
    }

    public static String getSubtableName(Fk fk, String databaseProductName) {
        String subTableName;
        if (databaseProductName.equals("MySQL")) {
            subTableName = fk.inverted ? fk.pktable : fk.fktable;
        } else {
            subTableName = (fk.inverted ? fk.pktable : fk.fktable).toLowerCase();
        }
        return subTableName;
    }

    /**
     * Do the table1 and table2 have a FK relationship?
     */
    public static Optional<Fk> getFkOfTwoTables(Connection connection, String table1, String table2, Cache<String, List<Fk>> cache) throws SQLException {
        List<Fk> fksOfTable = Fk.getFksOfTable(connection, table1, cache);

        for (Fk fk : fksOfTable) {
            if ((fk.getPktable().equalsIgnoreCase(table1) && fk.getFktable().equalsIgnoreCase(table2)) ||
                    (fk.getPktable().equalsIgnoreCase(table2) && fk.getFktable().equalsIgnoreCase(table1))) {
                return Optional.of(fk);
            }
        }
        return Optional.empty();
    }

    /**
     * Do the table1 and table2 have a FK relationship?
     */
    public static Optional<Fk> getFkOfTwoTables(Connection connection, String table1, String table2) throws SQLException {
        Cache<String, List<Fk>> cache = Caffeine.newBuilder().maximumSize(10_000).build();
        return getFkOfTwoTables(connection, table1, table2, cache);
    }

}
