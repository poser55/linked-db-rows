package org.oser.tools.jdbc;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.Getter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.oser.tools.jdbc.JdbcHelpers.adaptCaseForDb;

/** Represents one foreign key constraint */
@Getter
public class Fk {
    public String pktable;
    public String pkcolumn;

    public String fktable;
    public String fkcolumn;

    public boolean inverted; // excluded in equals!


    public Fk(String pktable, String pkcolumn, String fktable, String fkcolumn, boolean inverted) {
        this.pktable = pktable;
        this.pkcolumn = pkcolumn;
        this.fktable = fktable;
        this.fkcolumn = fkcolumn;
        this.inverted = inverted;
    }

    public static List<Fk> getFksOfTable(Connection connection, String table, Cache<String, List<Fk>> cache) throws SQLException {
        List<Fk> result = cache.getIfPresent(table);
        if (result == null){
            result = getFksOfTable(connection, table);
        }
        cache.put(table, result);
        return result;
    }

    /**
     * get FK metadata of one table (both direction of metadata, exported and imported FKs)
     */
    public static List<Fk> getFksOfTable(Connection connection, String table) throws SQLException {
        List<Fk> fks = new CopyOnWriteArrayList<>();
        DatabaseMetaData dm = connection.getMetaData();

        String adaptedTableName = adaptCaseForDb(table, dm.getDatabaseProductName());

        try (ResultSet rs = dm.getExportedKeys(null, null, adaptedTableName)) {
            addFks(fks, rs, false);
        }

        try (ResultSet rs = dm.getImportedKeys(null, null, adaptedTableName)) {
            addFks(fks, rs, true);
        }

        fks.sort(Comparator.comparing(Fk::getFktable).thenComparing(Fk::getFkcolumn));

        return fks;
    }

    private static void addFks(List<Fk> fks, ResultSet rs, boolean inverted) throws SQLException {
        while (rs.next()) {
            Fk fk = new Fk(getStringFromResultSet(rs,"pktable_name"), getStringFromResultSet(rs,"pkcolumn_name"),
                    getStringFromResultSet(rs,"fktable_name"), getStringFromResultSet(rs,"fkcolumn_name"), inverted);

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
        if (string != null && string.startsWith("\"") && string.endsWith("\"")){
            string = string.substring(1, string.length() - 1);
        }
        return string;
    }

    @Override
    public String toString() {
        return "Fk{" +
                "fktable='" + fktable + '\'' +
                ", fkcolumn='" + fkcolumn + '\'' +
                ", pktable='" + pktable + '\'' +
                ", pfcolumn='" + pkcolumn + '\'' +
                ", inverted=" + inverted +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Fk fk = (Fk) o;

        if (!Objects.equals(fktable, fk.fktable)) return false;
        if (!Objects.equals(pkcolumn, fk.pkcolumn)) return false;
        if (!Objects.equals(pktable, fk.pktable)) return false;
        return Objects.equals(fkcolumn, fk.fkcolumn);
    }

    @Override
    public int hashCode() {
        int result = fktable != null ? fktable.hashCode() : 0;
        result = 31 * result + (pkcolumn != null ? pkcolumn.hashCode() : 0);
        result = 31 * result + (pktable != null ? pktable.hashCode() : 0);
        result = 31 * result + (fkcolumn != null ? fkcolumn.hashCode() : 0);
        return result;
    }

    /**
     * mysql does not return the indirekt FKs with {@link DatabaseMetaData#getExportedKeys(String, String, String)}
     *  (it only shows the direct links via {@link DatabaseMetaData#getImportedKeys(String, String, String)})
     *  - so we fake it here and add these indirect keys to the cache
     * (goes through all database tables for this)
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
                current.add(new Fk(fk.pktable, fk.pkcolumn, fk.fktable, fk.fkcolumn, false));
                fkCache.put(fk.pktable, current);
            }
        }
    }

    /** Like  {@link #initFkCacheForMysql(Cache, Connection)} but logs exceptions to stdout */
    public static void initFkCacheForMysql_LogException(Connection demo, Cache<String, List<Fk>> fkCache) {
        try {
            Fk.initFkCacheForMysql(fkCache, demo);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
