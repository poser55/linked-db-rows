package org.oser.tools.jdbc;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.Getter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Represents one foreign key constraint */
@Getter
public class Fk {
    public String pktable;
    public String pkcolumn;

    public String fktable;
    public String fkcolumn;

    public String type;

    public boolean inverted; // excluded in equals!

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

            fk.pktable = rs.getString("pktable_name");
            fk.pkcolumn = rs.getString("pkcolumn_name");
            fk.fktable = rs.getString("fktable_name");
            fk.fkcolumn = rs.getString("fkcolumn_name");
            fk.inverted = inverted;

            fks.add(fk);
        }
    }

    @Override
    public String toString() {
        return "Fk{" +
                "fktable='" + fktable + '\'' +
                ", fkcolumn='" + fkcolumn + '\'' +
                ", pktable='" + pktable + '\'' +
                ", pfcolumn='" + pkcolumn + '\'' +
                ", type='" + type + '\'' +
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
        if (!Objects.equals(type, fk.type)) return false;
        if (!Objects.equals(pktable, fk.pktable)) return false;
        return Objects.equals(fkcolumn, fk.fkcolumn);
    }

    @Override
    public int hashCode() {
        int result = fktable != null ? fktable.hashCode() : 0;
        result = 31 * result + (pkcolumn != null ? pkcolumn.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (pktable != null ? pktable.hashCode() : 0);
        result = 31 * result + (fkcolumn != null ? fkcolumn.hashCode() : 0);
        return result;
    }
}
