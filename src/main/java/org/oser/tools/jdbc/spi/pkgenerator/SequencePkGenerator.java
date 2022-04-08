package org.oser.tools.jdbc.spi.pkgenerator;

import org.oser.tools.jdbc.PkGenerator;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

/** Simple sequence primary key generator strategy: by default adds "_id_seq" suffix to table name.
 *  This suffix can be overridden. For more exceptions, use the Map<String, String> tableToSequence name mapping. */
public class SequencePkGenerator implements PkGenerator {
    public static final String DEFAULT_SUFFIX_ID_SEQ = "_id_seq";

    public SequencePkGenerator() {
    }

    public SequencePkGenerator(Map<String, String> tableNameToSequence) {
        this.tableNameToSequence = tableNameToSequence;
    }

    public SequencePkGenerator(String defaultSuffix, Map<String, String> tableNameToSequence) {
        this.defaultSuffix = defaultSuffix;
        this.tableNameToSequence = tableNameToSequence;
    }

    private String defaultSuffix = DEFAULT_SUFFIX_ID_SEQ;

    private Map<String, String> tableNameToSequence = Collections.emptyMap();

    @Override
    public Object generatePk(Connection connection, String tableName, String pkType, String pkName) throws SQLException {
        return getNextSequenceValue(connection, getSequenceName(tableName));
    }

    String getSequenceName(String tableName) {
        return tableNameToSequence.containsKey(tableName) ? tableNameToSequence.get(tableName) : tableName + defaultSuffix;
    }

    public static long getNextSequenceValue(Connection connection, String sequenceName) throws SQLException {
        if (!sequenceName.matches("[\\w-]*")) {
            throw new IllegalArgumentException("Wrong sequence name format (\\w- characters only):" + sequenceName);
        }

        DatabaseMetaData dm = connection.getMetaData();
        String nextSequenceValue = dm.getDatabaseProductName().toLowerCase().equals("oracle") ? "SELECT "+sequenceName+".nextval FROM DUAL" : "SELECT nextval('"+sequenceName+"')";
        try (PreparedStatement pkSelectionStatement = connection.prepareStatement(nextSequenceValue)) { // NOSONAR

            try (ResultSet rs = pkSelectionStatement.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }
        throw new IllegalArgumentException("Issue with getting next pk");
    }
}
