package org.oser.tools.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/** Trivial primary key generator strategy */
public class NextValuePkGenerator implements PkGenerator {

    @Override
    public Object generatePk(Connection connection, String tableName, String pkType, String pkName) throws SQLException {
        if (pkType.toUpperCase().equals("VARCHAR")) {
            return UUID.randomUUID().toString();
        } else if (pkType.toUpperCase().startsWith("INT") || pkType.equals("NUMERIC") || pkType.toUpperCase().equals("DECIMAL")) {
            return  getMaxUsedIntPk(connection, tableName, pkName) + 1;
        }
        throw new IllegalArgumentException("not yet supported type for pk " + pkType);
    }

    public static long getMaxUsedIntPk(Connection connection, String tableName, String pkName) throws SQLException {
        String selectPk = "SELECT max(" + pkName + ") from " + tableName;

        try (PreparedStatement pkSelectionStatement = connection.prepareStatement(selectPk)) { // NOSONAR: unchecked values all via prepared statement

            try (ResultSet rs = pkSelectionStatement.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }
        throw new IllegalArgumentException("issue with getting next pk");
    }

}
