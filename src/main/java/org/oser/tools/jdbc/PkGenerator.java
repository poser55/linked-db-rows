package org.oser.tools.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

public interface PkGenerator {
    Object generatePk(Connection connection, String tableName, String pkType, String pkName) throws SQLException;
}
