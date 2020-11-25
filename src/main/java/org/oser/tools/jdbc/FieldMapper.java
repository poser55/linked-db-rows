package org.oser.tools.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * In case your field needs special treatment when inserting into the db: define how to add it to the PreparedStatement.
 */
@FunctionalInterface
public interface FieldMapper {
	void mapField(JdbcHelpers.ColumnMetadata metadata, PreparedStatement statement, int insertIndex, String value) throws SQLException;

	FieldMapper NOP_FIELDMAPPER = (metadata, statement, insertIndex, value) -> statement.setArray(insertIndex, null);
}
