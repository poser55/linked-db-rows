package org.oser.tools.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * In case your field or type needs special treatment when inserting into the db: define how to add it to the PreparedStatement.
 *  Can also do nothing (and therefore NOT import the field) or use the default treatment.
 */
@FunctionalInterface
public interface FieldImporter {
	/**
	 * Sets the "value" on the "statement"
	 * @param tableName what table name the field should be matched - in certain case is "" (when it cannot be easily found)
	 * @return true if we should bypass normal treatment afterwards! */
	boolean importField(String tableName, JdbcHelpers.ColumnMetadata metadata, PreparedStatement statement, int insertIndex, String value) throws SQLException;

	/** Ignores a field when importing
	 *  @deprecated use {@link #NOP_FIELDIMPORTER} instead */
	@Deprecated
	FieldImporter NOP_FIELDMAPPER = (tableName, metadata, statement, insertIndex, value) -> {
		statement.setArray(insertIndex, null);
		return true;
	};

	/** Ignores a field when importing */
	FieldImporter NOP_FIELDIMPORTER = (tableName, metadata, statement, insertIndex, value) -> {
		statement.setArray(insertIndex, null);
		return true;
	};
}
