package org.oser.tools.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * In case your field needs special treatment when inserting into the db: define how to add it to the PreparedStatement.
 *  Can also do nothing (and therefore NOT import the field).
 */
@FunctionalInterface
public interface FieldImporter {
	/**
	 * @param tableName what table name the field should be matched - can be null to match all such fields */
	void importField(String tableName, JdbcHelpers.ColumnMetadata metadata, PreparedStatement statement, int insertIndex, String value) throws SQLException;

	/** Ignores a field when importing
	 *  @deprecated use {@link #NOP_FIELDIMPORTER} instead */
	@Deprecated
	FieldImporter NOP_FIELDMAPPER = (tableName, metadata, statement, insertIndex, value) -> statement.setArray(insertIndex, null);

	/** Ignores a field when importing */
	FieldImporter NOP_FIELDIMPORTER = (tableName, metadata, statement, insertIndex, value) -> statement.setArray(insertIndex, null);
}
