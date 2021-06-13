package org.oser.tools.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * In case your field or type needs special treatment when inserting into the db: define how to add it to the PreparedStatement.
 *  Can also do nothing (and therefore NOT import the field) or use the default treatment.
 *
 *   * <br/> <br/>
 *  *   Example FieldImporter:
 *  * <pre>{@code
 *      FieldImporter clobImporter = (tableName, metadata, statement, insertIndex, value ) -> {
 *      	   if (value == null) {
 *      	        statement.setObject(insertIndex, value, Types.OTHER);
 *      	   } else {
 *             		Clob clob = statement.getConnection().createClob();
 *             		clob.setString(1, (String) value);
 *             		statement.setClob(insertIndex, clob);
 *             }
 *             return true;
 *         };
 *   }</pre>
 *
 */
@FunctionalInterface
public interface FieldImporter {
	/**
	 * Sets the "value" on the "statement"
	 * @param tableName what table name the field should be matched - in certain case is "" (when it cannot be easily found)
	 * @return true if we should bypass normal treatment afterwards! */
	boolean importField(String tableName, JdbcHelpers.ColumnMetadata metadata, PreparedStatement statement, int insertIndex, Object value) throws SQLException;

	/** Ignores a field when importing
	 *  @deprecated use {@link #NOP_FIELDIMPORTER} instead */
	@Deprecated
	FieldImporter NOP_FIELDMAPPER = (tableName, metadata, statement, insertIndex, value) -> {
		statement.setArray(insertIndex, null);
		return true;
	};

	/** Ignores a field when importing.
	 *  CAVEAT: does e.g. not work for Clob in oracle! */
	FieldImporter NOP_FIELDIMPORTER = (tableName, metadata, statement, insertIndex, value) -> {
		statement.setArray(insertIndex, null);
		return true;
	};
}
