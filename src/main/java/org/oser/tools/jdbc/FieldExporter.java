package org.oser.tools.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Define special treatment to export a field from the db.
 */
@FunctionalInterface
public interface FieldExporter {
	/** @return can return null to indicate to skip the field */
	Record.FieldAndValue exportField(String name, JdbcHelpers.ColumnMetadata metadata, Object value, ResultSet rs)  throws SQLException;

	/** Ignores a field in the export */
	FieldExporter NOP_FIELDEXPORTER = (name, metadata, value, rs) -> {
		return null;
	};
}
