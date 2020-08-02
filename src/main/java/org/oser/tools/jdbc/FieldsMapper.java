package org.oser.tools.jdbc;

import java.sql.PreparedStatement;

/**
 * In case your field needs special treatment: define how to add it to the PreparedStatement.
 */
@FunctionalInterface
public interface FieldsMapper {
	void mapField(DbExporter.ColumnMetadata metadata, PreparedStatement statement, int insertIndex, String csvValue);
}
