package org.oser.tools.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Define special treatment to export a field from the db. <br/> <br/>
 *
 * CAVEAT for implementers: h2 has some special quirks: string typs should be read via ResultSet#getString()
 *    // this is a bit hacky for now (as we do not yet have full type support and h2 behaves strangely)
 *         boolean useGetString = context.getDbProductName().equals("H2") &&
 *                 STRING_TYPES.contains(columns.get(columnName.toLowerCase()).getDataType());
 *         Object valueAsObject = useGetString ? rs.getString(i) : rs.getObject(i);
 *
 * <br/> <br/>
 *   Example FieldExporter:
 * <pre>{@code
 *     FieldExporter clobExporter = (tableName, fieldName, metadata, resultSet) -> {
 *             Clob clob = resultSet.getClob(fieldName);
 *             return new Record.FieldAndValue(fieldName, metadata, clob == null ? null : clob.getSubString(1, (int) clob.length()));
 *         };
 * }</pre>
 */
@FunctionalInterface
public interface FieldExporter {
	/**
	 * @param tableName only match on tables with the given name (can be null to match on all tables)
	 * @return can return null to indicate to skip the field */
	DbRecord.FieldAndValue exportField(String tableName, String fieldName, JdbcHelpers.ColumnMetadata metadata, ResultSet rs)  throws SQLException;

	/** Ignores a field in the export */
	FieldExporter NOP_FIELDEXPORTER = (tableName, name, metadata, rs) -> null;
}
