package org.oser.tools.jdbc.spi.statements;

import org.oser.tools.jdbc.JdbcHelpers;

import java.sql.PreparedStatement;

/** Plugin interface to handle certain db types in a particular way when updating them on "setType" kind statements.
 *   Can be activated in @see JdbcHelper#innerSetStatementField
 */
public interface JdbcStatementSetter {
     /** Sets the "valueToInsert" on the "preparedStatement"
      * @return true if we should bypass normal treatment! */
    boolean innerSetStatementField(PreparedStatement preparedStatement,
                                int statementIndex,
                                JdbcHelpers.ColumnMetadata columnMetadata,
                                String valueToInsert);

}
