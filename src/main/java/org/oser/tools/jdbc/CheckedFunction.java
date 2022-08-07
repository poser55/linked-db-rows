package org.oser.tools.jdbc;

import java.sql.SQLException;

/** Function that can throw a checked SQL exception */
@FunctionalInterface
public interface CheckedFunction<T, R> {
   R apply(T t) throws SQLException;
}