package org.oser.tools.jdbc;

/** Function that can throw a checked exception */
@FunctionalInterface
public interface CheckedFunction<T, R> {
   R apply(T t) throws Exception;
}