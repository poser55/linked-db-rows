package org.oser.tools.jdbc;

@FunctionalInterface
/** Function that can throw a checked exception */
public interface CheckedFunction<T, R> {
   R apply(T t) throws Exception;
}