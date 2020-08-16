package org.oser.tools.jdbc;

@FunctionalInterface
public interface CheckedFunction<T, R> {
   R apply(T t) throws Exception;
}