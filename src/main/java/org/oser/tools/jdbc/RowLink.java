package org.oser.tools.jdbc;

import lombok.Getter;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A table and 1 concrete primary key  (uniquely identifies a db row)
 */
@Getter
public class RowLink {
    public RowLink(String tableName, Object ... pks) {
        this.setTableName(tableName);
        if (pks != null) {
            this.setPks(Stream.of(pks).map(RowLink::normalizePk).toArray(Object[]::new));
        }
    }

    public static Object normalizePk(Object pk) {
        return pk instanceof Number ? ((Number) pk).longValue() : pk;
    }

    public RowLink(String shortExpression) {
        if (shortExpression == null) {
            throw new IllegalArgumentException("Must not be null");
        }

        String[] split = shortExpression.split("/");

        if ( split.length < 2) {
            throw new IllegalArgumentException("Wrong format, missing /:"+shortExpression);
        }
        setTableName(split[0]);
        setPks(Stream.of(split).skip(1).map(this::parseOneKey).map(RowLink::normalizePk).toArray(Object[]::new));
    }

    private Object parseOneKey(String rest) {
        long optionalLongValue = 0;
        try {
            optionalLongValue = Long.parseLong(rest);
        } catch (NumberFormatException e) {
            return rest;
        }
        return optionalLongValue;
    }

    String tableName;
    Object[] pks;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        String asString = o.toString();
        return asString.equals(this.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.toString());
    }

    @Override
    public String toString() {
        return tableName + "/" + (notNull(pks) ? Stream.of(pks).map(Object::toString).collect(Collectors.joining("/")): "null");
    }

    private boolean notNull(Object[] pks) {
        return pks != null && Arrays.stream(pks).allMatch(e -> e != null);
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setPks(Object[] pks) {
        this.pks = pks;
    }
}
