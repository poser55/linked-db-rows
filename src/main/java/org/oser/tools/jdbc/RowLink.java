package org.oser.tools.jdbc;

import lombok.Getter;

import java.sql.DatabaseMetaData;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A table and a list of concrete primary keys  (uniquely identifies a row in a db)<p>
 *  What is the order of the pks?  As we get it from the database metadata.
 *    E.g. via {@link JdbcHelpers#getPrimaryKeys(DatabaseMetaData, String)}
 */
@Getter
public class RowLink {
    public RowLink(String tableName, Object ... pks) {
        this.setTableName(tableName.toLowerCase());
        if (pks != null) {
            for (int i = 0; i < pks.length; i++){
                pks[i] = RowLink.normalizePk(pks[i]);
            }
            this.setPks(pks);
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
        setTableName(split[0].toLowerCase());
        setPks(Stream.of(split).skip(1).map(this::parseOneKey).map(RowLink::normalizePk).toArray(Object[]::new));
    }

    public RowLink(RowLink other){
        tableName = other.getTableName();
        pks = other.getPks().clone();
    }

    private Object parseOneKey(String rest) {
        long optionalLongValue;
        try {
            optionalLongValue = Long.parseLong(rest);
        } catch (NumberFormatException e) {
            return rest;
        }
        return optionalLongValue;
    }

    private String tableName;
    private Object[] pks;

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
        this.tableName = tableName.toLowerCase();
    }

    public void setPks(Object[] pks) {
        this.pks = pks;
    }
}
