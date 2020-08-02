package org.oser.tools.jdbc;

import java.util.Objects;

/**
 * A table & its pk  (uniquely identifies a db row)
 */
public class RowLink {
    public RowLink(String tableName, Object pk) {
        this.tableName = tableName;
        this.pk = normalizePk(pk);
    }

    public static Object normalizePk(Object pk) {
        return pk instanceof Number ? ((Number) pk).longValue() : pk;
    }

    public RowLink(String shortExpression) {
        if (shortExpression == null) {
            throw new IllegalArgumentException("Must not be null");
        }
        int i = shortExpression.indexOf("/");
        if ( i == -1) {
            throw new IllegalArgumentException("Wrong format, missing /:"+shortExpression);
        }
        tableName = shortExpression.substring(0, i);
        String rest = shortExpression.substring(i+1);

        long optionalLongValue = 0;
        try {
            optionalLongValue = Long.parseLong(rest);
        } catch (NumberFormatException e) {
            pk = normalizePk(rest);
        }
        pk = normalizePk(optionalLongValue);
    }

    public String tableName;
    public Object pk;

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
        return tableName + "/" + pk ;
    }
}
