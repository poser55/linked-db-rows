package org.oser.tools.jdbc;

import lombok.Getter;
import lombok.ToString;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.oser.tools.jdbc.DbImporter.JSON_SUBTABLE_SUFFIX;

/**
 * Contains full data of 1 record=row of a database table. A RowLink identifies the root of such a record.
 * Can hold nested sub-records (those that were linked to this RowLink via FKs) - but this is optional.
 * Organizes data as a <em>tree</em>.
 */
@Getter
@ToString
public class Record {
    DbExporter.RowLink rowLink;
    List<FieldAndValue> content = new ArrayList<>();
    String pkName;
    List<Fk> optionalFks = new ArrayList<>(); // the fks from here to other tables

    Map<RecordMetadata, Object> optionalMetadata = new HashMap<>();

    public Record(String tableName, Object pk) {
        rowLink = new DbExporter.RowLink(tableName, pk);
    }

    /**
     * returns json String of this record
     */
    public String asJson() {
        return "{ " +
                content.stream().map(FieldAndValue::toString).collect(Collectors.joining(",")) +
                " }";
    }

    public FieldAndValue findElementWithName(String columnName) {
        for (FieldAndValue d : content) {
            if (d.name.toUpperCase().equals(columnName.toUpperCase())) {
                return d;
            }
        }
        return null;
    }

    public List<String> getFieldNames() {
        return content.stream().map(e -> e.name).collect(toList());
    }

    public List<String> getFieldNamesWithSubrows() {
        return content.stream().filter(e -> !e.subRow.isEmpty()).map(e -> e.name).collect(toList());
    }

    public void setPkValue(Object value) {
        rowLink.pk = DbExporter.RowLink.normalizePk(value);
    }

    public String metadata() {
        if (optionalMetadata.isEmpty()) {
            return "-";
        } else {
            DbExporter.ExportContext o = (DbExporter.ExportContext) optionalMetadata.get(RecordMetadata.EXPORT_CONTEXT);

            return o.visitedNodes.keySet() + ((o == null) ? "" : " #nodes:" + o.visitedNodes.size());
        }
    }

    /** @return the element with the name if it is contained in this record (not considering sub-records) */
    public FieldAndValue elementByName(String name) {
        for (FieldAndValue element : content) {
            if (element.name.equals(name)) {
                return element;
            }
        }
        return null;
    }

    /** @return all nodes=RowLinks that are contained in the record */
    public Set<DbExporter.RowLink> getAllNodes(){
        Set<DbExporter.RowLink> result = new HashSet<>();
        result.add(rowLink);
        result.addAll(content.stream()
                .filter(e -> !e.subRow.isEmpty())
                .flatMap(e -> e.subRow.values().stream()).flatMap(e -> e.stream()).flatMap(e->e.getAllNodes().stream())
                .collect(toSet()));
        return result;
    }

    /** visit all Records (breath first search). You can ignore the result of this method (used internally). */
    public Set<Record> visitAllRecords(Consumer<Record> visitor){

        visitor.accept(this);

        return content.stream().filter(e -> !e.subRow.isEmpty())
                .flatMap(e -> e.subRow.values().stream())
                .flatMap(e -> e.stream())
                .flatMap(e -> e.visitAllRecords(visitor).stream()).collect(toSet());
    }

    /** visit all Records in insertion order */
    public void visitRecordsInInsertionOrder(Connection connection, Consumer<Record> visitor) throws SQLException {
        List<String> insertionOrder = JdbcHelpers.determineOrder(connection, rowLink.tableName);

        Map<String, List<Record>> tableToRecords = new HashMap<>();
        visitAllRecords(r -> {
            if (!tableToRecords.containsKey(r.rowLink.tableName)) {
                tableToRecords.put(r.rowLink.tableName, new ArrayList<>());
            }
            tableToRecords.get(r.rowLink.tableName).add(r);
        });

        for (String tableName : insertionOrder) {
            tableToRecords.get(tableName).forEach(r -> visitor.accept(r));
        }
    }


    /**
     * Holds one field with metadata (and potentially nested content)
     */
    public static class FieldAndValue {
        public String name;
        public Object value;
        public JdbcHelpers.ColumnMetadata metadata;
        public Map<String, List<Record>> subRow = new HashMap<>();

        @Override
        public String toString() {
            return "\"" + name +
                    "\":" + getValueWithQuoting() +
                    ((!(subRow.isEmpty() || subRow.values().stream().map(List::size).max(Integer::compareTo).orElseGet(() -> 0) == 0)) ? (", " + maplistlist2jsonString(subRow)) : "") +
                    "";
        }

        private String getValueWithQuoting() {
            switch (metadata.type) {
                case "BOOLEAN":
                case "bool":
                case "int4":
                case "int8":
                case "numeric":
                case "DECIMAL":
                    return value != null ? value.toString() : null;
                case "timestamp":
                    return value != null ? ("\"" + DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(((Timestamp) value).toLocalDateTime()) + "\"") : null;
                case "date":
                    return value != null ? ("\"" + ((java.sql.Date) value).toLocalDate() + "\"") : null;
                case "VARCHAR":
                default:
                    if (value == null) {
                        return null;
                    }

                    Object valueEscaped = value instanceof String ? ((String) value).replace("\"", "\\\"") : value;
                    return ("\"" + valueEscaped + "\"");
            }

        }

        private String maplistlist2jsonString(Map<String, List<Record>> map) {
            // a "*" (star) at the end of a key means this is a subrow added on this level
            return map.keySet().stream().map(key -> "\"" + key + JSON_SUBTABLE_SUFFIX + "\":[" + listOfData2jsonString(map.get(key)) + "]").collect(Collectors.joining(","));
        }

        private String listOfData2jsonString(List<Record> lists) {
            return lists.stream().map(this::row2json).collect(Collectors.joining(","));
        }

        public String row2json(Record row) {
            return "{" + row.content.stream().map(FieldAndValue::toString).collect(Collectors.joining(",")) + "}";
        }
    }
}
