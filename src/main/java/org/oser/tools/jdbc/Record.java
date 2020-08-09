package org.oser.tools.jdbc;

import lombok.Getter;
import lombok.ToString;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.oser.tools.jdbc.DbImporter.JSON_SUBTABLE_SUFFIX;

/**
 * Contains full data of 1 record= 1 row of a database table. A RowLink identifies the root of such a record.
 * Can hold nested sub-records (those that were linked to this RowLink via FKs) - but this is optional.
 * Organizes data as a <em>tree</em>.
 */
@Getter
@ToString
public class Record {
    RowLink rowLink;
    List<FieldAndValue> content = new ArrayList<>();
    String pkName;
    List<Fk> optionalFks = new ArrayList<>(); // the fks from here to other tables

    Map<RecordMetadata, Object> optionalMetadata = new EnumMap<>(RecordMetadata.class);
    private Map<String, JdbcHelpers.ColumnMetadata> columnMetadata;

    public Record(String tableName, Object pk) {
        rowLink = new RowLink(tableName, pk);
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
        rowLink.pk = RowLink.normalizePk(value);
    }

    public String metadata() {
        if (optionalMetadata.isEmpty()) {
            return "-";
        } else {
            DbExporter.ExportContext o = (DbExporter.ExportContext) optionalMetadata.get(RecordMetadata.EXPORT_CONTEXT);

            return ((o == null) ? "" : o.visitedNodes.keySet() + " #nodes:" + o.visitedNodes.size());
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
    public Set<RowLink> getAllNodes(){
        Set<RowLink> result = new HashSet<>();
        result.add(rowLink);
        result.addAll(content.stream()
                .filter(e -> !e.subRow.isEmpty())
                .flatMap(e -> e.subRow.values().stream()).flatMap(Collection::stream).flatMap(e->e.getAllNodes().stream())
                .collect(toSet()));
        return result;
    }

    /** visit all Records (breath first search). You can ignore the result of this method (used internally). */
    public Set<Record> visitRecords(Consumer<Record> visitor){
        visitor.accept(this);

        return content.stream().filter(e -> !e.subRow.isEmpty())
                .flatMap(e -> e.subRow.values().stream())
                .flatMap(Collection::stream)
                .flatMap(e -> e.visitRecords(visitor).stream()).collect(toSet());
    }

    /** visit all Records in insertion order */
    public void visitRecordsInInsertionOrder(Connection connection, Consumer<Record> visitor) throws SQLException {
        List<String> insertionOrder = JdbcHelpers.determineOrder(connection, rowLink.tableName);

        Map<String, List<Record>> tableToRecords = new HashMap<>();
        visitRecords(r -> {
            if (!tableToRecords.containsKey(r.rowLink.tableName)) {
                tableToRecords.put(r.rowLink.tableName, new ArrayList<>());
            }
            tableToRecords.get(r.rowLink.tableName).add(r);
        });

        for (String tableName : insertionOrder) {
            List<Record> records = tableToRecords.get(tableName);
            if (records != null) {
                records.forEach(r -> visitor.accept(r));
            }
        }
    }

    /** count number of each table */
    public static Map<String, Integer> classifyNodes(Set<RowLink> allNodes) {
        return allNodes.stream().collect(Collectors.groupingBy(RowLink::getTableName)).entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> entry.getKey(),
                        entry -> entry.getValue().size()));
    }

    public void setColumnMetadata(Map<String, JdbcHelpers.ColumnMetadata> columnMetadata) {
        this.columnMetadata = columnMetadata;
    }

    public Map<String, JdbcHelpers.ColumnMetadata> getColumnMetadata() {
        return columnMetadata;
    }


    /**
     * Holds one field with metadata (and potentially nested content)
     */
    public static class FieldAndValue {
        public String name;
        public Object value;
        public JdbcHelpers.ColumnMetadata metadata;
        public Map<String, List<Record>> subRow = new HashMap<>();

        public FieldAndValue(String name, JdbcHelpers.ColumnMetadata metadata, Object value) {
            this.name = name;
            this.metadata = metadata;

            this.value = convertTypeForValue(metadata, value);
        }

        private Object convertTypeForValue(JdbcHelpers.ColumnMetadata metadata, Object value) {
            switch (metadata.type.toUpperCase()) {
                case "BOOLEAN":
                case "BOOL":
                    Boolean bool = null;
                    if (value instanceof String) {
                        try {
                            bool = Boolean.parseBoolean((String) value); // todo make a bit more intelligent (also accept T/F...)
                        } catch (NumberFormatException e) {
                            // ok
                        }
                    }
                    return bool != null ? bool : value;
                case "SERIAL":
                case "INT2":
                case "INT4":
                case "INT8":
                    Long l = null;
                    if (value instanceof String) {
                        try {
                            l = Long.parseLong((String) value);
                        } catch (NumberFormatException e) {
                            // ok
                        }
                    }
                    return l != null ? l : value;
                case "NUMERIC":
                case "DECIMAL":
                    Double d = null;
                    if (value instanceof String) {
                        try {
                            d = Double.parseDouble((String) value);
                        } catch (NumberFormatException e) {
                            // ok
                        }
                    }
                    return  d != null ? d : value;
                case "TIMESTAMP":
                    Timestamp ts = null;
                    if (value instanceof String) {
                        try {
                            ts = Timestamp.valueOf(((String) value).replace("T", " "));
                        } catch (IllegalArgumentException e) {
                            // ok
                        }
                    }
                    return  ts != null ? ts : value;
                case "DATE":
                    Date date = null;
                    if (value instanceof String) {
                        try {
                            date = Date.valueOf((String) value);
                        } catch (IllegalArgumentException e) {
                            // ok
                        }
                    }
                    return  date != null ? date : value;
                case "VARCHAR":
                case "TEXT":
                default:
                    return value;
            }
        }

        @Override
        public String toString() {
            return "\"" + name +
                    "\":" + getValueWithQuoting() +
                    ((!(subRow.isEmpty() || subRow.values().stream().map(List::size).max(Integer::compareTo).orElseGet(() -> 0) == 0)) ?
                            (", " + maplistlist2jsonString(name, subRow)) :
                            "") +
                    "";
        }

        String getValueWithQuoting() {
            switch (metadata.type.toUpperCase()) {
                case "BOOLEAN":
                case "BOOL":
                case "INT4":
                case "INT8":
                case "NUMERIC":
                case "DECIMAL":
                    return value != null ? value.toString() : null;
                case "TIMESTAMP":
                    return value != null ? ("\"" + DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(((Timestamp) value).toLocalDateTime()) + "\"") : null;
                case "DATE":
                    return value != null ? ("\"" + ((Date) value).toLocalDate() + "\"") : null;
                case "VARCHAR":
                case "_TEXT":
                case "TEXT":
                default:
                    if (value == null) {
                        return null;
                    }
                    String valueEscaped = Objects.toString(value);

                    return ("\"" + valueEscaped.replace("\"", "\\\"") + "\"");
            }

        }

        private String maplistlist2jsonString(String name, Map<String, List<Record>> map) {
            // a "*" (star) at the end of a key means this is a subrow added on this level
            return map.keySet().stream().map(key -> "\"" + getSubtableKeyName(name, key) + "\":[" + listOfData2jsonString(map.get(key)) + "]").collect(Collectors.joining(","));
        }

        private String listOfData2jsonString(List<Record> lists) {
            return lists.stream().map(this::row2json).collect(Collectors.joining(","));
        }

        public String row2json(Record row) {
            return "{" + row.content.stream().map(FieldAndValue::toString).collect(Collectors.joining(",")) + "}";
        }
    }

    private static String getSubtableKeyName(String name, String key) {
        return name + JSON_SUBTABLE_SUFFIX + key + JSON_SUBTABLE_SUFFIX;
    }
}
