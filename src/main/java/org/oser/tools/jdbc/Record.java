package org.oser.tools.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Getter;
import lombok.ToString;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
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
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.oser.tools.jdbc.DbImporter.JSON_SUBTABLE_SUFFIX;
import static org.oser.tools.jdbc.Fk.getFksOfTable;

/**
 * Contains full data of 1 record= 1 row of a database table. A RowLink identifies the root of such a record.
 * Can hold nested sub-records (those that were linked to this RowLink via foreign keys) - but this is optional.
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

    public Record(String tableName, Object[] pks) {
        rowLink = new RowLink(tableName, pks);
    }

    ObjectMapper mapper = JdbcHelpers.getObjectMapper();

    /** JsonNode representation  */
    public JsonNode asJsonNode() {
        ObjectNode record = mapper.createObjectNode();
        content.forEach(field -> field.addToJsonNode(record));
        return record;
    }

    /**
     * returns json String of this record
     * @deprecated use #asJsonNode() as it handles quoting of number types better
     */
    @Deprecated
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

    public Integer findElementPositionWithName(String columnName) {
        int position = 1;
        for (FieldAndValue d : content) {
            if (d.name.toUpperCase().equals(columnName.toUpperCase())) {
                return position;
            }
            position++;
        }
        return null;
    }

    public List<String> getFieldNames() {
        return content.stream().map(e -> e.name).collect(toList());
    }

    public List<String> getFieldNamesWithSubrows() {
        return content.stream().filter(e -> !e.subRow.isEmpty()).map(e -> e.name).collect(toList());
    }

    public void setPkValue(Object[] value) {
        rowLink.setPks(Stream.of(value).map(RowLink::normalizePk).toArray(Object[]::new));
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

        // not fully implemented via streams (as nested streams use more stack levels)
        List<Record> subRecords = content.stream().filter(e -> !e.subRow.isEmpty()).flatMap(e -> e.subRow.values().stream()).flatMap(Collection::stream).collect(toList());
        for (Record record : subRecords) {
            result.addAll(record.getAllNodes());
        }
        return result;
    }

    /** visit all Records (breath first search). You can ignore the result of this method (used internally). */
    public Set<Record> visitRecords(Consumer<Record> visitor){
        visitor.accept(this);

        List<Record> toVisit = content.stream().filter(e -> !e.subRow.isEmpty())
                .flatMap(e -> e.subRow.values().stream())
                .flatMap(Collection::stream).collect(toList());

        return toVisit.stream().flatMap(e -> e.visitRecords(visitor).stream()).collect(toSet());
    }

    public void visitRecordsInInsertionOrder(Connection connection, CheckedFunction<Record, Void> visitor) throws Exception {
        visitRecordsInInsertionOrder(connection, visitor, Caffeine.newBuilder().maximumSize(10_000).build());
    }

    /** visit all Records in insertion order */
    public void visitRecordsInInsertionOrder(Connection connection, CheckedFunction<Record, Void> visitor, Cache<String, List<Fk>> cache) throws Exception {
        List<String> insertionOrder = JdbcHelpers.determineOrder(connection, rowLink.tableName, cache);

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
                for (Record record : records) {
                    visitor.apply(record);
                }
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

        private ObjectMapper mapper = JdbcHelpers.getObjectMapper();

        private Object convertTypeForValue(JdbcHelpers.ColumnMetadata metadata, Object value) {
            if ("null".equals(value)) {
                return null;
            }
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
                    BigDecimal d = null;
                    if (value instanceof String) {
                        try {
                            d = new BigDecimal((String) value);
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

        public void addToJsonNode(ObjectNode topLevelNode) {
            putFieldToJsonNode(topLevelNode);
            if  ((!(subRow.isEmpty() || subRow.values().stream().map(List::size).max(Integer::compareTo).orElseGet(() -> 0) == 0))) {
                addSubRowToJsonNode(topLevelNode);
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
                case "INT2":
                case "INT4":
                case "INT8":
                case "FLOAT8":
                case "FLOAT4":
                case "NUMERIC":
                case "SERIAL":
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

        void putFieldToJsonNode(ObjectNode node) {
            if (value == null) {
                node.put(name, (String)null);
            } else if (value instanceof Integer) {
                    node.put(name, (Integer) value);
            } else if (value instanceof BigDecimal) {
                node.put(name, (BigDecimal) value);
            } else if (value instanceof Long) {
                node.put(name, (Long) value);
            } else if (value instanceof String) {
                node.put(name, (String) value);
            } else if (value instanceof Boolean) {
                node.put(name, (Boolean) value);
            } else if (value instanceof Timestamp) {
                node.put(name,  DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(((Timestamp) value).toLocalDateTime()));
            } else if (value instanceof Date) {
                node.put(name,  ((Date) value).toLocalDate().toString());
            } else {
                node.put(name, value.toString());
            }
        }


        private void addSubRowToJsonNode(ObjectNode topLevelNode) {
            // a "*" (star) at the end of a key means this is a subrow added on this level

            for (Map.Entry<String, List<Record>> entry : subRow.entrySet()){
                ArrayNode jsonNodes = topLevelNode.putArray(getSubtableKeyName(name, entry.getKey()));
                addAllSubtableElements(jsonNodes, entry.getValue());
            }
        }

        private void addAllSubtableElements(ArrayNode array, List<Record> lists) {
            for (Record subrow : lists) {
                ObjectNode subRecord = mapper.createObjectNode();

                for (FieldAndValue field : subrow.content) {
                    field.addToJsonNode(subRecord);
                }
                array.add(subRecord);
            }
        }


        private String maplistlist2jsonString(String name, Map<String, List<Record>> map) {
            // a "*" (star) at the end of a key means this is a subrow added on this level

            List<String> subResult = new ArrayList<>();
            for (Map.Entry<String, List<Record>> entry : map.entrySet()){
                subResult.add("\"" + getSubtableKeyName(name, entry.getKey()) + "\":[" + listOfData2jsonString(entry.getValue()) + "]");
            }

            return String.join(",", subResult);
        }

        private String listOfData2jsonString(List<Record> lists) {
            // splitting the stream in 2 steps (to be more stack friendly)
            List<String> records = new ArrayList<>();
            for (Record record : lists) {
                records.add(row2json(record));
            }

            return String.join(",", records);
        }

        public static String row2json(Record row) {
            // splitting the stream in 2 steps (to be more stack friendly)
            List<String> fieldList = new ArrayList<>();
            for (FieldAndValue field : row.content) {
                fieldList.add(field.toString());
            }

            return "{" + String.join(",", fieldList) + "}";
        }
    }

    private static String getSubtableKeyName(String name, String key) {
        return name + JSON_SUBTABLE_SUFFIX + key + JSON_SUBTABLE_SUFFIX;
    }

    /**
     *  todo: THIS DOES NOT YET WORK!
     *  emerging: remap the keys so that they are canonical (2 same exports with the same data lead to the same json structure)
     *   the id oder is determined based on the original order (so assuming integer primary keys this should be stable for equality)
     *
     *   needs the connection to determine the insertionOrder traversal
     *
     *   Updates this record and all other records (assumes that it works on the root record)
     */
    public void canonicalizeIds(Connection connection, Cache<String, List<Fk>> fkCache) throws Exception {
        List<String> insertionOrder = JdbcHelpers.determineOrder(connection, rowLink.tableName);

        Map<String, List<Record>> tableToRecords = new HashMap<>();
        visitRecords(r -> {
            if (!tableToRecords.containsKey(r.rowLink.tableName)) {
                tableToRecords.put(r.rowLink.tableName, new ArrayList<>());
            }
            tableToRecords.get(r.rowLink.tableName).add(r);
        });

        Map<RowLink, Object> newKeys = new HashMap<>();

        for (String tableName : insertionOrder) {
            List<Record> records = tableToRecords.get(tableName);

            DatabaseMetaData metaData = connection.getMetaData();
            List<String> primaryKeys = JdbcHelpers.getPrimaryKeys(metaData, tableName);
            List<List<Object>> primaryKeyValues = records.stream().map(record -> primaryKeys.stream().map(pkName -> record.findElementWithName(pkName).value).collect(toList())).collect(toList());

            List<Fk> fksOfTable = getFksOfTable(connection, records.get(0).rowLink.tableName, fkCache);

            Map<String, List<Fk>> fksByColumnName = fksOfTable.stream().collect(Collectors.groupingBy(fk1 -> (fk1.inverted ? fk1.getFkcolumn() : fk1.getPkcolumn()).toUpperCase()));
            List<Boolean> isFreePk = new ArrayList<>(primaryKeys.size());

            // we only need to get the free PKs
            DbImporter.remapPrimaryKeyValues(records.get(0), newKeys, primaryKeys, fksByColumnName, isFreePk);

            for (int i = 0; i < records.size(); i++) {
                // todo: does this work for FKs to the same table? Probably yes?
                remapKeysAndUpdateNewKeys(records.get(i), i, primaryKeys, primaryKeyValues.get(i), isFreePk, newKeys);
            }
        }
    }

    /**
     * 1. determine new keys for the entries of the record
     * 2. remap fields to with foreign keys that were remapped before */
    private void remapKeysAndUpdateNewKeys(Record r, int i, List<String> primaryKeys, List<Object> primaryKeyValues, List<Boolean> isFreePk, Map<RowLink, Object> newKeys) {
        // 1. determine new keys for the entries of the record
        List<Object> newKeysForThisRecord = determineNewCanonicalPrimaryKeys(i, primaryKeyValues, isFreePk);



    }

    private List<Object> determineNewCanonicalPrimaryKeys(int i, List<Object> primaryKeyValues, List<Boolean> isFreePk) {
        List<Object> newKeys = new ArrayList <>();
        primaryKeyValues.forEach(currentValue -> createAndAddKey(i, currentValue, isFreePk.get(i), newKeys));
        return newKeys;
    }

    private void createAndAddKey(int i, Object currentValue, Boolean isAFreePkValue, List<Object> newKeys) {
        Object newValue = isAFreePkValue ? getKeyForIndex(i, currentValue)  : currentValue;
        newKeys.add(newValue);
    }

    // todo: only works for Long and String
    private Object getKeyForIndex(int i, Object currentValue) {
        if (currentValue instanceof Number) {
            return (long) i;
        }
        // todo treat uuid/ string pks correctly
        return getCharForNumber(i);
    }

    private String getCharForNumber(int i) {
        return i > 0 && i < 27 ? String.valueOf((char)(i + 64)) : null;
    }

}
