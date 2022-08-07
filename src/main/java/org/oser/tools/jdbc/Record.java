package org.oser.tools.jdbc;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Getter;
import lombok.Setter;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Consumer;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.oser.tools.jdbc.DbImporter.JSON_SUBTABLE_SUFFIX;

/**
 * Contains full data of 1 db record= 1 row of a database table. A RowLink identifies the root of such a record.
 * Can hold nested sub-records (those that were linked to this RowLink via foreign keys) - but this is optional.
 * Organizes data as a <em>tree</em>.
 */
@Getter
public class Record {
    private final RowLink rowLink;
    private final List<FieldAndValue> content = new ArrayList<>();

    @Getter
    @Setter
    private List<String> pkNames;

    /**  the fks from here to other tables */
    @Setter
    private List<Fk> optionalFks = new ArrayList<>();

    Map<RecordMetadata, Object> optionalMetadata = new EnumMap<>(RecordMetadata.class);
    private Map<String, JdbcHelpers.ColumnMetadata> columnMetadata;

    public Record(String tableName, Object[] pks) {
        rowLink = new RowLink(tableName, pks);
    }

    private static final ObjectMapper mapper = getObjectMapper();

    public static ObjectMapper getObjectMapper() {
        if (mapper != null) {
            return mapper;
        }

        ObjectMapper privateMapper = new ObjectMapper();
        privateMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
        privateMapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
        privateMapper.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
        return privateMapper;
    }

    /** JsonNode representation, with metadata by default  */
    public JsonNode asJsonNode() {
       return asJsonNode(true);
    }

    /** JsonNode representation  */
    public JsonNode asJsonNode(boolean withMetadata) {
        ObjectNode dbRecord = mapper.createObjectNode();
        content.forEach(field -> field.addToJsonNode(dbRecord));
        if (withMetadata) {
            addMetadata(dbRecord);
        }
        return dbRecord;
    }

    private void addMetadata(ObjectNode dbRecord) {
        ObjectNode metadata = mapper.createObjectNode();
        metadata.put("version", getGitVersion());
        metadata.put("rootTable", getTableName());
        ArrayNode primaryKeys = metadata.putArray("primaryKeys");
        Arrays.stream(getRowLink().getPks()).forEach(e -> primaryKeys.add(e.toString()));
        dbRecord.set("_metadata", metadata);
    }

    String getGitVersion() {
        String version = findPathInJarFile();
        if (version != null) {
            return version;
        }
        try {
            InputStream inputStream = this.getClass().getResourceAsStream("/git.properties");
            Properties properties = new Properties();
            properties.load(inputStream);

            return properties.getProperty("git.build.version", "undef");
        } catch (Exception e){
            return "undef";
        }
    }

    private String findPathInJarFile() {
        try {
            Class theClass = DbExporter.class;
            String classPath = theClass.getResource(theClass.getSimpleName() + ".class").toString();
            String libPath = classPath.substring(10, classPath.lastIndexOf("!"));

            Properties properties = new Properties();
            JarFile jarFile = new JarFile(libPath);
            InputStream inputStream =
                    jarFile.getInputStream(jarFile.getEntry("META-INF/maven/org.oser.tools.jdbc/linked-db-rows/pom.properties"));

            properties.load(inputStream);

            String version = properties.getProperty("version");
            return version;
        } catch (Exception e){
            // ignore
        }
        return null;
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

    /** @return the element with the name if it is contained in this record (not considering sub-records), is case insensitive
     *   returns null if not found */
    public FieldAndValue findElementWithName(String columnName) {
        for (FieldAndValue d : content) {
            if (d.name.equalsIgnoreCase(columnName)) {
                return d;
            }
        }
        return null;
    }

    public String getTableName(){
        return getRowLink().getTableName();
    }


    /** is 1-based */
    public Integer findElementPositionWithName(String columnName) {
        int position = 1;
        for (FieldAndValue d : content) {
            if (d.name.equalsIgnoreCase(columnName)) {
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

    public void setPkValue(Object[] values) {
        for (int i = 0; i < values.length; i++){
            values[i] = RowLink.normalizePk(values[i]);
        }
        rowLink.setPks(values);
    }

    public String metadata() {
        if (optionalMetadata.isEmpty()) {
            return "-";
        } else {
            DbExporter.ExportContext o = (DbExporter.ExportContext) optionalMetadata.get(RecordMetadata.EXPORT_CONTEXT);

            return ((o == null) ? "" : o.visitedNodes.keySet() + " #nodes:" + o.visitedNodes.size());
        }
    }



    /** @return all nodes=RowLinks that are contained in the record */
    public Set<RowLink> getAllNodes(){
        Set<RowLink> result = new HashSet<>();
        result.add(rowLink);

        // not fully implemented via streams (as nested streams use more stack levels)
        List<Record> subRecords = content.stream().filter(e -> !e.subRow.isEmpty()).flatMap(e -> e.subRow.values().stream()).flatMap(Collection::stream).collect(toList());
        for (Record dbRecord : subRecords) {
            result.addAll(dbRecord.getAllNodes());
        }
        return result;
    }

    /** @return all records contained */
    public Set<Record> getAllRecords(){
        Set<Record> result = new HashSet<>();
        result.add(this);

        // not fully implemented via streams (as nested streams use more stack levels)
        List<Record> subRecords = content.stream().filter(e -> !e.subRow.isEmpty()).flatMap(e -> e.subRow.values().stream()).flatMap(Collection::stream).collect(toList());
        for (Record dbRecord : subRecords) {
            result.addAll(dbRecord.getAllRecords());
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

    /** visit all Records in insertion order */
    public void visitRecordsInInsertionOrder(Connection connection, CheckedFunction<Record, Void> visitor, boolean exceptionWithCycles) throws SQLException {
        visitRecordsInInsertionOrder(connection, visitor, exceptionWithCycles, Caffeine.newBuilder().maximumSize(10_000).build());
    }

    /** visit all Records in insertion order */
    public void visitRecordsInInsertionOrder(Connection connection, CheckedFunction<Record, Void> visitor, boolean exceptionWithCycles, Cache<String, List<Fk>> cache) throws SQLException {
        JdbcHelpers.Pair<List<String>, Set<String>> insertionOrder = JdbcHelpers.determineOrderWithDetails(connection, rowLink.getTableName(), exceptionWithCycles, cache);

        Map<String, List<Record>> tableToRecords = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        visitRecords(r -> {
            if (!tableToRecords.containsKey(r.rowLink.getTableName())) {
                tableToRecords.put(r.rowLink.getTableName(), new ArrayList<>());
            }
            tableToRecords.get(r.rowLink.getTableName()).add(r);
        });

        for (String tableName : insertionOrder.getLeft()) {
            List<Record> records = tableToRecords.get(tableName);
            if (records != null) {
                List<Fk> fksOfTable = Fk.getFksOfTable(connection, tableName, cache);
                if (Fk.hasSelfLink(fksOfTable)) {
                    records = orderRecordsForInsertion(connection, records, cache);
                }
                for (Record dbRecord : records) {
                    visitor.apply(dbRecord);
                }
            }
        }

        // treat entries that exist in cycles (they are not in the insertionOrder list)
        HashSet<String> treatedTables = new HashSet<>(insertionOrder.getLeft());
        List<Record> untreated = this.getAllRecords().stream().filter(r -> !treatedTables.contains(r.getTableName())).collect(toList());

        untreated = orderRecordsForInsertion(connection, untreated, cache);
        for (Record dbRecord:untreated) {
            visitor.apply(dbRecord);
        }
    }


    /** order record instances given their concrete FK constraints */
    //@VisibleForTesting
    static List<Record> orderRecordsForInsertion(Connection connection, List<Record> records, Cache<String, List<Fk>> cache) throws SQLException {
        Map<Record, Set<Record>> dependencies = determineRowDependencies(connection, records, cache);

        Set<Record> allRecords = new HashSet<>(records);
        return JdbcHelpers.topologicalSort(dependencies, allRecords, true);
    }

    /** what FK dependencies are there between a list of records ? */
    public static Map<Record, Set<Record>> determineRowDependencies(Connection connection, List<Record> records, Cache<String, List<Fk>> cache) throws SQLException {
        Map<String, List<Record>> tableToRecord = records.stream().collect(Collectors.groupingBy(r -> r.getRowLink().getTableName(), mapping(r -> r, toList())));
        Map<Record, Set<Record>> dependencies = new HashMap<>();
        for (Record left : records) {
            List<Fk> fks = Fk.getFksOfTable(connection, left.getRowLink().getTableName(), cache);
            for (Fk fk : fks) {
                if (!fk.isInverted()){
                    continue;
                }

                String rightTableName = (fk.getPktable().toLowerCase().equals(left.getTableName()) ? fk.getFktable() : fk.getPktable()).toLowerCase();
                List<Record> potentialTargetRecords = tableToRecord.get(rightTableName);

                if (potentialTargetRecords != null) {
                    for (Record potentialMatch : potentialTargetRecords) {
                        if (match(fk, left, potentialMatch)) {
                            dependencies.computeIfAbsent(potentialMatch, r -> new HashSet<>());
                            dependencies.get(potentialMatch).add(left);
                        }
                    }
                }
            }
        }
        return dependencies;
    }

    /** if left has a concrete fk to potentialMatch */
    private static boolean match(Fk fk, Record left, Record right) {
        boolean direct = fk.getPktable().toLowerCase().equals(left.getTableName());
        String[] leftFieldNames = direct ? fk.getPkcolumn() : fk.getFkcolumn();
        List<FieldAndValue> leftValues = Arrays.stream(leftFieldNames).map(fieldName -> left.findElementWithName(fieldName)).collect(toList());

        String[] rightFieldNames = direct ?  fk.getFkcolumn() : fk.getPkcolumn();
        List<FieldAndValue> rightValues = Arrays.stream(rightFieldNames).map(fieldName -> right.findElementWithName(fieldName)).collect(toList());

        for (int i = 0; i < leftValues.size(); i++) {
            if (!Objects.equals(leftValues.get(i).getValue(), rightValues.get(i).getValue())){
                return false;
            }
        }
        return true;
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
    @Getter
    @Setter
    public static class FieldAndValue {
        private String name;
        private Object value;
        private JdbcHelpers.ColumnMetadata metadata;
        private Map<String, List<Record>> subRow = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        public FieldAndValue(String name, JdbcHelpers.ColumnMetadata metadata, Object value) {
            this.name = name.toLowerCase();
            this.metadata = metadata;

            this.value = convertTypeForValue(metadata, value);
        }

        private final ObjectMapper mapper = getObjectMapper();

        Object convertTypeForValue(JdbcHelpers.ColumnMetadata metadata, Object value) {
            if ("null".equals(value)) {
                return null;
            }
            switch (metadata.type.toUpperCase()) {
                case "BOOLEAN":
                case "BIT": // mysql uses this for booleans
                case "BOOL":
                    Boolean bool = null;
                    if (value instanceof String) {
                        try {
                            bool = Boolean.parseBoolean((String) value); // todo make a bit more intelligent (also accept T/F...)
                        } catch (NumberFormatException e) {
                            // ok
                        }
                    } else if (value instanceof Boolean) {
                        bool = (Boolean) value;
                    }
                    return bool != null ? bool : value;
                case "SERIAL":
                case "INT":
                case "INT2":
                case "INT4":
                case "INTEGER":
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
                case "NUMBER":
                case "DECIMAL":
                    BigDecimal d = null;
                    if (value instanceof String) {
                        try {
                            d = new BigDecimal((String) value);
                        } catch (NumberFormatException e) {
                            // ok
                        }
                    }
                    return d != null ? d : value;
                case "TIMESTAMP":
                    Timestamp ts = null;
                    if (value instanceof String) {
                        try {
                            ts = Timestamp.valueOf(((String) value).replace("T", " "));
                        } catch (IllegalArgumentException e) {
                            // ok
                        }
                    }
                    return ts != null ? ts : value;
                case "DATE":
                    Date date = null;
                    if (value instanceof String) {
                        try {
                            date = Date.valueOf((String) value);
                        } catch (IllegalArgumentException e) {
                            // ok
                        }
                    }
                    return date != null ? date : value;
                case "BLOB":
                case "BYTEA":
                    byte[] decodedBytes;
                    if (value instanceof String) { // todo unsure this is ok in all cases (why is this sometimes a string?)
                        try {
                            decodedBytes = Base64.getDecoder().decode((String) value);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.println(metadata);
                            throw e;
                        }
                    } else {
                        decodedBytes = (byte[]) value;
                    }

                    return decodedBytes;
                case "VARCHAR":
                case "TEXT":
                default:
                    return value;
            }
        }

        public void addToJsonNode(ObjectNode topLevelNode) {
            putFieldToJsonNode(topLevelNode);
            if  ((!(subRow.isEmpty() || subRow.values().stream().map(List::size).max(Integer::compareTo).orElse(0) == 0))) {
                addSubRowToJsonNode(topLevelNode);
            }
        }

        private static String getSubtableKeyName(String name, String key) {
            return name + JSON_SUBTABLE_SUFFIX + key + JSON_SUBTABLE_SUFFIX;
        }


        @Override
        public String toString() {
            return "\"" + name +
                    "\":" + getValueWithQuoting() +
                    ((!(subRow.isEmpty() || subRow.values().stream().map(List::size).max(Integer::compareTo).orElse(0) == 0)) ?
                            (", " + maplistlist2jsonString(name, subRow)) :
                            "") +
                    "";
        }

        String getValueWithQuoting() {
            switch (metadata.type.toUpperCase()) {
                case "BOOLEAN":
                case "BOOL":
                case "INT":
                case "INT2":
                case "INT4":
                case "INTEGER":
                case "INT8":
                case "FLOAT8":
                case "FLOAT4":
                case "NUMBER":
                case "NUMERIC":
                case "SERIAL":
                case "DECIMAL":
                    return value != null ? value.toString() : null;
                case "TIMESTAMP":
                    return value != null ? ("\"" + DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(((Timestamp) value).toLocalDateTime()) + "\"") : null;
                case "DATE":
                    if (value instanceof Timestamp) {
                        // oracle seem to return timestamp for "DATE"
                        return  ("\"" + DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(((Timestamp) value).toLocalDateTime()) + "\"");
                    } else if (value instanceof String) {
                        return "\"" +  value + "\"";
                    } else {
                            return value != null ? ("\"" + ((Date) value).toLocalDate() + "\"") : null;
                    }
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

        void putFieldToJsonNode(ObjectNode node)  {
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
            } else if (value instanceof byte[]) {
                node.put(name, (byte[]) value);
            } else if (value instanceof Blob) {
                try {
                    node.put(name, ((Blob)value).getBytes(1, (int) ((Blob) value).length()));
                } catch (SQLException throwables) {
                    throw new IllegalStateException("could not convert blob to byte[]", throwables);
                }
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
            for (Record dbRecord : lists) {
                records.add(row2json(dbRecord));
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

    @Override
    public String toString() {
        return this.asJsonNode().toString();
    }
}
