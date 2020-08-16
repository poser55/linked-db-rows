package org.oser.tools.jdbc;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.oser.tools.jdbc.Fk.getFksOfTable;


/**
 * Import a JSON structure exported with {@link DbExporter} into the db again.
 * <p>
 * License: Apache 2.0
 */
public class DbImporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbImporter.class);
    public static final String JSON_SUBTABLE_SUFFIX = "*";

    // options
    private boolean forceInsert = true;
    private final Map<String, PkGenerator> overriddenPkGenerators = new HashMap<>();
    private final PkGenerator defaultPkGenerator = new NextValuePkGenerator();
    private final Map<String, FieldMapper> fieldMappers = new HashMap<>();

    private final Cache<String, List<Fk>> fkCache = Caffeine.newBuilder()
            .maximumSize(10_000).build();

    private final Cache<String, List<String>> pkCache = Caffeine.newBuilder()
            .maximumSize(1000).build();

    private final Cache<String, SortedMap<String, JdbcHelpers.ColumnMetadata>> metadataCache = Caffeine.newBuilder()
            .maximumSize(1000).build();


    public DbImporter() {
        // nothing to do
    }



    private static List<Map.Entry<String, JsonNode>> getCompositeJsonElements(JsonNode json) {
        Iterable<Map.Entry<String, JsonNode>> iterable = json::fields;
        return StreamSupport
                .stream(iterable.spliterator(), false).filter(e -> !e.getValue().isValueNode())
                .collect(Collectors.toList());
    }




    /** Does the row of the table tableName and primary key pkName and the record record exist? */
    // todo: remove dependency on record, mv to JdbHelpers
    @Deprecated
    public static boolean doesPkTableExist(Connection connection, String tableName, String pkName, Record record) throws SQLException {
        String selectPk = "SELECT " + pkName + " from " + tableName + " where  " + pkName + " = ?";

        boolean isInsert;
        try (PreparedStatement pkSelectionStatement = connection.prepareStatement(selectPk)) { // NOSONAR: now unchecked values all via prepared statement
            Record.FieldAndValue elementWithName = record.findElementWithName(pkName);
            JdbcHelpers.innerSetStatementField(pkSelectionStatement, elementWithName.metadata.type, 1, elementWithName.value.toString(), null);

            try (ResultSet rs = pkSelectionStatement.executeQuery()) {
                isInsert = !rs.next();
            }
        }
        return isInsert;
    }


    private static String prepareVarcharToInsert(String typeAsString, String currentFieldName, String valueToInsert) {
        if (typeAsString.toUpperCase().equals("VARCHAR")) {
            valueToInsert = getInnerValueToInsert(valueToInsert);
        }
        return valueToInsert;
    }


    private static String prepareVarcharToInsert(Map<String, JdbcHelpers.ColumnMetadata> columns, String currentFieldName, String valueToInsert) {
        if (columns.get(currentFieldName).getType().toUpperCase().equals("VARCHAR")) {
            valueToInsert = getInnerValueToInsert(valueToInsert);
        }
        return valueToInsert;
    }

    private static String getInnerValueToInsert(String valueToInsert) {
        String testForEscaping = valueToInsert.trim();
        if (testForEscaping.length() > 1 && ((testForEscaping.startsWith("\"") && testForEscaping.endsWith("\"")) ||
                ((testForEscaping.startsWith("'") && testForEscaping.endsWith("'"))))) {
            valueToInsert = testForEscaping.substring(1, testForEscaping.length() - 1);
        } else if (testForEscaping.equals("null")) {
            valueToInsert = null;
        }
        return valueToInsert;
    }

    private static List<String> getJsonFieldNames(JsonNode json) {
        Iterable<Map.Entry<String, JsonNode>> iterable = json::fields;
        return StreamSupport
                .stream(iterable.spliterator(), false).filter(e -> e.getValue().isValueNode()).map(Map.Entry::getKey)
                .map(String::toUpperCase).collect(Collectors.toList());
    }

    private static String removeQuotes(String valueToInsert) {
        if ((valueToInsert != null) && valueToInsert.startsWith("\"")) {
            valueToInsert = valueToInsert.substring(1, valueToInsert.length() - 1);
        }
        return valueToInsert;
    }

    ////// code partially from csvToDb

    public Record jsonToRecord(Connection connection, String rootTable, String jsonString) throws IOException, SQLException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
        mapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
        mapper.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

        JsonNode json = mapper.readTree(jsonString);

        return innerJsonToRecord(connection, rootTable, json);
    }


    Record innerJsonToRecord(Connection connection, String rootTable, JsonNode json) throws SQLException {
        Record record = new Record(rootTable, null);

        DatabaseMetaData metadata = connection.getMetaData();
        Map<String, JdbcHelpers.ColumnMetadata> columns = JdbcHelpers.getColumnMetadata(metadata, rootTable, metadataCache);
        List<String> pks = JdbcHelpers.getPrimaryKeys(metadata, rootTable, pkCache);

        final String pkName = pks.get(0);
        record.pkName = pkName;
        record.setColumnMetadata(columns);

        List<String> jsonFieldNames = getJsonFieldNames(json);
        // fields must both be in json AND in db metadata, remove those missing in db metadata
        Set<String> columnsDbNames = columns.keySet();
        jsonFieldNames.removeIf(e -> !columnsDbNames.contains(e));
        // todo log if there is a delta between the 2 sets

        Map<String, Integer> primaryKeyArrayPosition = JdbcHelpers.getStringIntegerMap(pks);
        Object[] primaryKeyValues = new Object[pks.size()];

        for (String currentFieldName : jsonFieldNames) {
            String valueToInsert = json.get(currentFieldName.toLowerCase()).asText();

            valueToInsert = prepareVarcharToInsert(columns, currentFieldName, valueToInsert);

            if (primaryKeyArrayPosition.containsKey(currentFieldName.toUpperCase())){
                primaryKeyValues[primaryKeyArrayPosition.get(currentFieldName.toUpperCase())] = valueToInsert;
            }

            Record.FieldAndValue d = new Record.FieldAndValue(currentFieldName, columns.get(currentFieldName), valueToInsert);
            record.content.add(d);
        }
        record.setPkValue(primaryKeyValues);

        // convert subrecords

        if (getCompositeJsonElements(json).isEmpty()) {
            return record;
        }

        for (Fk fk : getFksOfTable(connection, rootTable, fkCache)) {

            Record.FieldAndValue elementWithName = record.findElementWithName((fk.inverted ? fk.fkcolumn : fk.pkcolumn).toUpperCase());
            if (elementWithName != null) {
                String subTableName = fk.inverted ? fk.pktable : fk.fktable;
                JsonNode subJsonNode = json.get(elementWithName.name.toLowerCase() + JSON_SUBTABLE_SUFFIX  + subTableName + JSON_SUBTABLE_SUFFIX);
                ArrayList<Record> records = new ArrayList<>();

                if (fk.inverted) {
                    record.optionalFks.add(fk);
                }

                if (subJsonNode != null) {
                    if (subJsonNode.isArray()) {
                        Iterator<JsonNode> elements = subJsonNode.elements();

                        while (elements.hasNext()) {
                            Record subrecord = this.innerJsonToRecord(connection, subTableName, elements.next());
                            records.add(subrecord);
                        }
                    } else if (subJsonNode.isObject()) {
                        records.add(this.innerJsonToRecord(connection, subTableName, subJsonNode));
                    }
                }

                if (!fk.inverted) {
                    records.forEach(r -> r.optionalFks.add(fk));
                }

                if (!records.isEmpty()) {
                    elementWithName.subRow.put(subTableName, records);
                }
            }
        }

        return record;
    }


    /**
     * Insert a record into a database - doing the remapping to other primary keys where needed.
     * Assumes someone external handles the transaction or autocommit
     * @return the remapped keys (RowLink -> new primary key)
     *
     * CAVEAT: it cannot handle cycles in the db tables!
     * Will insert just partial data (the first tables without cycles).
     */
    public Map<RowLink, Object> insertRecords(Connection connection, Record record) throws Exception {
        Map<RowLink, Object> newKeys = new HashMap<>();

        return insertRecords(connection, record, newKeys);
    }

    /** Alternative to {@link #insertRecords(Connection, Record)} that allows to remap certain elements (e.g. if you want to connect
     *    some nodes of the JSON tree to an existing RowLink)
     *     E.g. insert a blog entry to a new person. Refer to the test org.oser.tools.jdbc.DbExporterBasicTests#blog()
     *     CAVEAT: newKeys must be a mutable map (it will add all the other remappings also) */
    public Map<RowLink, Object> insertRecords(Connection connection, Record record, Map<RowLink, Object> newKeys) throws Exception {
        Set<RowLink> rowLinksNotToInsert = newKeys.keySet();

        CheckedFunction<Record, Void> insertOneRecord = (CheckedFunction<Record, Void>) (Record r) -> {
            if (!rowLinksNotToInsert.contains(r.rowLink)) {
                this.insertOneRecord(connection, r, newKeys, fieldMappers);
            }
            return null; // strange that we need this hack
        };
        record.visitRecordsInInsertionOrder(connection, insertOneRecord);

        return newKeys;
    }

    private void insertOneRecord(Connection connection, Record record, Map<RowLink, Object> newKeys, Map<String, FieldMapper> mappers) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        List<String> primaryKeys = JdbcHelpers.getPrimaryKeys(metadata, record.getRowLink().getTableName(), pkCache);

        // todo : bug sometimes the optionalFk is not correct on record (e.g. on node)
        //  so for now, we get it from the cache:
        List<Fk> fksOfTable = getFksOfTable(connection, record.rowLink.tableName, fkCache);

        Map<String, List<Fk>> fksByColumnName = fksOfTable.stream().collect(Collectors.groupingBy(fk1 -> (fk1.inverted ? fk1.getFkcolumn() : fk1.getPkcolumn()).toUpperCase()));
        List<Boolean> isFreePk = new ArrayList<>(primaryKeys.size());

        List<Object> pkValues = remapPrimaryKeyValues(record, newKeys, primaryKeys, fksByColumnName, isFreePk);

        boolean entryExists = JdbcHelpers.doesPkTableExist(connection, record.getRowLink().getTableName(), primaryKeys, pkValues, record.getColumnMetadata());
        boolean isInsert = forceInsert || entryExists;

        Object candidatePk = null;
        String primaryKeyFieldWithNewValue = null;
        if (isInsert && entryExists) {
            // iterate over all entries of the primary key, generate a candidate for first that is possible

            for (int i = 0; i < primaryKeys.size(); i++) {
                if (isFreePk.get(i)) {
                    Record.FieldAndValue elementWithName = record.findElementWithName(primaryKeys.get(i));

                    candidatePk = getCandidatePk(connection, record.getRowLink().tableName, elementWithName.metadata.type, primaryKeys.get(i));
                    RowLink key = new RowLink(record.getRowLink().tableName, record.findElementWithName(record.pkName).value);
                    newKeys.put(key, candidatePk);

                    pkValues.set(i, candidatePk); // maybe not needed (catched by later remapping?)
                    primaryKeyFieldWithNewValue = elementWithName.name; // maybe not needed (catched by later remapping?)
                    break;
                }
            }
        }

        List<String> fieldNames = record.getFieldNames();

        // fields must both be in json AND in db metadata, remove those missing in db metadata
        Set<String> columnsDbNames = record.content.stream().map(e -> e.name).collect(Collectors.toSet());
        fieldNames.removeIf(e -> !columnsDbNames.contains(e));
        // todo log if there is a delta between the 2 sets, ok for those who map subrows !

        Map<String, JdbcHelpers.ColumnMetadata> columnMetadata = record.getColumnMetadata();
        String sqlStatement = JdbcHelpers.getSqlInsertOrUpdateStatement(record.rowLink.tableName, fieldNames, record.pkName, isInsert, columnMetadata);
        PreparedStatement savedStatement = null;
        try (PreparedStatement statement = connection.prepareStatement(sqlStatement)) {
            final String[] valueToInsert = {"-"};

            for (String currentFieldName : fieldNames) {
                Record.FieldAndValue currentElement = record.findElementWithName(currentFieldName);
                valueToInsert[0] = prepareVarcharToInsert(currentElement.metadata.type, currentFieldName, Objects.toString(currentElement.value));

                boolean fieldIsPk = primaryKeys.stream().map(String::toUpperCase).anyMatch(e -> currentFieldName.toUpperCase().equals(e));

                if (fieldIsPk) {
                    valueToInsert[0] = Objects.toString(pkValues.get(JdbcHelpers.getStringIntegerMap(primaryKeys).get(currentFieldName.toUpperCase())));
                } else if (isInsert && fksByColumnName.containsKey(currentFieldName)) {
                    // remap fks!
                    List<Fk> fks = fksByColumnName.get(currentFieldName);

                    String earlierIntendedFk = valueToInsert[0];
                    fks.forEach(fk -> {
                        Object potentialNewValue = newKeys.get(new RowLink(fk.pktable, earlierIntendedFk));
                        valueToInsert[0] = potentialNewValue != null ? Objects.toString(potentialNewValue) : valueToInsert[0];
                    });
                }

                int statementPosition = columnMetadata.get(currentElement.metadata.name.toUpperCase()).getOrdinalPos();
                valueToInsert[0] = removeQuotes(valueToInsert[0]);

                if (mappers.containsKey(currentFieldName)) {
                    mappers.get(currentFieldName).mapField(currentElement.metadata, statement, statementPosition, valueToInsert[0]);
                } else {
                    JdbcHelpers.innerSetStatementField(statement, currentElement.metadata.type, statementPosition, valueToInsert[0], currentElement.metadata);

                }
            }

            savedStatement = statement;
            int result = statement.executeUpdate();

            LOGGER.debug("statement called: {} updateCount:{}", statement, result);

        } catch (SQLException e) {
            LOGGER.info("issue with statement: {} ", savedStatement);
            throw e;
        }
    }

    /**
     * @return the primary key values that are remapped if needed (if e.g. another inserted row has a pk that was remapped before)
     *         CAVEAT: also updates the isFreePk List (to determine what pk values are "free")
     * */
    private List<Object> remapPrimaryKeyValues(Record record, Map<RowLink, Object> newKeys, List<String> primaryKeys, Map<String, List<Fk>> fksByColumnName, List<Boolean> isFreePk) {
        List<Object> pkValues = new ArrayList<>(primaryKeys.size());
        int i = 0;
        for (String primaryKey : primaryKeys) {
            Record.FieldAndValue elementWithName = record.findElementWithName(primaryKey.toUpperCase());

            // do the remapping from the newKeys
            Object[] potentialValueToInsert = {null};
            if (fksByColumnName.containsKey(primaryKey.toUpperCase())) {
                List<Fk> fks = fksByColumnName.get(primaryKey.toUpperCase());
                fks.forEach(fk -> {
                    potentialValueToInsert[0] = newKeys.get(new RowLink(fk.pktable, elementWithName.value));
                });
            }
            // if it is remapped, it is a fk from somewhere else -> so we cannot set it freely
            isFreePk.add(potentialValueToInsert[0] == null);

            pkValues.add(potentialValueToInsert[0] != null ? potentialValueToInsert[0]: elementWithName.value);
            i++;
        }
        return pkValues;
    }


    private Object getCandidatePk(Connection connection, String tableName, String type, String pkName) throws SQLException {
        PkGenerator generatorToUse = defaultPkGenerator;
        if (overriddenPkGenerators.containsKey(tableName)){
            generatorToUse = overriddenPkGenerators.get(tableName);
        }

        return generatorToUse.generatePk(connection, tableName, type, pkName);
    }

    public void setForceInsert(boolean forceInsert) {
        this.forceInsert = forceInsert;
    }

    public Map<String, FieldMapper> getFieldMappers() {
        return fieldMappers;
    }
}
