package org.oser.tools.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private Map<String, PkGenerator> overriddenPkGenerators = new HashMap<>();
    private PkGenerator defaultPkGenerator = new NextValuePkGenerator();
    private Map<String, FieldMapper> fieldMappers = new HashMap<>();

    private Cache<String, List<Fk>> fkCache = Caffeine.newBuilder()
            .maximumSize(10_000).build();

    private Cache<String, List<String>> pkCache = Caffeine.newBuilder()
            .maximumSize(1000).build();

    private Cache<String, SortedMap<String, JdbcHelpers.ColumnMetadata>> metadataCache = Caffeine.newBuilder()
            .maximumSize(1000).build();


    public DbImporter() {
    }



    private static List<Map.Entry<String, JsonNode>> getCompositeJsonElements(JsonNode json) {
        Iterable<Map.Entry<String, JsonNode>> iterable = () -> json.fields();
        return StreamSupport
                .stream(iterable.spliterator(), false).filter(e -> !e.getValue().isValueNode())
                .collect(Collectors.toList());
    }




    /** Does the row of the table tableName and primary key pkName and the record record exist? */
    // todo: remove dependency on record, mv to JdbHelpers
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
        } else if (testForEscaping == "null") {
            valueToInsert = null;
        }
        return valueToInsert;
    }

    private static List<String> getJsonFieldNames(JsonNode json) {
        Iterable<Map.Entry<String, JsonNode>> iterable = () -> json.fields();
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

    /**
     * recursively add insertion statements starting from the rootTable and the record
     *  Old variant
     */
    private void addInsertionStatements(Connection connection, String tableName, Record record, Map<String, FieldMapper> mappers, Map<String, String> insertionStatements) throws SQLException {
        String pkName = record.pkName;

        boolean isInsert = doesPkTableExist(connection, tableName, pkName, record);


        if (forceInsert) {
            isInsert = true;
        }


        List<String> jsonFieldNames = record.getFieldNames();
        Iterable<Map.Entry<String, JsonNode>> iterable;

        // fields must both be in json AND in db metadata, remove those missing in db metadata
        Set<String> columnsDbNames = record.content.stream().map(e -> e.name).collect(Collectors.toSet());
        jsonFieldNames.removeIf(e -> !columnsDbNames.contains(e));
        // todo log if there is a delta between the 2 sets

        String sqlStatement = JdbcHelpers.getSqlInsertOrUpdateStatement(tableName, jsonFieldNames, pkName, isInsert, record.getColumnMetadata());
        try (PreparedStatement statement = connection.prepareStatement(sqlStatement)) {

            String pkValue = null;
            String pkType = "";

            int statementIndex = 1; // statement param
            JdbcHelpers.ColumnMetadata pkMetadata = null;
            for (String currentFieldName : jsonFieldNames) {
                Record.FieldAndValue currentElement = record.findElementWithName(currentFieldName);
                String valueToInsert = Objects.toString(currentElement.value);

                valueToInsert = prepareVarcharToInsert(currentElement.metadata.type, currentFieldName, valueToInsert);

                boolean fieldIsPk = currentFieldName.equals(pkName.toUpperCase());
                if (isInsert || !fieldIsPk) {
                    valueToInsert = removeQuotes(valueToInsert);

                    if (mappers.containsKey(currentFieldName)) {
                        mappers.get(currentFieldName).mapField(currentElement.metadata, statement, statementIndex, valueToInsert);
                    } else {
                        JdbcHelpers.innerSetStatementField(statement, currentElement.metadata.type, statementIndex, valueToInsert, currentElement.metadata);
                    }

                    statementIndex++;
                } else {
                    pkValue = valueToInsert;
                    pkType = currentElement.metadata.type;
                    pkMetadata = currentElement.metadata;
                }
            }
            if (isInsert) {
                // do not set version
                //updateStatement.setLong(statementIndex, 0);
            } else {
                if (pkValue != null) {
                    pkValue = pkValue.trim();
                }

                JdbcHelpers.innerSetStatementField(statement, pkType, statementIndex, pkValue, pkMetadata);
            }

            if (insertionStatements.containsKey(tableName)) {
                insertionStatements.put(tableName, insertionStatements.get(tableName) + ";" + statement.toString());
            } else {
                insertionStatements.put(tableName, statement.toString());
            }


            // do recursion (treat subtables)
            for (String subrowName : record.getFieldNamesWithSubrows()) {

                Record.FieldAndValue data = record.findElementWithName(subrowName);
                for (String linkedTable : data.subRow.keySet()) {

                    for (Record subrecord : data.subRow.get(linkedTable)) {
                        this.addInsertionStatements(connection, linkedTable, subrecord, mappers, insertionStatements);
                    }
                }
            }
        }
    }

    /** older variant */
    public String recordAsInserts(Connection connection, Record record) throws SQLException {
        List<String> tableInsertOrder = JdbcHelpers.determineOrder(connection, record.getRowLink().tableName);

        // tableName -> insertionStatement
        Map<String, String> insertionStatements = new HashMap<>();

        addInsertionStatements(connection, record.getRowLink().tableName, record, fieldMappers, insertionStatements);

        String result = "";
        for (String table : tableInsertOrder) {
            if (insertionStatements.containsKey(table)) {
                String inserts = insertionStatements.get(table) + ";";
                result += inserts + "\n";
            }
        }
        return result;
    }

    public Record jsonToRecord(Connection connection, String rootTable, String jsonString) throws IOException, SQLException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(jsonString);

        return innerJsonToRecord(connection, rootTable, json);
    }


    Record innerJsonToRecord(Connection connection, String rootTable, JsonNode json) throws IOException, SQLException {
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

        for (String currentFieldName : jsonFieldNames) {
            String valueToInsert = json.get(currentFieldName.toLowerCase()).asText();

            valueToInsert = prepareVarcharToInsert(columns, currentFieldName, valueToInsert);

            if (currentFieldName.equals(pkName.toUpperCase())) {
                record.setPkValue(valueToInsert);
            }

            Record.FieldAndValue d = new Record.FieldAndValue(currentFieldName, columns.get(currentFieldName), valueToInsert);
            record.content.add(d);
        }


        // convert subrecords

        if (getCompositeJsonElements(json).isEmpty()) {
            return record;
        }

        for (Fk fk : getFksOfTable(connection, rootTable, fkCache)) {

            Record.FieldAndValue elementWithName = record.findElementWithName((fk.inverted ? fk.fkcolumn : fk.pkcolumn).toUpperCase());
            if (elementWithName != null) {
                String subTableName = fk.inverted ? fk.pktable : fk.fktable;
                JsonNode subJsonNode = json.get(subTableName + JSON_SUBTABLE_SUFFIX);
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

                elementWithName.subRow.put(subTableName, records);
            }
        }

        return record;
    }


    /**
     * Insert a record into a database - doing the remapping to other primary keys where needed.
     * Assumes someone external handles the transaction or autocommit
     * @return the remapped keys (RowLink -> new primary key)
     *
     * CAVEAT: cannot handle cycles in the graph! Will insert just partial data (the first tables without cycles).
     */
    public Map<RowLink, Object> insertRecords(Connection connection, Record record) throws SQLException {
        Map<RowLink, Object> newKeys = new HashMap<>();

        record.visitRecordsInInsertionOrder(connection, r -> this.insertOneRecord(connection, r, newKeys, fieldMappers));

        // todo treat errors

        return newKeys;
    }

    private void insertOneRecord(Connection connection, Record record, Map<RowLink, Object> newKeys, Map<String, FieldMapper> mappers) {
        try {
            boolean isInsert = forceInsert || doesPkTableExist(connection, record.getRowLink().tableName, record.pkName, record);

            Object candidatePk = null;
            if (isInsert) {
                candidatePk = getCandidatePk(connection, record.getRowLink().tableName, record.findElementWithName(record.pkName).metadata.type, record.pkName);
                RowLink key = new RowLink(record.getRowLink().tableName, record.findElementWithName(record.pkName).value);
                newKeys.put(key, candidatePk);
            }

            List<String> jsonFieldNames = record.getFieldNames();

            // fields must both be in json AND in db metadata, remove those missing in db metadata
            Set<String> columnsDbNames = record.content.stream().map(e -> e.name).collect(Collectors.toSet());
            jsonFieldNames.removeIf(e -> !columnsDbNames.contains(e));
            // todo log if there is a delta between the 2 sets, ok for those who map subrows !

            Map<String, JdbcHelpers.ColumnMetadata> columnMetadata = record.getColumnMetadata();
            String sqlStatement = JdbcHelpers.getSqlInsertOrUpdateStatement(record.rowLink.tableName, jsonFieldNames, record.pkName, isInsert, columnMetadata);
            PreparedStatement savedStatement = null;
            try (PreparedStatement statement = connection.prepareStatement(sqlStatement)) {

                String pkValue = null;
                String pkType = "";

                Map<String, List<Fk>> fksByColumnName = record.optionalFks.stream().collect(Collectors.groupingBy(fk1 -> fk1.getFkcolumn().toUpperCase()));

                final String[] valueToInsert = {"-"};

                int statementIndex = 1; // statement param
                int pkStatementIndex = 0;
                JdbcHelpers.ColumnMetadata fieldMetadata = null;
                for (String currentFieldName : jsonFieldNames) {
                    Record.FieldAndValue currentElement = record.findElementWithName(currentFieldName);
                    valueToInsert[0] = prepareVarcharToInsert(currentElement.metadata.type, currentFieldName, Objects.toString(currentElement.value));

                    boolean fieldIsPk = currentFieldName.equals(record.pkName.toUpperCase());

                    // remap fks!
                    if (isInsert && fksByColumnName.containsKey(currentFieldName)) {
                        List<Fk> fks = fksByColumnName.get(currentFieldName);

                        String earlierIntendedFk = valueToInsert[0];
                        fks.stream().forEach(fk -> {
                            valueToInsert[0] = Objects.toString(newKeys.get(new RowLink(fk.pktable, earlierIntendedFk)));
                        });
                    }


                    if (isInsert || !fieldIsPk) {
                        valueToInsert[0] = removeQuotes(valueToInsert[0]);

                        if (mappers.containsKey(currentFieldName)) {
                            mappers.get(currentFieldName).mapField(currentElement.metadata, statement, columnMetadata.get(currentElement.metadata.name.toUpperCase()).getOrdinalPos(), valueToInsert[0]);
                        } else {
                            JdbcHelpers.innerSetStatementField(statement, currentElement.metadata.type, columnMetadata.get(currentElement.metadata.name.toUpperCase()).getOrdinalPos(), valueToInsert[0], currentElement.metadata);
                        }
                    }

                    if (fieldIsPk) {
                        pkValue = isInsert ? Objects.toString(candidatePk) : valueToInsert[0];
                        pkType = currentElement.metadata.type;
                        pkStatementIndex = statementIndex;
                        fieldMetadata = currentElement.metadata;
                    }
                    statementIndex++;
                }

                if (pkValue != null) {
                    pkValue = pkValue.trim();
                }

                pkValue = isInsert ? Objects.toString(candidatePk) : valueToInsert[0];
                JdbcHelpers.innerSetStatementField(statement, pkType, pkStatementIndex, pkValue, fieldMetadata);


                savedStatement = statement;
                int result = statement.executeUpdate();

                LOGGER.info("statement called: {} updateCount:{}", statement.toString(), result);


            } catch (SQLException throwables) {
                LOGGER.info("optional statement: {} ", Objects.toString(savedStatement));
                throwables.printStackTrace(); // todo do this differently later
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
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
