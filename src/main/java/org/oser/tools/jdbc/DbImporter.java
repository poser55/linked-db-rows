package org.oser.tools.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.oser.tools.jdbc.Fk.getFksOfTable;


// todo errors:
//  jsonToRecord: needs to convert the data types (not just all to string), e.g. for timestamp


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

    private @NonNull Cache<String, List<Fk>> fkCache = Caffeine.newBuilder()
            .maximumSize(10_000).build();


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
            innerSetStatementField(pkSelectionStatement, elementWithName.metadata.type, 1, elementWithName.value.toString());

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


    private static void setStatementField(PreparedStatement preparedStatement, String typeAsString, int statementIndex, String header, String valueToInsert) throws SQLException {
        innerSetStatementField(preparedStatement, typeAsString, statementIndex, valueToInsert);
    }

    /**
     * set a value on a jdbc Statement
     */
    private static void innerSetStatementField(PreparedStatement preparedStatement, String typeAsString, int statementIndex, String valueToInsert) throws SQLException {
        switch (typeAsString) {
            case "BOOLEAN":
            case "bool":
                preparedStatement.setBoolean(statementIndex, Boolean.parseBoolean(valueToInsert.trim()));
                break;
            case "int4":
            case "int8":
                if (valueToInsert.trim().isEmpty() || valueToInsert.equals("null")) {
                    preparedStatement.setNull(statementIndex, Types.NUMERIC);
                } else {
                    preparedStatement.setLong(statementIndex, Long.parseLong(valueToInsert.trim()));
                }
                break;
            case "numeric":
            case "DECIMAL":
                if (valueToInsert.trim().isEmpty() || valueToInsert.equals("null")) {
                    preparedStatement.setNull(statementIndex, Types.NUMERIC);
                } else {
                    preparedStatement.setDouble(statementIndex, Double.parseDouble(valueToInsert.trim()));
                }
                break;
            case "date":
            case "timestamp":
                if (valueToInsert.trim().isEmpty() || valueToInsert.equals("null")) {
                    preparedStatement.setNull(statementIndex, Types.TIMESTAMP);
                } else {
                    if (typeAsString.equals("timestamp")) {
                        preparedStatement.setTimestamp(statementIndex, Timestamp.valueOf(LocalDateTime.parse(valueToInsert.replace(" ", "T"))));
                    } else {
                        preparedStatement.setDate(statementIndex, Date.valueOf(LocalDate.parse(valueToInsert)));
                    }
                }
                break;
            default:
                preparedStatement.setObject(statementIndex, valueToInsert);
        }
    }

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

        String sqlStatement = JdbcHelpers.getSqlInsertOrUpdateStatement(tableName, jsonFieldNames, pkName, isInsert);
        try (PreparedStatement statement = connection.prepareStatement(sqlStatement)) {

            String pkValue = null;
            String pkType = "";

            int statementIndex = 1; // statement param
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
                        setStatementField(statement, currentElement.metadata.type, statementIndex, currentFieldName, valueToInsert);
                    }

                    statementIndex++;
                } else {
                    pkValue = valueToInsert;
                    pkType = currentElement.metadata.type;
                }
            }
            if (isInsert) {
                // do not set version
                //updateStatement.setLong(statementIndex, 0);
            } else {
                if (pkValue != null) {
                    pkValue = pkValue.trim();
                }

                setStatementField(statement, pkType, statementIndex, pkName, pkValue);
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
                        new DbImporter().addInsertionStatements(connection, linkedTable, subrecord, mappers, insertionStatements);
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

        addInsertionStatements(connection, record.getRowLink().tableName, record, new HashMap<>(), insertionStatements);

        String result = "";
        for (String table : tableInsertOrder) {
            if (insertionStatements.containsKey(table)) {
                String inserts = insertionStatements.get(table) + ";";
                result += inserts + "\n";
            }
        }
        return result;
    }

    Record innerJsonToRecord(Connection connection, String rootTable, JsonNode json) throws IOException, SQLException {
        Record record = new Record(rootTable, null);

        DatabaseMetaData metadata = connection.getMetaData();
        Map<String, JdbcHelpers.ColumnMetadata> columns = JdbcHelpers.getColumnMetadata(metadata, rootTable);
        List<String> pks = JdbcHelpers.getPrimaryKeys(metadata, rootTable);

        final String pkName = pks.get(0);
        record.pkName = pkName;

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
                            Record subrecord = new DbImporter().innerJsonToRecord(connection, subTableName, elements.next());
                            records.add(subrecord);
                        }
                    } else if (subJsonNode.isObject()) {
                        records.add(new DbImporter().innerJsonToRecord(connection, subTableName, subJsonNode));
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

    public Record jsonToRecord(Connection connection, String rootTable, String jsonString) throws IOException, SQLException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(jsonString);

        return innerJsonToRecord(connection, rootTable, json);
    }

    /**
     * Insert a record into a database - doing the remapping to other primary keys where needed.
     * Assumes someone external handles the transaction or autocommit
     * @return the remapped keys (RowLink -> new primary key)
     */
    public Map<RowLink, Object> insertRecords(Connection connection, Record record) throws SQLException {
        Map<RowLink, Object> newKeys = new HashMap<>();

        record.visitRecordsInInsertionOrder(connection, r -> new DbImporter().insertOneRecord(connection, r, newKeys, new HashMap<>()));

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

            String sqlStatement = JdbcHelpers.getSqlInsertOrUpdateStatement(record.rowLink.tableName, jsonFieldNames, record.pkName, isInsert);
            PreparedStatement savedStatement = null;
            try (PreparedStatement statement = connection.prepareStatement(sqlStatement)) {

                String pkValue = null;
                String pkType = "";

                Map<String, List<Fk>> fksByColumnName = record.optionalFks.stream().collect(Collectors.groupingBy(fk1 -> fk1.getFkcolumn().toUpperCase()));

                final String[] valueToInsert = {"-"};

                int statementIndex = 1; // statement param
                int pkStatementIndex = 0;
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
                            mappers.get(currentFieldName).mapField(currentElement.metadata, statement, statementIndex, valueToInsert[0]);
                        } else {
                            setStatementField(statement, currentElement.metadata.type, statementIndex, currentFieldName, valueToInsert[0]);
                        }
                    }

                    if (fieldIsPk) {
                        pkValue = isInsert ? Objects.toString(candidatePk) : valueToInsert[0];
                        pkType = currentElement.metadata.type;
                        pkStatementIndex = statementIndex;
                    }
                    statementIndex++;
                }

                if (pkValue != null) {
                    pkValue = pkValue.trim();
                }

                pkValue = isInsert ? Objects.toString(candidatePk) : valueToInsert[0];
                setStatementField(statement, pkType, pkStatementIndex, record.pkName, pkValue);


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
}
