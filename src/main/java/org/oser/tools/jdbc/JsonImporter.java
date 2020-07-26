package org.oser.tools.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.oser.tools.jdbc.Db2Graph.findElementWithName;
import static org.oser.tools.jdbc.Db2Graph.table2Fk;
import static org.oser.tools.jdbc.TreatmentOptions.ForceInsert;
import static org.oser.tools.jdbc.TreatmentOptions.RemapPrimaryKeys;


// todo errors:
//  jsonToRecord: needs to convert the data types (not just all to string), e.g. for timestamp


/** Import a JSON structure exported with {@link Db2Graph} into the db again.
 *
 *  License: Apache 2.0 */
public class JsonImporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonImporter.class);

    /*
      importing:
         *check order  [x]
         *check whether entries (table/PK) already exist  [x] (insert or update)
         *warn about schema deltas   []
         *allow to re-map to other  PKs  []

        JDBC batching: correct dependency order is still needed. :-(

     */

    private final Connection connection;
    private EnumSet<TreatmentOptions> options;

    public JsonImporter(Connection connection) {
        this.connection = connection;
        options = EnumSet.noneOf(TreatmentOptions.class);
    }



    public static String asInserts(Connection connection, Record record, EnumSet<TreatmentOptions> options) throws SQLException {
        List<String> tableInsertOrder = determineOrder(record.getPkTable().tableName, connection);

        // tableName -> insertionStatement
        Map<String, String> insertionStatements = new HashMap<>();

        addInsertionStatements(record.getPkTable().tableName, record, /* todo */new HashMap<>(), insertionStatements, connection, options);

        String result = "";
        for (String table : tableInsertOrder) {
            if (insertionStatements.containsKey(table)) {
                // apply inserts
                boolean remapPrimaryKeys = TreatmentOptions.getValue(RemapPrimaryKeys, options);

                String inserts = insertionStatements.get(table) + ";";
                result += inserts + "\n";
            }
        }
        return result;
    }


    public static Record jsonToRecord(Connection connection, String rootTable, String jsonString) throws IOException, SQLException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(jsonString);

        return innerJsonToRecord(connection, rootTable, json);
    }


    public static Record innerJsonToRecord(Connection connection, String rootTable, JsonNode json) throws IOException, SQLException {
        Record record = new Record(rootTable, null);

        DatabaseMetaData metadata = connection.getMetaData();
        Map<String, Db2Graph.ColumnMetadata> columns = Db2Graph.getColumnNamesAndTypes(metadata, rootTable);
        List<String> pks = Db2Graph.getPrimaryKeys(metadata, rootTable);

        final String pkName = pks.get(0);
        record.pkName = pkName;

        List<String> jsonFieldNames = getJsonFieldNames(json);
        // fields must both be in json AND in db metadata, remove those missing in db metadata
        Set<String> columnsDbNames = columns.keySet();
        jsonFieldNames.removeIf(e -> !columnsDbNames.contains(e));
        // todo log if there is a delta between the 2 sets

        for (String currentFieldName : jsonFieldNames) {
            String valueToInsert = json.get(currentFieldName.toLowerCase()).toString();

            valueToInsert = prepareVarcharToInsert(columns, currentFieldName, valueToInsert);

            if (currentFieldName.equals(pkName.toUpperCase())){
                record.setPkValue(valueToInsert);
            }

            Record.Data d = new Record.Data();
            d.name = currentFieldName;
            d.metadata = columns.get(currentFieldName);
            d.value = valueToInsert;
            record.content.add(d);
        }


        // convert subrecords

        if (getCompositeJsonElements(json).isEmpty()){
            return record;
        }

        for (Db2Graph.Fk fk : table2Fk(connection, rootTable)) {

            Record.Data elementWithName = findElementWithName(record, (fk.inverted ? fk.targetColumn : fk.columnName).toUpperCase());
            if (elementWithName != null) {
                String subTableName = fk.inverted ? fk.originTable : fk.targetTable;
                JsonNode subJsonNode = json.get(subTableName);
                ArrayList<Record> records = new ArrayList<>();

                if (subJsonNode != null){
                    if (subJsonNode.isArray()) {
                        Iterator<JsonNode> elements = subJsonNode.elements();

                        while (elements.hasNext()) {
                            Record subrecord = innerJsonToRecord(connection, subTableName, elements.next());
                            if (subrecord != null) {
                                records.add(subrecord);
                            }
                        }
                    } else if (subJsonNode.isObject()) {
                        records.add(innerJsonToRecord(connection, subTableName, subJsonNode));
                    }
                }

                elementWithName.subRow.put(subTableName, records);
            }
        }

        return record;
    }

    private static List<Map.Entry<String, JsonNode>> getCompositeJsonElements(JsonNode json) {
        Iterable<Map.Entry<String, JsonNode>> iterable = () -> json.fields();
        return StreamSupport
                .stream(iterable.spliterator(), false).filter(e -> !e.getValue().isValueNode())
                .collect(Collectors.toList());
    }


    /** recursively add insertion statements starting from the rootTable and the record */
    private static void addInsertionStatements(String tableName, Record record, Map<String, FieldsMapper> mappers, Map<String, String> insertionStatements, Connection connection, EnumSet<TreatmentOptions> options) throws SQLException {
        String pkName = record.pkName;

        String selectPk = "SELECT " + pkName + " from " + tableName + " where  " + pkName + " = ?";

        boolean isInsert;
        try (PreparedStatement pkSelectionStatement = connection.prepareStatement(selectPk)) { // NOSONAR: now unchecked values all via prepared statement
            Record.Data elementWithName = record.findElementWithName(pkName);
            innerSetStatementField(elementWithName.metadata.type, pkSelectionStatement, 1, elementWithName.value.toString());

            try (ResultSet rs = pkSelectionStatement.executeQuery()) {
                isInsert = !rs.next();
            }
        }


        if (TreatmentOptions.getValue(ForceInsert, options)){
            isInsert = true;
        }
        boolean remapPrimaryKeys = TreatmentOptions.getValue(RemapPrimaryKeys, options) && isInsert;


        List<String> jsonFieldNames = record.getFieldNames();
        Iterable<Map.Entry<String, JsonNode>> iterable;

        // fields must both be in json AND in db metadata, remove those missing in db metadata
        Set<String> columnsDbNames = record.content.stream().map(e -> e.name).collect(Collectors.toSet());
        jsonFieldNames.removeIf(e -> !columnsDbNames.contains(e));
        // todo log if there is a delta between the 2 sets

        String sqlStatement = getSqlStatement(tableName, jsonFieldNames, pkName, isInsert);
        try (PreparedStatement statement = connection.prepareStatement(sqlStatement)) {

            String pkValue = null;
            String pkType = "";

            int statementIndex = 1; // statement param
            for (String currentFieldName : jsonFieldNames) {
                Record.Data currentElement = record.findElementWithName(currentFieldName);
                String valueToInsert = Objects.toString(currentElement.value);

                valueToInsert = prepareVarcharToInsert(currentElement.metadata.type, currentFieldName, valueToInsert);

                boolean fieldIsPk = currentFieldName.equals(pkName.toUpperCase());
                if (isInsert || !fieldIsPk) {
                    valueToInsert = removeQuotes(valueToInsert);

                    if (mappers.containsKey(currentFieldName)) {
                        mappers.get(currentFieldName).mapField(currentElement.metadata, statement, statementIndex, valueToInsert);
                    } else {
                        setStatementField(currentElement.metadata.type, statement, statementIndex, currentFieldName, valueToInsert);
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

                setStatementField(pkType, statement, statementIndex, pkName, pkValue);
            }

            if (insertionStatements.containsKey(tableName)){
                insertionStatements.put(tableName, insertionStatements.get(tableName) + ";" + statement.toString());
            } else {
                insertionStatements.put(tableName, statement.toString());
            }


            // do recursion (treat subtables)
            for (String subrowName : record.getFieldNamesWithSubrows()){

                Record.Data data = record.findElementWithName(subrowName);
                for (String linkedTable : data.subRow.keySet()) {

                    for (Record subrecord : data.subRow.get(linkedTable)) {
                        addInsertionStatements(linkedTable, subrecord, mappers, insertionStatements, connection, options);
                    }
                }
            }
        }
    }



    private static String prepareVarcharToInsert(String typeAsString, String currentFieldName, String valueToInsert) {
        if (typeAsString.toUpperCase().equals("VARCHAR")) {
            String testForEscaping = valueToInsert.trim();
            if (testForEscaping.length() > 1 && ((testForEscaping.startsWith("\"") && testForEscaping.endsWith("\"")) ||
                    ((testForEscaping.startsWith("'") && testForEscaping.endsWith("'"))))) {
                valueToInsert = testForEscaping.substring(1, testForEscaping.length() - 1);
            } else if (testForEscaping == "null") {
                valueToInsert = null;
            }
        }
        return valueToInsert;
    }


    private static String prepareVarcharToInsert(Map<String, Db2Graph.ColumnMetadata> columns, String currentFieldName, String valueToInsert) {
        if (columns.get(currentFieldName).getType().toUpperCase().equals("VARCHAR")) {
            String testForEscaping = valueToInsert.trim();
            if (testForEscaping.length() > 1 && ((testForEscaping.startsWith("\"") && testForEscaping.endsWith("\"")) ||
                    ((testForEscaping.startsWith("'") && testForEscaping.endsWith("'"))))) {
                valueToInsert = testForEscaping.substring(1, testForEscaping.length() - 1);
            } else if (testForEscaping == "null") {
                valueToInsert = null;
            }
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

    public static List<String> determineOrder(String rootTable, Connection connection) throws SQLException {
        Set<String> treated = new HashSet<>();

        Map<String, Set<String>> dependencyGraph = calculateDependencyGraph(rootTable, treated, connection);
        List<String> orderedTables = new ArrayList<>();

        Set<String> stillToTreat = new HashSet<>(treated);
        while(!stillToTreat.isEmpty()){
            // remove all for which we have a constraint
            Set<String> treatedThisTime = new HashSet<>(stillToTreat);
            treatedThisTime.removeAll(dependencyGraph.keySet());

            orderedTables.addAll(treatedThisTime);
            stillToTreat.removeAll(treatedThisTime);

            // remove the constraints that get eliminiated by treating those
            for (String key : dependencyGraph.keySet()) {
                dependencyGraph.get(key).removeAll(treatedThisTime);
            }
            dependencyGraph.entrySet().removeIf(e -> e.getValue().isEmpty());
        }

        return orderedTables;
    }


    private static Map<String, Set<String>> calculateDependencyGraph(String rootTable, Set<String> treated, Connection connection) throws SQLException {
        Set<String> tablesToTreat = new HashSet<>();
        tablesToTreat.add(rootTable);

        Map<String, Set<String>> dependencyGraph = new HashMap<>();

        while (!tablesToTreat.isEmpty()) {
            String next = tablesToTreat.stream().findFirst().get();
            tablesToTreat.remove(next);

            List<Db2Graph.Fk> fks = table2Fk(connection, next);
            for (Db2Graph.Fk fk : fks){
                String tableToAdd = fk.originTable;
                String otherTable = fk.targetTable;

                addToTreat(tablesToTreat, treated, tableToAdd);
                addToTreat(tablesToTreat, treated, otherTable);

                addDependency(dependencyGraph, tableToAdd, otherTable);
            }

            treated.add(next);
        }
        return dependencyGraph;
    }

    private static void addToTreat(Set<String> tablesToTreat, Set<String> treated, String tableToAdd) {
        if (!treated.contains(tableToAdd)) {
            tablesToTreat.add(tableToAdd);
        }
    }

    private static void addDependency(Map<String, Set<String>> dependencyGraph, String lastTable, String tableToAdd) {
        if (!lastTable.equals(tableToAdd)) {
            dependencyGraph.putIfAbsent(tableToAdd, new HashSet<>());
            dependencyGraph.get(tableToAdd).add(lastTable);
        }
    }

    // code from CsvToDb


    private static void setStatementField(String typeAsString, PreparedStatement preparedStatement, int statementIndex, String header, String valueToInsert) throws SQLException {
        innerSetStatementField(typeAsString, preparedStatement, statementIndex, valueToInsert);
    }

    private static void setStatementField(Map<String, Db2Graph.ColumnMetadata> columns, PreparedStatement preparedStatement, int statementIndex, String header, String valueToInsert) throws SQLException {
        innerSetStatementField(columns.get(header.toUpperCase()).getType(), preparedStatement, statementIndex, valueToInsert);
    }

    /** set a value on a jdbc Statement */
    private static void innerSetStatementField(String typeAsString, PreparedStatement preparedStatement, int statementIndex, String valueToInsert) throws SQLException {
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
                        preparedStatement.setTimestamp(statementIndex, Timestamp.valueOf(LocalDateTime.parse(valueToInsert)));
                    } else {
                        preparedStatement.setDate(statementIndex, Date.valueOf(LocalDate.parse(valueToInsert)));
                    }
                }
                break;
            default:
                preparedStatement.setObject(statementIndex, valueToInsert);
        }
    }

    /** generate insert or update statement to insert columnNames into tableName */
    private static String getSqlStatement(String tableName, List<String> columnNames, String pkName, boolean isInsert) {
        String result;
        String fieldList = columnNames.stream().filter(name -> (isInsert || !name.equals(pkName.toUpperCase()))).collect(Collectors.joining(isInsert ? ", " : " = ?, "));

        if (isInsert) {
            String questionsMarks = columnNames.stream().map(name -> "").collect(Collectors.joining("?, ")) + "?";

            result = "insert into " + tableName + " (" + fieldList + ") values (" + questionsMarks + ");";
        } else {
            fieldList += " = ? ";

            result = "update " + tableName + " set " + fieldList + " where " + pkName + " = ?";
        }

        return result;
    }




    //// unused code

    public void setOptions(EnumSet<TreatmentOptions> options) {
        this.options = options;
    }

    public void importJson(String rootTable, String jsonString, HashMap<String, FieldsMapper> mappers) throws IOException, SQLException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(jsonString);

        List<String> tableInsertOrder = determineOrder(rootTable, connection);

        // tableName -> insertionStatement
        Map<String, String> insertionStatements = new HashMap<>();

        addInsertionStatements(rootTable, jsonToRecord(connection, rootTable, jsonString), mappers, insertionStatements, connection, options);

        for (String table : tableInsertOrder) {
            if (insertionStatements.containsKey(table)) {
                // apply inserts
                boolean remapPrimaryKeys = TreatmentOptions.getValue(RemapPrimaryKeys, options);

                System.out.println(insertionStatements.get(table)+";");
            }
        }
    }

    // todo remove this old version
    /** recursively add insertion statements starting from the rootTable and the json structure */
    private static void addInsertionStatements(String tableName, JsonNode json, Map<String, FieldsMapper> mappers, Map<String, String> insertionStatements, Connection connection, EnumSet<TreatmentOptions> options) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        Map<String, Db2Graph.ColumnMetadata> columns = Db2Graph.getColumnNamesAndTypes(metadata, tableName);
        List<String> pks = Db2Graph.getPrimaryKeys(metadata, tableName);

        final String pkName = pks.get(0);

        String selectPk = "SELECT " + pkName + " from " + tableName + " where  " + pkName + " = ?";

        boolean isInsert;
        try (PreparedStatement pkSelectionStatement = connection.prepareStatement(selectPk)) { // NOSONAR: now unchecked values all via prepared statement
            innerSetStatementField(columns.get(pkName.toUpperCase()).getType(), pkSelectionStatement, 1, json.get(pkName).toString());

            try (ResultSet rs = pkSelectionStatement.executeQuery()) {
                isInsert = !rs.next();
            }
        }


        if (TreatmentOptions.getValue(ForceInsert, options)){
            isInsert = true;
        }
        boolean remapPrimaryKeys = TreatmentOptions.getValue(RemapPrimaryKeys, options) && isInsert;


        List<String> jsonFieldNames = getJsonFieldNames(json);
        Iterable<Map.Entry<String, JsonNode>> iterable;

        // fields must both be in json AND in db metadata, remove those missing in db metadata
        Set<String> columnsDbNames = columns.keySet();
        jsonFieldNames.removeIf(e -> !columnsDbNames.contains(e));
        // todo log if there is a delta between the 2 sets

        String sqlStatement = getSqlStatement(tableName, jsonFieldNames, pkName, isInsert);
        try (PreparedStatement statement = connection.prepareStatement(sqlStatement)) {

            String pkValue = null;

            int statementIndex = 1; // statement param
            for (String currentFieldName : jsonFieldNames) {
                String valueToInsert = json.get(currentFieldName.toLowerCase()).toString();

                valueToInsert = prepareVarcharToInsert(columns, currentFieldName, valueToInsert);

                boolean fieldIsPk = currentFieldName.equals(pkName.toUpperCase());
                if (isInsert || !fieldIsPk) {
                    valueToInsert = removeQuotes(valueToInsert);

                    if (mappers.containsKey(currentFieldName)) {
                        mappers.get(currentFieldName).mapField(columns.get(currentFieldName), statement, statementIndex, valueToInsert);
                    } else {
                        setStatementField(columns, statement, statementIndex, currentFieldName, valueToInsert);
                    }

                    statementIndex++;
                } else {
                    pkValue = valueToInsert;
                }
            }
            if (isInsert) {
                // do not set version
                //updateStatement.setLong(statementIndex, 0);
            } else {
                if (pkValue != null) {
                    pkValue = pkValue.trim();
                }

                setStatementField(columns, statement, statementIndex, pkName, pkValue);
            }


            if (insertionStatements.containsKey(tableName)){
                insertionStatements.put(tableName, insertionStatements.get(tableName) + ";" + statement.toString());
            } else {
                insertionStatements.put(tableName, statement.toString());
            }


            // do recursion (treat subtables)

            for (Map.Entry<String, JsonNode> field : getCompositeJsonElements(json)) {
                if (field.getValue().isArray()){
                    Iterator<JsonNode> elements = field.getValue().elements();

                    while (elements.hasNext()) {
                        addInsertionStatements(field.getKey().toString(), elements.next(), mappers, insertionStatements, connection, options);
                    }

                } else if (field.getValue().isObject()) {
                    addInsertionStatements(field.getKey().toString(), field.getValue(), mappers, insertionStatements, connection, options);
                }
            }

        }

    }

    public List<String> determineOrder_old(String rootTable) throws SQLException {
        Set<String> treated = new HashSet<>();

        Map<String, Set<String>> dependencyGraph = calculateDependencyGraph(rootTable, treated, connection);

        System.out.println("dependencyGraph:" + dependencyGraph);

        Map<String, Integer> numberOfOutgoingFks = new HashMap<>();
        for (String key : dependencyGraph.keySet()) {
            numberOfOutgoingFks.put(key, dependencyGraph.get(key).size());
        }

        System.out.println("numberOfOutgoingFks:" + numberOfOutgoingFks);

        // invert the Map
        Map<Integer, List<String>> outgoingFksToTable = new HashMap<>();
        for (String key : numberOfOutgoingFks.keySet()) {
            outgoingFksToTable.putIfAbsent(numberOfOutgoingFks.get(key), new ArrayList<>());
            outgoingFksToTable.get(numberOfOutgoingFks.get(key)).add(key);
        }

        System.out.println("outgoingFksToTable:" + outgoingFksToTable);

        List<String> orderedTables = new ArrayList<>();
        outgoingFksToTable.keySet().stream(). sorted(Comparator.reverseOrder()). forEach(key -> orderedTables.addAll(0, outgoingFksToTable.get(key)));

        System.out.println("orderedTables*:" + orderedTables);

        System.out.println("treatedTables: "+treated);
        for (String table : treated){
            if (!orderedTables.contains(table)) {
                orderedTables.add(0, table);
            }
        }
        return orderedTables;
    }

}
