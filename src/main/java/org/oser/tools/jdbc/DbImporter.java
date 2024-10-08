package org.oser.tools.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.oser.tools.jdbc.spi.pkgenerator.NextValuePkGenerator;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.oser.tools.jdbc.Fk.getFksOfTable;


/**
 * Import a JSON structure exported with {@link DbExporter} into the db again.
 */
public class DbImporter implements FkCacheAccessor {
    public static final String JSON_SUBTABLE_SUFFIX = "*";

    // options
    private boolean forceInsert = true;
    private final Map<String, PkGenerator> overriddenPkGenerators = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private PkGenerator defaultPkGenerator = new NextValuePkGenerator();

    /**  fieldName : String -> <optionalTableName : String, FieldImporter> */
    private final Map<String, Map<String, FieldImporter>> fieldImporters = new HashMap<>();

    private final Cache<String, List<Fk>> fkCache = Caffeine.newBuilder()
            .maximumSize(10_000).build();

    private final Cache<String, List<String>> pkCache = Caffeine.newBuilder()
            .maximumSize(1000).build();

    private final Cache<String, SortedMap<String, JdbcHelpers.ColumnMetadata>> metadataCache = Caffeine.newBuilder()
            .maximumSize(1000).build();

    /** to overwrite how special JDBC types are set on a preparedStatement. Refer to
     *  {@link JdbcHelpers#innerSetStatementField(PreparedStatement, int, JdbcHelpers.ColumnMetadata, Object, Map)}
     */
    private final Map<String, FieldImporter> typeFieldImporters = new HashMap<>();

    {
        FieldImporter postgresImporter = (tableName, metadata, statement, insertIndex, value ) -> {
            if (value != null) {
                InputStream inputStream = new ByteArrayInputStream( JdbcHelpers.valueToByteArray(value));
                statement.setBinaryStream(insertIndex, inputStream);
            } else {
                statement.setArray(insertIndex, null);
            }
            return true;
        };
        typeFieldImporters.put("BYTEA", postgresImporter);

        FieldImporter blobImporter = (tableName, metadata, statement, insertIndex, value ) -> {
            if (value != null) {
                Blob blob = statement.getConnection().createBlob();

                blob.setBytes(1, JdbcHelpers.valueToByteArray(value));
                statement.setBlob(insertIndex, blob);
            } else {
                statement.setNull(insertIndex, Types.BLOB);
            }
            return true;
        };
        typeFieldImporters.put("BLOB", blobImporter);

    }


    /** with cycles in the FKs we would throw an exception - we can try inserting what we can anyway */
    private boolean ignoreFkCycles = false;

    private static List<Map.Entry<String, JsonNode>> getCompositeJsonElements(JsonNode json) {
        Iterable<Map.Entry<String, JsonNode>> iterable = json::fields;
        return StreamSupport
                .stream(iterable.spliterator(), false).filter(e -> !e.getValue().isValueNode())
                .collect(Collectors.toList());
    }


    private static Object prepareStringTypeToInsert(String typeAsString, Object valueToInsert) {
        if (valueToInsert != null &&
                (typeAsString.equalsIgnoreCase("VARCHAR") || typeAsString.equalsIgnoreCase("TEXT")) &&
                (valueToInsert instanceof String)) {
            valueToInsert = getInnerValueToInsert((String)valueToInsert);
        }
        return valueToInsert;
    }


    private static Object prepareStringTypeToInsert(Map<String, JdbcHelpers.ColumnMetadata> columns, String currentFieldName, Object valueToInsert) {
        return prepareStringTypeToInsert(columns.get(currentFieldName).getType(), valueToInsert);
    }

    private static String getInnerValueToInsert(String valueToInsert) {
        String testForEscaping = valueToInsert.trim();
        if (testForEscaping.length() > 1 && ((testForEscaping.startsWith("\"") && testForEscaping.endsWith("\"")) ||
                (testForEscaping.startsWith("'") && testForEscaping.endsWith("'")))) {
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
                .map(String::toLowerCase).collect(Collectors.toList());
    }

    private static Object removeQuotes(Object valueToInsert) {
        if ((valueToInsert instanceof String) && ((String) valueToInsert).startsWith("\"")) {
            valueToInsert = ((String)valueToInsert).substring(1, ((String)valueToInsert).length() - 1);
        }
        return valueToInsert;
    }

    /** Convert jsonString to record */
    public DbRecord jsonToRecord(Connection connection, String rootTable, String jsonString) throws IOException, SQLException {
        if (jsonString == null || jsonString.isEmpty()){
            throw new IllegalArgumentException("JSON string is null or empty.");
        }
        ObjectMapper mapper = DbRecord.getObjectMapper();

        JsonNode json = mapper.readTree(jsonString);

        return jsonToRecord(connection, rootTable, json);
    }

    /** Convert JsonNode to Record */
    public DbRecord jsonToRecord(Connection connection, String rootTable, JsonNode json) throws SQLException {
        if (pkCache.getIfPresent(rootTable) == null) {
            JdbcHelpers.assertTableExists(connection, rootTable);
        }

        DbRecord dbRecord = new DbRecord(rootTable, null);

        DatabaseMetaData metadata = connection.getMetaData();
        Map<String, JdbcHelpers.ColumnMetadata> columns = JdbcHelpers.getColumnMetadata(metadata, rootTable, metadataCache);
        List<String> pks = JdbcHelpers.getPrimaryKeys(metadata, rootTable, pkCache);

        dbRecord.setPkNames(pks);
        dbRecord.setColumnMetadata(columns);

        List<String> jsonFieldNames = getJsonFieldNames(json);
        // fields must both be in json AND in db metadata, remove those missing in db metadata
        Set<String> columnsDbNames = columns.keySet();
        jsonFieldNames.removeIf(e -> !columnsDbNames.contains(e));
        // todo log if there is a delta between the 2 sets

        Map<String, Integer> primaryKeyArrayPosition = JdbcHelpers.getStringIntegerMap(pks);
        Object[] primaryKeyValues = new Object[pks.size()];

        for (String currentFieldName : jsonFieldNames) {
            Object valueToInsert = getJavaTypeOfJsonNode(json, currentFieldName.toLowerCase());

            valueToInsert = prepareStringTypeToInsert(columns, currentFieldName, valueToInsert);

            if (primaryKeyArrayPosition.containsKey(currentFieldName.toLowerCase())){
                primaryKeyValues[primaryKeyArrayPosition.get(currentFieldName.toLowerCase())] = valueToInsert;
            }

            DbRecord.FieldAndValue d = new DbRecord.FieldAndValue(currentFieldName, columns.get(currentFieldName), valueToInsert);
            dbRecord.getContent().add(d);
        }
        dbRecord.setPkValue(primaryKeyValues);

        // treat subtables
        if (getCompositeJsonElements(json).isEmpty()) {
            return dbRecord;
        }
        for (Fk fk : getFksOfTable(connection, rootTable, fkCache)) {
            treatOneFk(connection, json, dbRecord, metadata, fk);
        }

        return dbRecord;
    }

    private void treatOneFk(Connection connection, JsonNode json, DbRecord dbRecord, DatabaseMetaData metadata, Fk fk) throws SQLException {
        String[] elementPkName = fk.isInverted() ? fk.getFkcolumn() : fk.getPkcolumn();
        List<DbRecord.FieldAndValue> elementsWithName = Stream.of(elementPkName).map(dbRecord::findElementWithName).map(Optional::ofNullable)
                .flatMap(Optional::stream).collect(toList());

        if (!elementsWithName.isEmpty()) {
            String subTableName = Fk.getSubtableName(fk, metadata.getDatabaseProductName());

            JsonNode subJsonNode = json.get(elementsWithName.get(0).getName().toLowerCase() + JSON_SUBTABLE_SUFFIX  + subTableName + JSON_SUBTABLE_SUFFIX);
            ArrayList<DbRecord> dbRecords = new ArrayList<>();

            if (fk.isInverted()) {
                dbRecord.getOptionalFks().add(fk);
            }

            if (subJsonNode != null) {
                if (subJsonNode.isArray()) {
                    Iterator<JsonNode> elements = subJsonNode.elements();

                    while (elements.hasNext()) {
                        DbRecord subrecord = this.jsonToRecord(connection, subTableName, elements.next());
                        dbRecords.add(subrecord);
                    }
                } else if (subJsonNode.isObject()) {
                    dbRecords.add(this.jsonToRecord(connection, subTableName, subJsonNode));
                }
            }

            if (!fk.isInverted()) {
                dbRecords.forEach(r -> r.getOptionalFks().add(fk));
            }

            if (!dbRecords.isEmpty()) {
                elementsWithName.get(0).getSubRow().put(subTableName, dbRecords);
            }
        }
    }



    private Object getJavaTypeOfJsonNode(JsonNode json, String fieldName) {
        JsonNode jsonNode = json.get(fieldName);
        if (jsonNode.isIntegralNumber()){
            return jsonNode.asLong();
        } else if (jsonNode.isBigDecimal()) {
            return new BigDecimal(jsonNode.bigIntegerValue());
        } else if (jsonNode.isBoolean()) {
            return jsonNode.asBoolean();
        } else if (jsonNode.isDouble()) {
            return jsonNode.asDouble();
        } else {
            return jsonNode.asText();
        }
    }


    /**
     * Insert a record into a database - doing the remapping to other primary keys where needed.
     * Assumes someone external handles the transaction or autocommit
     * @return the remapped keys (RowLink -> new primary key)
     *
     * CAVEAT: it cannot handle cycles in the db tables!
     * Will insert just partial data (the first tables without cycles).
     */
    public Map<RowLink, Remap> insertRecords(Connection connection, DbRecord dbRecord) throws SQLException {
        Map<RowLink, Remap> newKeys = new HashMap<>();

        return insertRecords(connection, dbRecord, newKeys);
    }

    /** Holds the pkField and the position within the PK (0-based). The latter is important if the PK has multiple fields */
    @AllArgsConstructor
    @ToString
    @Getter
    public static class Remap {
        Object pkField;
        /** 0-based: what is the position of the pkField (in case the PK has a list of PK fields */
        int position = 0;
    }

    /** Alternative to {@link #insertRecords(Connection, DbRecord)} that allows to remap certain elements (e.g. if you want to connect
     *    some nodes of the JSON tree to an existing RowLink). The previously remapped elements (those in newKeys) are NOT inserted. <br/>
     *     Sample use case: insert a blog entry at another person that the one in the Record. Refer to the test org.oser.tools.jdbc.DbExporterBasicTests#blog()
     *     CAVEAT: newKeys must be a mutable map (it will add all the other remappings also) */
    public Map<RowLink, Remap> insertRecords(Connection connection, DbRecord dbRecord, Map<RowLink, Remap> newKeys) throws SQLException {
        Set<RowLink> rowLinksNotToInsert = newKeys.keySet();

        CheckedFunction<DbRecord, Void> insertOneRecord = (DbRecord r) -> {
            if (!rowLinksNotToInsert.contains(r.getRowLink())) {
                this.insertOneRecord(connection, r, newKeys);
            }
            return null; // strange that we need this hack
        };
        dbRecord.visitRecordsInInsertionOrder(connection, insertOneRecord, !ignoreFkCycles, fkCache);

        return newKeys;
    }

    private void insertOneRecord(Connection connection, DbRecord dbRecord, Map<RowLink, Remap> newKeys) throws SQLException {
        List<String> primaryKeys = dbRecord.getPkNames();

        // todo : bug sometimes the optionalFk is not correct on record (e.g. on node)
        //  so for now, we get it from the cache:
        List<Fk> fksOfTable = getFksOfTable(connection, dbRecord.getRowLink().getTableName(), fkCache);

        Map<String, List<Fk>> fksByColumnName = Fk.fksByColumnName(fksOfTable);
        List<Boolean> isFreePk = new ArrayList<>(primaryKeys.size());

        List<Object> newPkValues = remapPrimaryKeyValues(dbRecord, newKeys, primaryKeys, fksByColumnName, isFreePk);

        boolean entryExists = JdbcHelpers.doesRowWithPrimaryKeysExist(connection, dbRecord.getRowLink().getTableName(), primaryKeys, newPkValues, dbRecord.getColumnMetadata());
        boolean isInsert = forceInsert || !entryExists;

        Object candidatePk;
        if (isInsert && entryExists) {
            // iterate over all entries of the primary key, generate a candidate for first that is possible

            for (int i = 0; i < primaryKeys.size(); i++) {
                if (isFreePk.get(i)) {
                    DbRecord.FieldAndValue pkFieldWithValue = dbRecord.findElementWithName(primaryKeys.get(i));
                    candidatePk = getCandidatePk(connection, dbRecord.getRowLink().getTableName(), pkFieldWithValue.getMetadata().type, primaryKeys.get(i));

                    newKeys.put(dbRecord.getRowLink(), new Remap(candidatePk, i));

                    newPkValues.set(i, candidatePk); // maybe not needed (caught by later remapping?)
                    break;
                }
            }
        }

        List<String> fieldNames = dbRecord.getFieldNames();

        // fields must both be in json AND in db metadata, remove those missing in db metadata
        Set<String> columnsDbNames = dbRecord.getContent().stream().map(e -> e.getName()).collect(Collectors.toSet());
        fieldNames.removeIf(e -> !columnsDbNames.contains(e));
        // todo log if there is a delta between the 2 sets, ok for those who map subrows !

        // special case: if the entry needs updating and there are no other fields to set, we are done
        if (!isInsert && (fieldNames.size() == primaryKeys.size())) {
            return;
        }

        Map<String, JdbcHelpers.ColumnMetadata> columnMetadata = dbRecord.getColumnMetadata();
        JdbcHelpers.SqlChangeStatement sqlStatement = JdbcHelpers.getSqlInsertOrUpdateStatement(dbRecord.getRowLink().getTableName(), fieldNames, dbRecord.getPkNames(), isInsert, columnMetadata);
        PreparedStatement savedStatement = null;
        Map<String, Object> insertedValues = new HashMap<>();
        try (PreparedStatement statement = connection.prepareStatement(sqlStatement.getStatement())) {
            final Object[] valueToInsert = {"-"};

            for (String currentFieldName : fieldNames) {
                DbRecord.FieldAndValue currentElement = dbRecord.findElementWithName(currentFieldName);
                valueToInsert[0] = prepareStringTypeToInsert(currentElement.getMetadata().type, currentElement.getValue());

                boolean fieldIsPk = primaryKeys.stream().map(String::toLowerCase).anyMatch(e -> currentFieldName.toLowerCase().equals(e));

                if (fieldIsPk) {
                    valueToInsert[0] = Objects.toString(newPkValues.get(JdbcHelpers.getStringIntegerMap(primaryKeys).get(currentFieldName.toLowerCase())));
                } else if (isInsert && fksByColumnName.containsKey(currentFieldName)) {
                    // remap fks!
                    List<Fk> fks = fksByColumnName.get(currentFieldName);

                    Object earlierIntendedFk = valueToInsert[0];
                    fks.forEach(fk -> {
                        Remap potentialNewValue = newKeys.get(new RowLink(fk.getPktable(), earlierIntendedFk));
                        valueToInsert[0] = potentialNewValue != null ? Objects.toString(potentialNewValue.getPkField()) : valueToInsert[0];
                    });
                }

                int statementPosition = sqlStatement.getFields().indexOf(currentFieldName.toLowerCase()) + 1;

                valueToInsert[0] = removeQuotes(valueToInsert[0]);

                FieldImporter fieldImporter = getFieldImporter(dbRecord.getRowLink().getTableName(), currentFieldName);
                boolean bypassNormalInsertion = false;
                insertedValues.put(currentElement.getName(), valueToInsert[0]);
                if (fieldImporter != null) {
                    bypassNormalInsertion = fieldImporter.importField(dbRecord.getRowLink().getTableName(), currentElement.getMetadata(), statement, statementPosition, valueToInsert[0]);
                }
                if (!bypassNormalInsertion) {
                    JdbcHelpers.innerSetStatementField(statement, statementPosition, currentElement.getMetadata(), valueToInsert[0], typeFieldImporters);
                }
            }

            savedStatement = statement;
            int optionalUpdateCount = statement.executeUpdate();

            Loggers.logChangeStatement(statement, sqlStatement.getStatement(), insertedValues, optionalUpdateCount);

        } catch (SQLException e) {
            Loggers.logChangeStatement(savedStatement, sqlStatement.getStatement(), insertedValues, 0);
            Loggers.LOGGER_WARNING.info("issue with statement: {} ", savedStatement);

            throw e;
        }
    }



    /**
     * @return the primary key values that can be inserted for this record.
     *  The primary keys are remapped if needed (if e.g. another inserted row has a pk that was remapped before)
     *         CAVEAT: also updates the isFreePk List (to determine what pk values are "free")
     *
     *         A free PK is one that has no FK from somewhere else and is not remapped there => so we can assign it freely
     *
     * */
    // todo: fix this
    static List<Object> remapPrimaryKeyValues(DbRecord dbRecord,
                                              Map<RowLink, DbImporter.Remap> newKeys,
                                              List<String> primaryKeys,
                                              Map<String, List<Fk>> fksByColumnName,
                                              List<Boolean> isFreePk) {
        List<Object> pkValues = new ArrayList<>(primaryKeys.size());

        for (String primaryKey : primaryKeys) {
            DbRecord.FieldAndValue pkFieldWithValue = dbRecord.findElementWithName(primaryKey);

            // do the remapping from the newKeys
            Remap[] potentialValueToInsert = {null};
            if (fksByColumnName.containsKey(primaryKey.toLowerCase())) {
                List<Fk> fks = fksByColumnName.get(primaryKey.toLowerCase());
                // todo: fix next line: rowlink is wrong if more than 1 primary key?
                fks.forEach(fk -> potentialValueToInsert[0] = newKeys.get(new RowLink(fk.getPktable(), pkFieldWithValue.getValue())));
            }
            // if it is remapped, it is a fk from somewhere else -> so we cannot set it freely
            isFreePk.add(potentialValueToInsert[0] == null);

            pkValues.add(potentialValueToInsert[0] != null ? potentialValueToInsert[0].getPkField(): pkFieldWithValue.getValue());
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

    /** If true, always insert new records if the PKs already exist (via remapping if necessary).
     *  If false, try updating if entries exist.
     * Setting this to false is experimental: it has limitations with 1:n mappings (keeps already existing 1:n entries,
     * so there might be more than what is in the JSON (after the update)), with multiple primary keys and fieldMappers <p>
     * Default: true */
    public void setForceInsert(boolean forceInsert) {
        this.forceInsert = forceInsert;
    }

    public Map<String, PkGenerator> getOverriddenPkGenerators() {
        return overriddenPkGenerators;
    }

    public void setDefaultPkGenerator(PkGenerator generator) {
        defaultPkGenerator = generator;
    }

    /** @param tableName the table for which the FieldImporter should be used, may be null, means all tables
     *  @param fieldName the field for which the FieldImporter should be used */
    public void registerFieldImporter(String tableName, String fieldName, FieldImporter newFieldImporter){
        Objects.requireNonNull(fieldName, "Field must not be null");
        Objects.requireNonNull(newFieldImporter, "FieldImporter must not be null");

        fieldName = fieldName.toLowerCase();
        tableName = (tableName != null) ? tableName.toLowerCase() : null;

        if (!fieldImporters.containsKey(fieldName)){
            fieldImporters.put(fieldName, new HashMap<>());
        }
        fieldImporters.get(fieldName).put(tableName, newFieldImporter);
    }

    /** Access to internal fieldImporters */
    public Map<String, Map<String, FieldImporter>> getFieldImporters() {
        return fieldImporters;
    }


    //@VisibleForTesting
    /** Internal retrieval, @return null if none is found */
    FieldImporter getFieldImporter(String tableName, String fieldName) {
        fieldName = fieldName.toLowerCase();
        tableName = tableName.toLowerCase();

        // find best match
        Map<String, FieldImporter> fieldMatch = fieldImporters.get(fieldName);
        FieldImporter match;
        if (fieldMatch != null) {
            match = fieldMatch.get(tableName);

            if (match == null) {
                match = fieldMatch.get(null);
            }
            return match;
        }
        return null;
    }

    @Override
    public Cache<String, List<Fk>> getFkCache() {
        return fkCache;
    }

    /** Allows overriding how we set a value on a jdbc prepared statement.
     * Refer to {@link JdbcHelpers#innerSetStatementField(PreparedStatement, int, JdbcHelpers.ColumnMetadata, Object, Map)} */
    public Map<String, FieldImporter> getTypeFieldImporters() {
        return typeFieldImporters;
    }


    /** with cycles in the FKs we would throw an exceptions - we can try inserting what we can anyway (default: false) */
    public void setIgnoreFkCycles(boolean ignoreFkCycles) {
        this.ignoreFkCycles = ignoreFkCycles;
    }
}
