package org.oser.tools.jdbc;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.oser.tools.jdbc.Fk.getFksOfTable;

/** Adapt primary keys of a Record so that same data can be more easily compared (differences in PK values are eliminated). */
public class RecordCanonicalizer {

    private RecordCanonicalizer() {  }

    /**
     *  Remap the keys so that they are canonical (so that 2 same exports with the same data lead to the same json structure,
     *  the primary keys are assumed to not hold any meaning (beyond linking))
     *   the id order is determined based on the original order (so assuming integer primary keys this
     *   should be stable for equality) <p>
     *
     *   Needs the connection to determine the insertionOrder traversal and other metadata<p>
     *
     *   Updates this record and all other records (assumes that it works on the root record) <p>
     *
     *   Can be used with cycles in FK DDL: but the canonicalization is not done for the part with cycles. <p>
     *
     * @return the remapped primary keys (here all the values can be remapped, not just the first free value as in DbImporter)
     */
    public static Map<RowLink, List<Object>> canonicalizeIds(Connection connection,
                                                             Record record) throws Exception {
        return canonicalizeIds(connection,
                record,
                Caffeine.newBuilder().maximumSize(10_000).build(),
                Caffeine.newBuilder().maximumSize(1000).build());
    }


    /** refer to
     * {@link RecordCanonicalizer#canonicalizeIds(Connection, Record)} */
    public static Map<RowLink, List<Object>> canonicalizeIds(Connection connection,
                                                             Record record,
                                                             Cache<String, List<Fk>> fkCache,
                                                             Cache<String, List<String>> pkCache) throws Exception {
        Map<RowLink, List<Object>> newKeys = new HashMap<>();
        DatabaseMetaData metaData = connection.getMetaData();

        Map<String, Integer> counterPerTableName = new HashMap<>();

        CheckedFunction<Record, Void> canonicalizeOneRecord = (Record r) -> {
            canonicalizeOneRecord(connection, r, newKeys, metaData, counterPerTableName, fkCache, pkCache);
            return null; // strange that we need this hack
        };

        record.visitRecordsInInsertionOrder(connection, canonicalizeOneRecord, false);

        return newKeys;
    }


    private static void canonicalizeOneRecord(Connection connection,
                                              Record record,
                                              Map<RowLink, List<Object>> newKeys,
                                              DatabaseMetaData metaData,
                                              Map<String, Integer> counterPerTableName,
                                              Cache<String, List<Fk>> fkCache,
                                              Cache<String, List<String>> pkCache) throws SQLException {
        List<String> primaryKeys = JdbcHelpers.getPrimaryKeys(metaData, record.getRowLink().getTableName(), pkCache);
        List<Fk> fksOfTable = getFksOfTable(connection, record.getRowLink().getTableName(), fkCache);
        Map<String, List<Fk>> fksByColumnName = Fk.fksByColumnName(fksOfTable);

        List<Boolean> isFreePk = new ArrayList<>(primaryKeys.size());
        List<Object> newPkValues = remapPrimaryKeyValuesFull(record, newKeys, primaryKeys, fksByColumnName, isFreePk);

        remapKeysAndUpdateNewKeys(record, counterPerTableName, primaryKeys, newPkValues, isFreePk, newKeys, fksByColumnName);
    }


    /**
     * Adapted from DbImporter (here we do the remapping of ALL primary key values not just the first free primary key) it is here
     *  Map<RowLink, List<Object>> newKeys)
     *
     * @return the primary key values that are remapped if needed (if e.g. another inserted row has a pk that was remapped before)

     *         CAVEAT: also updates the isFreePk List (to determine what pk values are "free")
     * */
    static List<Object> remapPrimaryKeyValuesFull(Record record,
                                                  Map<RowLink, List<Object>> newKeys,
                                                  List<String> primaryKeys,
                                                  Map<String, List<Fk>> fksByColumnName,
                                                  List<Boolean> isFreePk) {
        List<Object> pkValues = new ArrayList<>(primaryKeys.size());

        for (String primaryKey : primaryKeys) {
            Record.FieldAndValue elementWithName = record.findElementWithName(primaryKey);

            // find primaryKey values that were remapped before
            Object[] potentialValueToInsert = {null};
            if (fksByColumnName.containsKey(primaryKey.toLowerCase())) {
                List<Fk> fks = fksByColumnName.get(primaryKey.toLowerCase());
                fks.forEach(fk -> {
                    RowLink rowLinkToFind = new RowLink(fk.pktable, elementWithName.value);
                    potentialValueToInsert[0] = newKeys.get(rowLinkToFind);
                });
            }

            // if it is remapped, it is a fk from somewhere else -> so we cannot set it freely
            isFreePk.add(potentialValueToInsert[0] == null);

            pkValues.add(potentialValueToInsert[0] != null ? ((List<Object>)potentialValueToInsert[0]).get(0): elementWithName.value);
        }
        return pkValues;
    }


    /**
     * 1. determine new keys this record
     * 2. remap foreign key fields that were adapted before */
    private static void remapKeysAndUpdateNewKeys(Record r,
                                                  Map<String, Integer> counterPerTableName,
                                                  List<String> primaryKeys,
                                                  List<Object> primaryKeyValues,
                                                  List<Boolean> isFreePk,
                                                  Map<RowLink, List<Object>> newKeys,
                                                  Map<String, List<Fk>> fksByColumnName) {

        // 1. determine new keys for the entries of the record
        List<Object> newKeysForThisRecord = determineNewCanonicalPrimaryKeys(r.getRowLink().getTableName(), counterPerTableName, primaryKeyValues, isFreePk);

        newKeys.put(new RowLink(r.getRowLink()), newKeysForThisRecord);


        // remap pk values of this record
        int pkIndex = 0;
        for (String primaryKeyName : primaryKeys) {
            r.findElementWithName(primaryKeyName).value = newKeysForThisRecord.get(pkIndex);
            pkIndex++;
        }


        // 2. remap other fields (from earlier remappings)
        for (String fieldName : fksByColumnName.keySet()) {
            List<Fk> fks = fksByColumnName.get(fieldName);

            fks.forEach(fk -> {
                List<Object> potentialNewValue = newKeys.get(new RowLink(fk.pktable, r.findElementWithName(fieldName).value));

                if (potentialNewValue != null) {
                    if (potentialNewValue.size() > 1) {
                        System.out.println("issue: potentialNewValue is more than 1 value");
                    }

                    r.findElementWithName(fieldName).value = potentialNewValue.get(0);
                }
            });
        }

        r.getRowLink().setPks(newKeysForThisRecord.toArray(new Object[0]));
    }

    private static List<Object> determineNewCanonicalPrimaryKeys(String tableName,
                                                                 Map<String, Integer> counterPerTableName,
                                                                 List<Object> primaryKeyValues,
                                                                 List<Boolean> isFreePk) {
        List<Object> newPrimaryKeys = new ArrayList <>();
        final int[] i = {0};
        primaryKeyValues.forEach(currentValue -> createAndAddKey(tableName, counterPerTableName, currentValue, isFreePk.get(i[0]++), newPrimaryKeys));
        return newPrimaryKeys;
    }

    // starts counting at 1
    private static void createAndAddKey(String tableName,
                                        Map<String, Integer> counterPerTableName,
                                        Object currentValue,
                                        boolean isAFreePkValue,
                                        List<Object> newKeys) {
        int ithRecord = counterPerTableName.getOrDefault(tableName, 1);
        Object newValue = isAFreePkValue ? getKeyForIndex(ithRecord, currentValue)  : currentValue;
        counterPerTableName.put(tableName, ithRecord + 1);
        newKeys.add(newValue);
    }

    // only works for Long and String
    private static Object getKeyForIndex(int i, Object currentValue) {
        if (currentValue instanceof Number) {
            return (long) i;
        }
        // for now we use strings of number even if it is a String PK
        return String.valueOf(i); // getCharForNumberInternal(i - 1);
    }

    private static String getCharForNumberInternal(int i) {
        return i > 0 && i < 27 ? String.valueOf((char)(i + 64)) : null;
    }

    static String getCharForNumber(int i) {
        String result = "";

        do {
            result =  getCharForNumberInternal((i % 26)+1) + result;
            i = (i / 26)-1;
        } while (i > -1);

        return result;
    }
}
