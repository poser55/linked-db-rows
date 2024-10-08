package org.oser.tools.jdbc;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DbRecordCanonicalizerTest {

    private static final Cache<String, List<Fk>> fkCache = Caffeine.newBuilder()
            .maximumSize(10_000).build();

    private static final Cache<String, List<String>> pkCache = Caffeine.newBuilder()
            .maximumSize(1000).build();


    @Test
    void basicTest() throws Exception {
        String toInsert = "{ \t\"id\": 99, \t\"author_id\": 77, \t\"author_id*author*\": [ \t\t{ \t\t\t\"id\": 77, \t\t\t\"last_name\": \"Huxley22\" \t\t} \t], \"title\": \"Brave new world2\", \"newfield\": \"xxx\"  }";

        Connection connection = TestHelpers.getConnection("demo");
        DbRecord dbRecord = new DbImporter().jsonToRecord(connection, "book", toInsert);
        RecordCanonicalizer.canonicalizeIds(connection, dbRecord, fkCache, pkCache);

        System.out.println(dbRecord);

        assertEquals(1L, dbRecord.findElementWithName("id").getValue());
        assertEquals(1L, dbRecord.findElementWithName( "author_id").getValue());
        DbRecord authorDbRecord = dbRecord.findElementWithName("author_id").getSubRow().get("author").get(0);

        assertEquals(1L, authorDbRecord.findElementWithName("id").getValue());

        assertTrue(dbRecord.getSubRecordFieldAndValues().size()> 0);
    }

    @Test
    @DisabledIfSystemProperty(named = "mixedCaseTableNames", matches = "false")
    void testGraph_canonicalized() throws Exception {
        Connection demo = TestHelpers.getConnection("demo");
        final DbExporter[] localDbExporter = new DbExporter[1];

        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(demo,
                dbExporter -> {
                    Fk.initFkCacheForMysql_LogException(demo, dbExporter.getFkCache());
                    localDbExporter[0] = dbExporter;
                },
                dbImporter -> {
                    Fk.initFkCacheForMysql_LogException(demo, dbImporter.getFkCache());
                },
                "Nodes", 1, 10);

        DbRecord newDbRecord =
                localDbExporter[0].contentAsTree(demo, "Nodes", basicChecksResult.getRowLinkObjectMap().get(new RowLink("Nodes", 1)).getPkField());
        System.out.println("before:" + newDbRecord +"\n\n");

        RecordCanonicalizer.canonicalizeIds(demo, newDbRecord, fkCache, pkCache);
        System.out.println("canonicalized:" + newDbRecord);

        RecordCanonicalizer.canonicalizeIds(demo, basicChecksResult.getAsDbRecord(), fkCache, pkCache);

        // now compare the 2 records:
        ObjectWriter objectWriter = DbRecord.getObjectMapper().writerWithDefaultPrettyPrinter();
        assertEquals(objectWriter.writeValueAsString(newDbRecord.asJsonNode()),
                objectWriter.writeValueAsString(basicChecksResult.getAsDbRecord().asJsonNode()));
    }

    @Test
    @Disabled
    void charForInteger() {
        System.out.println(RecordCanonicalizer.getCharForNumber(0));
        System.out.println(RecordCanonicalizer.getCharForNumber(20));
        System.out.println(RecordCanonicalizer.getCharForNumber(27));
        System.out.println(RecordCanonicalizer.getCharForNumber(28));
        System.out.println(RecordCanonicalizer.getCharForNumber(100));

        for (int i = 25; i < 35; i++) {
            System.out.println(i+" "+RecordCanonicalizer.getCharForNumber(i));
        }
    }
}