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

class RecordCanonicalizerTest {

    private static final Cache<String, List<Fk>> fkCache = Caffeine.newBuilder()
            .maximumSize(10_000).build();

    private static final Cache<String, List<String>> pkCache = Caffeine.newBuilder()
            .maximumSize(1000).build();


    @Test
    void basicTest() throws Exception {
        String toInsert = "{ \t\"id\": 99, \t\"author_id\": 77, \t\"author_id*author*\": [ \t\t{ \t\t\t\"id\": 77, \t\t\t\"last_name\": \"Huxley22\" \t\t} \t], \"title\": \"Brave new world2\", \"newfield\": \"xxx\"  }";

        Connection connection = TestHelpers.getConnection("demo");
        Record record = new DbImporter().jsonToRecord(connection, "book", toInsert);
        RecordCanonicalizer.canonicalizeIds(connection, record, fkCache, pkCache);

        System.out.println(record);

        assertEquals(1L, record.findElementWithName("id").value);
        assertEquals(1L, record.findElementWithName( "author_id").value);
        Record authorRecord = record.findElementWithName("author_id").subRow.get("author").get(0);

        assertEquals(1L, authorRecord.findElementWithName("id").value);
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

        Record newRecord =
                localDbExporter[0].contentAsTree(demo, "Nodes", basicChecksResult.getRowLinkObjectMap().get(new RowLink("Nodes", 1)));
        System.out.println("before:" + newRecord +"\n\n");

        RecordCanonicalizer.canonicalizeIds(demo, newRecord, fkCache, pkCache);
        System.out.println("canonicalized:" + newRecord);

        RecordCanonicalizer.canonicalizeIds(demo, basicChecksResult.getAsRecord(), fkCache, pkCache);

        // now compare the 2 records:
        ObjectWriter objectWriter = Record.getObjectMapper().writerWithDefaultPrettyPrinter();
        assertEquals(objectWriter.writeValueAsString(newRecord.asJsonNode()),
                objectWriter.writeValueAsString(basicChecksResult.getAsRecord().asJsonNode()));
    }

    @Test
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