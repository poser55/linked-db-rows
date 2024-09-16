package org.oser.tools.jdbc;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNull;


class SelfLinkTest {

    @Test
    void basicSelfLink() throws Exception {
        Connection demo = TestHelpers.getConnection("demo");

        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(demo,
                dbExporter -> {
                    Fk.initFkCacheForMysql_LogException(demo, dbExporter.getFkCache());
                },
                dbImporter -> {
                    Fk.initFkCacheForMysql_LogException(demo, dbImporter.getFkCache());
                },
                "link2self", 2, 5);

        Set<DbRecord> allDbRecords = basicChecksResult.getAsDbRecord().getAllRecords();
        List<DbRecord> link2self = allDbRecords.stream().filter(r -> r.getRowLink().getTableName().equalsIgnoreCase("link2self")).collect(Collectors.toList());

        System.out.println(link2self);
        Cache<String, List<Fk>> cache = Caffeine.newBuilder().maximumSize(10_000).build();
        link2self = DbRecord.orderRecordsForInsertion(demo, link2self, cache);
        System.out.println(link2self + "\n");

        Collections.reverse(link2self);
        link2self = DbRecord.orderRecordsForInsertion(demo, link2self, cache);
        System.out.println(link2self);
        assertNull(link2self.get(0).findElementWithName("peer").getValue());
    }

    @Test
    void recordOrderingForInsertion() throws Exception {
        Connection demo = TestHelpers.getConnection("demo");

        DbExporter dbExporter = new DbExporter();
        DbRecord link2self = dbExporter.contentAsTree(demo, "link2self", 2);

        List<DbRecord> allDbRecords = new ArrayList(link2self.getAllRecords());
        Collections.reverse(allDbRecords);
        System.out.println(allDbRecords.stream().map(DbRecord::getRowLink).collect(Collectors.toList()) );
        System.out.println(allDbRecords +"\n\n");

        List<DbRecord> ordered = DbRecord.orderRecordsForInsertion(demo, allDbRecords, Caffeine.newBuilder().maximumSize(10_000).build());

        System.out.println(ordered.stream().map(DbRecord::getRowLink).collect(Collectors.toList()) +"\n\n");
        System.out.println(ordered);

    }
}