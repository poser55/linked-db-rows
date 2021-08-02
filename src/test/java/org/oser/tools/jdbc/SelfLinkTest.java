package org.oser.tools.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNull;


class SelfLinkTest {

    @Test
    void basicSelfLink() throws Exception {
        Connection demo = TestHelpers.getConnection("demo");

        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(TestHelpers.getConnection("demo"),
                "link2self", 2, 5);
        Set<Record> allRecords = basicChecksResult.getAsRecord().getAllRecords();
        List<Record> link2self = allRecords.stream().filter(r -> r.getRowLink().getTableName().toLowerCase().equals("link2self")).collect(Collectors.toList());

        System.out.println(link2self);
        Record.orderRecordsForInsertion(link2self, Fk.getFksOfTable(demo, "link2self"));
        System.out.println(link2self);
        Collections.reverse(link2self);
        Record.orderRecordsForInsertion(link2self, Fk.getFksOfTable(demo, "link2self"));
        System.out.println(link2self);
        assertNull(link2self.get(0).findElementWithName("peer").getValue());
    }
}
