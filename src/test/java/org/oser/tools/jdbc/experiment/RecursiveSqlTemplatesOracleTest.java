package org.oser.tools.jdbc.experiment;

import org.junit.jupiter.api.Test;
import org.oser.tools.jdbc.FkHolder;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.oser.tools.jdbc.experiment.RecursiveSqlTemplates.recurseSql;

class RecursiveSqlTemplatesOracleTest {
    public static final String ORACLE_JSON_EXPORT_STATEMENT = "select json_object( $recurse{ $table.*  , '$parentKey' value json_object ( | ) | $table.* | , }" +
            "from $recurse{ $table , | | $table | , }" +
            // 1-1 is here to avoid that the previous AND causes a problem
            "where $recurse{ $table.$parentKey = $subTable.$subKey AND | | 1=1 | AND $table.$parentKey = $subTable.$subKey AND }";


//    @Test
//    void basic() throws SQLException, IOException, ClassNotFoundException {
//        if (TestHelpers.getActiveDB().contains("oracle")) {
//            Connection connection = TestHelpers.getConnection("demo");
//            String stat = "select json_object( $recurse{ $table.*  , '$parentKey' value json_object ( | ) | $table.*  }" +
//                    "from $recurse{ $table , | | $table}" +
//                    "where $recurse{ $table.$parentKey = $subTable.$subKey AND | | $table.$parentKey = $subTable.$subKey}";
//            String result = recurseSql(connection, "bla", stat, Caffeine.newBuilder()
//                    .maximumSize(10_000).build());
//
//            System.out.println(result);
//        }
//
//    }

    @Test
    void simpleMockedRecursion() throws SQLException {
        Connection connection = null;

        FkHolder fkHolder = new FkHolder();
        fkHolder.addFk("bla(bla_x)-bli(id)");
        fkHolder.addFk("bli(bli_x)-blu(id)");

        String result = recurseSql("bla", ORACLE_JSON_EXPORT_STATEMENT, fkHolder.getFkCache());

        assertNotNull(result);
        assertTrue(result.contains("bli.bli_x = blu.id"));
        System.out.println(result);
    }


    @Test
    void parallelMockedRecursion2() throws SQLException {
        Connection connection = null;

//  Example query:
//        SELECT JSON_OBJECT(
//                p.*,
//                'children' VALUE c.children
//       )
//        FROM   Parent p
//        INNER JOIN (
//                SELECT parent_id,
//                JSON_ARRAYAGG(JSON_OBJECT(*)) AS children
//        FROM   Child
//        GROUP BY parent_id
//       ) c
//        ON c.parent_id = p.id
//        WHERE  p.id = 1;


        FkHolder fkHolder = new FkHolder();
        fkHolder.addFk("bla(bla_x)-bli(id)");
        fkHolder.addFk("bla(bla_x)-blj(id)");
        fkHolder.addFk("bli(bli_x)-blu(id)");
        fkHolder.addFk("blj(blj_x)-blv(id)");

        String result = recurseSql("bla", ORACLE_JSON_EXPORT_STATEMENT, fkHolder.getFkCache());

        assertNotNull(result);
        assertTrue(result.contains("bli.bli_x = blu.id"));
        System.out.println(result);
    }
}