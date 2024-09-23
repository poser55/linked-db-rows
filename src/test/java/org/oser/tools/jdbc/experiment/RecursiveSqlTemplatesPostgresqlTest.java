package org.oser.tools.jdbc.experiment;

import com.github.benmanes.caffeine.cache.Cache;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.oser.tools.jdbc.DbExporter;
import org.oser.tools.jdbc.DbRecord;
import org.oser.tools.jdbc.Fk;
import org.oser.tools.jdbc.FkHolder;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.oser.tools.jdbc.experiment.RecursiveSqlTemplates.recurseSql;

class RecursiveSqlTemplatesPostgresqlTest {

// example statement:
//    select json_agg(
//            to_jsonb(f) || jsonb_build_object(
//		'categories', (
//        select json_agg(
//                to_jsonb(c)
// 			) from category c, film_category fc where f.film_id = fc.film_id and c.category_id = fc.category_id
// 		)
//                )
//                )
//    from film f where title like '%AIR%' ;

    @Test
    void simpleMockedRecursion() throws SQLException {
        Connection connection = null;

        // § is used as ||

        String template = """
                select json_agg( to_jsonb( 
                $recurse{ $table) § jsonb_build_object( '$parentKey*$subTable*',( select json_agg(  to_jsonb(  |
                )) from $subTable where $table.$parentKey = $subTable.$subKey )|
                 $table |
                  ,'$parentKey*$subTable*',(select json_agg( to_jsonb( }   )) from  film  where title like '%AIR%' """;

        FkHolder fkHolder = new FkHolder();
        // sakila example
        fkHolder.addFk("film(film_id)-film_category(film_id)");
        fkHolder.addFk("category(category_id)-film_category(category_id)");
        fkHolder.addFk("film(language_id)-language(language_id)");

        // todo: bug is shown when next line is on
        //      fkHolder.addFk("film(original_language_id)-language(language_id)");

        //    fkHolder.addFk("bli(bli_x)-blu(id)");

        String result = recurseSql("film", template, fkHolder.getFkCache());

        assertNotNull(result);
        //      assertTrue(result.contains("bli.bli_x = blu.id"));
        String replaced = result.replace("§", "||");
        System.out.println("opening: " + StringUtils.countOccurrencesOf(replaced, "(") + "\n" +
                "closing: " + StringUtils.countOccurrencesOf(replaced, ")"));
        System.out.println(replaced);
    }

    String postgreslJsonExportTemplate = """
            select json_agg( to_jsonb( 
            $recurse{ $table) § jsonb_build_object( '$parentKey*$subTable*',( select json_agg(  to_jsonb(  |
            )) from $subTable where $table.$parentKey = $subTable.$subKey )|
             $table |
              ,'$parentKey*$subTable*',(select json_agg( to_jsonb( }   )) from   """;


    @Disabled // requires a local postgresql db with sakila
    @Test
    void firstTestWithDb() throws SQLException, IOException {
        PGSimpleDataSource datasource = new PGSimpleDataSource();

        datasource.setURL("jdbc:postgresql://localhost:5432/postgres?user=postgres&password=admin");

        Connection connection = datasource.getConnection();

        DbExporter exporter = new DbExporter();
        Set<String> stopTablesExcluded = exporter.getStopTablesExcluded();
        stopTablesExcluded.add("payment");
        stopTablesExcluded.add("payment_p2007_01");
        stopTablesExcluded.add("payment_p2007_02");
        stopTablesExcluded.add("payment_p2007_03");
        stopTablesExcluded.add("payment_p2007_04");
        stopTablesExcluded.add("payment_p2007_05");
        stopTablesExcluded.add("payment_p2007_06");
        stopTablesExcluded.add("store");
        stopTablesExcluded.add("customer");

        exporter.getStopTablesIncluded().add("film");

        DbRecord film = exporter.contentAsTree(connection, "film", 4);

        //    RecordAsGraph.toSimpleGraph(connection, film, "film.png");

        // System.out.println(film);

        Cache<String, List<Fk>> fkCacheSubset = exporter.getFilteredFkCache();
        System.out.println(fkCacheSubset.asMap());

        String result = recurseSql("film", postgreslJsonExportTemplate, fkCacheSubset);
        String replaced = result.replace("§", "||");

        System.out.println(replaced + " film where film_id = 4");
        //" film  where title like '%AIR%'");
    }

}