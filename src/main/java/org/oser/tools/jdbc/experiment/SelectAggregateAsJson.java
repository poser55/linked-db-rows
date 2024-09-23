package org.oser.tools.jdbc.experiment;

import com.github.benmanes.caffeine.cache.Cache;
import org.oser.tools.jdbc.Fk;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.oser.tools.jdbc.experiment.RecursiveSqlTemplates.recurseSql;

/**
 * Experimental!
 * CAVEAT: Does not work for data structures where the same entity links to itself (such as graphs).
 */
public class SelectAggregateAsJson {

    static String postgreslJsonExportTemplate = """
            select json_agg( to_jsonb( 
            $recurse{ $table) § jsonb_build_object( '$parentKey*$subTable*',( select json_agg(  to_jsonb(  |
            )) from $subTable where $table.$parentKey = $subTable.$subKey )|
             $table |
              ,'$parentKey*$subTable*',(select json_agg( to_jsonb( }   )) from   """;

    static String oracleJsonExportTemplate =
            "select json_object( $recurse{ $table.*  , '$parentKey' value json_object ( | ) | $table.* | , }" +
                    ") from $recurse{ $table , | | $table | , }" +
                    // 1-1 is here to avoid that the previous AND causes a problem
                    "where $recurse{ $table.$parentKey = $subTable.$subKey AND | | 1=1 | AND $table.$parentKey = $subTable.$subKey AND }";

    // todo add other templates
    private static final Map<String, String> TEMPLATE_FOR_EACH_DATABASE = Map.of("postgres", postgreslJsonExportTemplate,
            "oracle", oracleJsonExportTemplate);

    /**
     * In the json, how shall the subtable be named?
     */
    public static final String DEFAULT_SUBTABLE_NAME = "$parentKey*$subTable*";

    public static Set<String> supportedDatabases = TEMPLATE_FOR_EACH_DATABASE.keySet();

    /**
     * Generate select statement to export an aggregate starting from rootTable and following all foreign keys in fkCache. <br/>
     * Variant allowing different namings of the subtables (default {@link #DEFAULT_SUBTABLE_NAME}
     *
     * @param rootTable         start from this table
     * @param fkCache                 ensure there are just the foreign keys you want to follow
     * @param alternativeSubtableName  how to name subtables? By default it is  parentKey*subTable* (as in the default DbExporter)
     * @param dbName                  for now only postgres supported
     * @return the select statement, sometimes with or without a where clause (plan to check the string by hand!)
     */
    public static String selectStatementForAggregateSelection(String rootTable, Cache<String, List<Fk>> fkCache, String alternativeSubtableName, String dbName) {
        if (!TEMPLATE_FOR_EACH_DATABASE.containsKey(dbName)) {
            throw new IllegalArgumentException("No template defined for " + dbName);
        }
        String template = TEMPLATE_FOR_EACH_DATABASE.get(dbName);
        template = template.replace(DEFAULT_SUBTABLE_NAME, alternativeSubtableName);

        String result = recurseSql(rootTable, template, fkCache);
        return result.replace("§", "||") + " " + rootTable;
    }

    /**
     * Generate select statement to export an aggregate starting from rootTable and following all foreign keys in fkCache
     *
     * @param rootTable  start from this table
     * @param fkCache   ensure there are just the foreign keys you want to follow
     * @param dbName    for now only postgres supported
     * @return the select statement (with or without a where clause, check it by hand!)
     */
    public static String selectStatementForAggregateSelection(String rootTable, Cache<String, List<Fk>> fkCache, String dbName) {
        return selectStatementForAggregateSelection(rootTable, fkCache, DEFAULT_SUBTABLE_NAME, dbName);
    }

}
