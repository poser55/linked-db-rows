package org.oser.tools.jdbc.experiment;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.oser.tools.jdbc.Fk;
import org.oser.tools.jdbc.JdbcHelpers;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Create SQL statements recursively, based on entities and their foreign keys.   <br/><br/>
 *
 * Syntax of the templates (emerging!!):     <br/>
 *  <code>
 *  some string   $recurse{ S1: while recurring before recursion | S2: while recurring after recursion | S3: terminal part | S4: same level iteration}   some string
 *  </code>
 *
 * <ol>
 *  <li> Each $recurse block is recursed, i.e. it is evaluated for the root table and then for each foreign key that starts there.
 *    A recurse block starts with '$recurse{' , ends with '}' and the 4 arguments are separated by '|'.
 *  <li> A recurse block either recurses or terminates. In case it recurses, the sub template S1 is inserted,
 *  then the recursion continues and afterwards the sub template S2 is inserted.
 *  <li> Same level iteration (S4 sub template) means: one table has multiple foreign keys (the 2nd one is treated via the S4 subtemplate).
 *  <li> Once the recursion ends, the sub template S3 (terminal part) is evaluated
 *  <li> The following variables can be used: $table (the current table), $subTable (the subtable we work with), $subKey (the fk-key on the subtable), $parentKey (the fk-key on the parent table)
 *  <li> As illustration: often in a where clause we have the following expression:
 *     <code>$table.$parentKey = $subTable.$subKey</code>
 *
 * </ol>
 *   CAVEAT: || must be written as | |   for the moment
 *
 *   Refer to the examples in {@link SelectAggregateAsJson}, {@link SqlTemplatingTest} and {@link SqlTemplatingPostgresqlTest }
 */
@Slf4j
public class RecursiveSqlTemplates {

    /**
     * Returns an SQL String with recursive template substitution.
     * Refer to class javadoc for syntax of the template
     *
     * @param rootTable the table from which to start the recursion
     * @param fkCache   foreign key cache with all the foreign keys to follow
     * @throws IllegalStateException in case of cycles of primary keys from the rootTable
     */
    public static String recurseSql(String rootTable, String template, Cache<String, List<Fk>> fkCache) {
        Set<String> treated = new HashSet<>();

        Map<String, Set<String>> dependencyGraph = null;
        try {
            dependencyGraph = JdbcHelpers.initDependencyGraph(rootTable, treated, null, fkCache);
        } catch (SQLException e) {
            throw new RuntimeException("We expected to have the Fks in the cache (so no connection is needed)."+e);
        }

        // The next line throws an exception in case of fk cycles
        List<String> strings = JdbcHelpers.topologicalSort(dependencyGraph, treated, true);

        List<TemplatePart> templateParts = parseTemplate(template);

        log.debug("Template parts: {}",templateParts);

        Context context = new Context(fkCache, dependencyGraph);

        return templateParts.stream().map(p -> p.getResult(rootTable, context, 1)).collect(Collectors.joining(" "));
    }

    private static List<TemplatePart> parseTemplate(String template) {
        List<TemplatePart> result = new ArrayList<>();

        // explanation for the regex: we want the following regex \\$recurse\\{|\\}|\\$terminate\\{|\\|
        //  but we also want the patterns that were parsed as separators. So we put these options twice in the following
        //  sequence with the options (:|#|@)              ((?=:|#|@)|(?<=:|#|@))
        //  Refer to https://www.baeldung.com/java-split-string-keep-delimiters for more details
        String[] split = template.split("((?=\\$recurse\\{|\\}|\\$terminate\\{|\\|)|(?<=\\$recurse\\{|\\}|\\$terminate\\{|\\|))");

        int i = 0;
        while (i < split.length) {
            if (split[i].equals("$recurse{")){
                i++;

                // todo: ensure the intermediate tokens are correct
                result.add(new Recursion(split[i], split[i+2], split[i+4], split[i+6]));
                i = i + 7;
            } else {
                result.add(new Constant(split[i]));
            }
            i++;
        }

        return result;
    }


    // data structures
    static class Context {
        private Cache<String, List<Fk>> cache;
        private Map<String, Set<String>> dependencyGraph;
        private Set<String> visitedTables = new HashSet<>();

        private Set<Fk> usedFks = new HashSet<>();

        public Context(Cache<String, List<Fk>> cache, Map<String, Set<String>> dependencyGraph) {
            this.cache = cache;
            this.dependencyGraph = dependencyGraph;
        }

        public void setFkConsumed(Fk fk) {
            usedFks.add(fk);
        }

        public boolean isFkConsumed(Fk fk) {
            return (usedFks.contains(fk));
        }

        /** Called on top level of the recursion (can reinit data structures) */
        public void reinit() {
            usedFks.clear();
            visitedTables.clear();
        }
    }


    static abstract class TemplatePart {
        String getResult(Context context) {
            return getResult(context);
        }

        abstract String getResult(String tableName, Context context, int level);

    }

    @ToString
    static class Constant extends TemplatePart {
        public Constant(String string) {
            this.string = string;
        }

        String string;

        @Override
        String getResult(String tableName, Context context, int level) {
            return string;
        }
    }


    @ToString
    static class Recursion extends TemplatePart {

        public Recursion(String whileRecuringBefore, String whileRecuringAfter, String terminalPart, String sameLevelIteration) {
            this.whileRecurringBefore = whileRecuringBefore;
            this.whileRecurringAfter = whileRecuringAfter;
            this.terminalPart = terminalPart;
            this.sameLevelIteration = sameLevelIteration;
        }

        String whileRecurringBefore;
        String whileRecurringAfter;
        String terminalPart;
        String sameLevelIteration;

        @Override
        String getResult(String tableName, Context context, int level) {
            if (level == 1) {
                context.reinit();
            }

            Map<String, String> values = new HashMap<>();

            List<Fk> fks = context.cache.getIfPresent(tableName);

            values.put("table", tableName);

            if (fks != null && !fks.isEmpty() && !context.visitedTables.contains(tableName)) {
                context.visitedTables.add(tableName);
                
                String result = "";

                List<Fk> unconsumedFks = fks.stream().filter(fk -> !context.isFkConsumed(fk)).collect(Collectors.toList());

                if (!unconsumedFks.isEmpty()){

                    // first part
                    result = getCombined(tableName, context, level, values, unconsumedFks.get(0), whileRecurringBefore);

                    if (unconsumedFks.size() > 1) {
                        // treat same level iterations

                        result += unconsumedFks.stream().skip(1)
                                .map(fk -> getCombined(tableName, context, level, values, fk, sameLevelIteration)).collect(Collectors.joining(" "));
                    }
                    return result;
                }
            }

            return StringSubstitutor.substitute(terminalPart, values);
        }

        private String getCombined(String tableName, Context context, int level, Map<String, String> values, Fk fk, String usedTemplate) {
            String subtable = null;
            if (tableName.equals(fk.getFirstTable())) {
                subtable = fk.getSecondTable();

                values.put("parentKey", fk.getFirstColumn()[0]); // Todo simplified
                values.put("subKey", fk.getSecondColumn()[0]);
            } else {
                subtable = fk.getFirstTable();

                values.put("parentKey", fk.getSecondColumn()[0]); // Todo simplified
                values.put("subKey", fk.getFirstColumn()[0]);
            }
            values.put("subTable", subtable);

            String replacedBefore = StringSubstitutor.substitute(usedTemplate, values);
            String replacedAfter = StringSubstitutor.substitute(whileRecurringAfter, values);

            context.setFkConsumed(fk);
            return replacedBefore + getResult(subtable, context, level + 1) + replacedAfter;
        }
    }

}


