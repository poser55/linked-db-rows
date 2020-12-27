package org.oser.tools.jdbc;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/** Poor man's flyway */
public class ExecuteDbScriptFiles {


    /** simpler than flyway (but also free with oracle) */
    public static void executeDbScriptFiles(String sqlDirectory, Connection connection, Map<String, String> placeholders) throws IOException, SQLException {
        try (Stream<Path> walk = Files.walk(Path.of(sqlDirectory))) {

            List<String> sqlFiles = walk.filter(Files::isRegularFile).filter(f -> f.getFileName().toString().endsWith(".sql"))
                    .map(Path::toString).sorted().collect(toList());

            for (String fileName : sqlFiles) {
                executeSqlFile(connection, fileName, placeholders);
            }
        }
    }

    /** Insert one file into db (take into account comments, placeholders, split into individual commands, throws first error) */
    public static void executeSqlFile(Connection connection, String fileName, Map<String, String> placeholders) throws SQLException, IOException {
        try (Statement stmt = connection.createStatement()) {
            String sql = new String(Files.readAllBytes(Paths.get(fileName)));

            String sqlRaw = replacePlaceholders(sql, placeholders);

            sqlRaw = removeSqlComments(sqlRaw);

            sqlRaw = removeComments(sqlRaw);

            // treat each line
            List<Optional<?>> optionalIssues = Arrays.stream(sqlRaw.split(";")).map(String::trim).filter(line -> !line.isEmpty()).map(oneStatements -> {
                try {
                    stmt.execute(oneStatements);
                    return Optional.empty();
                } catch (SQLException throwable) {
                    return Optional.of(throwable);
                }
            }).collect(toList());

            List<?> issues = optionalIssues.stream().flatMap(Optional::stream).collect(toList());
            System.out.println("executed: "+fileName+" #lines:"+optionalIssues.size()+" #issues: "+ issues.size() +((issues.size() > 0)?(" \n Issues:\n"+issues):""));
            if (issues.size() > 0) {
                throw ((SQLException) issues.get(0));
            }
        }
    }


    private static String replacePlaceholders(String sql, Map<String, String> placeholders) {
        for (Map.Entry<String, String> entry : placeholders.entrySet()) {
            sql = sql.replace("${"+entry.getKey() +"}", entry.getValue());
        }
        return sql;
    }

    public static String removeSqlComments(String sql) {
        return Arrays.stream(sql.split("\n")).
                map(line -> (line.contains("--") ? line.substring(0, line.indexOf("--")) : line)).collect(joining("\n"));
    }


    /** Remove java-like comments (does not handle /* in strings :-( )
     * source: adapted from https://stackoverflow.com/a/23334050/4558800 */
    static String removeComments(String code) {
        StringBuilder newCode = new StringBuilder();
        try (StringReader sr = new StringReader(code)) {
            boolean inBlockComment = false;
            boolean inLineComment = false;
            boolean out = true;

            int prev = sr.read();
            int cur;
            for(cur = sr.read(); cur != -1; cur = sr.read()) {
                if(inBlockComment) {
                    if (prev == '*' && cur == '/') {
                        inBlockComment = false;
                        out = false;
                    }
                } else if (inLineComment) {
                    if (cur == '\r') { // start untested block
                        sr.mark(1);
                        int next = sr.read();
                        if (next != '\n') {
                            sr.reset();
                        }
                        inLineComment = false;
                        out = false; // end untested block
                    } else if (cur == '\n') {
                        inLineComment = false;
                        out = false;
                    }
                } else {
                    if (prev == '/' && cur == '*') {
                        sr.mark(1); // start untested block
                        int next = sr.read();
                        if (next != '*') {
                            inBlockComment = true; // tested line (without rest of block)
                        }
                        sr.reset(); // end untested block
                    } else if (prev == '/' && cur == '/') {
                        inLineComment = true;
                    } else if (out){
                        newCode.append((char)prev);
                    } else {
                        out = true;
                    }
                }
                prev = cur;
            }
            if (prev != -1 && out && !inLineComment) {
                newCode.append((char)prev);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return newCode.toString();
    }

    // some usage demos
    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        //executeDbScriptFiles(".\\src\\test\\resources\\db\\migration\\", TestHelpers.getConnection("demo"));

        String s = "abc -- xxx\n uvw \n \n \n n -- yyy";
        System.out.println(removeSqlComments(s));

        Map<String, String> placeholders = Map.of ("a", "AAA", "d", "D", "u", "UUU");
        System.out.println(replacePlaceholders(" ${a} } bbb ccc ${d} ", placeholders));


        System.out.println(removeComments("/* aaa */ '/* */' aadfda \n /* adsfasdf */ -- /* \n */ - adsfa "));

    }

}
