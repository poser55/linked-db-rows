///usr/bin/env jbang "$0" "$@" ; exit $?
////REPOS local=file:///Users/phil/mavenrepo
//DEPS org.oser.tools.jdbc:linked-db-rows:0.14
//DEPS info.picocli:picocli:4.7.6
//DEPS ch.qos.logback:logback-classic:1.5.8
//DEPS guru.nidi:graphviz-java:0.18.1
//DEPS org.postgresql:postgresql:42.7.4
import static java.lang.System.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import picocli.CommandLine.Command;
import picocli.CommandLine;
import org.oser.tools.jdbc.*;
import org.oser.tools.jdbc.cli.DynJarLoader;
import org.oser.tools.jdbc.cli.ExecuteDbScriptFiles;
import org.oser.tools.jdbc.graphviz.RecordAsGraph;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.engine.Format;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import static picocli.CommandLine.*;
import java.io.File;

import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;



/** Json export via script, needs https://www.jbang.dev/
 *  call 'jbang JsonExport.java -h' for help */
@Command(name = "JsonExport", mixinStandardHelpOptions = true, version = "JsonExport 0.7",
        description = "Exports a table and its linked tables as JSON. Writes the JSON to stdout (other output to stderr), you can use > myFile.json to get it in a file.", showDefaultValues = true)
public class JsonExport implements Callable<Integer> {

	@Option(names = {"-u","--url"}, description = "jdbc connection-url")
    private String url = "jdbc:postgresql://localhost/demo";
	
	@Option(names = {"-t","--tableName"}, required = true, description = "table name to export")
    private String tableName;
	
	@Option(names = {"-p","--pkValue"}, required = true, description = "Primary key value of the root table to export")
    private String pkValue;

    @Option(names = {"-l","--login"}, description = "Login name of database")
    private String username = "postgres";

    @Option(names = {"-pw","--password"}, description = "Password")
    private String password = "admin";

    @Option(names = {"--stopTablesExcluded"}, description = "Stop tables excluded, comma-separated")
    private List<String> stopTablesExcluded;

    @Option(names = {"--stopTablesIncluded"}, description = "Stop tables included, comma-separated")
    private List<String> stopTablesIncluded;

    @Option(names = {"--oneStopTablesIncluded"}, description = "If one these tables occurs in collecting the graph (with depth first search), we stop before we collect the 2nd instance.\n" +
            "The goal is to follow the FKs of these stop tables as well (but not collect subsequent instances). This is experimental")
    private List<String> stopTablesIncludeOne;

    @Option(names = {"--canon"}, description = "Should we canonicalize the primary keys of the output? (default: false)")
    private boolean doCanonicalize = false;

    @Option(names = {"-db"}, description = "What jdbc driver to use? (options: postgres, h2, hsqldb, mysql, sqlserver, oracle) ")
    private String  databaseShortName = "postgres";

    @Option(names = {"-fks"}, description = "Virtual foreign key configurations. " +
            "Example: 'user_table(id)-preferences(user_id)'  " +
            "This sets a foreign key from table user_table to the preferences table, id is the FK column in user_table, " +
            "user_id is the FK id in preferences. Use ';' to separate multiple FKs.")
    private String fks;

    @Option(names = {"--log"}, description = "What to log (change, select, delete, all)")
    private List<String> logs;

    @Option(names = {"--sqlScript"}, description = "SQL file name to execute before exporting (useful for tests)")
    private String sqlScriptFileName;

    @Option(names = {"--diagram"}, description = "Generate a graphviz png diagram from the exported graph with this name (show generated output if no .png suffix). ")
    private String optionalPngDiagramName;


    public static void main(String... args) throws SQLException, ClassNotFoundException {
		int exitCode = new CommandLine(new JsonExport()).execute(args);
        System.exit(exitCode);
    }

	@Override
	public Integer call() throws SQLException, ClassNotFoundException, JsonProcessingException, Exception {
        err.println("Exporting table "+tableName);
		DbExporter dbExporter = new DbExporter();
		if (stopTablesExcluded != null){
            err.println("stopTablesExcluded:"+stopTablesExcluded);
		    dbExporter.getStopTablesExcluded().addAll(stopTablesExcluded);
        }
        if (stopTablesIncluded != null){
            err.println("stopTablesIncluded:"+stopTablesIncluded);
            dbExporter.getStopTablesIncluded().addAll(stopTablesIncluded);
        }

        if (stopTablesIncludeOne != null){
            err.println("stopTablesIncludeOne:"+stopTablesIncludeOne);
            dbExporter.getStopTablesIncludeOne().addAll(stopTablesIncludeOne);
        }

        Connection connection = DynJarLoader.getConnection(databaseShortName, url, username, password, this.getClass().getClassLoader());
        if (connection == null) {
            err.println("Could not get jdbc connection for:"+databaseShortName);
            return -1;
        }

        if (logs != null) {
            Loggers.enableLoggers(Loggers.stringListToLoggerSet(logs));
        }

        optionalInitDb(connection, sqlScriptFileName);

        if (fks != null) {
            System.err.println("Virtual foreign keys:"+fks);
            Fk.addVirtualForeignKeyAsString(connection, dbExporter, fks);
        }

        DbRecord asRecord = dbExporter.contentAsTree(connection, tableName, pkValue);

        if (doCanonicalize) {
            RecordCanonicalizer.canonicalizeIds(connection, asRecord, dbExporter.getFkCache(), dbExporter.getPkCache());
        }

        if (optionalPngDiagramName != null) {
            // disable log of graphviz integration
            disableGraphvizLogging();

            RecordAsGraph asGraph = new RecordAsGraph();
            System.err.println("Saving graph of export:" + optionalPngDiagramName);
            MutableGraph graph = asGraph.recordAsGraph(connection, asRecord);

            if (optionalPngDiagramName.endsWith(".png")) {
                asGraph.renderGraph(graph, Format.PNG, new File(optionalPngDiagramName));
            } else {
                // somehow error for parsing here?
               // System.err.println("Dotfile:\n" + RecordAsGraph.renderGraphAsDotFile(graph));
            }
        }
        ObjectMapper mapper = DbRecord.getObjectMapper();
        String asString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(asRecord.asJsonNode());
		err.println("Data: \n");
		out.println(asString);
		return 0;
	}

    private void disableGraphvizLogging() {
        ch.qos.logback.classic.Logger concreteLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("guru.nidi.graphviz");
        ch.qos.logback.classic.Level logbackLevel = ch.qos.logback.classic.Level.valueOf("WARN");
        if (concreteLogger != null) {
            concreteLogger.setLevel(logbackLevel);
        }
    }

    void optionalInitDb(Connection connection, String sqlScriptFileName) throws Exception {
        if (sqlScriptFileName == null) {
            return;
        }
        err.println("Initializing Database with SQL script:"+sqlScriptFileName);
        try {
            ExecuteDbScriptFiles.executeSqlFile(connection, sqlScriptFileName, new HashMap());
        } catch (Exception e) {
            err.println("Error when initializing the script - ignoring it:");
            e.printStackTrace(); // ignore errors
        }
    }

}
