///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS org.oser.tools.jdbc:linked-db-rows:0.10-SNAPSHOT
//DEPS info.picocli:picocli:4.5.0
//DEPS ch.qos.logback:logback-classic:1.2.3
import static java.lang.System.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import picocli.CommandLine.Command;
import picocli.CommandLine;
import org.oser.tools.jdbc.*;
import org.oser.tools.jdbc.cli.DynJarLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import static picocli.CommandLine.*;
import java.nio.file.Files;
import java.nio.file.Path;

import java.util.List;
import java.util.concurrent.Callable;

/** Json import via script, needs https://www.jbang.dev/
 *  call 'jbang JsonImport.java -h' for help */
@Command(name = "JsonImport", mixinStandardHelpOptions = true, version = "JsonImport 0.7",
        description = "Imports a table and its linked tables from JSON.", showDefaultValues = true)
public class JsonImport implements Callable<Integer> {

	@Option(names = {"-u","--url"}, description = "jdbc connection-url")
    private String url = "jdbc:postgresql://localhost/demo";
	
	@Option(names = {"-t","--tableanName"}, required = true, description = "Table name to import")
    private String tableName;

    @Option(names = {"-l","--login"}, description = "Login name of database")
    private String username = "postgres";

    @Option(names = {"-pw","--password"}, description = "Password")
    private String password = "admin";

    @Option(names = {"-j","--jsonFile"}, required = true, description = "Name of the file with the JSON content.")
    private String jsonFile;

    @Option(names = {"-db"}, description = "What jdbc driver to use? (options: postgres, h2, hsqldb, mysql, sqlserver, oracle) ")
    private String  databaseShortName = "postgres";

    @Option(names = {"-e","--exclude-fields"}, required = false, description = "Name of fields to be excluded.")
    private List<String> excludedFields;

    @Option(names = {"-fks"}, description = "Virtual foreign key configurations. " +
            "Example: 'user_table(id)-preferences(user_id)'  " +
            "This sets a foreign key from table user_table to the preferences table, id is the FK column in user_table, " +
            "user_id is the FK id in preferences. Use ';' to separate multiple FKs;")
    private String fks;

    @Option(names = {"--log"}, description = "What to log (change, select, delete, all)")
    private List<String> logs;

    public static void main(String... args) throws SQLException, ClassNotFoundException {
        Loggers.disableDefaultLogs();
		int exitCode = new CommandLine(new JsonImport()).execute(args);
        System.exit(exitCode);
    }

	@Override
	public Integer call() throws SQLException, ClassNotFoundException, JsonProcessingException, Exception {
        out.println("Importing table "+tableName);
		DbImporter dbImporter = new DbImporter();

        Connection dbConnection = DynJarLoader.getConnection(databaseShortName, url, username, password, this.getClass().getClassLoader());
        if (dbConnection == null) {
            err.println("Could not get jdbc connection for:"+databaseShortName);
            return -1;
        }

        if (logs != null) {
            Loggers.enableLoggers(Loggers.stringListToLoggerSet(logs));
        }

        if (fks != null) {
            System.out.println("Virtual foreign keys:"+fks);
            Fk.addVirtualForeignKeyAsString(dbConnection, dbImporter, fks);
        }

        String json = "";
        Path fileName = Path.of(jsonFile);
        try {
            json = Files.readString(fileName);
        } catch (Exception e) {
            System.err.println("Issue in reading json file"+e);
            return -2;
        }

        if (excludedFields != null) {
            excludedFields.forEach(f -> dbImporter.registerFieldImporter(null, f, FieldImporter.NOP_FIELDIMPORTER));
        }

        dbImporter.insertRecords(dbConnection, dbImporter.jsonToRecord(dbConnection, tableName, json));
        return 0;
	}

}
