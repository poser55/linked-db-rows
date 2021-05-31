///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS org.oser.tools.jdbc:linked-db-rows:0.5-SNAPSHOT
//DEPS info.picocli:picocli:4.5.0
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
@Command(name = "JsonImport", mixinStandardHelpOptions = true, version = "JsonImport 0.2",
        description = "Imports a table and its linked tables from JSON.", showDefaultValues = true)
public class JsonImport implements Callable<Integer> {

	@Option(names = {"-u","--url"}, description = "jdbc connection-url")
    private String url = "jdbc:postgresql://localhost/demo";
	
	@Option(names = {"-t","--tableanName"}, description = "Table name to import")
    private String tableName = "blogpost";

    @Option(names = {"-l","--login"}, description = "Login name of database")
    private String username = "postgres";

    @Option(names = {"-pw","--password"}, description = "Password")
    private String password = "admin";

    @Option(names = {"-j","--jsonFile"}, required = true, description = "Name of the file with the JSON content.")
    private String jsonFile;

    @Option(names = {"-db"}, description = "What jdbc driver to use? (default:postgres) ")
    private String  databaseShortName = "postgres";

    @Option(names = {"-e","--exclude-fields"}, required = false, description = "Name of fields to be excluded.")
    private List<String> excludedFields;

    public static void main(String... args) throws SQLException, ClassNotFoundException {
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
