///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS org.oser.tools.jdbc:linked-db-rows:0.2-SNAPSHOT
//DEPS info.picocli:picocli:4.5.0
//DEPS org.postgresql:postgresql:42.2.6
import static java.lang.System.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import picocli.CommandLine.Command;
import picocli.CommandLine;
import org.oser.tools.jdbc.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import static picocli.CommandLine.*;

import java.util.List;
import java.util.concurrent.Callable;

/** experiment with jbang */
@Command(name = "QuickExport", mixinStandardHelpOptions = true, version = "QuickExport 0.1",
        description = "Exports a table and its linked tables as json (at the moment only with postgres)", showDefaultValues = true)
public class QuickExport implements Callable<Integer> {

	@Option(names = {"-u","--url"}, description = "jdbc connection-url")
    private String url = "jdbc:postgresql://localhost/demo";
	
	@Option(names = {"-t","--tableName"}, description = "table name to export")
    private String tableName = "blogpost";
	
	@Option(names = {"-p","--pkValue"}, description = "Primary key value of the root table to export")
    private String pkValue = "2";

    @Option(names = {"-l","--login"}, description = "Login name of database")
    private String username = "postgres";

    @Option(names = {"-pw","--password"}, description = "Password")
    private String password = "admin";

    @Option(names = {"--stopTablesExcluded"}, description = "Stop tables excluded, comma-separated")
    private List<String> stopTablesExcluded;

    @Option(names = {"--stopTablesIncluded"}, description = "Stop tables included, comma-separated")
    private List<String> stopTablesIncluded;


    public static void main(String... args) throws SQLException, ClassNotFoundException {
		int exitCode = new CommandLine(new QuickExport()).execute(args);
        System.exit(exitCode);
    }

	@Override
	public Integer call() throws SQLException, ClassNotFoundException, JsonProcessingException {
        out.println("Exporting table "+tableName);
		DbExporter dbExporter = new DbExporter();
		if (stopTablesExcluded != null){
            out.println("stopTablesExcluded:"+stopTablesExcluded);
		    dbExporter.getStopTablesExcluded().addAll(stopTablesExcluded);
        }
        if (stopTablesIncluded != null){
            out.println("stopTablesIncluded:"+stopTablesIncluded);
            dbExporter.getStopTablesIncluded().addAll(stopTablesIncluded);
        }

        Connection demoConnection = getConnection(url, username, password);
        Record asRecord = dbExporter.contentAsTree(demoConnection, tableName, pkValue);

        ObjectMapper mapper = Record.getObjectMapper();
        String asString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(asRecord.asJsonNode());
		out.println("Data: \n"+asString);
		return 0;
	}
	
	  public Connection getConnection(String url, String userName, String password) throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");

        Connection con = DriverManager.getConnection(url, userName, password);

        con.setAutoCommit(true);
        return con;
    }
}
