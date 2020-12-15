///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS org.oser.tools.jdbc:linked-db-rows:0.1-SNAPSHOT
//DEPS info.picocli:picocli:4.5.0
//DEPS org.postgresql:postgresql:42.2.6
import static java.lang.System.*;
import picocli.CommandLine.Command;
import picocli.CommandLine;
import org.oser.tools.jdbc.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import static picocli.CommandLine.*;
import java.util.concurrent.Callable;

/** experiment with jbang
 *  todo: complete with more flags */
@Command(name = "QuickExport", mixinStandardHelpOptions = true, version = "QuickExport 0.1",
        description = "exporting a table as json", showDefaultValues = true)
public class QuickExport implements Callable<Integer> {

	@Option(names = {"-u","--url"}, description = "jdbc connection-url")
    private String url = "jdbc:postgresql://localhost/demo";
	
	@Option(names = {"-t","--tableName"}, description = "table name to export")
    private String tableName = "blogpost";
	
	@Option(names = {"-p","--pkValue"}, description = "Pk value")
    private String pkValue = "2";

    public static void main(String... args) throws SQLException, ClassNotFoundException {
		int exitCode = new CommandLine(new QuickExport()).execute(args);
        System.exit(exitCode);
    }

	@Override
	public Integer call() throws SQLException, ClassNotFoundException {
        out.println("Exporting table "+tableName);
		DbExporter dbExporter = new DbExporter();
		Connection demoConnection = getConnection(url);
        Record asRecord = dbExporter.contentAsTree(demoConnection, tableName, pkValue);
        String asString = asRecord.asJson();
		out.println("Data: \n"+asString);
		return 0;
	}
	
	  public Connection getConnection(String url) throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");

        Connection con = DriverManager.getConnection(url, "postgres", "admin");

        con.setAutoCommit(true);
        return con;
    }
}
