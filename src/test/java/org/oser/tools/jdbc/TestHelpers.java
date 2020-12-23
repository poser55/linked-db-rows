package org.oser.tools.jdbc;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.ToString;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.internal.jdbc.DriverDataSource;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Helper methods for testing. <\br>
 *  Allows choosing the db via the env variable <code>ACTIVE_DB</code> (default: postgres).
 *  Add new databases in <code>DB_CONFIGS_LIST</code>*/
public class TestHelpers {
    static ObjectMapper mapper = Record.getObjectMapper();


    public static OracleContainer oracleContainer = new OracleContainer("oracleinanutshell/oracle-xe-11g");

    static {
        String active_db = Objects.toString(System.getenv("ACTIVE_DB"), "postgres");
        if (active_db.equals("oracle")){
            oracleContainer.start();
        }
    }


    static List<DbBaseConfig> DB_CONFIG_LIST =
            List.of(
                    new DbBaseConfig("postgres", "org.postgresql.Driver",
                            ()-> "jdbc:postgresql://localhost/","postgres", "admin", false, Collections.EMPTY_MAP),
                    new DbBaseConfig("h2", "org.h2.Driver",
                            ()-> "jdbc:h2:mem:","sa", "", true, Map.of("sakila","false")),

                    //org.flywaydb.core.internal.license.FlywayEditionUpgradeRequiredException: Flyway Enterprise Edition
                    // or Oracle upgrade required: Oracle 11.2 is no longer supported by Flyway Community Edition, but
                    // still supported by Flyway Enterprise Edition.
                    new DbBaseConfig("oracle", "oracle.jdbc.driver.OracleDriver",
                            ()-> oracleContainer.getJdbcUrl(), oracleContainer.getUsername(), oracleContainer.getPassword(), true, Map.of("sakila","false")));

        // add here also container dbs

//    DriverDataSource ds = new DriverDataSource(TestContainerTest.class.getClassLoader(),
//            "", "jdbc:tc:postgresql:12.3:///" + dbName + "?TC_DAEMON=true", "postgres", "admin");




    static Map<String, DbBaseConfig> DB_CONFIGS =
            DB_CONFIG_LIST.stream().collect(Collectors.toMap(DbBaseConfig::getShortname, Function.identity()));

    static String activeDB = Objects.toString(System.getenv("ACTIVE_DB"), "postgres");

    static Set<String> firstTimeForEachDb = new HashSet<>();

    /** side-effects: inits db if necessary (the first time only, inits all dbNames with all sql scripts - for now) */
    public static Connection getConnection(String dbName) throws SQLException, ClassNotFoundException {
        DbBaseConfig baseConfig = Objects.requireNonNullElse(DB_CONFIGS.get(activeDB), DB_CONFIG_LIST.get(0));

        boolean initDbNow = false;

        if (!firstTimeForEachDb.contains(dbName)) {
            System.out.println("activeDb:"+baseConfig);
            if (baseConfig.isInitDb()) {
                initDbNow = true;
            }
            firstTimeForEachDb.add(dbName);
        }

        Class.forName(baseConfig.driverName);

        DriverDataSource ds = new DriverDataSource(TestHelpers.class.getClassLoader(),
                baseConfig.driverName, baseConfig.getUrlPrefix() + /*todo: rm for oracle */ dbName, baseConfig.getDefaultUser(), baseConfig.defaultPassword);

        Connection con = ds.getConnection();
        con.setAutoCommit(true);

        if (initDbNow) {
            //  add here placeholders such a h2_exclude_start, postgres_include_end
            Map<String, String> placeholdersMap = new HashMap<>();
            placeholdersMap.put(baseConfig.shortname+"_include_start", "");
            placeholdersMap.put(baseConfig.shortname+"_include_end", "");
            for (DbBaseConfig config : DB_CONFIG_LIST) {
                if (!config.getShortname().equals(baseConfig.shortname)) {
                    placeholdersMap.put(baseConfig.shortname+"_exclude_start", "/*");
                    placeholdersMap.put(baseConfig.shortname+"_exclude_end", "*/");
                }
            }

            Flyway flyway = Flyway.configure().placeholders(placeholdersMap).dataSource(ds).load();
            flyway.migrate();

            baseConfig.getSysProperties().forEach(System::setProperty);
        }

        // db console at  http://localhost:8082/
       // Server webServer = Server.createWebServer("-webAllowOthers", "-webPort", "8082", "-webAdminPassword", "admin").start();

        return con;
    }

    @Getter
    @ToString
    static class DbBaseConfig {
        public DbBaseConfig(String shortname, String driverName, Supplier<String> urlPrefix, String defaultUser, String defaultPassword, boolean initDb, Map<String, String> sysProperties) {
            this.shortname = shortname;
            this.driverName = driverName;
            this.urlPrefix = urlPrefix;
            this.defaultUser = defaultUser;
            this.defaultPassword = defaultPassword;
            this.initDb = initDb;
            this.sysProperties = sysProperties;
        }

        String shortname;
        String driverName;
        /**
         * prefix without db name
         */
        Supplier<String> urlPrefix;
        String defaultUser;
        String defaultPassword;
        private boolean initDb;
        private Map<String, String> sysProperties;

        public String getUrlPrefix(){
            return urlPrefix.get();
        }
    }


    /**
     * Makes a full test with a RowLink
     */
    public static BasicChecksResult testExportImportBasicChecks(Connection demoConnection, String tableName, Object primaryKeyValue, int numberNodes) throws Exception {
        return testExportImportBasicChecks(demoConnection, null, null, tableName, primaryKeyValue, numberNodes);
    }

    public static BasicChecksResult testExportImportBasicChecks(Connection demoConnection, Consumer<DbExporter> optionalExporterConfigurer, Consumer<DbImporter> optionalImporterConfigurer, String tableName, Object primaryKeyValue, int numberNodes) throws Exception {
        return testExportImportBasicChecks(demoConnection, optionalExporterConfigurer, optionalImporterConfigurer, null, new HashMap<>(), tableName, primaryKeyValue, numberNodes
        );
    }

    /**
     * Makes a full test with a RowLink <br/>
     * Allows to configure the DbImporter/ DbExporter
     */
    public static BasicChecksResult testExportImportBasicChecks(Connection connection, Consumer<DbExporter> optionalExporterConfigurer, Consumer<DbImporter> optionalImporterConfigurer, Consumer<Record> optionalRecordChanger, Map<RowLink, Object> remapping, String tableName, Object primaryKeyValue, int numberNodes) throws Exception {
        Map<String, Integer> before = JdbcHelpers.getNumberElementsInEachTable(connection);


        DbExporter dbExporter = new DbExporter();
        if (optionalExporterConfigurer != null) {
            optionalExporterConfigurer.accept(dbExporter);
        }
        Record asRecord = dbExporter.contentAsTree(connection, tableName, primaryKeyValue);
        String asString = asRecord.asJson();

        System.out.println("export as json2:" + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(asRecord.asJsonNode()));
        // this assertion was dropped, as the old .asJson() method handles int types wrong (it quotes them in some cases)
        //assertEquals(mapper.readTree(asRecord.asJson()).toString(), asRecord.asJsonNode().toString());

        DbImporter dbImporter = new DbImporter();
        if (optionalImporterConfigurer != null) {
            optionalImporterConfigurer.accept(dbImporter);
        }
        Record asRecordAgain = dbImporter.jsonToRecord(connection, tableName, asString);
        String asStringAgain = asRecordAgain.asJson();

        assertEquals(numberNodes, asRecord.getAllNodes().size());
        assertEquals(numberNodes, asRecordAgain.getAllNodes().size());

        // still subtle differences in asString operation
        assertEquals(canonicalize(asString), canonicalize(asStringAgain));

        if (optionalRecordChanger != null) {
            optionalRecordChanger.accept(asRecordAgain);
        }

        // todo: bug: asRecord is missing columnMetadata/ other values
        Map<RowLink, Object> rowLinkObjectMap = dbImporter.insertRecords(connection, asRecordAgain, remapping);

        Map<String, Integer> after = JdbcHelpers.getNumberElementsInEachTable(connection);
        assertNumberInserts(before, after, numberNodes);

        return new BasicChecksResult(asRecord, asString, asRecordAgain, asStringAgain, rowLinkObjectMap);
    }

    private static void assertNumberInserts(Map<String, Integer> before, Map<String, Integer> after, int numberNodes) {
        int numberRowsBefore = before.values().stream().reduce((l, r) -> l+r).get();
        int numberRowsAfter = after.values().stream().reduce((l, r) -> l+r).get();
        assertEquals(numberNodes, numberRowsAfter -numberRowsBefore);
    }

    private static String canonicalize(String asString) {
        return asString.toLowerCase()
                // remove precision differences
                .replace(".000,", ",")
                .replace(".0,", ",");
    }

    @Getter
    public static class BasicChecksResult {
        private final Record asRecord;
        private final String asString;
        private final Record asRecordAgain;
        private final String asStringAgain;
        private final Map<RowLink, Object> rowLinkObjectMap;

        public BasicChecksResult(Record asRecord, String asString, Record asRecordAgain, String asStringAgain, Map<RowLink, Object> rowLinkObjectMap) {
            this.asRecord = asRecord;
            this.asString = asString;
            this.asRecordAgain = asRecordAgain;
            this.asStringAgain = asStringAgain;
            this.rowLinkObjectMap = rowLinkObjectMap;
        }
    }

    public static void initLogback() {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger rootLogger = lc.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.INFO);
    }

    // todo make more generic, move globally
    public static void setLoggerLevel(EnumSet<DbImporter.Loggers> loggers, ch.qos.logback.classic.Level newLevel) {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        for (DbImporter.Loggers logger : loggers) {
            ch.qos.logback.classic.Logger rootLogger = lc.getLogger(DbImporter.class + "." + logger.name());
            rootLogger.setLevel(newLevel);
        }
    }

}
