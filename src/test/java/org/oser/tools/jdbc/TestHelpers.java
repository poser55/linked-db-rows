package org.oser.tools.jdbc;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Getter;
import lombok.ToString;
import org.flywaydb.core.internal.jdbc.DriverDataSource;
import org.h2.tools.Server;
import org.oser.tools.jdbc.cli.ExecuteDbScriptFiles;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.OracleContainer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
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
 *  Allows choosing the db via the env variable <code>ACTIVE_DB</code> (default: h2).
 *  Add new databases in <code>DB_CONFIGS_LIST</code>*/
public class TestHelpers {
    static ObjectMapper mapper = Record.getObjectMapper();


    public static OracleContainer oracleContainer = new OracleContainer("oracleinanutshell/oracle-xe-11g");

        // the following should make mysql - on linux - not case sensitive for table names, but the container cannot be started
        // (whether a mysql db is case sensitive or not is usually dependent on the operating system it runs on!)
        //.withConfigurationOverride("mysqlconfig/mysql.cnf");

    public static MSSQLServerContainer mssqlserver = new MSSQLServerContainer()
            .acceptLicense();

    static {
        try {
            String active_db = Objects.toString(System.getenv("ACTIVE_DB"), "h2");
            if (active_db.equals("oracle")){
                oracleContainer.start();
            }
            if (active_db.equals("sqlserver")){
                mssqlserver.start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    static List<DbConfig> DB_CONFIG_LIST =
            List.of(
                    new DbConfig("h2", "org.h2.Driver",
                            ()-> "jdbc:h2:mem:","sa", "", true, Map.of("sakila","false", "h2", "true")),

                    // old local postgres
                    new DbConfig("postgres_local", "org.postgresql.Driver",
                            ()-> "jdbc:postgresql://localhost/","postgres", "admin", false, Collections.EMPTY_MAP),

                    new DbConfig("postgres", "org.testcontainers.jdbc.ContainerDatabaseDriver",
                            ()-> "jdbc:tc:postgresql:14.1:///","postgres", "admin", true, Collections.EMPTY_MAP),

                    new DbConfig("oracle", "oracle.jdbc.driver.OracleDriver",
                            ()-> oracleContainer.getJdbcUrl(), oracleContainer.getUsername(), oracleContainer.getPassword(), true,
                            Map.of("sakila","false")).disableAppendDbName(),
                    new DbConfig("mysql", "org.testcontainers.jdbc.ContainerDatabaseDriver",
                            ()-> "jdbc:tc:mysql:5.7.34://localhost/demo", "user", "passswd", true,
                            Map.of("sakila","false", "sequences", "false","mixedCaseTableNames", "false")).disableAppendDbName(),
                    new DbConfig("sqlserver", mssqlserver.getDriverClassName(),
                            ()-> mssqlserver.getJdbcUrl(), mssqlserver.getUsername(), mssqlserver.getPassword(), true,
                            Map.of("sakila","false", "sequences", "false")).disableAppendDbName(),
                    new DbConfig("hsqldb", "org.hsqldb.jdbcDriver",
                            ()-> "jdbc:hsqldb:mem:demo", "SA", "", true,
                            Map.of("sakila","false", "sequences", "false")).disableAppendDbName());


//    DriverDataSource ds = new DriverDataSource(TestContainerTest.class.getClassLoader(),
//            "", "jdbc:tc:postgresql:12.3:///" + dbName + "?TC_DAEMON=true", "postgres", "admin");


    static Map<String, DbConfig> DB_CONFIGS =
            DB_CONFIG_LIST.stream().collect(Collectors.toMap(DbConfig::getShortname, Function.identity()));

    static String activeDB = Objects.toString(System.getenv("ACTIVE_DB"), "h2");

    static Set<String> firstTimeForEachDb = new HashSet<>();

    private static final Cache<String, Connection> connectionCache = Caffeine.newBuilder()
            .maximumSize(10).build();

    /** side-effects: inits db if necessary (the first time only, inits all dbNames with all sql scripts - for now) $
     *    uses a cache (to allow re-configuring db connection in the sql script) */
    public static Connection getConnection(String dbName) throws SQLException, ClassNotFoundException, IOException {
        Connection connection = connectionCache.getIfPresent(dbName);

        if (connection == null || connection.isClosed()) {
            connection = internalGetConnection(dbName);
            connectionCache.put(dbName, connection);
        }

        return connection;
    }

    static void clearCache(){
        // todo: we might need to close the connection as well (at least for h2?)
        connectionCache.invalidateAll();
        firstTimeForEachDb.clear();
    }

    /** side-effects: inits db if necessary (the first time only, inits all dbNames with all sql scripts - for now) */
    static Connection internalGetConnection(String dbName) throws SQLException, ClassNotFoundException, IOException {
        DbConfig baseConfig = getDbConfig();

        return internalGetConnection(dbName, baseConfig, true);
    }

        /** side-effects: inits db if necessary (the first time only, inits all dbNames with all sql scripts - for now) */
    static Connection internalGetConnection(String dbName, DbConfig baseConfig, boolean useCache) throws SQLException, ClassNotFoundException, IOException {
        boolean initDbNow = false;

        if (!firstTimeForEachDb.contains(dbName) || !useCache) {
            System.out.println("activeDb:"+baseConfig);
            if (baseConfig.isInitDb()) {
                initDbNow = true;
            }
            if (useCache) {
                firstTimeForEachDb.add(dbName);
            }
        }
        if (runningOnOsX()) {
            // strange issue: only in mvn on osx the init cache detection does not seem to work
            //  also mvn debugging does not halt on my breakpoints (only in some JDK code?)
            initDbNow = true;
        }

        Class.forName(baseConfig.driverName, true, Thread.currentThread().getContextClassLoader());

        DriverDataSource ds = new DriverDataSource(Thread.currentThread().getContextClassLoader(),
                baseConfig.driverName, baseConfig.getUrlPrefix(dbName), baseConfig.getDefaultUser(), baseConfig.defaultPassword);

        Connection con = ds.getConnection();
        con.setAutoCommit(true);

        if (initDbNow) {
            //  add here placeholders such a h2_exclude_start, postgres_include_end
            Map<String, String> placeholdersMap = new HashMap<>();
            placeholdersMap.put(baseConfig.shortname+"_include_start", "");
            placeholdersMap.put(baseConfig.shortname+"_include_end", "");
            placeholdersMap.put(baseConfig.shortname+"_exclude_start", "/*");
            placeholdersMap.put(baseConfig.shortname+"_exclude_end", "*/");

            for (DbConfig config : DB_CONFIG_LIST) {
                if (!config.getShortname().equals(baseConfig.shortname)) {
                    placeholdersMap.put(config.getShortname()+"_exclude_start", "");
                    placeholdersMap.put(config.getShortname()+"_exclude_end", "");
                    placeholdersMap.put(config.getShortname()+"_include_start", "/*");
                    placeholdersMap.put(config.getShortname()+"_include_end", "*/");
                }
            }
            ExecuteDbScriptFiles.executeDbScriptFiles( "./src/test/resources/db/migration/", con, placeholdersMap);

            //initWithFlyway(baseConfig, ds, placeholdersMap);

            baseConfig.getSysProperties().forEach(System::setProperty);
        }



        // db console at  http://localhost:8082/
        try {
            Server webServer = Server.createWebServer("-webAllowOthers", "-webPort", "8082", "-webAdminPassword", "admin").start();
        } catch (Exception e) {
            // ignore
            System.out.println("H2 console not opened "+ e);
        }

        return con;
    }

    private static boolean runningOnOsX() {
        String osName = System.getProperty("os.name").toLowerCase();
        return osName.startsWith("mac os x");
    }

    public static DbConfig getDbConfig() {
        return Objects.requireNonNullElse(DB_CONFIGS.get(activeDB), DB_CONFIG_LIST.get(0));
    }

//    public static void initWithFlyway(DbConfig baseConfig, DriverDataSource ds, Map<String, String> placeholdersMap) {
//        Flyway flyway = Flyway.configure().placeholders(placeholdersMap).dataSource(ds).load();
//        flyway.migrate();
//    }

    @Getter
    @ToString
    static class DbConfig {
        private boolean appendDbName = true;

        public DbConfig(String shortname, String driverName, Supplier<String> urlPrefix, String defaultUser, String defaultPassword, boolean initDb, Map<String, String> sysProperties) {
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

        public String getUrlPrefix(String dbName){
            return urlPrefix.get() +  (appendDbName ? dbName : "");
        }

        public DbConfig disableAppendDbName() {
            appendDbName = false;
            return this;
        }
    }


    /**
     * Makes a full test with a RowLink
     */
    public static BasicChecksResult testExportImportBasicChecks(Connection demoConnection, String tableName, Object primaryKeyValue, int numberNodes) throws Exception {
        return testExportImportBasicChecks(demoConnection, null, null, tableName, primaryKeyValue, numberNodes);
    }

    public static BasicChecksResult testExportImportBasicChecks(Connection demoConnection, Consumer<DbExporter> optionalExporterConfigurer,
                                                                Consumer<DbImporter> optionalImporterConfigurer,
                                                                String tableName, Object primaryKeyValue, int numberNodes) throws Exception {
        return testExportImportBasicChecks(demoConnection, optionalExporterConfigurer, optionalImporterConfigurer, null, new HashMap<>(), tableName, primaryKeyValue, numberNodes
        ,true);
    }

    /**
     * Makes a full test with a table and its primary keys (export and import again, with many checks) <p>
     * Allows to configure the DbImporter/ DbExporter/ Record/ Remappings
     */
    public static BasicChecksResult testExportImportBasicChecks(Connection connection, Consumer<DbExporter> optionalExporterConfigurer,
                                                                Consumer<DbImporter> optionalImporterConfigurer, Consumer<Record> optionalRecordChanger,
                                                                Map<RowLink, DbImporter.Remap> remapping, String tableName, Object primaryKeyValue,
                                                                int numberNodes, boolean checkUpdates) throws Exception {
        List<String> testSchemas = getDefaultSchemaPlusDoc(connection);
        Map<String, Integer> before = JdbcHelpers.getNumberElementsInEachTable(connection, testSchemas);


        DbExporter dbExporter = new DbExporter();
        if (optionalExporterConfigurer != null) {
            optionalExporterConfigurer.accept(dbExporter);
        }
        Record asRecord = dbExporter.contentAsTree(connection, tableName, primaryKeyValue);
        String asString = asRecord.asJsonNode().toString();

        System.out.println("export as json2:" + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(asRecord.asJsonNode()));
        // this assertion was dropped, as the old .asJsonNode().toString() method handles int types wrong (it quotes them in some cases)
        //assertEquals(mapper.readTree(asRecord.asJsonNode().toString()).toString(), asRecord.asJsonNode().toString());

        DbImporter dbImporter = new DbImporter();
        if (optionalImporterConfigurer != null) {
            optionalImporterConfigurer.accept(dbImporter);
        }
        Record asRecordAgain = dbImporter.jsonToRecord(connection, tableName, asString);
        String asStringAgain = asRecordAgain.asJsonNode().toString();

        assertEquals(numberNodes, asRecord.getAllNodes().size());
        assertEquals(numberNodes, asRecordAgain.getAllNodes().size());

        // still subtle differences in asString operation
        assertEquals(canonicalize(asString), canonicalize(asStringAgain));

        if (optionalRecordChanger != null) {
            optionalRecordChanger.accept(asRecordAgain);
        }

        // todo: bug: asRecord is missing columnMetadata/ other values
        Map<RowLink, DbImporter.Remap> rowLinkObjectMap = dbImporter.insertRecords(connection, asRecordAgain, remapping);

        Map<String, Integer> after = JdbcHelpers.getNumberElementsInEachTable(connection, testSchemas);

        if (checkUpdates) {
            // when updating, the number of inserted db entries does not increase, so we do not check it here
            assertNumberInserts(before, after, numberNodes);

            // try updating as well
            dbImporter.setForceInsert(false);
            Map<RowLink, DbImporter.Remap> rowLinkObjectMap2 = dbImporter.insertRecords(connection, asRecordAgain);

            // there should be no remappings with forceInsert
            assertEquals(0, rowLinkObjectMap2.size());
        }

        Map<RowLink, List<Object>> rowLinkListMap = RecordCanonicalizer.canonicalizeIds(connection, asRecord, dbExporter.getFkCache(), pkCache);

        return new BasicChecksResult(asRecord, asString, asRecordAgain, asStringAgain, rowLinkObjectMap, rowLinkListMap);
    }

    private static List<String> getDefaultSchemaPlusDoc(Connection connection) {
        String defaultSchema = "public";
        try {
            defaultSchema = connection.getSchema();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Arrays.asList("doc", defaultSchema);
    }

    private static void assertNumberInserts(Map<String, Integer> before, Map<String, Integer> after, int numberNodes) {
        int numberRowsBefore = before.values().stream().reduce((l, r) -> l+r).get();
        int numberRowsAfter = after.values().stream().reduce((l, r) -> l+r).get();
        assertEquals(numberNodes, numberRowsAfter - numberRowsBefore);
    }

    private static String canonicalize(String asString) {
        return asString.toLowerCase()
                // remove precision differences
                .replace(".000,", ",")
                .replace(".0,", ",");
    }

    /** Holds test results */
    @Getter
    public static class BasicChecksResult {
        private final Record asRecord;
        private final String asString;
        private final Record asRecordAgain;
        private final String asStringAgain;
        private final Map<RowLink, DbImporter.Remap> rowLinkObjectMap;
        private Map<RowLink, List<Object>> rowLinkListMap;

        public BasicChecksResult(Record asRecord,
                                 String asString,
                                 Record asRecordAgain,
                                 String asStringAgain,
                                 Map<RowLink, DbImporter.Remap> rowLinkObjectMap,
                                 Map<RowLink, List<Object>> rowLinkListMap) {
            this.asRecord = asRecord;
            this.asString = asString;
            this.asRecordAgain = asRecordAgain;
            this.asStringAgain = asStringAgain;
            this.rowLinkObjectMap = rowLinkObjectMap;
            this.rowLinkListMap = rowLinkListMap;
        }
    }

    public static void initLogback() {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger rootLogger = lc.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.INFO);
    }

    private static final Cache<String, List<String>> pkCache = Caffeine.newBuilder()
            .maximumSize(1000).build();
}
