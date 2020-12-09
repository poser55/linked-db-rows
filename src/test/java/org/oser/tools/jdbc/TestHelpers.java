package org.oser.tools.jdbc;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.ToString;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.internal.jdbc.DriverDataSource;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Helper methods for testing. <\br>
 *  Allows choosing the db via the env variable <code>ACTIVE_DB</code> (default: postgres).
 *  Add new databases in <code>DB_CONFIGS_LIST</code>*/
public class TestHelpers {
    static ObjectMapper mapper = JdbcHelpers.getObjectMapper();

    static List<DbBaseConfig> DB_CONFIG_LIST =
            List.of(
                    new DbBaseConfig("postgres", "org.postgresql.Driver",
                            "jdbc:postgresql://localhost/","postgres", "admin", false, Collections.EMPTY_MAP),
                    new DbBaseConfig("h2", "org.h2.Driver",
                            "jdbc:h2:mem:","sa", "", true,Map.of("sakila","false")));

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
                baseConfig.driverName, baseConfig.urlPrefix + dbName, baseConfig.getDefaultUser(), baseConfig.defaultPassword);

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

            baseConfig.getSysProperties().forEach((key, value) -> {
                System.getProperty(key, value);
            });
        }

        // db console at  http://localhost:8082/
       // Server webServer = Server.createWebServer("-webAllowOthers", "-webPort", "8082", "-webAdminPassword", "admin").start();

        return con;
    }

    @Getter
    @ToString
    static class DbBaseConfig {
        public DbBaseConfig(String shortname, String driverName, String urlPrefix, String defaultUser, String defaultPassword, boolean initDb, Map<String, String> sysProperties) {
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
        String urlPrefix;
        String defaultUser;
        String defaultPassword;
        private boolean initDb;
        private Map<String, String> sysProperties;
    }


    /**
     * Makes a full test with a RowLink
     */
    public static BasicChecksResult testExportImportBasicChecks(Connection demoConnection, int numberNodes,
                                                                String tableName, Object primaryKeyValue) throws Exception {
        return testExportImportBasicChecks(demoConnection, numberNodes, null, null, tableName, primaryKeyValue);
    }

    public static BasicChecksResult testExportImportBasicChecks(Connection demoConnection, int numberNodes,
                                                                Consumer<DbExporter> optionalExporterConfigurer,
                                                                Consumer<DbImporter> optionalImporterConfigurer,
                                                                String tableName, Object primaryKeyValue) throws Exception {
        return testExportImportBasicChecks(demoConnection, numberNodes, optionalExporterConfigurer, optionalImporterConfigurer, null,
                new HashMap<>(), tableName, primaryKeyValue);
    }

    /**
     * Makes a full test with a RowLink <br/>
     * Allows to configure the DbImporter/ DbExporter
     */
    public static BasicChecksResult testExportImportBasicChecks(Connection demoConnection, int numberNodes,
                                                                Consumer<DbExporter> optionalExporterConfigurer,
                                                                Consumer<DbImporter> optionalImporterConfigurer,
                                                                Consumer<Record> optionalRecordChanger,
                                                                Map<RowLink, Object> remapping,
                                                                String tableName, Object primaryKeyValue) throws Exception {
        DbExporter dbExporter = new DbExporter();
        if (optionalExporterConfigurer != null) {
            optionalExporterConfigurer.accept(dbExporter);
        }
        Record asRecord = dbExporter.contentAsTree(demoConnection, tableName, primaryKeyValue);
        String asString = asRecord.asJson();

        System.out.println("export as json2:" + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(asRecord.asJsonNode()));
        // this assertion was dropped, as the old .asJson() method handles int types wrong (it quotes them in some cases)
        //assertEquals(mapper.readTree(asRecord.asJson()).toString(), asRecord.asJsonNode().toString());

        DbImporter dbImporter = new DbImporter();
        if (optionalImporterConfigurer != null) {
            optionalImporterConfigurer.accept(dbImporter);
        }
        Record asRecordAgain = dbImporter.jsonToRecord(demoConnection, tableName, asString);
        String asStringAgain = asRecordAgain.asJson();

        assertEquals(numberNodes, asRecord.getAllNodes().size());
        assertEquals(numberNodes, asRecordAgain.getAllNodes().size());

        // still subtle differences in asString operation
        assertEquals(canonicalize(asString), canonicalize(asStringAgain));

        if (optionalRecordChanger != null) {
            optionalRecordChanger.accept(asRecordAgain);
        }

        // todo: bug: asRecord is missing columnMetadata/ other values
        Map<RowLink, Object> rowLinkObjectMap = dbImporter.insertRecords(demoConnection, asRecordAgain, remapping);

        return new BasicChecksResult(asRecord, asString, asRecordAgain, asStringAgain, rowLinkObjectMap);
    }

    @NotNull
    private static String canonicalize(String asString) {
        return asString.toUpperCase()
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
}
