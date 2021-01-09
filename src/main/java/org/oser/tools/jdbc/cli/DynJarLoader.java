package org.oser.tools.jdbc.cli;

import lombok.Getter;
import lombok.ToString;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Load jdbc driver from maven central and use it without having it included in classpath */
public class DynJarLoader {

    private DynJarLoader() {
    }

    public static Class loadClassDynamically(String className, URLClassLoader child) throws ClassNotFoundException {
        return Class.forName(className, true, child);
    }

    public static URLClassLoader getUrlClassLoader(String jarLocation, ClassLoader classloader) throws MalformedURLException, URISyntaxException {
        URL myJar = new URL(jarLocation);

        return new URLClassLoader(new URL[] {myJar.toURI().toURL()}, classloader);
    }

    /** Load the jar of urlOfJar and add to context class loader*/
    public static void loadJarAndAddToContextClassLoader(String urlOfJar, String classToLoad, ClassLoader parentClassloader) throws MalformedURLException, URISyntaxException, ClassNotFoundException {
        URLClassLoader urlClassLoader = getUrlClassLoader(urlOfJar, parentClassloader);
        Class aClass = loadClassDynamically(classToLoad, urlClassLoader);
        Thread.currentThread().setContextClassLoader(urlClassLoader);
    }

    /** Return shortName.jar from tmp dir cache. If needed,  download it first from downloadUrl and put in tmp dir under that name. */
    public static String getJarCacheUrl(String shortName, String downloadUrl) throws IOException {
        String tmpDirUrl =  System.getProperty("java.io.tmpdir") + File.separator + shortName + ".jar";

        if (!new File(tmpDirUrl).exists()) {
            System.out.println("downloading to file: "+tmpDirUrl);
            downloadUrlToFile(downloadUrl, tmpDirUrl);
        }

        return "file:///" +tmpDirUrl;
    }

    private static void downloadUrlToFile(String downloadUrl, String tmpDirUrl) throws IOException {
        URL url = new URL(downloadUrl);
        try (ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream())) {

            try (FileOutputStream fileOutputStream = new FileOutputStream(tmpDirUrl)) {
                fileOutputStream.getChannel()
                        .transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
            }
        }
    }

    /** Determine a database from the dbShortName and create a connection. May download a jdbc driver if needed.
     *  Refer to {@link #JDBC_DRIVERS} for the link of supported databases.
     * @return  null if no connection could be determined  */
    public static Connection getConnection(String dbShortName, String jdbcUrl, String username, String password, ClassLoader classloader)
            throws IOException, URISyntaxException, ClassNotFoundException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, InstantiationException, SQLException {
        JdbcDriverConfig jdbcDriverConfig = JDBC_DRIVERS_MAP.get(dbShortName);
        if (jdbcDriverConfig == null) {
            return null;
        }

        String urlOfClass = getJarCacheUrl(dbShortName, jdbcDriverConfig.getMavenCentralUrl());
        loadJarAndAddToContextClassLoader(urlOfClass, jdbcDriverConfig.getDriverName(), classloader);

        Driver driver = (Driver) Class.forName(jdbcDriverConfig.getDriverName(), true, Thread.currentThread().getContextClassLoader()).
                getDeclaredConstructor().newInstance();

        Properties properties = new Properties();  // later allow to set additional properties
        if (username != null) {
            properties.setProperty("user", username);
        }
        if (password != null) {
            properties.setProperty("password", password);
        }

        return driver.connect(jdbcUrl, properties);
    }

    static final List<JdbcDriverConfig> JDBC_DRIVERS =
            List.of(
                    new JdbcDriverConfig("postgres",
                            "https://search.maven.org/remotecontent?filepath=org/postgresql/postgresql/42.2.18.jre7/postgresql-42.2.18.jre7.jar",
                            "org.postgresql.Driver"),
                    new JdbcDriverConfig("h2",
                            "https://search.maven.org/remotecontent?filepath=com/h2database/h2/1.4.200/h2-1.4.200.jar",
                            "org.h2.Driver"),
                    new JdbcDriverConfig("hsqldb",
                            "https://search.maven.org/remotecontent?filepath=org/hsqldb/hsqldb/2.5.1/hsqldb-2.5.1.jar",
                            "org.hsqldb.jdbcDriver"),
                    new JdbcDriverConfig("mysql",
                            "https://search.maven.org/remotecontent?filepath=mysql/mysql-connector-java/8.0.22/mysql-connector-java-8.0.22.jar",
                            "com.mysql.cj.jdbc.Driver"),
                    new JdbcDriverConfig("sqlserver",
                            "https://search.maven.org/remotecontent?filepath=com/microsoft/sqlserver/mssql-jdbc/8.4.1.jre11/mssql-jdbc-8.4.1.jre11.jar",
                            "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
                    new JdbcDriverConfig("oracle",
                            "https://search.maven.org/remotecontent?filepath=com/oracle/ojdbc/ojdbc10/19.3.0.0/ojdbc10-19.3.0.0.jar",
                            "oracle.jdbc.driver.OracleDriver")
            );

    static final Map<String, JdbcDriverConfig> JDBC_DRIVERS_MAP =
            JDBC_DRIVERS.stream().collect(Collectors.toMap(JdbcDriverConfig::getShortname, Function.identity()));


    @Getter
    @ToString
    /** Hold metadata for a jdbc driver */
    static class JdbcDriverConfig {
        public JdbcDriverConfig(String shortname, String mavenCentralUrl, String driverName) {
            this.shortname = shortname;
            this.mavenCentralUrl = mavenCentralUrl;
            this.driverName = driverName;
        }

        String shortname;
        String mavenCentralUrl;
        String driverName;
    }
}
