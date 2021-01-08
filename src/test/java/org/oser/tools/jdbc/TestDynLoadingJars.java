package org.oser.tools.jdbc;

import lombok.Getter;
import lombok.ToString;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.oser.tools.jdbc.TestHelpers.DB_CONFIGS;

/** idea: load jdbc driver from maven central without having it included in classpath */
public class TestDynLoadingJars {

    void loadJarAndInvoke(Object o, String jarLocation, String className) throws Exception {

        Class classToLoad = loadClassDynamically(className, getUrlClassLoader(jarLocation));
        Method method = classToLoad.getDeclaredMethod("clone", Object.class);
        Object instance = classToLoad.newInstance();
        method.setAccessible(true);
        Object result = method.invoke(instance, o);

        System.out.println("result:"+result);
    }

    @Test
    void testDynLoading() throws Exception {

        loadJarAndInvoke(new CloneableString("abc"), "file:///C:/Users/pos/.m2/repository/commons-lang/commons-lang/2.6/commons-lang-2.6.jar", "org.apache.commons.lang.ObjectUtils");
    }

    public static class CloneableString implements Cloneable {
        String value;

        public CloneableString(String value) {
            this.value = value;
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

        @Override
        public String toString() {
            return "CloneableString{" +
                    "value='" + value + '\'' +
                    '}';
        }
    }

    @Test
    @Disabled // Works but I do not want to stress maven central
    void loadJdbcDriverFromMavenCentral_hsqldb() throws Exception {
        String hsqldburl = "https://search.maven.org/remotecontent?filepath=org/hsqldb/hsqldb/2.5.1/hsqldb-2.5.1.jar";

        loadJarAndAddToContextClassLoader(hsqldburl, "org.hsqldb.jdbcDriver");

        Connection connection = TestHelpers.internalGetConnection("demo", DB_CONFIGS.get("hsqldb"));
        assertNotNull(connection);

        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(connection,
                "book", 1, 2);
        assertNotNull(basicChecksResult);
    }

    @Test
    void dynamicLoadingJdbcJar() throws Exception {
        String hsqldburl = "https://search.maven.org/remotecontent?filepath=org/hsqldb/hsqldb/2.5.1/hsqldb-2.5.1.jar";

        String hsqldb = getJarCacheUrl("hsqldb", hsqldburl);
        loadJarAndAddToContextClassLoader(hsqldb, "org.hsqldb.jdbcDriver");

        Connection connection = TestHelpers.internalGetConnection("demo", DB_CONFIGS.get("hsqldb"));
        assertNotNull(connection);

        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(connection,
                "book", 1, 2);
        assertNotNull(basicChecksResult);
    }


    @Test
    void getJarCacheUrl() throws IOException {
        System.out.println(getJarCacheUrl("hsqldb", "https://search.maven.org/remotecontent?filepath=org/hsqldb/hsqldb/2.5.1/hsqldb-2.5.1.jar"));
    }

    private Class loadClassDynamically(String className, URLClassLoader child) throws MalformedURLException, URISyntaxException, ClassNotFoundException {
        return Class.forName(className, true, child);
    }

    private URLClassLoader getUrlClassLoader(String jarLocation) throws MalformedURLException, URISyntaxException {
        URL myJar = new URL(jarLocation);

        URLClassLoader child = new URLClassLoader(
                new URL[] {myJar.toURI().toURL()},
                this.getClass().getClassLoader()
        );
        return child;
    }

    /** Load the jar of urlOfJar and add to context class loader*/
    private void loadJarAndAddToContextClassLoader(String urlOfJar, String classToLoad) throws MalformedURLException, URISyntaxException, ClassNotFoundException {
        URLClassLoader urlClassLoader = getUrlClassLoader(urlOfJar);
        Class aClass = loadClassDynamically(classToLoad, urlClassLoader);
        Thread.currentThread().setContextClassLoader(urlClassLoader);
    }

    /** Return shortName.jar from tmp dir cache. If needed,  download it first from downloadUrl and put in tmp dir under that name. */
    String getJarCacheUrl(String shortName, String downloadUrl) throws IOException {
        String tmpDirUrl =  System.getProperty("java.io.tmpdir") + shortName + ".jar";

        if (!new File(tmpDirUrl).exists()) {
            System.out.println("downloading file: "+tmpDirUrl);
            downloadUrlToFile(downloadUrl, tmpDirUrl);
        }

        return "file:///" +tmpDirUrl;
    }

    private void downloadUrlToFile(String downloadUrl, String tmpDirUrl) throws IOException {
        URL url = new URL(downloadUrl);
        ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream());

        FileOutputStream fileOutputStream = new FileOutputStream(tmpDirUrl);
        FileChannel fileChannel = fileOutputStream.getChannel();

        fileOutputStream.getChannel()
                .transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
    }

    static List<TestDynLoadingJars.JdbcDriver> JDBC_DRIVERS =
            List.of(
                    new JdbcDriver("postgres",
                            "https://search.maven.org/remotecontent?filepath=org/postgresql/postgresql/42.2.18.jre7/postgresql-42.2.18.jre7.jar",
                            "org.postgresql.Driver")
            );

    static Map<String, TestDynLoadingJars.JdbcDriver> JDBC_DRIVERS_MAP =
            JDBC_DRIVERS.stream().collect(Collectors.toMap(TestDynLoadingJars.JdbcDriver::getShortname, Function.identity()));


    @Getter
    @ToString
    static class JdbcDriver {
        public JdbcDriver(String shortname, String mavenCentralUrl, String driverName) {
            this.shortname = shortname;
            this.mavenCentralUrl = mavenCentralUrl;
            this.driverName = driverName;
        }

        String shortname;
        String mavenCentralUrl;
        String driverName;
    }

}
