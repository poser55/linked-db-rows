package org.oser.tools.jdbc;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.oser.tools.jdbc.cli.DynJarLoader;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.oser.tools.jdbc.TestHelpers.DB_CONFIGS;


public class DynJarLoaderTest {

    public static final String HSQLDB_JAR_URL = "https://search.maven.org/remotecontent?filepath=org/hsqldb/hsqldb/2.5.1/hsqldb-2.5.1.jar";

    void loadJarAndInvoke(Object o, String jarLocation, String className) throws Exception {

        Class loadedClass = DynJarLoader.loadClassDynamically(className, DynJarLoader.getUrlClassLoader(jarLocation, this.getClass().getClassLoader()));
        Method method = loadedClass.getDeclaredMethod("clone", Object.class);
        Object instance = loadedClass.getDeclaredConstructor().newInstance();
        method.setAccessible(true);
        Object result = method.invoke(instance, o);

        System.out.println("result:"+result);
    }

    void loadJarAndInvoke2(String jarLocation, String className) throws Exception {

        Class loadedClass = DynJarLoader.loadClassDynamically(className, DynJarLoader.getUrlClassLoader(jarLocation, this.getClass().getClassLoader()));
        Object instance = loadedClass.getDeclaredConstructor().newInstance();
    }

    @Test
    void testDynLoading() throws Exception {

        loadJarAndInvoke2(HSQLDB_JAR_URL,  "org.hsqldb.jdbcDriver");
        // this line somehow fails on linux - commenting it out (the feature we are really interested in works in linux as well)
        //loadJarAndInvoke(new CloneableString("abc"), "file:///C:/Users/pos/.m2/repository/commons-lang/commons-lang/2.6/commons-lang-2.6.jar", "org.apache.commons.lang.ObjectUtils");
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
        DynJarLoader.loadJarAndAddToContextClassLoader(HSQLDB_JAR_URL, "org.hsqldb.jdbcDriver", this.getClass().getClassLoader());

        Connection connection = TestHelpers.internalGetConnection("demo", DB_CONFIGS.get("hsqldb"), false);
        assertNotNull(connection);

        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(connection,
                "book", 1, 2);
        assertNotNull(basicChecksResult);
    }

    @Test
    void dynamicLoadingJdbcJar() throws Exception {
        String urlOfClass = DynJarLoader.getJarCacheUrl("hsqldb", HSQLDB_JAR_URL);
        DynJarLoader.loadJarAndAddToContextClassLoader(urlOfClass, "org.hsqldb.jdbcDriver", this.getClass().getClassLoader());

        Connection connection = TestHelpers.internalGetConnection("demo", DB_CONFIGS.get("hsqldb"), false);
        assertNotNull(connection);

        TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(connection,
                "book", 1, 2);
        assertNotNull(basicChecksResult);
    }


    @Test
    void getJarCacheUrl() throws IOException {
        System.out.println(DynJarLoader.getJarCacheUrl("hsqldb", HSQLDB_JAR_URL));
    }

    @Test
    void dynJdbcJarLoading() throws Exception {
        Connection connection = DynJarLoader.getConnection("hsqldb", "jdbc:hsqldb:mem:demo", "SA", "", this.getClass().getClassLoader());
        assertNotNull(connection);
        //Connection connection = DynJarLoader.getConnection("postgres", this.getClass().getClassLoader(), "postgres", "admin","jdbc:postgresql://localhost/demo");
        //TestHelpers.BasicChecksResult basicChecksResult = TestHelpers.testExportImportBasicChecks(connection,
        //        "book", 1, 2);

        //Connection connection2 = DynJarLoader.getConnection("sqlserver", "jdbc:sqlserver://localhost:1433;databaseName=PUBS", "SA", "", this.getClass().getClassLoader());
        //Connection connection2 = DynJarLoader.getConnection("oracle", "jdbc:oracle:thin:scott/tiger@localhost:1521:orcl", "SA", "", this.getClass().getClassLoader());
        //Connection connection2 = DynJarLoader.getConnection("mysql", "jdbc:mysql://host1:33060/sakila", "SA", "", this.getClass().getClassLoader());

    }
}
