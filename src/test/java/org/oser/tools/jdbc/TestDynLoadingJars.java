package org.oser.tools.jdbc;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.oser.tools.jdbc.TestHelpers.DB_CONFIGS;

// idea: load jdbc driver from maven central without having it included, not yet finished
public class TestDynLoadingJars {

    void loadJarAndInvoke(Object o, String jarLocation, String className) throws Exception {

        Class classToLoad = loadClassDynamically(jarLocation, className);
        Method method = classToLoad.getDeclaredMethod("clone", Object.class);
        Object instance = classToLoad.newInstance();
        method.setAccessible(true);
        Object result = method.invoke(instance, o);

        System.out.println("result:"+result);
    }

    private Class loadClassDynamically(String jarLocation, String className) throws MalformedURLException, URISyntaxException, ClassNotFoundException {
        URLClassLoader child = getUrlClassLoader(jarLocation);
        Class classToLoad = Class.forName(className, true, child);
        return classToLoad;
    }

    private URLClassLoader getUrlClassLoader(String jarLocation) throws MalformedURLException, URISyntaxException {
        URL myJar = new URL(jarLocation);

        URLClassLoader child = new URLClassLoader(
                new URL[] {myJar.toURI().toURL()},
                this.getClass().getClassLoader()
        );
        return child;
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
    @Disabled // todo continue work
    void hsqldb() throws ClassNotFoundException, URISyntaxException, IOException, SQLException {
        String hsqldburl = "https://search.maven.org/remotecontent?filepath=org/hsqldb/hsqldb/2.5.1/hsqldb-2.5.1.jar";

        ClassLoader classloader = getUrlClassLoader
                (hsqldburl);

        // this works
        Class aClass = loadClassDynamically(hsqldburl, "org.hsqldb.jdbcDriver");

        // does not seem to work
        Thread.currentThread().setContextClassLoader(classloader);

        Connection connection = TestHelpers.internalGetConnection("demo", DB_CONFIGS.get("hsqldb"));

        assertNotNull(connection);
    }
}
