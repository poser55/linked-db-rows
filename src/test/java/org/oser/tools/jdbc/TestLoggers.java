package org.oser.tools.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.List;

public class TestLoggers {

    @Test
    void testStringToLoggers() {
        Assertions.assertEquals(EnumSet.of(Loggers.INFO, Loggers.CHANGE, Loggers.SELECT), Loggers.stringListToLoggerSet(List.of("info", "change", "select", "bla")));
    }

    @Test
    void logAll() {
        // fix this stackoverflow error
        Loggers.enableLoggers(Loggers.ALL);
    }
}
