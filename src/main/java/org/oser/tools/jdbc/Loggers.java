package org.oser.tools.jdbc;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Global logging convenience abstraction (to globally enable certain details in logs, such as all select statements or all insert/update statements).
 *  Just uses Log4j. */
public enum Loggers {

        /** SQL select statements */
        SELECT,
        /** SQL update and insert statements */
        CHANGE,
        /** SQL delete statements */
        DELETE,
        WARNING,
        INFO,
        /** meta Logger to mean all db operations */
        DB_OPERATIONS,
        /** meta Logger means all Loggers */
        ALL;


    static final Set<Loggers> CONCRETE_LOGGERS = EnumSet.of(Loggers.SELECT, Loggers.CHANGE, Loggers.DELETE, Loggers.WARNING,  Loggers.INFO);
    static final Set<Loggers> CONCRETE_DB_OPERATIONS = EnumSet.of(Loggers.SELECT, Loggers.CHANGE, Loggers.DELETE);
    static final Set<Loggers> ALL_LOGGERS = EnumSet.allOf(Loggers.class);

    public static final Logger LOGGER_SELECT = LoggerFactory.getLogger(Loggers.class.getName() + "." + Loggers.SELECT.name());
    static final Logger LOGGER_CHANGE = LoggerFactory.getLogger(Loggers.class.getName() + "." + Loggers.CHANGE.name());
    static final Logger LOGGER_DELETE = LoggerFactory.getLogger(Loggers.class.getName() + "." + Loggers.DELETE.name());
    static final Logger LOGGER_WARNING = LoggerFactory.getLogger(Loggers.class.getName() + "." + Loggers.WARNING.name());
    static final Logger LOGGER_INFO = LoggerFactory.getLogger(Loggers.class.getName() + "." + Loggers.INFO.name());

    /** Convenience method to enable what you would like to see in the logs */
    public static void enableLoggers(Set<Loggers> loggers) {
        setLoggerLevel(loggers, Level.INFO);
    }

    /** Convenience method to enable what you would like to see in the logs */
    public static void enableLoggers(Loggers... loggers) {
        setLoggerLevel(new HashSet(Arrays.asList(loggers)), Level.INFO);
    }

    static void setLoggerLevel(Set<Loggers> loggers, ch.qos.logback.classic.Level newLevel) {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        for (Loggers logger : loggers) {
            if (logger.equals(Loggers.DB_OPERATIONS)) {
                setLoggerLevel(CONCRETE_DB_OPERATIONS, newLevel);
            } else if (logger.equals(Loggers.ALL)) {
                setLoggerLevel(ALL_LOGGERS, newLevel);
            } else {
                ch.qos.logback.classic.Logger rootLogger = lc.getLogger(Loggers.class.getName() + "." + logger.name());
                if (rootLogger != null) {
                    rootLogger.setLevel(newLevel);
                }
            }
        }
    }

    // by default do not log SQL statements (only on request)
    static {
        disableDefaultLogs();
    }

    /** Convenience method to only show warning logs */
    public static void disableDefaultLogs() {
        setLoggerLevel(CONCRETE_LOGGERS, Level.WARN);
    }


    public static Set<Loggers> stringListToLoggerSet(List<String> logs){
        return logs.stream().map(String::toUpperCase).map(n -> optionalGetLogger(n)).flatMap(Optional::stream).collect(Collectors.toSet());
    }

    private static Optional<Loggers> optionalGetLogger(String logger){
        try {
            return Optional.of(Loggers.valueOf(logger));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    public static void logSelectStatement(PreparedStatement pkSelectionStatement, String selectPk, List<Object> values) throws SQLException {
        // postgres has nicest toString of prepareStatements
        if (Loggers.LOGGER_SELECT.isInfoEnabled() && isPostgreSQL(pkSelectionStatement)) {
            LOGGER_SELECT.info("{}", pkSelectionStatement);
        } else {
            LOGGER_SELECT.info("{} {}", selectPk, values);
        }
    }

    public static void logChangeStatement(PreparedStatement statement, String stringStatement, Map<String, Object> insertedValues, int optionalUpdateCount) throws SQLException {
        if (Loggers.LOGGER_SELECT.isInfoEnabled() && isPostgreSQL(statement)) {
            Loggers.LOGGER_CHANGE.info("{} -- updateCount:{}", statement, optionalUpdateCount);
        } else {
            Loggers.LOGGER_CHANGE.info("{} {} -- updateCount:{}", stringStatement, insertedValues, optionalUpdateCount);
        }
    }

    private static boolean isPostgreSQL(PreparedStatement pkSelectionStatement) throws SQLException {
        return pkSelectionStatement.getConnection().getMetaData().getDatabaseProductName().equals("PostgreSQL");
    }
}
