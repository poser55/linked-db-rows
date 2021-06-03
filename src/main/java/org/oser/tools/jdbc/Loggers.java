package org.oser.tools.jdbc;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.List;
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
        WARNINGS,
        INFO,
        /** meta Logger to mean all db operations */
        DB_OPERATIONS,
        /** meta Logger means all Loggers */
        ALL;


    static final Set<Loggers> CONCRETE_LOGGERS = EnumSet.of(Loggers.SELECT, Loggers.CHANGE, Loggers.DELETE, Loggers.WARNINGS,  Loggers.INFO);
    static final Set<Loggers> CONCRETE_DB_OPERATIONS = EnumSet.of(Loggers.SELECT, Loggers.CHANGE, Loggers.DELETE);
    static final Set<Loggers> ALL_LOGGERS = EnumSet.allOf(Loggers.class);

    static final Logger LOGGER_SELECT = LoggerFactory.getLogger(Loggers.class.getName() + "." + Loggers.SELECT.name());
    static final Logger LOGGER_CHANGE = LoggerFactory.getLogger(Loggers.class.getName() + "." + Loggers.CHANGE.name());
    static final Logger LOGGER_DELETE = LoggerFactory.getLogger(Loggers.class.getName() + "." + Loggers.DELETE.name());
    static final Logger LOGGER_WARNINGS = LoggerFactory.getLogger(Loggers.class.getName() + "." + Loggers.WARNINGS.name());
    static final Logger LOGGER_INFO = LoggerFactory.getLogger(Loggers.class.getName() + "." + Loggers.INFO.name());

    /** Convenience method to enable what you would like to see in the logs */
    public static void enableLoggers(Set<Loggers> loggers) {
        setLoggerLevel(loggers, Level.INFO);
    }

    static void setLoggerLevel(Set<Loggers> loggers, ch.qos.logback.classic.Level newLevel) {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        for (Loggers logger : loggers) {
            if (logger.equals(Loggers.DB_OPERATIONS)) {
                setLoggerLevel(CONCRETE_DB_OPERATIONS, newLevel);
            } else if (logger.equals(Loggers.ALL)) {
                setLoggerLevel(ALL_LOGGERS, newLevel);
            } else {
                ch.qos.logback.classic.Logger rootLogger = lc.getLogger(Loggers.class + "." + logger.name());
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

}
