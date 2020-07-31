package org.oser.tools.jdbc;

import ch.qos.logback.classic.util.LogbackMDCAdapter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@ToString
public class RemappingDetails {

    Map<Db2Graph.PkTable, Boolean> occupiedPkTables = new HashMap<>();
    Map<Db2Graph.PkTable, Object> newKeys = new HashMap<>();
    Map<Db2Graph.PkTable, Throwable> errorDetails = new HashMap<>();

}
