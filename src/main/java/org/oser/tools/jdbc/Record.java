package org.oser.tools.jdbc;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Contains 1 record=row of a database table. Can hold nested records (that are linked to via FKs).
 */
public class Record {
    Db2Graph.PkTable pkTable;
    List<Data> content = new ArrayList<>();

    Map<RecordMetadata, Object> optionalMetadata = new HashMap<>();

    public Record(String tableName, Object pk) {
        pkTable = new Db2Graph.PkTable(tableName, pk);
    }

    /** returns json */
    public String asJson() {
        return "{ " +
                content.stream().map(Data::toString).collect(Collectors.joining(",")) +
                " }";
    }

    public void setPkValue(Object value) {
        pkTable.pk = value;
    }

    public String metadata(){
        if (optionalMetadata.isEmpty()) {
            return "-";
        }
        else {
            Db2Graph.ExportContext o = (Db2Graph.ExportContext) optionalMetadata.get(RecordMetadata.EXPORT_CONTEXT);

            return o.visitedNodes.keySet() + ((o== null) ? "": " #nodes:"+o.visitedNodes.size() );
        }
    }

    public Data elementByName(String name) {
        for (Data element : content) {
            if (element.name.equals(name)) {
                return element;
            }
        }
        return null;
    }


    /**
     * Holds one field with metadata (and potentially nested content)
     */
    public static class Data {
        public String name;
        public Object value;
        public Db2Graph.ColumnMetadata metadata;
        public Map<String, List<Record>> subRow = new HashMap<>();
        public boolean existElsewhereInGraph;

        @Override
        public String toString() {
            return "\"" + name +
                    "\":" + getValueWithQuoting() +
                    ((!(subRow.isEmpty() || subRow.values().stream().map(List::size).max(Integer::compareTo).orElseGet(()->0) == 0)) ? (", " + maplistlist2jsonString(subRow)) : "") +
                    "";
        }

        private String getValueWithQuoting() {
            switch (metadata.type) {
                case "BOOLEAN":
                case "bool":
                case "int4":
                case "int8":
                case "numeric":
                case "DECIMAL":
                    return value != null ? value.toString() : null;
                case "timestamp":
                    return value != null ? ("\"" + DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(((Timestamp) value).toLocalDateTime()) + "\"") : null;
                case "date":
                    return value != null ? ("\"" + ((java.sql.Date) value).toLocalDate() + "\"") : null;
                case "VARCHAR":
                default:
                    if (value == null){
                        return null;
                    }

                    Object valueEscaped  = value instanceof String  ? ((String)value).replace("\"", "\\\"") : value;
                    return ("\"" + valueEscaped + "\"");
            }

        }

        private String maplistlist2jsonString(Map<String, List<Record>> map) {
            return map.keySet().stream().map(key -> "\"" + key + "\":[" + listOfData2jsonString(map.get(key)) + "]").collect(Collectors.joining(","));
        }

        private String listOfData2jsonString(List<Record> lists) {
            return lists.stream().map(this::row2json).collect(Collectors.joining(","));
        }

        public String row2json(Record row) {
            return "{" + row.content.stream().map(Data::toString).collect(Collectors.joining(",")) + "}";
        }
    }
}
