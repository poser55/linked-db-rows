package org.oser.tools.jdbc;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

// just experimenting
class DateHandlingTest {

    // testing https://en.wikipedia.org/wiki/ISO_8601

    // example:
    //        2007-03-01T13:00:00Z

    // value -> java Object
    //    fieldAndValue.convertTypeForValue(columnMetadata, "d");


    // java Object -> JSON
    //Record: void putFieldToJsonNode(ObjectNode node)  {

    // java Object -> JDBC
    // JdbcHelpers : public static void innerSetStatementField(PreparedStatement preparedStatement,



    @Test
    void dateConversionTest() {
        JdbcHelpers.ColumnMetadata columnMetadata = new JdbcHelpers.ColumnMetadata("dummy", "TIMESTAMP", 2,2,1, "", 1);
        DbRecord.FieldAndValue dummy = new DbRecord.FieldAndValue("test", columnMetadata, "xxx");

        // value -> java Object
        Object o = dummy.convertTypeForValue(columnMetadata, "2007-03-01T13:00:00Z");
        Object o2 = dummy.convertTypeForValue(columnMetadata, "2007-03-01");
        System.out.println(o+" "+o.getClass());
        System.out.println(o2+" "+o2.getClass());


        DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_TIME)
                .parseLenient()
                .appendOffsetId().toFormatter();
        //.toFormatter(ResolverStyle.STRICT, IsoChronology.INSTANCE);

        System.out.println("x"+LocalDateTime.parse("2007-03-01T13:00:00Z", dateTimeFormatter));
        //System.out.println("x2"+LocalDateTime.parse("2007-03-01T13:00:00", dateTimeFormatter));



    }

    static DateTimeFormatter LOOSE_ISO_DATE_TIME_ZONE_PARSER =  DateTimeFormatter.ofPattern("[yyyyMMdd][yyyy-MM-dd][yyyy-DDD]" +
            "['T'[HHmmss][HHmm][HH:mm:ss][HH:mm][.SSSSSSSSS][.SSSSSS][.SSS][.SS][.S]][OOOO][O][z][XXXXX][XXXX]['['VV']']");


    public static LocalDateTime parse(CharSequence text) {
        TemporalAccessor temporalAccessor = LOOSE_ISO_DATE_TIME_ZONE_PARSER.parseBest(text, LocalDateTime::from, LocalDate::from);
        if (temporalAccessor instanceof LocalDateTime) {
            return ((LocalDateTime) temporalAccessor);
        }
        return ((LocalDate) temporalAccessor).atStartOfDay();
    }


    // experimenting with other Time parsing (not yet ready), in Record.java

//     case "TIMESTAMP":
//    Timestamp ts = null;
//                    if (value instanceof String) {
//        try {
//            LocalDateTime localDateTime = LocalDateTime.parse(((String)value).replace(" ", "T"));
//            ts = Timestamp.valueOf(localDateTime);
//        } catch (DateTimeParseException e) {
//            // ok
//        }
//    }
//    return ts != null ? ts : value;

}