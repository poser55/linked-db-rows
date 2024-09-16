package org.oser.tools.jdbc;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import com.fasterxml.jackson.core.JsonParseException;
import org.junit.jupiter.api.Disabled;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UnknownFormatConversionException;

/** little experiment to fuzz linked-db-rows (uses https://github.com/CodeIntelligenceTesting/jazzer)
 *   Not yet very interesting, as a bug in version 0.12.0 crashed on windows with example jsons to start from (bug is fixed in mean time, but no new release out yet)
 *
 *   Later: put interesting json samples (to start from) and run with env variable JAZZER_FUZZ=1 to find new issues  */
public class FuzzingTest {

    private Connection demo;

    {
        try {
            demo = TestHelpers.getConnection("demo");
        } catch (SQLException | ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @FuzzTest(
            // Specify the maximum duration of a fuzzing run. Default:
            maxDuration = "100m"
            // seedCorpus = "/seed_corpus"
    )
    @Disabled
    void myFuzzTest(FuzzedDataProvider data) throws SQLException, IOException, ClassNotFoundException {
        DbImporter dbImporter = new DbImporter();
        String jsonString = data.consumeRemainingAsAsciiString();
        if (jsonString != null && !jsonString.equals("")) {

            try {
                DbRecord r = dbImporter.jsonToRecord(demo, "book", jsonString);
                // System.out.println("!!!"+jsonString+"|"+jsonString.length());
                dbImporter.insertRecords(demo, r);
                r = null;
            } catch (JsonParseException | UnknownFormatConversionException | java.lang.NumberFormatException e) {
                // ignore, these are typical issues that Jackson has with wrong JSON inputs
            }
        }
        dbImporter = null;

    }

}
