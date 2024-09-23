package org.oser.tools.jdbc.experiment;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SubstitutorTest {

    @Test
    void basic() {
        String substitute = StringSubstitutor.substitute("$abc $abc $uuu $iii", Map.of("abc", "xxx", "iii", "lll"));
        System.out.println(substitute);
        assertEquals("xxx xxx $uuu lll", substitute);
    }
}