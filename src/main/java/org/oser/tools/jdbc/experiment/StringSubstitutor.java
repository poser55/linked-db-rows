package org.oser.tools.jdbc.experiment;

import java.util.Map;

public class StringSubstitutor {

    /**
     * Replaces values à la $value with its value from the map in the template
     */
    public static String substitute(String template, Map<String, String> values) {
        for (Map.Entry<String, String> entry : values.entrySet()) {
            template = template.replace("$" + entry.getKey(), entry.getValue());
        }

        return template;
    }

}
