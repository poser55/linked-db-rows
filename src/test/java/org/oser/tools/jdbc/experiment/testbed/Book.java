package org.oser.tools.jdbc.experiment.testbed;

import lombok.Data;
import lombok.ToString;

import java.util.List;

@ToString
@Data
public class Book {
    int id;
    String title;
    int numberPages;

    int dmsId;

    Author author;

    List<Image> images;
}
