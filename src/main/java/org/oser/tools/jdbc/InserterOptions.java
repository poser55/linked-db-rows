package org.oser.tools.jdbc;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class InserterOptions {
    /** should we do an insert in any case (even if an update would be possible?) */
    boolean forceInsert = true;



}
