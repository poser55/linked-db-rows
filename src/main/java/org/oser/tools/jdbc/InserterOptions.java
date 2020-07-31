package org.oser.tools.jdbc;

import lombok.Getter;

@Getter
public class InserterOptions {
    /** should we do an insert in any case (even if an update would be possible) */
    boolean forceInsert = true;



}
