package org.oser.tools.jdbc;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.sql.SQLException;
import java.util.List;

/** To simulate a holder of a FK cache */
public class FkHolder implements FkCacheAccessor {
        Cache<String, List<Fk>> fks;

        public FkHolder(Cache<String, List<Fk>> fks) {
            this.fks = fks;
        }

        /** register virtualFK via String, e.g.,
         *    table1(field1,field2)-table2(field3,field4) */
        public void addFk(String fk) throws SQLException {
            Fk.addOneVirtualForeignKeyAsString( this, fk);
        }

        public FkHolder() {
            fks = Caffeine.newBuilder().maximumSize(10_000).build();
        }

        @Override
        public Cache<String, List<Fk>> getFkCache() {
            return fks;
        }
    }