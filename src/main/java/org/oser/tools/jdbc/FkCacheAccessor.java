package org.oser.tools.jdbc;

import com.github.benmanes.caffeine.cache.Cache;

import java.util.List;

/** Common feature of DbImporter and DbExporter */
public interface FkCacheAccessor {
    Cache<String, List<Fk>> getFkCache();
}
