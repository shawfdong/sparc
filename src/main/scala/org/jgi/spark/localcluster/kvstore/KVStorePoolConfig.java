package org.jgi.spark.localcluster.kvstore;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;


public class KVStorePoolConfig extends GenericObjectPoolConfig {
    public KVStorePoolConfig() {
        // defaults to make your life with connection pool easier :)
        setTestWhileIdle(true);
        setMinEvictableIdleTimeMillis(60000);
        setTimeBetweenEvictionRunsMillis(30000);
        setNumTestsPerEvictionRun(-1);
    }
}