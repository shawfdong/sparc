package org.jgi.spark.localcluster.kvstore;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.net.URI;



public class KVStorePool extends Pool<KVStoreClient> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 16379;
    public static final int DEFAULT_TIMEOUT = 20000;


    public KVStorePool() {
            this(DEFAULT_HOST, DEFAULT_PORT);
        }

        public KVStorePool(final GenericObjectPoolConfig poolConfig, final String host) {
            this(poolConfig, host, DEFAULT_PORT, DEFAULT_TIMEOUT,
                      null);
        }

        public KVStorePool(String host, int port) {
            this(new GenericObjectPoolConfig(), host, port, DEFAULT_TIMEOUT,   null);
        }

        public KVStorePool(final String host) {
            URI uri = URI.create(host);
            if (URIHelper.isValid(uri)) {
                String h = uri.getHost();
                int port = uri.getPort();
                String password = URIHelper.getPassword(uri);
                int database = URIHelper.getDBIndex(uri);
                this.internalPool = new GenericObjectPool<>(new KVStoreFactory(h, port,
                        DEFAULT_TIMEOUT, DEFAULT_TIMEOUT, null),
                        new GenericObjectPoolConfig());
            } else {
                this.internalPool = new GenericObjectPool<>(new KVStoreFactory(host,
                        DEFAULT_PORT, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT, null), new GenericObjectPoolConfig());
            }
        }

        public KVStorePool(final URI uri) {
            this(new GenericObjectPoolConfig(), uri, DEFAULT_TIMEOUT);
        }

        public KVStorePool(final URI uri, final int timeout) {
            this(new GenericObjectPoolConfig(), uri, timeout);
        }

        public KVStorePool(final GenericObjectPoolConfig poolConfig, final String host, int port,
                         int timeout) {
            this(poolConfig, host, port, timeout , null);
        }

        public KVStorePool(final GenericObjectPoolConfig poolConfig, final String host, final int port) {
            this(poolConfig, host, port, DEFAULT_TIMEOUT , null);
        }


        public KVStorePool(final GenericObjectPoolConfig poolConfig, final String host, int port,
                         int timeout,   final String clientName) {
            this(poolConfig, host, port, timeout, timeout , clientName);
        }

        public KVStorePool(final GenericObjectPoolConfig poolConfig, final String host, int port,
                         final int connectionTimeout, final int soTimeout ,
                         final String clientName) {
            super(poolConfig, new KVStoreFactory(host, port, connectionTimeout, soTimeout, clientName));
        }

        public KVStorePool(final GenericObjectPoolConfig poolConfig, final URI uri) {
            this(poolConfig, uri, DEFAULT_TIMEOUT);
        }

        public KVStorePool(final GenericObjectPoolConfig poolConfig, final URI uri, final int timeout) {
            this(poolConfig, uri, timeout, timeout);
        }

        public KVStorePool(final GenericObjectPoolConfig poolConfig, final URI uri,
                         final int connectionTimeout, final int soTimeout) {
            super(poolConfig, new KVStoreFactory(uri, connectionTimeout, soTimeout, null));
        }

        @Override
        public KVStoreClient getResource() {
            KVStoreClient obj = super.getResource();
            obj.setDataSource(this);
            return obj;
        }

    }
