package org.jgi.spark.localcluster.kvstore;


import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
     * PoolableObjectFactory custom impl.
     */
    class KVStoreFactory implements PooledObjectFactory<KVStoreClient> {
        private final AtomicReference<HostAndPort> hostAndPort = new AtomicReference<HostAndPort>();
        private final int connectionTimeout;
        private final int soTimeout;
        private final String clientName;

        public KVStoreFactory(final String host, final int port, final int connectionTimeout,
                            final int soTimeout,   final String clientName) {
            this.hostAndPort.set(new HostAndPort(host, port));
            this.connectionTimeout = connectionTimeout;
            this.soTimeout = soTimeout;
            this.clientName = clientName;
        }

        public KVStoreFactory(final URI uri, final int connectionTimeout, final int soTimeout,
                            final String clientName) {
            if (!URIHelper.isValid(uri)) {
                throw new RuntimeException(String.format(
                        "Cannot open KVStore connection due invalid URI. %s", uri.toString()));
            }

            this.hostAndPort.set(new HostAndPort(uri.getHost(), uri.getPort()));
            this.connectionTimeout = connectionTimeout;
            this.soTimeout = soTimeout;
            this.clientName = clientName;
        }

        public void setHostAndPort(final HostAndPort hostAndPort) {
            this.hostAndPort.set(hostAndPort);
        }

        @Override
        public void activateObject(PooledObject<KVStoreClient> pooledObj) throws Exception {
    

        }

        @Override
        public void destroyObject(PooledObject<KVStoreClient> pooledObj) throws Exception {
            final KVStoreClient kvstore = pooledObj.getObject();
            if (kvstore.isConnected()) {
                try {
                    try {
                        kvstore.quit();
                    } catch (Exception e) {
                    }
                    kvstore.disconnect();
                } catch (Exception e) {

                }
            }

        }

        @Override
        public PooledObject<KVStoreClient> makeObject() throws Exception {
            final HostAndPort hostAndPort = this.hostAndPort.get();
            final KVStoreClient kvstore = KVStoreClient.apply(hostAndPort.getHost(), hostAndPort.getPort(), connectionTimeout,
                    soTimeout);

            try {
                kvstore.connect();

                if (clientName != null) {
                    kvstore.clientSetname(clientName);
                }
            } catch (Exception je) {
                kvstore.close();
                throw je;
            }

            return new DefaultPooledObject<KVStoreClient>(kvstore);

        }

        @Override
        public void passivateObject(PooledObject<KVStoreClient> pooledObj) throws Exception {
            // TODO maybe should select db 0? Not sure right now.
        }

        @Override
        public boolean validateObject(PooledObject<KVStoreClient> pooledObj) {
            final KVStoreClient kvstore = pooledObj.getObject();
            try {
                HostAndPort hostAndPort = this.hostAndPort.get();

                String connectionHost = kvstore.host();
                int connectionPort = kvstore.port();

                return hostAndPort.getHost().equals(connectionHost)
                        && hostAndPort.getPort() == connectionPort && kvstore.isConnected()
                        && kvstore.ping().equals("PONG");
            } catch (final Exception e) {
                return false;
            }
        }
    }