package org.jgi.spark.localcluster.kvstore;

import com.google.protobuf.ByteString;
import org.jgi.spark.localcluster.JavaUtils;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


/**
 * Created by Lizhen Shi on 6/5/17.
 */

public class ROCKDBackend extends Backend {
    private static final String DB_NAME = "ROCKDB";
    private String data_folder;


    private String db_folder;

    static {
        RocksDB.loadLibrary();
    }

    private RocksDB _db = null;
    private Options options;

    public ROCKDBackend(String data_folder) {
        this.data_folder = data_folder;
        if (this.data_folder == null) {
            this.data_folder = System.getProperty("java.io.tmpdir");
        }
    }

    public ROCKDBackend() {
        this(null);
    }


    public synchronized RocksDB getDb() {
        if (this._db == null) {

            try {
                this.db_folder = data_folder + "/rocksdb_" + UUID.randomUUID().toString();
                System.out.println("rocksdb: use path " + db_folder);
                options = new Options().setCreateIfMissing(true).setCreateIfMissing(true)
                        .createStatistics()
                        .setWriteBufferSize(8 * SizeUnit.KB)
                        .setMaxWriteBufferNumber(10)
                        .setMaxBackgroundCompactions(10)
                        .setCompressionType(CompressionType.LZ4_COMPRESSION)
                        .setCompactionStyle(CompactionStyle.UNIVERSAL);
                _db = RocksDB.open(options, db_folder);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return this._db;
    }


    byte[] toByteArray(int value) {
        return new byte[]{
                (byte) (value >> 24),
                (byte) (value >> 16),
                (byte) (value >> 8),
                (byte) value};
    }

    @Override
    public void incr(List<ByteString> kmers) {
        RocksDB db = getDb();
        try {
            try (final WriteOptions writeOpt = new WriteOptions()) {
                try (final WriteBatch batch = new WriteBatch()) {
                    for (ByteString bs : kmers) {
                        byte[] key = bs.toByteArray();
                        byte[] valbytes = db.get(key);
                        int val = 0;
                        if (valbytes != null)
                            val = ByteBuffer.wrap(valbytes).getInt();
                        batch.put(key, toByteArray(val + 1));
                    }
                    db.write(writeOpt, batch);
                }
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<KmerCount> getKmerCounts(boolean useBloomFilter, int minimumCount) {

        RocksDB db = getDb();
        ArrayList<KmerCount> list = new ArrayList<>();
        try (final RocksIterator iterator = db.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                int count = ByteBuffer.wrap(iterator.value()).getInt();
                if (useBloomFilter) count += 1;
                ByteString kmer = ByteString.copyFrom(key);
                list.add(new KmerCount(kmer, count));
            }
        }
        return list;
    }

    @Override
    public synchronized void close() {
        if (_db != null) {
            try {
                _db.close();
                options.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            _db = null;
        }

    }

    @Override
    public synchronized void delete() {
        close();
        try {
            if (db_folder != null) {
                File path = new File(db_folder);
                if (path.exists()) {
                    JavaUtils.deleteFileOrFolder(Paths.get(db_folder));
                    System.out.println("rocksdb: delete folder " + db_folder);
                } else {
                    System.out.println("WARNING ********** rocksdb: folder " + db_folder + " does not exist.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
