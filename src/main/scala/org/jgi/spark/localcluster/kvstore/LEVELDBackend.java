package org.jgi.spark.localcluster.kvstore;

import com.google.protobuf.ByteString;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.jgi.spark.localcluster.JavaUtils;

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

public class LEVELDBackend extends Backend {
    private static final String DB_NAME = "LEVELDB";
    private String data_folder;


    private String db_folder;
    private DB _db;

    public LEVELDBackend(String data_folder) {
        this.data_folder = data_folder;
        if (this.data_folder == null) {
            this.data_folder = System.getProperty("java.io.tmpdir");
        }
    }

    public LEVELDBackend() {
        this(null);
    }


    public synchronized DB getDb() {
        if (this._db == null) {

            try {
                this.db_folder = data_folder + "/leveldb_" + UUID.randomUUID().toString();
                System.out.println("leveldb: use path " + db_folder);
                //JavaUtils.create_folder_if_not_exists(db_folder);

                Options options = new Options();
                options.createIfMissing(true);
                options.cacheSize(200 * 1048576); // 200MB cache
                _db = org.fusesource.leveldbjni.JniDBFactory.factory.open(new File(db_folder), options);
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
        DB db = getDb();
        JniDBFactory.pushMemoryPool(1024 * 512);
        try {
            WriteBatch batch = db.createWriteBatch();
            try {
                for (ByteString bs : kmers) {
                    byte[] key = bs.toByteArray();
                    byte[] valbytes = db.get(key);
                    int val = 0;
                    if (valbytes != null)
                        val = ByteBuffer.wrap(valbytes).getInt();
                    batch.put(key, toByteArray(val + 1));
                }

                db.write(batch);
            } finally {
                // Make sure you close the batch to avoid resource leaks.
                try {
                    batch.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        } finally {
            JniDBFactory.popMemoryPool();
        }
    }

    @Override
    public List<KmerCount> getKmerCounts(boolean useBloomFilter, int minimumCount) {
        ArrayList<KmerCount> list = new ArrayList<>();
        DB db = getDb();
        DBIterator iterator = db.iterator();
        try {
            for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                byte[] key = iterator.peekNext().getKey();
                int count = ByteBuffer.wrap(iterator.peekNext().getValue()).getInt();
                if (useBloomFilter) count += 1;
                ByteString kmer = ByteString.copyFrom(key);
                list.add(new KmerCount(kmer, count));
            }
        } finally {
            // Make sure you close the iterator to avoid resource leaks.
            try {
                iterator.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return list;
    }

    @Override
    public synchronized void close() {
        if (_db != null) {
            try {
                _db.close();
            } catch (IOException e) {
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
                    System.out.println("leveldb: delete folder " + db_folder);
                } else {
                    System.out.println("WARNING ********** leveldb: folder " + db_folder + " does not exist.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
