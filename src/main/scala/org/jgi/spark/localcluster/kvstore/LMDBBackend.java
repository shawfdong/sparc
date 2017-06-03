package org.jgi.spark.localcluster.kvstore;

import com.google.protobuf.ByteString;
import org.jgi.spark.localcluster.JavaUtils;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.CursorIterator.KeyVal;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.nio.ByteBuffer.allocateDirect;
import static org.lmdbjava.CursorIterator.IteratorType.FORWARD;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;


/**
 * Created by Lizhen Shi on 6/1/17.
 */

public class LMDBBackend extends Backend {
    private static final String DB_NAME = "LMDB";
    private String data_folder;

    private Env<ByteBuffer> _env = null;
    private Dbi<ByteBuffer> _db = null;
    private String db_folder;

    public LMDBBackend(String data_folder) {
        this.data_folder = data_folder;
        if (this.data_folder == null) {
            this.data_folder = System.getProperty("java.io.tmpdir");
        }
    }

    public LMDBBackend() {
        this(null);
    }

    public synchronized Env<ByteBuffer> getEnv() {
        if (this._env == null) {
            this.db_folder = data_folder + "/lmdb_" + UUID.randomUUID().toString();
            System.out.println("lmdb: use path " + db_folder);
            JavaUtils.create_folder_if_not_exists(db_folder);
            File path = new File(db_folder);
            long dbsize = 10l * 1024l * 1024l * 1024l;
            this._env = create()
                    .setMapSize(dbsize)
                    .setMaxDbs(1)
                    .open(path);
        }
        return this._env;
    }

    public synchronized Dbi<ByteBuffer> getDb() {
        if (this._db == null) {
            Env<ByteBuffer> env = getEnv();
            // We need a Dbi for each DB. A Dbi roughly equates to a sorted map. The
            // MDB_CREATE flag causes the DB to be created if it doesn't already exist.
            this._db = env.openDbi(DB_NAME, MDB_CREATE);
        }
        return this._db;
    }


    @Override
    public void incr(List<ByteString> kmers) {
        // We want to store some data, so we will need a direct ByteBuffer.
        // Note that LMDB keys cannot exceed maxKeySize bytes (511 bytes by default).
        // Values can be larger.
        Env<ByteBuffer> env = getEnv();
        Dbi<ByteBuffer> db = getDb();
        final ByteBuffer key = allocateDirect(env.getMaxKeySize());
        final ByteBuffer val = allocateDirect(128);

        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            for (ByteString bs : kmers) {
                key.clear();
                val.clear();

                key.put(bs.toByteArray()).flip();

                ByteBuffer bytes = db.get(txn, key);
                int cnt = 0;
                if (bytes != null) cnt = bytes.getInt();
                val.putInt(cnt + 1).flip();

                db.put(txn, key, val);

            }
            txn.commit();
        }
    }

    @Override
    public List<KmerCount> getKmerCounts(boolean useBloomFilter, int minimumCount) {
        ArrayList<KmerCount> list = new ArrayList<>();
        Env<ByteBuffer> env = getEnv();
        Dbi<ByteBuffer> db = getDb();
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            try (CursorIterator<ByteBuffer> it = db.iterate(txn, FORWARD)) {
                for (final KeyVal<ByteBuffer> kv : it.iterable()) {
                    int count = kv.val().getInt();
                    if (useBloomFilter) count += 1;
                    if (count >= minimumCount) {
                        ByteString kmer = ByteString.copyFrom(kv.key());
                        list.add(new KmerCount(kmer, count));
                    }
                }
            }
        }

        return list;
    }

    @Override
    public synchronized void close() {
        if (_db != null) {
            _db.close();
            _db = null;
        }
        if (_env != null) {
            _env.close();
            _env = null;
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
                    System.out.println("lmdb: delete folder " + db_folder);
                } else {
                    System.out.println("WARNING ********** lmdb: folder " + db_folder + " does not exist.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
